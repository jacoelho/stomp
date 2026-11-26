package stomp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jacoelho/stomp/internal/session"
	"github.com/jacoelho/stomp/stomptext"
)

// benchBroker is a minimal STOMP 1.2 broker over one connection. It answers
// every receipt and, once a subscription is confirmed, streams msg with at
// most window frames unacknowledged.
type benchBroker struct {
	conn     net.Conn
	msg      []byte
	window   int
	credit   chan int
	receipts chan string
	done     chan struct{}
	err      error
}

func newBenchBroker(conn net.Conn, msg []byte) *benchBroker {
	return &benchBroker{
		conn:     conn,
		msg:      msg,
		window:   max(1, min(64, (8<<20)/max(1, len(msg)))),
		credit:   make(chan int, 4096),
		receipts: make(chan string, 4096),
		done:     make(chan struct{}),
	}
}

func (k *benchBroker) run() {
	defer close(k.done)
	dec := stomptext.NewDecoder(k.conn)
	f, err := dec.ReadFrame()
	if err != nil || f.Command() != stomptext.Connect {
		k.err = fmt.Errorf("handshake: %v %v", f.Command(), err)
		return
	}
	reply, _ := stomptext.AppendFrame(nil, stomptext.Version12, stomptext.Connected, stomptext.Headers{{Name: "version", Value: "1.2"}, {Name: "heart-beat", Value: "0,0"}}, nil)
	if _, err := k.conn.Write(reply); err != nil {
		k.err = err
		return
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() { k.write(stop) })
	defer wg.Wait()
	defer close(stop)
	for {
		f, err := dec.ReadFrame()
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
				k.err = err
			}
			return
		}
		switch f.Command() {
		case stomptext.Ack:
			k.credit <- 1
		case stomptext.Subscribe:
			r, _ := f.Header("receipt")
			k.receipts <- string(r)
			if k.msg != nil {
				k.credit <- k.window
			}
		default:
			if r, ok := f.Header("receipt"); ok {
				k.receipts <- string(r)
			}
		}
	}
}

func (k *benchBroker) write(stop <-chan struct{}) {
	var buf []byte
	for {
		avail := 0
		select {
		case n := <-k.credit:
			avail += n
		case r := <-k.receipts:
			buf = appendReceipt(buf, r)
		case <-stop:
			return
		}
	drain:
		for {
			select {
			case n := <-k.credit:
				avail += n
			case r := <-k.receipts:
				buf = appendReceipt(buf, r)
			default:
				break drain
			}
		}
		for range avail {
			buf = append(buf, k.msg...)
		}
		if _, err := k.conn.Write(buf); err != nil {
			return
		}
		buf = buf[:0]
	}
}

func appendReceipt(dst []byte, id string) []byte {
	b, err := stomptext.AppendFrame(dst, stomptext.Version12, stomptext.Receipt, stomptext.Headers{{Name: "receipt-id", Value: id}}, nil)
	if err != nil {
		panic(err)
	}
	return b
}

func benchMessage(body int) []byte {
	b, err := stomptext.AppendFrame(nil, stomptext.Version12, stomptext.Message, stomptext.Headers{
		{Name: "subscription", Value: "sub-1"}, {Name: "message-id", Value: "ID:artemis-12345-1"},
		{Name: "destination", Value: "/queue/orders"}, {Name: "ack", Value: "ID:artemis-12345-1-ack"},
		{Name: "content-length", Value: strconv.Itoa(body)},
	}, bytes.Repeat([]byte("x"), body))
	if err != nil {
		panic(err)
	}
	return b
}

func openBench(b *testing.B, msg []byte) (*Client, func()) {
	b.Helper()
	cc, sc := net.Pipe()
	k := newBenchBroker(sc, msg)
	go k.run()
	used := false
	dial := func(context.Context, int) (net.Conn, error) {
		if used {
			return nil, errors.New("redial")
		}
		used = true
		return cc, nil
	}
	c, err := Open(context.Background(), dial, MaxAttempts(1), Logger(slog.New(slog.DiscardHandler)))
	if err != nil {
		b.Fatal(err)
	}
	return c, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			b.Error(err)
		}
		sc.Close()
		<-k.done
		if k.err != nil {
			b.Error(k.err)
		}
	}
}

func BenchmarkReceive(b *testing.B) {
	for _, mode := range []struct {
		name string
		ack  AckMode
	}{{"auto", AckAuto}, {"individual", AckClientIndividual}} {
		for _, size := range []int{64, 4096, 1 << 20} {
			for _, reuse := range []bool{false, true} {
				b.Run(fmt.Sprintf("ack=%s/body=%d/%s", mode.name, size, readName(reuse)), func(b *testing.B) {
					c, closeAll := openBench(b, benchMessage(size))
					defer closeAll()
					ctx := context.Background()
					s, err := c.Subscribe(ctx, SubscribeRequest{ID: "sub-1", Destination: "/queue/orders", Ack: mode.ack})
					if err != nil {
						b.Fatal(err)
					}
					b.SetBytes(int64(size))
					b.ReportAllocs()
					var m Message
					for b.Loop() {
						if err := receiveOne(ctx, s, &m, reuse); err != nil {
							b.Fatal(err)
						}
						if len(m.Body) != size || m.Destination != "/queue/orders" {
							b.Fatalf("message %q body %d", m.Destination, len(m.Body))
						}
						if mode.ack == AckClientIndividual {
							if err := s.Ack(ctx, m); err != nil {
								b.Fatal(err)
							}
						}
					}
				})
			}
		}
	}
}

func BenchmarkSend(b *testing.B) {
	body := bytes.Repeat([]byte("x"), 256)
	out := Outgoing{Destination: "/queue/orders", ContentType: "text/plain", Body: body}
	b.Run("sequential", func(b *testing.B) {
		c, closeAll := openBench(b, nil)
		defer closeAll()
		ctx := context.Background()
		b.ReportAllocs()
		for b.Loop() {
			if err := c.Send(ctx, out); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("parallel", func(b *testing.B) {
		c, closeAll := openBench(b, nil)
		defer closeAll()
		ctx := context.Background()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := c.Send(ctx, out); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}

func BenchmarkOwnHeaders(b *testing.B) {
	dec := stomptext.NewDecoder(bytes.NewReader(benchMessage(64)))
	f, err := dec.ReadFrame()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if hs := ownHeaders(f); len(hs) != 5 {
			b.Fatal(len(hs))
		}
	}
}

func BenchmarkPushRead(b *testing.B) {
	for _, size := range []int{64, 4096} {
		for _, depth := range []int{0, 128, 240, 253, 255} {
			for _, reuse := range []bool{false, true} {
				b.Run(fmt.Sprintf("body=%d/depth=%d/%s", size, depth, readName(reuse)), func(b *testing.B) {
					s := newSubscription(nil, SubscribeRequest{ID: "sub-1", Capacity: 256, CapacityBytes: 16 << 20, EndOnOverflow: true}, session.ClientIndividual)
					m, body := Message{Destination: "/queue/orders"}, make([]byte, size)
					for range depth {
						if !s.push(m, body) {
							b.Fatal("full")
						}
					}
					ctx := context.Background()
					var out Message
					b.ReportAllocs()
					for b.Loop() {
						if !s.push(m, body) {
							b.Fatal("full")
						}
						if err := receiveOne(ctx, s, &out, reuse); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}

func readName(reuse bool) string {
	if reuse {
		return "reuse"
	}
	return "new"
}

func receiveOne(ctx context.Context, s *Subscription, m *Message, reuse bool) error {
	if reuse {
		return s.ReadMessage(ctx, m)
	}
	var err error
	*m, err = readNew(ctx, s)
	return err
}
