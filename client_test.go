package stomp_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jacoelho/stomp"
	"github.com/jacoelho/stomp/stomptext"
)

// frame is an owned copy of a frame the client wrote.
type frame struct {
	cmd     stomptext.Command
	headers stomptext.Headers
	body    string
}

func (f frame) get(name string) string {
	v, _ := f.headers.Get(name)
	return v
}

// peer is the broker side of one client connection.
type peer struct {
	t      *testing.T
	ver    string
	conn   net.Conn
	frames chan frame // every frame the client wrote, in order
	eols   chan struct{}
}

// broker accepts connections created by its dial function and speaks STOMP version ver.
type broker struct {
	t     *testing.T
	ver   string
	peers chan *peer
	dial  stomp.DialFunc
	fail  chan error // a value makes the next dial fail
}

func newBroker(t *testing.T, ver string) *broker {
	b := &broker{t: t, ver: ver, peers: make(chan *peer, 16), fail: make(chan error, 16)}
	b.dial = func(ctx context.Context, _ int) (net.Conn, error) {
		select {
		case err := <-b.fail:
			return nil, err
		default:
		}
		client, server := net.Pipe()
		p := &peer{t: t, ver: ver, conn: server, frames: make(chan frame, 1024), eols: make(chan struct{}, 1024)}
		go p.readAll()
		b.peers <- p
		return client, nil
	}
	return b
}

func (p *peer) readAll() {
	defer close(p.frames)
	r := &eolCounter{r: p.conn, eols: p.eols}
	dec := stomptext.NewDecoder(r)
	for {
		f, err := dec.ReadFrame()
		if err != nil {
			return
		}
		out := frame{cmd: f.Command(), body: string(f.Body())}
		for k, v := range f.All() {
			out.headers = append(out.headers, stomptext.Header{Name: string(k), Value: string(v)})
		}
		p.frames <- out
	}
}

// eolCounter reports bare EOL heart-beats: octets that are '\n' at the start of a read.
type eolCounter struct {
	r    io.Reader
	eols chan struct{}
}

func (e *eolCounter) Read(b []byte) (int, error) {
	n, err := e.r.Read(b)
	if n == 1 && b[0] == '\n' {
		e.eols <- struct{}{}
	}
	return n, err
}

func (b *broker) accept() *peer {
	b.t.Helper()
	select {
	case p := <-b.peers:
		return p
	case <-time.After(time.Minute):
		b.t.Fatal("client did not dial")
		return nil
	}
}

// noFrame waits for the client to block and reports any frame it sent.
func (p *peer) noFrame() {
	p.t.Helper()
	synctest.Wait()
	select {
	case f, ok := <-p.frames:
		if ok {
			p.t.Errorf("client sent %s %v; want nothing", f.cmd, f.headers)
		}
	default:
	}
}

func (p *peer) next() frame {
	p.t.Helper()
	select {
	case f, ok := <-p.frames:
		if !ok {
			p.t.Fatal("connection closed while expecting a frame")
		}
		return f
	case <-time.After(time.Minute):
		p.t.Fatal("no frame from client")
		return frame{}
	}
}

func (p *peer) expect(cmd stomptext.Command) frame {
	p.t.Helper()
	f := p.next()
	if f.cmd != cmd {
		p.t.Fatalf("client sent %s %v; want %s", f.cmd, f.headers, cmd)
	}
	return f
}

func (p *peer) send(raw string) {
	p.t.Helper()
	if _, err := io.WriteString(p.conn, raw); err != nil {
		p.t.Fatalf("broker write: %v", err)
	}
}

func (p *peer) handshake(hb string) frame {
	p.t.Helper()
	f := p.expect(stomptext.Connect)
	p.send("CONNECTED\nversion:" + p.ver + "\nheart-beat:" + hb + "\n\n\x00")
	return f
}

func (p *peer) receipt(f frame) { p.send("RECEIPT\nreceipt-id:" + f.get("receipt") + "\n\n\x00") }

// errorFor answers f's receipt with an ERROR frame carrying message.
func (p *peer) errorFor(f frame, message string) {
	p.send("ERROR\nreceipt-id:" + f.get("receipt") + "\nmessage:" + message + "\n\n\x00")
}

// message encodes a MESSAGE. Every subscription is acknowledged on the wire,
// AckAuto included, so 1.2 always sends ack, defaulting to the message-id. In
// 1.1 the message-id is the acknowledgement id and ack is not sent.
func (b *broker) message(sub, id, ack, body string) string {
	s := "MESSAGE\nsubscription:" + sub + "\nmessage-id:" + id + "\ndestination:/queue/a\n"
	if ack == "" {
		ack = id
	}
	if b.ver == "1.2" {
		s += "ack:" + ack + "\n"
	}
	return s + "content-length:" + fmt.Sprint(len(body)) + "\n\n" + body + "\x00"
}

// open starts a client against b and completes the first handshake.
func open(t *testing.T, b *broker, o ...stomp.Options) (*stomp.Client, *peer) {
	t.Helper()
	c, p, _ := openHandshake(t, b, "0,0", o...)
	return c, p
}

// openHandshake starts a client against b, answers its CONNECT with heart-beat
// hb, and returns the CONNECT frame.
func openHandshake(t *testing.T, b *broker, hb string, o ...stomp.Options) (*stomp.Client, *peer, frame) {
	t.Helper()
	type result struct {
		c   *stomp.Client
		err error
	}
	done := make(chan result, 1)
	go func() {
		c, err := stomp.Open(t.Context(), b.dial, o...)
		done <- result{c, err}
	}()
	p := b.accept()
	f := p.handshake(hb)
	r := <-done
	if r.err != nil {
		t.Fatalf("Open: %v", r.err)
	}
	return r.c, p, f
}

// closeClient closes c, answering its DISCONNECT on p.
func closeClient(t *testing.T, c *stomp.Client, p *peer) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- c.Close(context.Background()) }()
	if p != nil {
		d := p.expect(stomptext.Disconnect)
		p.receipt(d)
	}
	if err := <-done; err != nil {
		t.Errorf("Close: %v", err)
	}
}

// subscribe subscribes and answers the SUBSCRIBE receipt.
func subscribe(t *testing.T, c *stomp.Client, p *peer, r stomp.SubscribeRequest) (*stomp.Subscription, frame) {
	t.Helper()
	type result struct {
		s   *stomp.Subscription
		err error
	}
	done := make(chan result, 1)
	go func() {
		s, err := c.Subscribe(t.Context(), r)
		done <- result{s, err}
	}()
	f := p.expect(stomptext.Subscribe)
	p.receipt(f)
	res := <-done
	if res.err != nil {
		t.Fatalf("Subscribe: %v", res.err)
	}
	return res.s, f
}

func eachVersion(t *testing.T, f func(*testing.T, *broker)) {
	for _, v := range []string{"1.1", "1.2"} {
		t.Run(v, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) { f(t, newBroker(t, v)) })
		})
	}
}

// ackID returns the acknowledgement id an ACK or NACK names, checking the
// version's required headers.
func (b *broker) ackID(f frame, sub string) string {
	b.t.Helper()
	if b.ver == "1.1" {
		if f.get("subscription") != sub {
			b.t.Errorf("1.1 %s subscription = %q; want %q", f.cmd, f.get("subscription"), sub)
		}
		return f.get("message-id")
	}
	return f.get("id")
}

func TestConnectHeaders(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		done := make(chan error, 1)
		var c *stomp.Client
		go func() {
			var err error
			c, err = stomp.Open(t.Context(), b.dial, stomp.Login("app", "s:e\\cret"), stomp.ClientID("billing"), stomp.Heartbeat(4*time.Second, 6*time.Second))
			done <- err
		}()
		p := b.accept()
		f := p.handshake("0,0")
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		want := stomptext.Headers{
			{Name: "accept-version", Value: "1.1,1.2"}, {Name: "host", Value: "/"}, {Name: "heart-beat", Value: "4000,6000"},
			{Name: "login", Value: "app"}, {Name: "passcode", Value: "s:e\\cret"}, {Name: "client-id", Value: "billing"},
		}
		if !slices.Equal(f.headers, want) {
			t.Errorf("CONNECT headers = %v; want %v", f.headers, want)
		}
		closeClient(t, c, p)
	})
}

func TestSendWritesFrameAndWaitsForReceipt(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		done := make(chan error, 1)
		go func() {
			done <- c.Send(t.Context(), stomp.Outgoing{
				Destination: "/queue/orders", ContentType: "application/octet-stream",
				Headers: stomp.Headers{{Name: "b", Value: "2"}, {Name: "a", Value: "1"}},
				Body:    []byte("a\x00b"),
			})
		}()
		f := p.expect(stomptext.Send)
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("Send returned %v before the receipt", err)
		default:
		}
		p.receipt(f)
		if err := <-done; err != nil {
			t.Fatalf("Send: %v", err)
		}
		if f.body != "a\x00b" || f.get("destination") != "/queue/orders" || f.get("content-length") != "3" ||
			f.get("content-type") != "application/octet-stream" {
			t.Errorf("SEND = %+v", f)
		}
		names := []string{}
		for _, h := range f.headers {
			names = append(names, h.Name)
		}
		if i, j := slices.Index(names, "b"), slices.Index(names, "a"); i < slices.Index(names, "receipt") || j != i+1 {
			t.Errorf("header order %v; want client headers, then b, then a", names)
		}
		closeClient(t, c, p)
	})
}

func TestSendText(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		if err := c.Send(t.Context(), stomp.Outgoing{Destination: "/q", Body: []byte("a\x00b"), Text: true}); !errors.Is(err, stomp.ErrTextHasNUL) {
			t.Errorf("Send text with NUL: %v; want ErrTextHasNUL", err)
		}
		done := make(chan error, 1)
		go func() {
			done <- c.Send(t.Context(), stomp.Outgoing{Destination: "/q", Body: []byte(`{"a":1}`), Text: true})
		}()
		f := p.expect(stomptext.Send)
		p.receipt(f)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if _, ok := f.headers.Get("content-length"); ok || f.body != `{"a":1}` {
			t.Errorf("text SEND = %+v; want no content-length", f)
		}
		closeClient(t, c, p)
	})
}

func TestSendRejectsInvalidRequests(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		for _, tt := range []struct {
			m    stomp.Outgoing
			want error
		}{
			{stomp.Outgoing{Body: []byte("x")}, stomp.ErrInvalidRequest},
			{stomp.Outgoing{Destination: "/q", Headers: stomp.Headers{{Name: "content-length", Value: "9"}}}, stomp.ErrReservedHeader},
			{stomp.Outgoing{Destination: "/q", Headers: stomp.Headers{{Name: "receipt", Value: "x"}}}, stomp.ErrReservedHeader},
		} {
			if err := c.Send(t.Context(), tt.m); !errors.Is(err, tt.want) {
				t.Errorf("Send(%+v) = %v; want %v", tt.m, err, tt.want)
			}
		}
		closeClient(t, c, p)
		p.noFrame()
	})
}

func TestSendOutcomeUnknownWhenConnectionDrops(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		done := make(chan error, 1)
		go func() { done <- c.Send(t.Context(), stomp.Outgoing{Destination: "/q", Body: []byte("x")}) }()
		p.expect(stomptext.Send)
		p.conn.Close()
		if err := <-done; !errors.Is(err, stomp.ErrOutcomeUnknown) {
			t.Errorf("Send = %v; want ErrOutcomeUnknown", err)
		}
		p2 := b.accept()
		p2.handshake("0,0")
		p2.noFrame()
		closeClient(t, c, p2)
	})
}

func TestSendContextEndsWhileWaitingForReceipt(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- c.Send(ctx, stomp.Outgoing{Destination: "/q"}) }()
		f := p.expect(stomptext.Send)
		err := <-done
		if !errors.Is(err, stomp.ErrOutcomeUnknown) || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Send = %v; want ErrOutcomeUnknown wrapping DeadlineExceeded", err)
		}
		p.receipt(f)
		closeClient(t, c, p)
	})
}

func TestMessagesRoutedBySubscriptionHeader(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		s1, f1 := subscribe(t, c, p, stomp.SubscribeRequest{Destination: "/queue/a"})
		s2, f2 := subscribe(t, c, p, stomp.SubscribeRequest{Destination: "/queue/a"})
		if f1.get("id") == f2.get("id") || f1.get("ack") != "client-individual" {
			t.Fatalf("SUBSCRIBE ids %q %q ack %q", f1.get("id"), f2.get("id"), f1.get("ack"))
		}
		p.send(b.message(f2.get("id"), "m1", "", "one") + "\n" + b.message(f1.get("id"), "m2", "", "two") + b.message(f2.get("id"), "m3", "", "three"))
		for _, tt := range []struct {
			s    *stomp.Subscription
			want []string
		}{{s1, []string{"two"}}, {s2, []string{"one", "three"}}} {
			for _, w := range tt.want {
				m, err := readNew(t.Context(), tt.s)
				if err != nil || string(m.Body) != w {
					t.Fatalf("%s ReadMessage = %q, %v; want %q", tt.s.ID(), m.Body, err, w)
				}
			}
		}
		synctest.Wait()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if m, err := readNew(ctx, s1); err == nil {
			t.Errorf("%s got extra message %q", s1.ID(), m.Body)
		}
		for _, want := range []struct{ sub, id string }{{f1.get("id"), "m2"}, {f2.get("id"), "m1"}, {f2.get("id"), "m3"}} {
			a := p.expect(stomptext.Ack)
			if got := b.ackID(a, want.sub); got != want.id || a.get("receipt") != "" {
				t.Errorf("auto ACK = %v; want %s without receipt", a.headers, want.id)
			}
		}
		closeClient(t, c, p)
	})
}

func TestOverflowEndsSubscription(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		s, f := subscribe(t, c, p, stomp.SubscribeRequest{Destination: "/queue/a", Capacity: 2, EndOnOverflow: true})
		id := f.get("id")
		p.send(b.message(id, "1", "", "a") + b.message(id, "2", "", "b") + b.message(id, "3", "", "c"))
		u := p.expect(stomptext.Unsubscribe)
		if u.get("id") != id {
			t.Errorf("UNSUBSCRIBE id %q; want %q", u.get("id"), id)
		}
		got, err := bodiesUntilEnd(t.Context(), s)
		if !slices.Equal(got, []string{"a", "b"}) || !errors.Is(err, stomp.ErrOverflow) {
			t.Errorf("ReadMessage stored %v then returned %v; want [a b] then ErrOverflow", got, err)
		}
		p.receipt(u)
		closeClient(t, c, p)
	})
}

func TestAckAndAckWait(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		s, f := subscribe(t, c, p, stomp.SubscribeRequest{Destination: "/queue/a", Ack: stomp.AckClientIndividual})
		if f.get("ack") != "client-individual" {
			t.Fatalf("ack header %q", f.get("ack"))
		}
		p.send(b.message(f.get("id"), "A1", "A1", "x") + b.message(f.get("id"), "A2", "A2", "y"))
		m1, _ := readNew(t.Context(), s)
		m2, _ := readNew(t.Context(), s)
		if err := s.Ack(t.Context(), m1); err != nil {
			t.Fatal(err)
		}
		a := p.expect(stomptext.Ack)
		if b.ackID(a, f.get("id")) != "A1" || a.get("receipt") != "" {
			t.Errorf("ACK = %v; want id A1 without receipt", a.headers)
		}
		done := make(chan error, 1)
		go func() { done <- s.NackWait(t.Context(), m2) }()
		n := p.expect(stomptext.Nack)
		if b.ackID(n, f.get("id")) != "A2" || n.get("receipt") == "" {
			t.Errorf("NACK = %v; want id A2 with receipt", n.headers)
		}
		p.receipt(n)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		closeClient(t, c, p)
	})
}

func TestAckErrors(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		auto, fa := subscribe(t, c, p, stomp.SubscribeRequest{Destination: "/a"})
		ind, fi := subscribe(t, c, p, stomp.SubscribeRequest{Destination: "/b", Ack: stomp.AckClient})
		p.send(b.message(fa.get("id"), "1", "", "x") + b.message(fi.get("id"), "A2", "A2", "y"))
		ma, _ := readNew(t.Context(), auto)
		mi, _ := readNew(t.Context(), ind)
		if err := auto.Ack(t.Context(), ma); !errors.Is(err, stomp.ErrAutoAck) {
			t.Errorf("Ack on auto = %v; want ErrAutoAck", err)
		}
		if err := ind.Ack(t.Context(), ma); !errors.Is(err, stomp.ErrWrongSubscription) {
			t.Errorf("Ack of other subscription's message = %v; want ErrWrongSubscription", err)
		}
		p.conn.Close()
		p2 := b.accept()
		p2.handshake("0,0")
		p2.expect(stomptext.Subscribe)
		p2.expect(stomptext.Subscribe)
		if err := ind.Ack(t.Context(), mi); !errors.Is(err, stomp.ErrStaleAck) {
			t.Errorf("Ack after reconnect = %v; want ErrStaleAck", err)
		}
		closeClient(t, c, p2)
		for f := range p2.frames {
			if f.cmd == stomptext.Ack {
				t.Errorf("stale ACK written: %v", f.headers)
			}
		}
	})
}

func TestReconnectReplaysSubscriptionsAndKeepsQueuedMessages(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		hdr := stomp.Headers{{Name: "durable-subscription-name", Value: "billing"}}
		cs, fc := subscribe(t, c, p, stomp.SubscribeRequest{ID: "orders", Destination: "/topic/orders", Ack: stomp.AckClientIndividual, Headers: hdr})
		as, fa := subscribe(t, c, p, stomp.SubscribeRequest{ID: "audit", Destination: "/topic/audit"})
		p.send(b.message(fc.get("id"), "A1", "A1", "unacked") + b.message(fa.get("id"), "2", "", "consumed"))
		synctest.Wait()
		p.conn.Close()

		p2 := b.accept()
		p2.handshake("0,0")
		replayed := map[string]frame{}
		for range 2 {
			f := p2.expect(stomptext.Subscribe)
			replayed[f.get("id")] = f
		}
		if r := replayed["orders"]; r.get("destination") != "/topic/orders" || r.get("ack") != "client-individual" || r.get("durable-subscription-name") != "billing" {
			t.Errorf("replayed orders = %v", r.headers)
		}
		if m, err := readNew(t.Context(), as); err != nil || string(m.Body) != "consumed" {
			t.Errorf("auto queue after reconnect: %q, %v; want consumed", m.Body, err)
		}
		kept, err := readNew(t.Context(), cs)
		if err != nil || string(kept.Body) != "unacked" {
			t.Fatalf("client queue after reconnect: %q, %v; want the unacked message kept", kept.Body, err)
		}
		if err := cs.Ack(t.Context(), kept); !errors.Is(err, stomp.ErrStaleAck) {
			t.Errorf("Ack of a message from the earlier session = %v; want ErrStaleAck", err)
		}
		p2.send(b.message("orders", "B1", "B1", "redelivered"))
		if m, err := readNew(t.Context(), cs); err != nil || string(m.Body) != "redelivered" {
			t.Errorf("after the kept message: %q, %v; want redelivered", m.Body, err)
		}
		closeClient(t, c, p2)
	})
}

func TestReplayRejectedEndsOnlyThatSubscription(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		gone, _ := subscribe(t, c, p, stomp.SubscribeRequest{ID: "gone", Destination: "/queue/gone"})
		kept, _ := subscribe(t, c, p, stomp.SubscribeRequest{ID: "kept", Destination: "/queue/kept"})
		p.conn.Close()

		p2 := b.accept()
		p2.handshake("0,0")
		for range 2 {
			f := p2.expect(stomptext.Subscribe)
			if f.get("id") == "gone" {
				p2.errorFor(f, "no such queue")
			}
		}
		p2.conn.Close()
		var serr *stomp.ServerError
		if _, err := readNew(t.Context(), gone); !errors.As(err, &serr) || serr.Message != "no such queue" {
			t.Errorf("gone.ReadMessage = %v; want ServerError no such queue", err)
		}
		p3 := b.accept()
		p3.handshake("0,0")
		if f := p3.expect(stomptext.Subscribe); f.get("id") != "kept" {
			t.Errorf("replayed %q; want only kept", f.get("id"))
		}
		_ = kept
		closeClient(t, c, p3)
	})
}

func TestHeartbeats(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		done := make(chan *stomp.Client, 1)
		go func() {
			c, err := stomp.Open(t.Context(), b.dial, stomp.Heartbeat(time.Second, time.Second))
			if err != nil {
				t.Error(err)
			}
			done <- c
		}()
		p := b.accept()
		p.handshake("2000,500")
		c := <-done

		time.Sleep(3500 * time.Millisecond)
		if got := len(p.eols); got != 3 {
			t.Errorf("client sent %d heart-beats in 3.5s at 1s; want 3", got)
		}
		for range 4 {
			p.send("\n")
			time.Sleep(2 * time.Second)
		}
		if c.State() != stomp.Connected {
			t.Errorf("state %v after broker heart-beats every 2s; want connected", c.State())
		}
		time.Sleep(4100 * time.Millisecond)
		p2 := b.accept()
		p2.handshake("0,0")
		time.Sleep(time.Hour)
		if c.State() != stomp.Connected {
			t.Errorf("state %v after an hour with heart-beat 0,0; want connected", c.State())
		}
		closeClient(t, c, p2)
	})
}

func TestHandshakeErrorIsTerminal(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		done := make(chan error, 1)
		go func() {
			_, err := stomp.Open(t.Context(), b.dial)
			done <- err
		}()
		p := b.accept()
		p.expect(stomptext.Connect)
		p.send("ERROR\nmessage:bad credentials\n\n\x00")
		var serr *stomp.ServerError
		if err := <-done; !errors.As(err, &serr) || serr.Message != "bad credentials" {
			t.Errorf("Open = %v; want ServerError bad credentials", err)
		}
		synctest.Wait()
		if len(b.peers) != 0 {
			t.Error("client dialled again after a handshake ERROR")
		}
	})
}

func TestOpenRetriesUntilMaxAttempts(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		for range 3 {
			b.fail <- errors.New("refused")
		}
		_, err := stomp.Open(t.Context(), b.dial, stomp.MaxAttempts(3))
		if err == nil || !strings.Contains(err.Error(), "3 connection attempts failed") {
			t.Errorf("Open = %v; want failure after 3 attempts", err)
		}
	})
}

func TestUnsupportedVersion(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		done := make(chan error, 1)
		go func() {
			_, err := stomp.Open(t.Context(), b.dial)
			done <- err
		}()
		p := b.accept()
		p.expect(stomptext.Connect)
		p.send("CONNECTED\nversion:1.0\n\n\x00")
		if err := <-done; !errors.Is(err, stomp.ErrUnsupportedVersion) {
			t.Errorf("Open = %v; want ErrUnsupportedVersion", err)
		}
	})
}

func TestCloseLifecycle(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		s, _ := subscribe(t, c, p, stomp.SubscribeRequest{Destination: "/q"})
		closeClient(t, c, p)
		select {
		case <-c.Done():
		default:
			t.Error("Done not closed after Close")
		}
		if !errors.Is(c.Err(), stomp.ErrClosed) || c.State() != stomp.Closed {
			t.Errorf("Err %v State %v; want ErrClosed, closed", c.Err(), c.State())
		}
		if err := c.Send(t.Context(), stomp.Outgoing{Destination: "/q"}); !errors.Is(err, stomp.ErrClosed) {
			t.Errorf("Send after Close = %v", err)
		}
		if _, err := readNew(t.Context(), s); !errors.Is(err, stomp.ErrClosed) {
			t.Errorf("ReadMessage after Close = %v", err)
		}
		if err := c.Close(t.Context()); err != nil {
			t.Errorf("second Close = %v", err)
		}
	})
}

func TestCloseInterruptsBackoff(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b, stomp.Backoff(time.Hour, time.Hour))
		p.conn.Close()
		synctest.Wait()
		start := time.Now()
		closeClient(t, c, nil)
		if d := time.Since(start); d > time.Second {
			t.Errorf("Close took %v during backoff", d)
		}
	})
}

func TestUnsubscribeDiscardsInFlightMessages(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		s, f := subscribe(t, c, p, stomp.SubscribeRequest{Destination: "/q"})
		done := make(chan error, 1)
		go func() { done <- s.Unsubscribe(t.Context()) }()
		u := p.expect(stomptext.Unsubscribe)
		p.send(b.message(f.get("id"), "late", "", "late"))
		p.receipt(u)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if _, err := readNew(t.Context(), s); !errors.Is(err, stomp.ErrUnsubscribed) {
			t.Errorf("ReadMessage after Unsubscribe = %v; want ErrUnsubscribed", err)
		}
		if c.State() != stomp.Connected {
			t.Error("session ended by a MESSAGE that raced UNSUBSCRIBE")
		}
		closeClient(t, c, p)
	})
}

func TestReuseIDAfterOverflowWaitsForUnsubscribeReceipt(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		subscribe(t, c, p, stomp.SubscribeRequest{ID: "x", Destination: "/q", Capacity: 1, EndOnOverflow: true})
		p.send(b.message("x", "1", "", "a") + b.message("x", "2", "", "b"))
		u := p.expect(stomptext.Unsubscribe)
		if _, err := c.Subscribe(t.Context(), stomp.SubscribeRequest{ID: "x", Destination: "/q", Ack: stomp.AckClient}); !errors.Is(err, stomp.ErrDuplicateID) {
			t.Errorf("Subscribe x before UNSUBSCRIBE receipt: %v; want ErrDuplicateID", err)
		}
		p.send(b.message("x", "3", "", "late"))
		p.receipt(u)
		synctest.Wait()
		s, _ := subscribe(t, c, p, stomp.SubscribeRequest{ID: "x", Destination: "/q", Ack: stomp.AckClient})
		p.send(b.message("x", "K4", "K4", "fresh"))
		if m, err := readNew(t.Context(), s); err != nil || string(m.Body) != "fresh" {
			t.Errorf("ReadMessage = %q, %v; want fresh", m.Body, err)
		}
		if c.State() != stomp.Connected {
			t.Error("session ended")
		}
		closeClient(t, c, p)
	})
}

func TestUnsubscribeWhileDisconnectedIsOutcomeUnknown(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b, stomp.Backoff(time.Hour, time.Hour))
		s, _ := subscribe(t, c, p, stomp.SubscribeRequest{Destination: "/q"})
		p.conn.Close()
		synctest.Wait()
		if err := s.Unsubscribe(t.Context()); !errors.Is(err, stomp.ErrOutcomeUnknown) {
			t.Errorf("Unsubscribe while disconnected = %v; want ErrOutcomeUnknown", err)
		}
		closeClient(t, c, nil)
	})
}

func TestOverflowDuringCloseWritesNothingAfterDisconnect(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		subscribe(t, c, p, stomp.SubscribeRequest{ID: "x", Destination: "/q", Capacity: 1, EndOnOverflow: true})
		done := make(chan error, 1)
		go func() { done <- c.Close(context.Background()) }()
		d := p.expect(stomptext.Disconnect)
		p.send(b.message("x", "1", "", "a") + b.message("x", "2", "", "b") + b.message("x", "3", "", "c"))
		synctest.Wait()
		p.receipt(d)
		if err := <-done; err != nil {
			t.Errorf("Close = %v; want nil once DISCONNECT is confirmed (CON-28)", err)
		}
		for f := range p.frames {
			t.Errorf("frame after DISCONNECT: %s %v", f.cmd, f.headers)
		}
	})
}

func begin(t *testing.T, c *stomp.Client, p *peer) (*stomp.Tx, frame) {
	t.Helper()
	type result struct {
		tx  *stomp.Tx
		err error
	}
	done := make(chan result, 1)
	go func() {
		tx, err := c.Begin(t.Context())
		done <- result{tx, err}
	}()
	f := p.expect(stomptext.Begin)
	p.receipt(f)
	r := <-done
	if r.err != nil {
		t.Fatalf("Begin: %v", r.err)
	}
	return r.tx, f
}

func TestTransactionCommit(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		s, sf := subscribe(t, c, p, stomp.SubscribeRequest{Destination: "/in", Ack: stomp.AckClientIndividual})
		p.send(b.message(sf.get("id"), "K1", "K1", "in"))
		m, _ := readNew(t.Context(), s)
		tx, bf := begin(t, c, p)
		if bf.get("transaction") != tx.ID() || bf.get("receipt") == "" {
			t.Errorf("BEGIN = %v", bf.headers)
		}
		if err := tx.Send(t.Context(), stomp.Outgoing{Destination: "/out", Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Ack(t.Context(), m); err != nil {
			t.Fatal(err)
		}
		send := p.expect(stomptext.Send)
		if send.get("transaction") != tx.ID() || send.get("receipt") != "" {
			t.Errorf("transactional SEND = %v; want transaction %s and no receipt", send.headers, tx.ID())
		}
		ack := p.expect(stomptext.Ack)
		if ack.get("transaction") != tx.ID() || b.ackID(ack, sf.get("id")) != "K1" {
			t.Errorf("transactional ACK = %v", ack.headers)
		}
		done := make(chan error, 1)
		go func() { done <- tx.Commit(t.Context()) }()
		commit := p.expect(stomptext.Commit)
		if commit.get("transaction") != tx.ID() {
			t.Errorf("COMMIT = %v", commit.headers)
		}
		p.receipt(commit)
		if err := <-done; err != nil {
			t.Fatalf("Commit: %v", err)
		}
		if err := tx.Send(t.Context(), stomp.Outgoing{Destination: "/out"}); !errors.Is(err, stomp.ErrTxDone) {
			t.Errorf("Send after Commit = %v; want ErrTxDone", err)
		}
		if err := tx.Abort(t.Context()); !errors.Is(err, stomp.ErrTxDone) {
			t.Errorf("Abort after Commit = %v; want ErrTxDone", err)
		}
		closeClient(t, c, p)
	})
}

func TestAckChecksOwnerAndModeWhileDisconnected(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b, stomp.Backoff(time.Hour, time.Hour))
		other, op := open(t, b)
		auto, fa := subscribe(t, c, p, stomp.SubscribeRequest{ID: "s", Destination: "/a"})
		ind, _ := subscribe(t, other, op, stomp.SubscribeRequest{ID: "s", Destination: "/a", Ack: stomp.AckClientIndividual})
		p.send(b.message(fa.get("id"), "1", "", "x"))
		op.send(b.message("s", "K", "K", "y"))
		ma, _ := readNew(t.Context(), auto)
		mo, _ := readNew(t.Context(), ind)
		p.expect(stomptext.Ack)
		mine, _ := subscribe(t, c, p, stomp.SubscribeRequest{ID: "t", Destination: "/b", Ack: stomp.AckClientIndividual})
		if err := mine.Ack(t.Context(), mo); !errors.Is(err, stomp.ErrWrongSubscription) {
			t.Errorf("Ack of another Client's message = %v; want ErrWrongSubscription", err)
		}
		tx, _ := begin(t, c, p)
		if err := tx.Ack(t.Context(), mo); !errors.Is(err, stomp.ErrWrongSubscription) {
			t.Errorf("Tx.Ack of another Client's message = %v; want ErrWrongSubscription", err)
		}
		p.conn.Close()
		synctest.Wait()
		if err := auto.Ack(t.Context(), ma); !errors.Is(err, stomp.ErrAutoAck) {
			t.Errorf("Ack on auto while disconnected = %v; want ErrAutoAck", err)
		}
		closeClient(t, c, nil)
		closeClient(t, other, op)
	})
}

func TestTransactionAbort(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		tx, _ := begin(t, c, p)
		done := make(chan error, 1)
		go func() { done <- tx.Abort(t.Context()) }()
		a := p.expect(stomptext.Abort)
		p.receipt(a)
		if err := <-done; err != nil || a.get("transaction") != tx.ID() {
			t.Errorf("Abort = %v, frame %v", err, a.headers)
		}
		closeClient(t, c, p)
	})
}

func TestTransactionLostWithSession(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b, stomp.Backoff(time.Hour, time.Hour))
		tx, _ := begin(t, c, p)
		p.conn.Close()
		synctest.Wait()
		if err := tx.Send(t.Context(), stomp.Outgoing{Destination: "/q"}); !errors.Is(err, stomp.ErrTxAborted) {
			t.Errorf("Send after session loss = %v; want ErrTxAborted", err)
		}
		if err := tx.Commit(t.Context()); !errors.Is(err, stomp.ErrTxAborted) {
			t.Errorf("Commit after session loss = %v; want ErrTxAborted", err)
		}
		closeClient(t, c, nil)
	})
}

func TestTransactionNotReplayedAfterReconnect(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		tx, _ := begin(t, c, p)
		p.conn.Close()
		p2 := b.accept()
		p2.handshake("0,0")
		p2.noFrame()
		if err := tx.Send(t.Context(), stomp.Outgoing{Destination: "/q"}); !errors.Is(err, stomp.ErrTxAborted) {
			t.Errorf("Send in the next session = %v; want ErrTxAborted", err)
		}
		closeClient(t, c, p2)
	})
}

func TestCommitOutcomeUnknownWhenSessionEnds(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b, stomp.Backoff(time.Hour, time.Hour))
		tx, _ := begin(t, c, p)
		done := make(chan error, 1)
		go func() { done <- tx.Commit(t.Context()) }()
		p.expect(stomptext.Commit)
		p.conn.Close()
		if err := <-done; !errors.Is(err, stomp.ErrOutcomeUnknown) {
			t.Errorf("Commit = %v; want ErrOutcomeUnknown", err)
		}
		closeClient(t, c, nil)
	})
}

func TestVersionNegotiated(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		if got := c.Version().String(); got != b.ver {
			t.Errorf("Version = %s; want %s", got, b.ver)
		}
		closeClient(t, c, p)
	})
}

func TestVersionsOptionRestrictsOffer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBroker(t, "1.2")
		done := make(chan error, 1)
		go func() {
			_, err := stomp.Open(t.Context(), b.dial, stomp.Versions(stomp.V11))
			done <- err
		}()
		p := b.accept()
		f := p.handshake("0,0")
		if v := f.get("accept-version"); v != "1.1" {
			t.Errorf("accept-version = %q; want 1.1", v)
		}
		if err := <-done; !errors.Is(err, stomp.ErrUnsupportedVersion) {
			t.Errorf("Open against a broker choosing an unoffered 1.2 = %v; want ErrUnsupportedVersion", err)
		}
	})
}

func TestUnsubscribeKeepsQueuedMessages(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		s, f := subscribe(t, c, p, stomp.SubscribeRequest{Destination: "/q", Ack: stomp.AckClientIndividual})
		p.send(b.message(f.get("id"), "K1", "K1", "queued"))
		synctest.Wait()
		done := make(chan error, 1)
		go func() { done <- s.Unsubscribe(t.Context()) }()
		p.receipt(p.expect(stomptext.Unsubscribe))
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		m, err := readNew(t.Context(), s)
		if err != nil || string(m.Body) != "queued" {
			t.Fatalf("ReadMessage after Unsubscribe = %q, %v; want the queued message", m.Body, err)
		}
		if _, err := readNew(t.Context(), s); !errors.Is(err, stomp.ErrUnsubscribed) {
			t.Errorf("ReadMessage after draining = %v; want ErrUnsubscribed", err)
		}
		closeClient(t, c, p)
	})
}

func TestAckFromEndedSubscriptionAfterIDReuseIsStale(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		old, _ := subscribe(t, c, p, stomp.SubscribeRequest{ID: "x", Destination: "/q", Ack: stomp.AckClientIndividual})
		p.send(b.message("x", "K1", "K1", "first"))
		m, err := readNew(t.Context(), old)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- old.Unsubscribe(t.Context()) }()
		p.receipt(p.expect(stomptext.Unsubscribe))
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		subscribe(t, c, p, stomp.SubscribeRequest{ID: "x", Destination: "/q", Ack: stomp.AckClientIndividual})
		if err := old.Ack(t.Context(), m); !errors.Is(err, stomp.ErrStaleAck) {
			t.Errorf("Ack of a message from the ended subscription x = %v; want ErrStaleAck", err)
		}
		closeClient(t, c, p)
	})
}

func TestAutoAckOfQueuedMessageAfterIDReuseWritesNothing(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		old, _ := subscribe(t, c, p, stomp.SubscribeRequest{ID: "x", Destination: "/q"})
		p.send(b.message("x", "K1", "K1", "queued"))
		synctest.Wait()
		done := make(chan error, 1)
		go func() { done <- old.Unsubscribe(t.Context()) }()
		p.receipt(p.expect(stomptext.Unsubscribe))
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		subscribe(t, c, p, stomp.SubscribeRequest{ID: "x", Destination: "/q"})
		if m, err := readNew(t.Context(), old); err != nil || string(m.Body) != "queued" {
			t.Fatalf("ReadMessage on the ended subscription = %q, %v; want the queued message", m.Body, err)
		}
		closeClient(t, c, p)
	})
}

func TestAutoAckWithEndedContextStillAcknowledges(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		s, f := subscribe(t, c, p, stomp.SubscribeRequest{Destination: "/q"})
		p.send(b.message(f.get("id"), "K1", "K1", "queued"))
		synctest.Wait()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if m, err := readNew(ctx, s); err != nil || string(m.Body) != "queued" {
			t.Fatalf("ReadMessage with an ended context = %q, %v; want the queued message", m.Body, err)
		}
		if got := b.ackID(p.expect(stomptext.Ack), f.get("id")); got != "K1" {
			t.Errorf("ACK for %q; want K1", got)
		}
		closeClient(t, c, p)
	})
}

func readNew(ctx context.Context, s *stomp.Subscription) (stomp.Message, error) {
	var m stomp.Message
	err := s.ReadMessage(ctx, &m)
	return m, err
}

func bodiesUntilEnd(ctx context.Context, s *stomp.Subscription) ([]string, error) {
	var bodies []string
	var m stomp.Message
	for {
		if err := s.ReadMessage(ctx, &m); err != nil {
			return bodies, err
		}
		bodies = append(bodies, string(m.Body))
	}
}
