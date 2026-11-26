package stomp_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jacoelho/stomp"
	"github.com/jacoelho/stomp/stomptext"
)

func TestWriteTimeoutDefaultEndsTheSession(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stalled := make(chan struct{})
		dial := func(context.Context, int) (net.Conn, error) {
			client, server := net.Pipe()
			go func() {
				dec := stomptext.NewDecoder(server)
				if _, err := dec.ReadFrame(); err != nil {
					return
				}
				server.Write([]byte("CONNECTED\nversion:1.2\nheart-beat:0,0\n\n\x00"))
				close(stalled)
			}()
			return client, nil
		}
		c, err := stomp.Open(t.Context(), dial, stomp.MaxAttempts(1), stomp.Backoff(time.Hour, time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		<-stalled
		start := time.Now()
		err = c.Send(t.Context(), stomp.Outgoing{Destination: "/q", Body: []byte("x")})
		if !errors.Is(err, stomp.ErrOutcomeUnknown) || time.Since(start) != 10*time.Second {
			t.Errorf("Send to a broker that stopped reading = %v after %v; want ErrOutcomeUnknown after the 10s WriteTimeout (CON-13)", err, time.Since(start))
		}
		c.Close(t.Context())
	})
}

type recordHandler struct {
	mu   sync.Mutex
	msgs []string
}

func (h *recordHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *recordHandler) WithGroup(string) slog.Handler            { return h }
func (h *recordHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.msgs = append(h.msgs, r.Message)
	return nil
}

func TestSessionTransitionsLogToSlogDefault(t *testing.T) {
	h := &recordHandler{}
	prev := slog.Default()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(prev) })
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		p.conn.Close()
		p2 := b.accept()
		p2.handshake("0,0")
		synctest.Wait()
		closeClient(t, c, p2)
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	if !slices.Contains(h.msgs, "stomp session ended") {
		t.Errorf("slog.Default records = %q; want %q without a Logger option (CON-22)", h.msgs, "stomp session ended")
	}
}

func TestReconnectNegotiatesTheVersionAfresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBroker(t, "1.2")
		c, p := open(t, b)
		s, _ := subscribe(t, c, p, stomp.SubscribeRequest{ID: "s", Destination: "/q", Ack: stomp.AckClientIndividual})
		p.conn.Close()
		p2 := b.accept()
		p2.expect(stomptext.Connect)
		p2.send("CONNECTED\nversion:1.1\nheart-beat:0,0\n\n\x00")
		p2.receipt(p2.expect(stomptext.Subscribe))
		if got := c.Version().String(); got != "1.1" {
			t.Fatalf("Version after reconnecting to a 1.1 broker = %s; want 1.1 (CON-27)", got)
		}
		p2.send("MESSAGE\nsubscription:s\nmessage-id:m1\ndestination:/q\n\nbody\x00")
		m, err := readNew(t.Context(), s)
		if err != nil || m.ID != "m1" {
			t.Fatalf("ReadMessage = %q, %v; want m1, a 1.1 MESSAGE without an ack header", m.ID, err)
		}
		if err := s.Ack(t.Context(), m); err != nil {
			t.Fatal(err)
		}
		if f := p2.expect(stomptext.Ack); f.get("message-id") != "m1" || f.get("subscription") != "s" {
			t.Errorf("ACK headers = %v; want 1.1 message-id:m1 and subscription:s (SUB-27)", f.headers)
		}
		closeClient(t, c, p2)
	})
}

func TestBackoffDefaults(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBroker(t, "1.2")
		var failing atomic.Bool
		dials := make(chan time.Time, 64)
		dial := func(ctx context.Context, attempt int) (net.Conn, error) {
			if failing.Load() {
				dials <- time.Now()
				return nil, errors.New("refused")
			}
			return b.dial(ctx, attempt)
		}
		done := make(chan *stomp.Client, 1)
		go func() {
			c, err := stomp.Open(t.Context(), dial)
			if err != nil {
				t.Error(err)
			}
			done <- c
		}()
		p := b.accept()
		p.handshake("0,0")
		c := <-done
		failing.Store(true)
		last := time.Now()
		p.conn.Close()
		longest := time.Duration(0)
		for n := range 40 {
			at := <-dials
			gap := at.Sub(last)
			last = at
			if ceiling := min(30*time.Second, 100*time.Millisecond<<min(n, 20)); gap > ceiling {
				t.Errorf("delay before attempt %d = %v; want at most %v from BaseBackoff 100ms and MaxBackoff 30s (RCN-8)", n, gap, ceiling)
			}
			if n >= 9 {
				longest = max(longest, gap)
			}
		}
		if longest <= 15*time.Second {
			t.Errorf("longest of 31 delays capped at 30s = %v; want above 15s, so MaxBackoff is 30s (RCN-8)", longest)
		}
		c.Close(t.Context())
	})
}

func TestBackoffResetsAfterASessionLastingMaxBackoff(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b, stomp.Backoff(time.Second, 8*time.Second))
		for trial := range 20 {
			for range 3 {
				p.conn.Close()
				p = b.accept()
				p.handshake("0,0")
				synctest.Wait()
			}
			time.Sleep(8 * time.Second)
			start := time.Now()
			p.conn.Close()
			p = b.accept()
			if d := time.Since(start); d > time.Second {
				t.Fatalf("trial %d: delay after a session lasting MaxBackoff = %v; want at most BaseBackoff 1s (RCN-9)", trial, d)
			}
			p.handshake("0,0")
			synctest.Wait()
		}
		closeClient(t, c, p)
	})
}

func TestSendDoesNotRetainTheBodyAfterReturning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		frames := make(chan stomptext.Frame, 4)
		dial := func(context.Context, int) (net.Conn, error) {
			client, server := net.Pipe()
			go func() {
				dec := stomptext.NewDecoder(server)
				if _, err := dec.ReadFrame(); err != nil {
					return
				}
				server.Write([]byte("CONNECTED\nversion:1.2\nheart-beat:0,0\n\n\x00"))
				<-gate
				for {
					f, err := dec.ReadFrame()
					if err != nil {
						close(frames)
						return
					}
					frames <- f.Clone()
				}
			}()
			return client, nil
		}
		c, err := stomp.Open(t.Context(), dial, stomp.MaxAttempts(1), stomp.Backoff(time.Hour, time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		firstCtx, endFirst := context.WithCancel(t.Context())
		defer endFirst()
		go c.Send(firstCtx, stomp.Outgoing{Destination: "/q", Body: []byte("first")})
		synctest.Wait()
		body := []byte("original")
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- c.Send(ctx, stomp.Outgoing{Destination: "/q", Body: body}) }()
		synctest.Wait()
		cancel()
		if err := <-done; !errors.Is(err, stomp.ErrOutcomeUnknown) {
			t.Fatalf("Send after ctx ended with the frame buffered = %v; want ErrOutcomeUnknown", err)
		}
		copy(body, "MUTATED!")
		close(gate)
		<-frames
		if f := <-frames; string(f.Body()) != "original" {
			t.Errorf("SEND body = %q after the caller reused the slice before the frame was written; want original (SND-14)", f.Body())
		}
		closeCtx, endClose := context.WithTimeout(t.Context(), time.Second)
		defer endClose()
		c.Close(closeCtx)
	})
}

func TestConcurrentReadMessageIntoNewMessagesReceivesEachMessageOnce(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		s, f := subscribe(t, c, p, stomp.SubscribeRequest{Destination: "/q", Ack: stomp.AckClientIndividual})
		const consumers, messages = 4, 100
		got := make(chan string, messages)
		var wg sync.WaitGroup
		for range consumers {
			wg.Go(func() {
				for {
					m, err := readNew(t.Context(), s)
					if err != nil {
						return
					}
					got <- m.ID
				}
			})
		}
		for i := range messages {
			p.send(b.message(f.get("id"), fmt.Sprint(i), "", "x"))
		}
		seen := map[string]int{}
		for range messages {
			seen[<-got]++
		}
		for i := range messages {
			if n := seen[fmt.Sprint(i)]; n != 1 {
				t.Errorf("message %d returned %d times across %d concurrent ReadMessage callers; want once (SUB-12)", i, n, consumers)
			}
		}
		closeClient(t, c, p)
		wg.Wait()
	})
}

func TestConcurrentReadMessageIntoReusedMessagesDeliversEachBodyOnce(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		s, f := subscribe(t, c, p, stomp.SubscribeRequest{Destination: "/q", Ack: stomp.AckClientIndividual})
		const consumers, messages = 4, 100
		body := func(i int) string { return strings.Repeat(fmt.Sprint(i%10), 1+i*13%200) }
		type delivery struct{ id, body string }
		got := make(chan delivery, messages)
		var wg sync.WaitGroup
		for range consumers {
			wg.Go(func() {
				var m stomp.Message
				for {
					if err := s.ReadMessage(t.Context(), &m); err != nil {
						return
					}
					got <- delivery{m.ID, string(m.Body)}
				}
			})
		}
		for i := range messages {
			p.send(b.message(f.get("id"), fmt.Sprint(i), "", body(i)))
		}
		seen := map[string]int{}
		for range messages {
			d := <-got
			seen[d.id]++
			var i int
			fmt.Sscan(d.id, &i)
			if d.body != body(i) {
				t.Errorf("message %s body %q, want %q", d.id, d.body, body(i))
			}
		}
		for i := range messages {
			if n := seen[fmt.Sprint(i)]; n != 1 {
				t.Errorf("message %d returned %d times across %d concurrent ReadMessage callers; want once (SUB-12)", i, n, consumers)
			}
		}
		closeClient(t, c, p)
		wg.Wait()
	})
}

func TestAckOfAReusedMessageAcknowledgesItsLatestDelivery(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		s, f := subscribe(t, c, p, stomp.SubscribeRequest{Destination: "/q", Ack: stomp.AckClientIndividual})
		var m stomp.Message
		for _, id := range []string{"m-1", "m-2"} {
			p.send(b.message(f.get("id"), id, "", "body-"+id))
			if err := s.ReadMessage(t.Context(), &m); err != nil {
				t.Fatal(err)
			}
			if err := s.Ack(t.Context(), m); err != nil {
				t.Fatal(err)
			}
			a := p.expect(stomptext.Ack)
			if got := a.get("id") + a.get("message-id"); got != id {
				t.Errorf("ACK after ReadMessage names %q, want %q", got, id)
			}
		}
		closeClient(t, c, p)
	})
}

func TestReturnedMessagesAreNotModifiedByLaterDeliveries(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		s, f := subscribe(t, c, p, stomp.SubscribeRequest{Destination: "/q", Ack: stomp.AckClientIndividual})
		p.send(b.message(f.get("id"), "first", "", "aaaaaaaa"))
		m, err := readNew(t.Context(), s)
		if err != nil {
			t.Fatal(err)
		}
		body, headers := bytes.Clone(m.Body), slices.Clone(m.Headers)
		for i := range 50 {
			p.send(b.message(f.get("id"), fmt.Sprintf("later-%02d", i), "", strings.Repeat("z", 8)))
			if _, err := readNew(t.Context(), s); err != nil {
				t.Fatal(err)
			}
		}
		if !bytes.Equal(m.Body, body) || !slices.Equal(m.Headers, headers) || m.ID != "first" {
			t.Errorf("first message changed after later deliveries: %q %v; want %q %v (SUB-24)", m.Body, m.Headers, body, headers)
		}
		closeClient(t, c, p)
	})
}

func TestSubscribeWritesNoWindowHeaderForAKnownBroker(t *testing.T) {
	for _, server := range []string{"RabbitMQ/4.0.0", "ActiveMQ/6.1.0", "ActiveMQ-Artemis/2.44.0"} {
		t.Run(server, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := newBroker(t, "1.2")
				done := make(chan *stomp.Client, 1)
				go func() {
					c, err := stomp.Open(t.Context(), b.dial)
					if err != nil {
						t.Error(err)
					}
					done <- c
				}()
				p := b.accept()
				p.expect(stomptext.Connect)
				p.send("CONNECTED\nversion:1.2\nheart-beat:0,0\nserver:" + server + "\n\n\x00")
				c := <-done
				_, f := subscribe(t, c, p, stomp.SubscribeRequest{ID: "s", Destination: "/q"})
				var names []string
				for _, h := range f.headers {
					names = append(names, h.Name)
				}
				slices.Sort(names)
				if want := []string{"ack", "destination", "id", "receipt"}; !slices.Equal(names, want) {
					t.Errorf("SUBSCRIBE headers to %s = %v; want only %v (SUB-40)", server, names, want)
				}
				closeClient(t, c, p)
			})
		})
	}
}

func TestTxSendValidatesLikeSend(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		tx, _ := begin(t, c, p)
		for _, tt := range []struct {
			name string
			m    stomp.Outgoing
			want error
		}{
			{"empty destination", stomp.Outgoing{Body: []byte("x")}, stomp.ErrInvalidRequest},
			{"NUL in a text body", stomp.Outgoing{Destination: "/q", Body: []byte("a\x00b"), Text: true}, stomp.ErrTextHasNUL},
			{"destination header", stomp.Outgoing{Destination: "/q", Headers: stomp.Headers{{Name: "destination", Value: "/x"}}}, stomp.ErrReservedHeader},
			{"content-length header", stomp.Outgoing{Destination: "/q", Headers: stomp.Headers{{Name: "content-length", Value: "1"}}}, stomp.ErrReservedHeader},
			{"content-type header", stomp.Outgoing{Destination: "/q", Headers: stomp.Headers{{Name: "content-type", Value: "a/b"}}}, stomp.ErrReservedHeader},
			{"receipt header", stomp.Outgoing{Destination: "/q", Headers: stomp.Headers{{Name: "receipt", Value: "r"}}}, stomp.ErrReservedHeader},
			{"transaction header", stomp.Outgoing{Destination: "/q", Headers: stomp.Headers{{Name: "transaction", Value: "other"}}}, stomp.ErrReservedHeader},
		} {
			if err := tx.Send(t.Context(), tt.m); !errors.Is(err, tt.want) {
				t.Errorf("Tx.Send with %s = %v; want %v (TX-3)", tt.name, err, tt.want)
			}
		}
		p.noFrame()
		if err := tx.Send(t.Context(), stomp.Outgoing{Destination: "/q", Body: []byte("text"), Text: true}); err != nil {
			t.Fatal(err)
		}
		if f := p.expect(stomptext.Send); f.body != "text" || f.get("transaction") != tx.ID() {
			t.Errorf("Tx.Send text = %+v; want body text in transaction %s", f, tx.ID())
		} else if _, ok := f.headers.Get("content-length"); ok {
			t.Errorf("Tx.Send text wrote content-length; want none (SND-3)")
		}
		closeClient(t, c, p)
	})
}

func TestCloseLeavesOpenTransactionsToTheBroker(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		tx, _ := begin(t, c, p)
		closeClient(t, c, p)
		for name, call := range map[string]func() error{
			"Send":   func() error { return tx.Send(t.Context(), stomp.Outgoing{Destination: "/q"}) },
			"Commit": func() error { return tx.Commit(t.Context()) },
			"Abort":  func() error { return tx.Abort(t.Context()) },
		} {
			if err := call(); !errors.Is(err, stomp.ErrClosed) {
				t.Errorf("Tx.%s after Close = %v; want ErrClosed (TX-9)", name, err)
			}
		}
		p.noFrame()
	})
}

func TestConcurrentTxSendsKeepEachCallersOrder(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		tx, _ := begin(t, c, p)
		const senders, each = 4, 25
		var wg sync.WaitGroup
		for g := range senders {
			wg.Go(func() {
				for i := range each {
					if err := tx.Send(t.Context(), stomp.Outgoing{Destination: "/q", Body: fmt.Appendf(nil, "%d-%02d", g, i)}); err != nil {
						t.Errorf("Tx.Send: %v", err)
						return
					}
				}
			})
		}
		next := make([]int, senders)
		for range senders * each {
			f := p.expect(stomptext.Send)
			var g, i int
			if _, err := fmt.Sscanf(f.body, "%d-%d", &g, &i); err != nil || f.get("transaction") != tx.ID() {
				t.Fatalf("SEND %q in transaction %q; want a sender's body in %s", f.body, f.get("transaction"), tx.ID())
			}
			if i != next[g] {
				t.Errorf("sender %d's message %d arrived when %d was next; want each sender's order kept (TX-11)", g, i, next[g])
			}
			next[g] = i + 1
		}
		wg.Wait()
		closeClient(t, c, p)
	})
}

func TestMessageWithoutSubscriptionHeaderEndsTheSession(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		s, f := subscribe(t, c, p, stomp.SubscribeRequest{ID: "s", Destination: "/q", Ack: stomp.AckClientIndividual})
		p.send(strings.Replace(b.message(f.get("id"), "m1", "", "x"), "subscription:s\n", "", 1))
		p2 := b.accept()
		p2.handshake("0,0")
		p2.receipt(p2.expect(stomptext.Subscribe))
		p2.send(b.message("s", "m2", "", "y"))
		if m, err := readNew(t.Context(), s); err != nil || m.ID != "m2" {
			t.Errorf("ReadMessage = %q, %v; want m2 from the next session, the MESSAGE without subscription having ended the first (SUB-50)", m.ID, err)
		}
		closeClient(t, c, p2)
	})
}

func TestShortSessionsCountTowardMaxAttempts(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b, stomp.MaxAttempts(3), stomp.Backoff(time.Second, 8*time.Second))
		synctest.Wait()
		p.conn.Close()
		p = b.accept()
		p.handshake("0,0")
		time.Sleep(8 * time.Second)
		p.conn.Close()
		for range 2 {
			p = b.accept()
			p.handshake("0,0")
			synctest.Wait()
			p.conn.Close()
		}
		p = b.accept()
		p.handshake("0,0")
		synctest.Wait()
		select {
		case <-c.Done():
			t.Fatalf("Client ended after a short session, a stable one and two short ones: %v; want the stable session to start the count over (RCN-17)", c.Err())
		default:
		}
		p.conn.Close()
		select {
		case <-c.Done():
		case <-b.peers:
			t.Fatal("Client dialed again after three consecutive short sessions with MaxAttempts 3; want it ended (RCN-17)")
		}
		if err := c.Err(); err == nil || !strings.Contains(err.Error(), "3 connection attempts failed") {
			t.Errorf("Err = %v; want 3 connection attempts failed", err)
		}
		c.Close(t.Context())
	})
}

func TestDialAttemptCountsConsecutiveFailuresSinceALastingSession(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		inner := b.dial
		var attempts []int
		b.dial = func(ctx context.Context, attempt int) (net.Conn, error) {
			attempts = append(attempts, attempt)
			return inner(ctx, attempt)
		}
		b.fail <- errors.New("refused")
		c, p := open(t, b, stomp.Backoff(time.Second, 8*time.Second))
		synctest.Wait()
		p.conn.Close()
		p = b.accept()
		p.handshake("0,0")
		time.Sleep(8 * time.Second)
		p.conn.Close()
		p = b.accept()
		p.handshake("0,0")
		synctest.Wait()
		if want := []int{0, 1, 2, 0}; !slices.Equal(attempts, want) {
			t.Errorf("dial attempts = %v; want %v: a refused dial, a short session, then a session lasting MaxBackoff (RCN-17)", attempts, want)
		}
		closeClient(t, c, p)
	})
}

func TestFailedDialsDuringOpenCountTowardMaxAttempts(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		b.fail <- errors.New("refused")
		b.fail <- errors.New("refused")
		c, p := open(t, b, stomp.MaxAttempts(3), stomp.Backoff(time.Second, 8*time.Second))
		synctest.Wait()
		p.conn.Close()
		select {
		case <-c.Done():
		case <-b.peers:
			t.Fatal("Client dialed again after two refused dials and one short session with MaxAttempts 3; want it ended (RCN-10, RCN-17)")
		}
		if err := c.Err(); err == nil || !strings.Contains(err.Error(), "3 connection attempts failed") {
			t.Errorf("Err = %v; want 3 connection attempts failed", err)
		}
		c.Close(t.Context())
	})
}

func TestHandshakeTimeoutDefault(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dial := func(context.Context, int) (net.Conn, error) {
			client, server := net.Pipe()
			go io.Copy(io.Discard, server)
			return client, nil
		}
		start := time.Now()
		_, err := stomp.Open(t.Context(), dial, stomp.MaxAttempts(1))
		if err == nil || time.Since(start) != 10*time.Second {
			t.Errorf("Open against a broker that never answers = %v after %v; want failure after the 10s HandshakeTimeout (CON-6)", err, time.Since(start))
		}
	})
}

func TestSilentBrokerEndsTheSessionWithErrHeartbeatTimeout(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		var mu sync.Mutex
		var waiting []error
		onState := stomp.OnState(func(s stomp.State, err error) {
			if s == stomp.Waiting {
				mu.Lock()
				waiting = append(waiting, err)
				mu.Unlock()
			}
		})
		c, p, _ := openHandshake(t, b, "1000,0", stomp.Heartbeat(0, time.Second), stomp.Backoff(time.Hour, time.Hour), onState)
		start := time.Now()
		time.Sleep(1900 * time.Millisecond)
		if s := c.State(); s != stomp.Connected {
			t.Fatalf("state %v after 1.9s of silence with a 1s receive interval; want connected", s)
		}
		<-p.frames
		ended := time.Since(start)
		synctest.Wait()
		mu.Lock()
		got := slices.Clone(waiting)
		mu.Unlock()
		if len(got) != 1 || !errors.Is(got[0], stomp.ErrHeartbeatTimeout) || ended != 2*time.Second {
			t.Errorf("Waiting causes %v, connection closed after %v; want one ErrHeartbeatTimeout after 2s, twice the receive interval (CON-10)", got, ended)
		}
		closeClient(t, c, nil)
	})
}

func TestPartialFrameOctetsKeepTheSessionAlive(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p, _ := openHandshake(t, b, "1000,0", stomp.Heartbeat(0, time.Second))
		for _, part := range []string{"MESSAGE\n", "subscription:zz\n", "message-id:1\n", "destination:/q\n", "\n\x00"} {
			time.Sleep(1500 * time.Millisecond)
			p.send(part)
		}
		if s := c.State(); s != stomp.Connected {
			t.Errorf("state %v after a frame arrived in parts 1.5s apart over 7.5s, receive interval 1s; want connected (CON-11)", s)
		}
		select {
		case <-b.peers:
			t.Error("client dialed again; want the session kept")
		default:
		}
		closeClient(t, c, p)
	})
}

func TestCloseInterruptsADialInProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBroker(t, "1.2")
		var blocking atomic.Bool
		dialEnded := make(chan error, 1)
		dial := func(ctx context.Context, attempt int) (net.Conn, error) {
			if blocking.Load() {
				<-ctx.Done()
				dialEnded <- ctx.Err()
				return nil, ctx.Err()
			}
			return b.dial(ctx, attempt)
		}
		done := make(chan *stomp.Client, 1)
		go func() {
			c, err := stomp.Open(t.Context(), dial, stomp.Backoff(time.Millisecond, time.Millisecond))
			if err != nil {
				t.Error(err)
			}
			done <- c
		}()
		p := b.accept()
		p.handshake("0,0")
		c := <-done
		blocking.Store(true)
		p.conn.Close()
		time.Sleep(time.Second)
		start := time.Now()
		if err := c.Close(t.Context()); err != nil {
			t.Errorf("Close during a dial = %v; want nil", err)
		}
		if d := time.Since(start); d != 0 {
			t.Errorf("Close took %v during a dial; want it to return without waiting (RCN-13)", d)
		}
		select {
		case err := <-dialEnded:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("dial ended with %v; want its ctx cancelled", err)
			}
		default:
			t.Error("dial still running after Close returned; want it cancelled (RCN-13)")
		}
	})
}

func TestCapacityBytesDefaultIs16MiB(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBroker(t, "1.2")
		c, p := open(t, b, stomptext.MaxFrameSize(32<<20))
		s, f := subscribe(t, c, p, stomp.SubscribeRequest{Destination: "/q", Ack: stomp.AckClientIndividual, EndOnOverflow: true})
		p.send(b.message(f.get("id"), "full", "", strings.Repeat("x", 16<<20)))
		p.send(b.message(f.get("id"), "over", "", "y"))
		p.expect(stomptext.Unsubscribe)
		if m, err := readNew(t.Context(), s); err != nil || m.ID != "full" {
			t.Errorf("ReadMessage = %q, %v; want the 16 MiB message queued (SUB-7)", m.ID, err)
		}
		if _, err := readNew(t.Context(), s); !errors.Is(err, stomp.ErrOverflow) {
			t.Errorf("ReadMessage after one more octet = %v; want ErrOverflow at the 16 MiB CapacityBytes default (SUB-7)", err)
		}
		closeClient(t, c, p)
	})
}
