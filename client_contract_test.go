package stomp_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jacoelho/stomp"
	"github.com/jacoelho/stomp/stomptext"
)

func TestConnectHeadersDefaultsAndVirtualHost(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p, f := openHandshake(t, b, "0,0")
		want := stomptext.Headers{{Name: "accept-version", Value: "1.1,1.2"}, {Name: "host", Value: "/"}, {Name: "heart-beat", Value: "10000,10000"}}
		if !slices.Equal(f.headers, want) {
			t.Errorf("default CONNECT headers = %v; want %v", f.headers, want)
		}
		closeClient(t, c, p)

		c, p, f = openHandshake(t, b, "0,0", stomp.VirtualHost("orders"))
		if f.get("host") != "orders" {
			t.Errorf("host = %q; want orders", f.get("host"))
		}
		closeClient(t, c, p)
	})
}

func TestMessageHeadersAreExactlyTheFrameHeaders(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		s, _ := subscribe(t, c, p, stomp.SubscribeRequest{ID: "s", Destination: "/q", Ack: stomp.AckClientIndividual})
		p.send("MESSAGE\nsubscription:s\nmessage-id:M\nack:M\ndestination:/q\nx:a\r\ncontent-length:1\n\nz\x00")
		m, err := readNew(t.Context(), s)
		if err != nil {
			t.Fatal(err)
		}
		x := "a"
		if b.ver == "1.1" {
			x = "a\r"
		}
		want := stomp.Headers{
			{Name: "subscription", Value: "s"}, {Name: "message-id", Value: "M"}, {Name: "ack", Value: "M"},
			{Name: "destination", Value: "/q"}, {Name: "x", Value: x}, {Name: "content-length", Value: "1"},
		}
		if !slices.Equal(m.Headers, want) || m.ID != "M" || m.Destination != "/q" || string(m.Body) != "z" {
			t.Errorf("message = %q %q %q %v; want M /q z %q", m.ID, m.Destination, m.Body, m.Headers, want)
		}
		closeClient(t, c, p)
	})
}

func TestFrameLimitOptionsReachTheDecoder(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b, stomptext.MaxFrameSize(128))
		_, f := subscribe(t, c, p, stomp.SubscribeRequest{Destination: "/q"})
		if _, err := io.WriteString(p.conn, b.message(f.get("id"), "1", "", strings.Repeat("x", 200))); err != nil && !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("broker write: %v", err)
		}
		p2 := b.accept()
		p2.handshake("0,0")
		p2.receipt(p2.expect(stomptext.Subscribe))
		closeClient(t, c, p2)
	})
}

func TestBrokerErrorForSendReceiptIsReturnedAsServerError(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b, stomp.Backoff(time.Hour, time.Hour))
		done := make(chan error, 1)
		go func() { done <- c.Send(t.Context(), stomp.Outgoing{Destination: "/q"}) }()
		f := p.expect(stomptext.Send)
		p.errorFor(f, "no such queue")
		err := <-done
		var serr *stomp.ServerError
		if !errors.As(err, &serr) || errors.Is(err, stomp.ErrOutcomeUnknown) || serr.Message != "no such queue" {
			t.Errorf("Send = %v; want the broker's ServerError, not ErrOutcomeUnknown", err)
		}
		if serr != nil && serr.Error() != "stomp: broker error: no such queue" {
			t.Errorf("Error() = %q", serr.Error())
		}
		synctest.Wait()
		closeClient(t, c, nil)
	})
}

func TestOnStateReportsEveryTransition(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		type change struct {
			st  stomp.State
			err bool
		}
		changes := make(chan change, 16)
		c, p := open(t, b, stomp.Backoff(time.Second, time.Second), stomp.OnState(func(st stomp.State, err error) {
			changes <- change{st, err != nil}
		}))
		p.conn.Close()
		p2 := b.accept()
		p2.handshake("0,0")
		synctest.Wait()
		closeClient(t, c, p2)
		close(changes)
		var got []change
		for ch := range changes {
			got = append(got, ch)
		}
		want := []change{
			{stomp.Connected, false}, {stomp.Waiting, true},
			{stomp.Connecting, false}, {stomp.Connected, false}, {stomp.Closed, true},
		}
		if !slices.Equal(got, want) {
			t.Errorf("state changes = %v; want %v", got, want)
		}
	})
}

func TestStateNames(t *testing.T) {
	for st, want := range map[stomp.State]string{
		stomp.Connecting: "connecting", stomp.Connected: "connected", stomp.Waiting: "waiting", stomp.Closed: "closed", stomp.State(9): "State(9)",
	} {
		if got := st.String(); got != want {
			t.Errorf("State(%d).String() = %q; want %q", int(st), got, want)
		}
	}
}

func TestMaxAttemptsCountsEveryDial(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		for _, n := range []int{1, 3} {
			for range n + 2 {
				b.fail <- errors.New("refused")
			}
			if _, err := stomp.Open(t.Context(), b.dial, stomp.MaxAttempts(n)); err == nil {
				t.Errorf("MaxAttempts(%d): Open succeeded", n)
			}
			if left := len(b.fail); left != 2 {
				t.Errorf("MaxAttempts(%d): %d dials; want %d", n, n+2-left, n)
			}
			for len(b.fail) > 0 {
				<-b.fail
			}
		}
	})
}

func TestReconnectGivingUpEndsClientWithTheCause(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b, stomp.MaxAttempts(2))
		b.fail <- errors.New("refused")
		b.fail <- errors.New("refused")
		p.conn.Close()
		<-c.Done()
		if left := len(b.fail); left != 1 {
			t.Errorf("%d refused dials used; want 1, the short session being the first failed attempt (RCN-17)", 2-left)
		}
		if err := c.Err(); errors.Is(err, stomp.ErrClosed) || err == nil || !strings.Contains(err.Error(), "2 connection attempts failed") {
			t.Errorf("Err = %v; want the connection failure", err)
		}
		if err := c.Close(t.Context()); err != nil {
			t.Errorf("Close after the Client ended on its own = %v; want nil", err)
		}
		if err := c.Close(t.Context()); err != nil {
			t.Errorf("second Close = %v; want nil", err)
		}
		if err := c.Err(); errors.Is(err, stomp.ErrClosed) {
			t.Errorf("Err after Close = %v; want the connection failure kept", err)
		}
	})
}

func TestStateIsNotConnectedOnceTheSessionEnded(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b, stomp.Backoff(time.Hour, time.Hour))
		done := make(chan stomp.State, 1)
		go func() {
			err := c.Send(t.Context(), stomp.Outgoing{Destination: "/q"})
			if !errors.Is(err, stomp.ErrOutcomeUnknown) {
				t.Errorf("Send = %v; want ErrOutcomeUnknown", err)
			}
			done <- c.State()
		}()
		p.expect(stomptext.Send)
		p.conn.Close()
		if st := <-done; st == stomp.Connected {
			t.Errorf("State when Send learned the session ended = %v; want not connected (RCN-15)", st)
		}
		closeClient(t, c, nil)
	})
}

func TestHandshakeTimesOutWhenBrokerNeverAnswers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dial := func(context.Context, int) (net.Conn, error) {
			client, server := net.Pipe()
			go io.Copy(io.Discard, server)
			return client, nil
		}
		start := time.Now()
		_, err := stomp.Open(t.Context(), dial, stomp.HandshakeTimeout(3*time.Second), stomp.MaxAttempts(1))
		if err == nil || time.Since(start) != 3*time.Second {
			t.Errorf("Open = %v after %v; want failure after 3s", err, time.Since(start))
		}
	})
}

func TestVersionIsZeroBetweenSessions(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b, stomp.Backoff(time.Hour, time.Hour))
		if c.Version().String() != b.ver {
			t.Errorf("Version = %v; want %s", c.Version(), b.ver)
		}
		p.conn.Close()
		synctest.Wait()
		if v := c.Version(); v != 0 {
			t.Errorf("Version between sessions = %v; want 0", v)
		}
		closeClient(t, c, nil)
	})
}

func TestReceiveTimeoutCountsFromLastOctet(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p, _ := openHandshake(t, b, "1000,0", stomp.Heartbeat(0, time.Second), stomp.Backoff(time.Hour, time.Hour))
		time.Sleep(500 * time.Millisecond)
		p.send("\n")
		time.Sleep(2*time.Second - time.Millisecond)
		if c.State() != stomp.Connected {
			t.Errorf("state %v 1.999s after the last octet; want connected", c.State())
		}
		time.Sleep(2 * time.Millisecond)
		if c.State() == stomp.Connected {
			t.Error("still connected 2.001s after the last octet with a 1s receive interval")
		}
		closeClient(t, c, nil)
	})
}

func TestSendHeartbeatIsWrittenWhenTheIntervalElapses(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p, _ := openHandshake(t, b, "300,1000", stomp.Heartbeat(time.Second, 300*time.Millisecond))
		keepAlive := make(chan struct{})
		go func() {
			defer close(keepAlive)
			for range 6 {
				time.Sleep(250 * time.Millisecond)
				p.send("\n")
			}
		}()
		time.Sleep(time.Second - time.Millisecond)
		if n := len(p.eols); n != 0 {
			t.Errorf("%d heart-beats before 1s", n)
		}
		time.Sleep(2 * time.Millisecond)
		if n := len(p.eols); n != 1 {
			t.Errorf("%d heart-beats at 1.001s; want 1", n)
		}
		<-keepAlive
		closeClient(t, c, p)
	})
}

func TestWriteBufferLimitHoldsCallsUntilTheWriterDrains(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := make(chan net.Conn, 1)
		dial := func(context.Context, int) (net.Conn, error) {
			client, s := net.Pipe()
			server <- s
			return client, nil
		}
		opened := make(chan *stomp.Client, 1)
		go func() {
			c, err := stomp.Open(t.Context(), dial, stomp.WriteTimeout(time.Hour))
			if err != nil {
				t.Error(err)
			}
			opened <- c
		}()
		s := <-server
		dec := stomptext.NewDecoder(s)
		if _, err := dec.ReadFrame(); err != nil {
			t.Fatal(err)
		}
		io.WriteString(s, "CONNECTED\nversion:1.2\n\n\x00")
		c := <-opened

		ctx, cancel := context.WithCancel(t.Context())
		big := make([]byte, 3<<19)
		send := func(body []byte) chan error {
			done := make(chan error, 1)
			go func() { done <- c.Send(ctx, stomp.Outgoing{Destination: "/q", Body: body}) }()
			synctest.Wait()
			return done
		}
		blocked := send(big)
		small1, small2 := send([]byte("b")), send([]byte("c"))
		over := send(big)
		cancel()
		for name, ch := range map[string]chan error{"first": blocked, "small b": small1, "small c": small2} {
			if err := <-ch; !errors.Is(err, stomp.ErrOutcomeUnknown) {
				t.Errorf("%s: %v; want buffered, so ErrOutcomeUnknown on cancel", name, err)
			}
		}
		if err := <-over; !errors.Is(err, context.Canceled) || errors.Is(err, stomp.ErrOutcomeUnknown) {
			t.Errorf("frame over the limit: %v; want it held, so context.Canceled", err)
		}
		for _, want := range []int{len(big), 1, 1} {
			f, err := dec.ReadFrame()
			if err != nil || len(f.Body()) != want {
				t.Fatalf("frame %v body %d; want %d", err, len(f.Body()), want)
			}
		}
		go io.Copy(io.Discard, s)
		cctx, ccancel := context.WithTimeout(context.Background(), time.Second)
		defer ccancel()
		c.Close(cctx)
	})
}

func TestQueueSpaceIsFreedByReadMessage(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		s, _ := subscribe(t, c, p, stomp.SubscribeRequest{ID: "s", Destination: "/q", Ack: stomp.AckClientIndividual, EndOnOverflow: true, Capacity: 2, CapacityBytes: 4})
		next := func(want string) {
			t.Helper()
			if m, err := readNew(t.Context(), s); err != nil || string(m.Body) != want {
				t.Errorf("ReadMessage = %q, %v; want %q", m.Body, err, want)
			}
		}
		p.send(b.message("s", "1", "", "ab") + b.message("s", "2", "", "cd"))
		p.noFrame()
		next("ab")
		p.send(b.message("s", "3", "", "ef"))
		p.noFrame()
		next("cd")
		next("ef")
		p.send(b.message("s", "4", "", "gh") + b.message("s", "5", "", "ij"))
		p.noFrame()
		p.send(b.message("s", "6", "", "k"))
		u := p.expect(stomptext.Unsubscribe)
		got, err := bodiesUntilEnd(t.Context(), s)
		if !slices.Equal(got, []string{"gh", "ij"}) || !errors.Is(err, stomp.ErrOverflow) {
			t.Errorf("ReadMessage stored %v then returned %v; want [gh ij] then ErrOverflow", got, err)
		}
		p.receipt(u)
		closeClient(t, c, p)
	})
}

func TestAckWaitAndNackWriteTheirCommands(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		s, _ := subscribe(t, c, p, stomp.SubscribeRequest{ID: "s", Destination: "/q", Ack: stomp.AckClientIndividual})
		p.send(b.message("s", "A1", "", "x") + b.message("s", "A2", "", "y"))
		m1, _ := readNew(t.Context(), s)
		m2, _ := readNew(t.Context(), s)
		done := make(chan error, 1)
		go func() { done <- s.AckWait(t.Context(), m1) }()
		a := p.expect(stomptext.Ack)
		if b.ackID(a, "s") != "A1" || a.get("receipt") == "" {
			t.Errorf("AckWait wrote %v; want ACK A1 with receipt", a.headers)
		}
		p.receipt(a)
		if err := <-done; err != nil {
			t.Error(err)
		}
		if err := s.Nack(t.Context(), m2); err != nil {
			t.Error(err)
		}
		n := p.expect(stomptext.Nack)
		if b.ackID(n, "s") != "A2" || n.get("receipt") != "" {
			t.Errorf("Nack wrote %v; want NACK A2 without receipt", n.headers)
		}
		closeClient(t, c, p)
	})
}

func TestAutoAckOfMessageQueuedBeforeConnectionLossWritesNothing(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b, stomp.Backoff(time.Hour, time.Hour))
		s, _ := subscribe(t, c, p, stomp.SubscribeRequest{ID: "s", Destination: "/q"})
		p.send(b.message("s", "1", "", "kept"))
		synctest.Wait()
		p.conn.Close()
		synctest.Wait()
		if m, err := readNew(t.Context(), s); err != nil || string(m.Body) != "kept" {
			t.Errorf("ReadMessage = %q, %v; want kept", m.Body, err)
		}
		closeClient(t, c, nil)
	})
}

func TestUnsubscribeDuringCloseReturnsErrClosed(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		s, _ := subscribe(t, c, p, stomp.SubscribeRequest{Destination: "/q"})
		done := make(chan error, 1)
		go func() { done <- c.Close(context.Background()) }()
		d := p.expect(stomptext.Disconnect)
		if err := s.Unsubscribe(t.Context()); !errors.Is(err, stomp.ErrClosed) {
			t.Errorf("Unsubscribe during Close = %v; want ErrClosed", err)
		}
		p.noFrame()
		p.receipt(d)
		if err := <-done; err != nil {
			t.Errorf("Close: %v", err)
		}
	})
}

func TestCloseWakesBlockedReadMessage(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		s, _ := subscribe(t, c, p, stomp.SubscribeRequest{Destination: "/q"})
		done := make(chan error, 1)
		go func() {
			_, err := readNew(t.Context(), s)
			done <- err
		}()
		synctest.Wait()
		closeClient(t, c, p)
		if err := <-done; !errors.Is(err, stomp.ErrClosed) {
			t.Errorf("blocked ReadMessage = %v; want ErrClosed", err)
		}
	})
}

func TestSecondCloseLeavesClientReadable(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		closeClient(t, c, p)
		if err := c.Close(t.Context()); err != nil {
			t.Errorf("second Close = %v", err)
		}
		if !errors.Is(c.Err(), stomp.ErrClosed) || c.State() != stomp.Closed {
			t.Errorf("after second Close: Err %v State %v", c.Err(), c.State())
		}
	})
}

func TestSubscribeContextEndingBeforeReceiptUnsubscribes(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() {
			_, err := c.Subscribe(ctx, stomp.SubscribeRequest{ID: "x", Destination: "/q"})
			done <- err
		}()
		sub := p.expect(stomptext.Subscribe)
		cancel()
		if err := <-done; !errors.Is(err, stomp.ErrOutcomeUnknown) {
			t.Errorf("Subscribe = %v; want ErrOutcomeUnknown", err)
		}
		u := p.expect(stomptext.Unsubscribe)
		if u.get("id") != "x" {
			t.Errorf("UNSUBSCRIBE id %q; want x", u.get("id"))
		}
		p.receipt(sub)
		p.send(b.message("x", "1", "", "late"))
		p.receipt(u)
		synctest.Wait()
		s, _ := subscribe(t, c, p, stomp.SubscribeRequest{ID: "x", Destination: "/q", Ack: stomp.AckClientIndividual})
		p.send(b.message("x", "2", "", "fresh"))
		if m, err := readNew(t.Context(), s); err != nil || string(m.Body) != "fresh" {
			t.Errorf("ReadMessage = %q, %v; want fresh", m.Body, err)
		}
		closeClient(t, c, p)
	})
}

func TestSubscribeRejectsInvalidRequests(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		for name, r := range map[string]stomp.SubscribeRequest{
			"unknown ack mode":       {Destination: "/q", Ack: stomp.AckMode(9)},
			"negative capacity":      {Destination: "/q", Capacity: -1},
			"negative capacityBytes": {Destination: "/q", CapacityBytes: -1},
			"reserved header":        {Destination: "/q", Headers: stomp.Headers{{Name: "ack", Value: "auto"}}},
		} {
			if _, err := c.Subscribe(t.Context(), r); !errors.Is(err, stomp.ErrInvalidRequest) && !errors.Is(err, stomp.ErrReservedHeader) {
				t.Errorf("%s: %v; want an invalid-request error", name, err)
			}
		}
		p.noFrame()
		closeClient(t, c, p)
	})
}

func TestBeginWhoseReceiptNeverArrivesIsAborted(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() {
			_, err := c.Begin(ctx)
			done <- err
		}()
		bf := p.expect(stomptext.Begin)
		cancel()
		if err := <-done; !errors.Is(err, stomp.ErrOutcomeUnknown) {
			t.Errorf("Begin = %v; want ErrOutcomeUnknown", err)
		}
		a := p.expect(stomptext.Abort)
		if a.get("transaction") != bf.get("transaction") {
			t.Errorf("ABORT names %q; want %q", a.get("transaction"), bf.get("transaction"))
		}
		p.receipt(bf)
		p.receipt(a)
		closeClient(t, c, p)
	})
}

func TestTxNackWritesNackInTheTransaction(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		s, _ := subscribe(t, c, p, stomp.SubscribeRequest{ID: "s", Destination: "/q", Ack: stomp.AckClientIndividual})
		p.send(b.message("s", "A1", "", "x"))
		m, _ := readNew(t.Context(), s)
		tx, _ := begin(t, c, p)
		if err := tx.Nack(t.Context(), m); err != nil {
			t.Fatal(err)
		}
		n := p.expect(stomptext.Nack)
		if b.ackID(n, "s") != "A1" || n.get("transaction") != tx.ID() {
			t.Errorf("Tx.Nack wrote %v", n.headers)
		}
		closeClient(t, c, p)
	})
}

func TestTxCallsAfterCommitWithUnknownOutcomeAreDone(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b, stomp.Backoff(time.Hour, time.Hour))
		tx, _ := begin(t, c, p)
		done := make(chan error, 1)
		go func() { done <- tx.Commit(t.Context()) }()
		p.expect(stomptext.Commit)
		p.conn.Close()
		if err := <-done; !errors.Is(err, stomp.ErrOutcomeUnknown) {
			t.Fatalf("Commit = %v; want ErrOutcomeUnknown", err)
		}
		if err := tx.Send(t.Context(), stomp.Outgoing{Destination: "/q"}); !errors.Is(err, stomp.ErrTxDone) {
			t.Errorf("Send after Commit returned = %v; want ErrTxDone (TX-7)", err)
		}
		if err := tx.Commit(t.Context()); !errors.Is(err, stomp.ErrTxDone) {
			t.Errorf("second Commit = %v; want ErrTxDone (TX-7)", err)
		}
		closeClient(t, c, nil)
	})
}

func TestOptionValidation(t *testing.T) {
	for name, tc := range map[string]struct {
		f     func()
		panic bool
	}{
		"Heartbeat negative send":   {func() { stomp.Heartbeat(-1, 0) }, true},
		"Heartbeat negative expect": {func() { stomp.Heartbeat(0, -1) }, true},
		"Heartbeat zero":            {func() { stomp.Heartbeat(0, 0) }, false},
		"HandshakeTimeout 0":        {func() { stomp.HandshakeTimeout(0) }, true},
		"HandshakeTimeout 1":        {func() { stomp.HandshakeTimeout(1) }, false},
		"WriteTimeout 0":            {func() { stomp.WriteTimeout(0) }, true},
		"WriteTimeout 1":            {func() { stomp.WriteTimeout(1) }, false},
		"WriteBufferLimit 0":        {func() { stomp.WriteBufferLimit(0) }, true},
		"WriteBufferLimit 1":        {func() { stomp.WriteBufferLimit(1) }, false},
		"Backoff base 0":            {func() { stomp.Backoff(0, 1) }, true},
		"Backoff max 0":             {func() { stomp.Backoff(1, 0) }, true},
		"Backoff 1 1":               {func() { stomp.Backoff(1, 1) }, false},
		"MaxAttempts -1":            {func() { stomp.MaxAttempts(-1) }, true},
		"MaxAttempts 0":             {func() { stomp.MaxAttempts(0) }, false},
		"TCP no address":            {func() { stomp.TCP() }, true},
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if got := recover() != nil; got != tc.panic {
					t.Errorf("panicked %v; want %v", got, tc.panic)
				}
			}()
			tc.f()
		})
	}
}

func TestTCPDialsTheAddressForTheAttempt(t *testing.T) {
	var ls [2]net.Listener
	for i := range ls {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Skipf("no loopback listener: %v", err)
		}
		defer l.Close()
		ls[i] = l
	}
	dial := stomp.TCP(ls[0].Addr().String(), ls[1].Addr().String())
	for _, tt := range []struct{ attempt, want int }{{0, 0}, {1, 1}, {2, 0}, {0, 0}, {3, 1}} {
		conn, err := dial(t.Context(), tt.attempt)
		if err != nil {
			t.Fatalf("attempt %d: %v", tt.attempt, err)
		}
		defer conn.Close()
		if got := conn.RemoteAddr().String(); got != ls[tt.want].Addr().String() {
			t.Errorf("attempt %d reached %s; want %s", tt.attempt, got, ls[tt.want].Addr())
		}
	}
}

func TestCapacityBytesBoundsQueuedBodies(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		s, _ := subscribe(t, c, p, stomp.SubscribeRequest{ID: "s", Destination: "/q", Ack: stomp.AckClientIndividual, EndOnOverflow: true, Capacity: 10, CapacityBytes: 4})
		p.send(b.message("s", "1", "", "ab") + b.message("s", "2", "", "cd"))
		p.noFrame()
		p.send(b.message("s", "3", "", "e"))
		p.receipt(p.expect(stomptext.Unsubscribe))
		got, err := bodiesUntilEnd(t.Context(), s)
		if !slices.Equal(got, []string{"ab", "cd"}) || !errors.Is(err, stomp.ErrOverflow) {
			t.Errorf("queued = %v then %v; want [ab cd] then ErrOverflow", got, err)
		}
		closeClient(t, c, p)
	})
}

func TestSubscribeRejectsContentLengthHeaderAndKeepsTheID(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		_, err := c.Subscribe(t.Context(), stomp.SubscribeRequest{ID: "x", Destination: "/q", Headers: stomp.Headers{{Name: "content-length", Value: "5"}}})
		if !errors.Is(err, stomp.ErrReservedHeader) {
			t.Errorf("Subscribe with content-length = %v; want ErrReservedHeader", err)
		}
		subscribe(t, c, p, stomp.SubscribeRequest{ID: "x", Destination: "/q"})
		closeClient(t, c, p)
	})
}

func TestGeneratedSubscriptionIDSkipsIDsInUse(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		subscribe(t, c, p, stomp.SubscribeRequest{ID: "sub-1", Destination: "/a"})
		s, f := subscribe(t, c, p, stomp.SubscribeRequest{Destination: "/b"})
		if s.ID() == "sub-1" || f.get("id") != s.ID() {
			t.Errorf("generated id %q, SUBSCRIBE id %q; want an id other than sub-1", s.ID(), f.get("id"))
		}
		closeClient(t, c, p)
	})
}

func TestCloseWithoutDisconnectReceiptIsOutcomeUnknown(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		start := time.Now()
		if err := c.Close(ctx); !errors.Is(err, stomp.ErrOutcomeUnknown) || !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != 2*time.Second {
			t.Errorf("Close against a silent broker = %v after %v; want ErrOutcomeUnknown at the 2s deadline", err, time.Since(start))
		}
		p.expect(stomptext.Disconnect)

		c, p = open(t, b)
		done := make(chan error, 1)
		go func() { done <- c.Close(t.Context()) }()
		p.expect(stomptext.Disconnect)
		p.conn.Close()
		if err := <-done; !errors.Is(err, stomp.ErrOutcomeUnknown) {
			t.Errorf("Close when the connection drops before the receipt = %v; want ErrOutcomeUnknown", err)
		}
	})
}

func TestCloseAnsweredWithErrorReturnsServerError(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		done := make(chan error, 1)
		go func() { done <- c.Close(t.Context()) }()
		d := p.expect(stomptext.Disconnect)
		p.errorFor(d, "bye")
		var serr *stomp.ServerError
		if err := <-done; !errors.As(err, &serr) || serr.Message != "bye" {
			t.Errorf("Close = %v; want the broker's ServerError (CON-29)", err)
		}
		if !errors.Is(c.Err(), stomp.ErrClosed) {
			t.Errorf("Err = %v; want ErrClosed", c.Err())
		}
	})
}

func TestBrokerErrorForAckReceiptKeepsTheSubscription(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b, stomp.Backoff(time.Second, time.Second))
		s, _ := subscribe(t, c, p, stomp.SubscribeRequest{ID: "s", Destination: "/q", Ack: stomp.AckClientIndividual})
		p.send(b.message("s", "A1", "", "x"))
		m, _ := readNew(t.Context(), s)
		done := make(chan error, 1)
		go func() { done <- s.AckWait(t.Context(), m) }()
		a := p.expect(stomptext.Ack)
		p.errorFor(a, "ack refused")
		var serr *stomp.ServerError
		if err := <-done; !errors.As(err, &serr) {
			t.Errorf("AckWait = %v; want the broker's ServerError", err)
		}
		p2 := b.accept()
		p2.handshake("0,0")
		if f := p2.expect(stomptext.Subscribe); f.get("id") != "s" {
			t.Errorf("replayed SUBSCRIBE id %q; want s", f.get("id"))
		}
		closeClient(t, c, p2)
	})
}

func TestPrefetchHeaderCarriesCapacityAndSurvivesReconnect(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b, stomp.PrefetchHeader("prefetch-count"), stomp.Backoff(time.Second, time.Second))
		_, def := subscribe(t, c, p, stomp.SubscribeRequest{ID: "d", Destination: "/q"})
		_, ten := subscribe(t, c, p, stomp.SubscribeRequest{ID: "t", Destination: "/q", Capacity: 10})
		_, own := subscribe(t, c, p, stomp.SubscribeRequest{ID: "o", Destination: "/q", Capacity: 10,
			Headers: stomp.Headers{{Name: "prefetch-count", Value: "3"}}})
		for _, tt := range []struct {
			f    frame
			want string
		}{{def, "256"}, {ten, "10"}, {own, "3"}} {
			var got []string
			for _, h := range tt.f.headers {
				if h.Name == "prefetch-count" {
					got = append(got, h.Value)
				}
			}
			if !slices.Equal(got, []string{tt.want}) {
				t.Errorf("SUBSCRIBE %s prefetch-count = %q; want [%s]", tt.f.get("id"), got, tt.want)
			}
		}
		p.conn.Close()
		p2 := b.accept()
		p2.handshake("0,0")
		replayed := map[string]string{}
		for range 3 {
			f := p2.expect(stomptext.Subscribe)
			replayed[f.get("id")] = f.get("prefetch-count")
		}
		if want := map[string]string{"d": "256", "t": "10", "o": "3"}; !maps.Equal(replayed, want) {
			t.Errorf("replayed prefetch-count = %v; want %v (RCN-2)", replayed, want)
		}
		closeClient(t, c, p2)
	})
}

func TestPrefetchHeaderValidation(t *testing.T) {
	for _, name := range []string{"id", "ack", "destination", "receipt", "content-length"} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("PrefetchHeader(%q) did not panic", name)
				}
			}()
			stomp.PrefetchHeader(name)
		}()
	}
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b, stomp.PrefetchHeader("prefetch-count"), stomp.PrefetchHeader(""))
		_, f := subscribe(t, c, p, stomp.SubscribeRequest{Destination: "/q"})
		if v, ok := f.headers.Get("prefetch-count"); ok {
			t.Errorf("SUBSCRIBE after PrefetchHeader(\"\") carries prefetch-count:%s; want none", v)
		}
		closeClient(t, c, p)
	})
}

func TestFullQueueKeepsQueueingByDefault(t *testing.T) {
	for name, r := range map[string]stomp.SubscribeRequest{
		"Capacity":      {ID: "s", Destination: "/q", Ack: stomp.AckClientIndividual, Capacity: 2},
		"CapacityBytes": {ID: "s", Destination: "/q", Ack: stomp.AckClientIndividual, CapacityBytes: 2},
	} {
		t.Run(name, func(t *testing.T) {
			eachVersion(t, func(t *testing.T, b *broker) {
				c, p := open(t, b)
				s, _ := subscribe(t, c, p, r)
				p.send(b.message("s", "1", "", "ab") + b.message("s", "2", "", "cd") + b.message("s", "3", "", "ef"))
				p.noFrame()
				for _, want := range []string{"ab", "cd", "ef"} {
					m, err := readNew(t.Context(), s)
					if err != nil || string(m.Body) != want {
						t.Fatalf("ReadMessage = %q, %v; want %q (SUB-8)", m.Body, err, want)
					}
				}
				closeClient(t, c, p)
			})
		})
	}
}

func TestKeptMessagesStayQueuedAfterReconnect(t *testing.T) {
	for _, end := range []bool{false, true} {
		t.Run(fmt.Sprintf("EndOnOverflow=%v", end), func(t *testing.T) {
			eachVersion(t, func(t *testing.T, b *broker) {
				c, p := open(t, b, stomp.PrefetchHeader("prefetch-count"), stomp.Backoff(time.Second, time.Second))
				s, _ := subscribe(t, c, p, stomp.SubscribeRequest{ID: "s", Destination: "/q", Ack: stomp.AckClientIndividual, Capacity: 2, EndOnOverflow: end})
				p.send(b.message("s", "1", "", "a") + b.message("s", "2", "", "b"))
				p.noFrame()
				p.conn.Close()
				p2 := b.accept()
				p2.handshake("0,0")
				p2.receipt(p2.expect(stomptext.Subscribe))
				p2.send(b.message("s", "1", "", "a"))
				want := []string{"a", "b", "a"}
				if end {
					p2.receipt(p2.expect(stomptext.Unsubscribe))
					want = want[:2]
				} else {
					p2.noFrame()
				}
				var got []string
				for range want {
					m, err := readNew(t.Context(), s)
					if err != nil {
						t.Fatalf("ReadMessage after %v = %v; want %v (SUB-35)", got, err, want)
					}
					got = append(got, string(m.Body))
				}
				if !slices.Equal(got, want) {
					t.Errorf("ReadMessage = %v; want %v (SUB-35)", got, want)
				}
				if end {
					if _, err := readNew(t.Context(), s); !errors.Is(err, stomp.ErrOverflow) {
						t.Errorf("ReadMessage after the kept messages = %v; want ErrOverflow (SUB-45)", err)
					}
				}
				closeClient(t, c, p2)
			})
		})
	}
}

func TestMessageAfterUnsubscribeReceiptKeepsTheSession(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		s, _ := subscribe(t, c, p, stomp.SubscribeRequest{ID: "x", Destination: "/q", Ack: stomp.AckClientIndividual})
		other, _ := subscribe(t, c, p, stomp.SubscribeRequest{ID: "y", Destination: "/q", Ack: stomp.AckClientIndividual})
		done := make(chan error, 1)
		go func() { done <- s.Unsubscribe(t.Context()) }()
		p.receipt(p.expect(stomptext.Unsubscribe))
		if err := <-done; err != nil {
			t.Fatalf("Unsubscribe = %v", err)
		}
		p.send(b.message("x", "1", "", "late") + b.message("y", "2", "", "kept"))
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		m, err := readNew(ctx, other)
		if err != nil || string(m.Body) != "kept" {
			t.Fatalf("ReadMessage on y = %q, %v; want kept, with the session still running (SUB-2)", m.Body, err)
		}
		if st := c.State(); st != stomp.Connected {
			t.Errorf("State = %v; want Connected", st)
		}
		closeClient(t, c, p)
	})
}

type subscribeResult struct {
	s   *stomp.Subscription
	err error
}

func subscribeAsync(ctx context.Context, c *stomp.Client, r stomp.SubscribeRequest) chan subscribeResult {
	ch := make(chan subscribeResult, 1)
	go func() {
		s, err := c.Subscribe(ctx, r)
		ch <- subscribeResult{s, err}
	}()
	return ch
}

func TestMessageBeforeSubscribeReceiptConfirmsTheSubscription(t *testing.T) {
	r := stomp.SubscribeRequest{ID: "s", Destination: "/q", Ack: stomp.AckClientIndividual}
	t.Run("session ends", func(t *testing.T) {
		eachVersion(t, func(t *testing.T, b *broker) {
			c, p := open(t, b, stomp.Backoff(time.Second, time.Second))
			res := subscribeAsync(t.Context(), c, r)
			p.expect(stomptext.Subscribe)
			p.send(b.message("s", "1", "", "early"))
			synctest.Wait()
			p.conn.Close()
			got := <-res
			if got.err != nil {
				t.Fatalf("Subscribe = %v; want the subscription the MESSAGE confirmed (SUB-48)", got.err)
			}
			p2 := b.accept()
			p2.handshake("0,0")
			p2.receipt(p2.expect(stomptext.Subscribe))
			m, err := readNew(t.Context(), got.s)
			if err != nil || string(m.Body) != "early" {
				t.Errorf("ReadMessage = %q, %v; want early (SUB-9)", m.Body, err)
			}
			closeClient(t, c, p2)
		})
	})
	t.Run("ctx ends", func(t *testing.T) {
		eachVersion(t, func(t *testing.T, b *broker) {
			c, p := open(t, b)
			ctx, cancel := context.WithCancel(t.Context())
			res := subscribeAsync(ctx, c, r)
			p.expect(stomptext.Subscribe)
			p.send(b.message("s", "1", "", "early"))
			synctest.Wait()
			cancel()
			got := <-res
			if got.err != nil {
				t.Fatalf("Subscribe = %v; want the subscription the MESSAGE confirmed (SUB-47)", got.err)
			}
			p.noFrame()
			m, err := readNew(t.Context(), got.s)
			if err != nil || string(m.Body) != "early" {
				t.Errorf("ReadMessage = %q, %v; want early (SUB-9)", m.Body, err)
			}
			closeClient(t, c, p)
		})
	})
	t.Run("no message", func(t *testing.T) {
		eachVersion(t, func(t *testing.T, b *broker) {
			c, p := open(t, b, stomp.Backoff(time.Second, time.Second))
			res := subscribeAsync(t.Context(), c, r)
			p.expect(stomptext.Subscribe)
			p.conn.Close()
			if got := <-res; !errors.Is(got.err, stomp.ErrOutcomeUnknown) {
				t.Fatalf("Subscribe = %v; want ErrOutcomeUnknown", got.err)
			}
			p2 := b.accept()
			p2.handshake("0,0")
			p2.noFrame()
			closeClient(t, c, p2)
		})
	})
	t.Run("message then ERROR", func(t *testing.T) {
		eachVersion(t, func(t *testing.T, b *broker) {
			c, p := open(t, b, stomp.Backoff(time.Second, time.Second))
			res := subscribeAsync(t.Context(), c, r)
			f := p.expect(stomptext.Subscribe)
			p.send(b.message("s", "1", "", "early"))
			p.errorFor(f, "forbidden")
			var serr *stomp.ServerError
			if got := <-res; !errors.As(got.err, &serr) {
				t.Fatalf("Subscribe = %v; want the broker's *ServerError", got.err)
			}
			p2 := b.accept()
			p2.handshake("0,0")
			p2.noFrame()
			closeClient(t, c, p2)
		})
	})
}

func TestCloseAfterAMessageConfirmedAPendingSubscribe(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		res := subscribeAsync(t.Context(), c, stomp.SubscribeRequest{ID: "s", Destination: "/q", Ack: stomp.AckClientIndividual})
		p.expect(stomptext.Subscribe)
		p.send(b.message("s", "1", "", "early"))
		synctest.Wait()
		closed := make(chan error, 1)
		go func() { closed <- c.Close(t.Context()) }()
		p.receipt(p.expect(stomptext.Disconnect))
		if err := <-closed; err != nil {
			t.Fatalf("Close = %v", err)
		}
		got := <-res
		if got.err != nil {
			t.Fatalf("Subscribe = %v; want the subscription the MESSAGE confirmed (SUB-48)", got.err)
		}
		m, err := readNew(t.Context(), got.s)
		if err != nil || string(m.Body) != "early" {
			t.Fatalf("ReadMessage = %q, %v; want early (SUB-9)", m.Body, err)
		}
		if _, err := readNew(t.Context(), got.s); !errors.Is(err, stomp.ErrClosed) {
			t.Errorf("ReadMessage after the queued message = %v; want ErrClosed (CON-19)", err)
		}
	})
}

func TestSubscribeRejectedWhileClosingReturnsTheServerError(t *testing.T) {
	eachVersion(t, func(t *testing.T, b *broker) {
		c, p := open(t, b)
		res := subscribeAsync(t.Context(), c, stomp.SubscribeRequest{ID: "s", Destination: "/q", Ack: stomp.AckClientIndividual})
		f := p.expect(stomptext.Subscribe)
		p.send(b.message("s", "1", "", "early"))
		synctest.Wait()
		closed := make(chan error, 1)
		go func() { closed <- c.Close(t.Context()) }()
		p.expect(stomptext.Disconnect)
		p.errorFor(f, "forbidden")
		var serr *stomp.ServerError
		if got := <-res; !errors.As(got.err, &serr) {
			t.Fatalf("Subscribe = %v, %v; want the broker's *ServerError (SUB-51)", got.s, got.err)
		}
		<-closed
	})
}
