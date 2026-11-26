//go:build integration

package stomp_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jacoelho/stomp"
)

func brokerAddr() string {
	if a := os.Getenv("STOMP_TEST_SERVER"); a != "" {
		return a
	}
	return "localhost:61613"
}

func openBroker(t *testing.T, o ...stomp.Options) *stomp.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	c, err := stomp.Open(ctx, stomp.TCP(brokerAddr()), append([]stomp.Options{stomp.Login("admin", "admin")}, o...)...)
	if err != nil {
		t.Fatalf("Open %s: %v", brokerAddr(), err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c.Close(ctx)
	})
	return c
}

func TestArtemisBinaryAndTextRoundTrip(t *testing.T) {
	c := openBroker(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	queue := fmt.Sprintf("/queue/it-%d", time.Now().UnixNano())
	sub, err := c.Subscribe(ctx, stomp.SubscribeRequest{Destination: queue, Ack: stomp.AckClientIndividual})
	if err != nil {
		t.Fatal(err)
	}
	binary := []byte{0, 1, 2, 0, 255}
	if err := c.Send(ctx, stomp.Outgoing{Destination: queue, Body: binary}); err != nil {
		t.Fatal(err)
	}
	if err := c.Send(ctx, stomp.Outgoing{Destination: queue, Body: []byte("héllo"), Text: true, ContentType: "text/plain"}); err != nil {
		t.Fatal(err)
	}
	m1, err := readNew(ctx, sub)
	if err != nil || !bytes.Equal(m1.Body, binary) {
		t.Fatalf("binary: %v, %v", m1.Body, err)
	}
	m2, err := readNew(ctx, sub)
	if err != nil || string(m2.Body) != "héllo" {
		t.Fatalf("text: %q, %v", m2.Body, err)
	}
	if err := sub.Ack(ctx, m1); err != nil {
		t.Fatal(err)
	}
	if err := sub.AckWait(ctx, m2); err != nil {
		t.Fatal(err)
	}
	if err := sub.Unsubscribe(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestArtemisAcceptsNackAndKeepsSession(t *testing.T) {
	c := openBroker(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	queue := fmt.Sprintf("/queue/it-nack-%d", time.Now().UnixNano())
	sub, err := c.Subscribe(ctx, stomp.SubscribeRequest{Destination: queue, Ack: stomp.AckClientIndividual})
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{"rejected", "accepted"} {
		if err := c.Send(ctx, stomp.Outgoing{Destination: queue, Body: []byte(body)}); err != nil {
			t.Fatal(err)
		}
	}
	m, err := readNew(ctx, sub)
	if err != nil || string(m.Body) != "rejected" {
		t.Fatalf("first: %q, %v", m.Body, err)
	}
	if err := sub.NackWait(ctx, m); err != nil {
		t.Fatalf("NackWait: %v", err)
	}
	m, err = readNew(ctx, sub)
	if err != nil || string(m.Body) != "accepted" {
		t.Fatalf("second: %q, %v", m.Body, err)
	}
	if err := sub.AckWait(ctx, m); err != nil {
		t.Fatalf("AckWait: %v", err)
	}
}

func TestArtemisRejectsBadCredentials(t *testing.T) {
	if os.Getenv("STOMP_TEST_SECURED") == "" {
		t.Skip("broker allows anonymous login; set STOMP_TEST_SECURED to run")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err := stomp.Open(ctx, stomp.TCP(brokerAddr()), stomp.Login("nobody", "wrong"))
	var serr *stomp.ServerError
	if !errors.As(err, &serr) {
		t.Fatalf("Open = %v; want *ServerError", err)
	}
}

// TestArtemisReconnectAfterRestart restarts the broker with the command in
// STOMP_TEST_RESTART, for example "docker restart stomp-test-artemis".
func TestArtemisReconnectAfterRestart(t *testing.T) {
	restart := os.Getenv("STOMP_TEST_RESTART")
	if restart == "" {
		t.Skip("set STOMP_TEST_RESTART to a command that restarts the broker")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	c, err := stomp.Open(ctx, stomp.TCP(brokerAddr()), stomp.Login("admin", "admin"), stomp.Backoff(200*time.Millisecond, 2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	queue := fmt.Sprintf("/queue/it-reconnect-%d", time.Now().UnixNano())
	sub, err := c.Subscribe(ctx, stomp.SubscribeRequest{Destination: queue, Ack: stomp.AckClientIndividual})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.CommandContext(ctx, "sh", "-c", restart).CombinedOutput(); err != nil {
		t.Fatalf("restart: %v: %s", err, out)
	}
	var sendErr error
	for {
		sendErr = c.Send(ctx, stomp.Outgoing{Destination: queue, Body: []byte("after restart")})
		if sendErr == nil || !errors.Is(sendErr, stomp.ErrOutcomeUnknown) {
			break
		}
	}
	if sendErr != nil {
		t.Fatalf("Send after restart: %v", sendErr)
	}
	m, err := readNew(ctx, sub)
	if err != nil || string(m.Body) != "after restart" {
		t.Fatalf("ReadMessage after restart: %q, %v", m.Body, err)
	}
	if err := sub.AckWait(ctx, m); err != nil {
		t.Fatal(err)
	}
}

func eachArtemisVersion(t *testing.T, f func(t *testing.T, c *stomp.Client)) {
	for _, v := range []stomp.Version{stomp.V11, stomp.V12} {
		t.Run(v.String(), func(t *testing.T) {
			c := openBroker(t, stomp.Versions(v))
			if got := c.Version(); got != v {
				t.Fatalf("negotiated %v; want %v", got, v)
			}
			f(t, c)
		})
	}
}

func TestArtemisAckAndNackPerVersion(t *testing.T) {
	eachArtemisVersion(t, func(t *testing.T, c *stomp.Client) {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		queue := fmt.Sprintf("/queue/it-ver-%d", time.Now().UnixNano())
		sub, err := c.Subscribe(ctx, stomp.SubscribeRequest{Destination: queue, Ack: stomp.AckClientIndividual})
		if err != nil {
			t.Fatal(err)
		}
		for _, body := range []string{"nacked", "acked"} {
			if err := c.Send(ctx, stomp.Outgoing{Destination: queue, Body: []byte(body), Headers: stomp.Headers{{Name: "k", Value: "a:b\\c"}}}); err != nil {
				t.Fatal(err)
			}
		}
		m, err := readNew(ctx, sub)
		if err != nil || string(m.Body) != "nacked" {
			t.Fatalf("first: %q, %v", m.Body, err)
		}
		if v, _ := m.Headers.Get("k"); v != "a:b\\c" {
			t.Errorf("escaped header round trip = %q", v)
		}
		if err := sub.NackWait(ctx, m); err != nil {
			t.Fatalf("NackWait: %v", err)
		}
		m, err = readNew(ctx, sub)
		if err != nil || string(m.Body) != "acked" {
			t.Fatalf("second: %q, %v", m.Body, err)
		}
		if err := sub.AckWait(ctx, m); err != nil {
			t.Fatalf("AckWait: %v", err)
		}
	})
}

func TestArtemisTransactions(t *testing.T) {
	eachArtemisVersion(t, func(t *testing.T, c *stomp.Client) {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		queue := fmt.Sprintf("/queue/it-tx-%d", time.Now().UnixNano())
		sub, err := c.Subscribe(ctx, stomp.SubscribeRequest{Destination: queue, Ack: stomp.AckClientIndividual})
		if err != nil {
			t.Fatal(err)
		}
		aborted, err := c.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := aborted.Send(ctx, stomp.Outgoing{Destination: queue, Body: []byte("aborted")}); err != nil {
			t.Fatal(err)
		}
		if err := aborted.Abort(ctx); err != nil {
			t.Fatalf("Abort: %v", err)
		}
		committed, err := c.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := committed.Send(ctx, stomp.Outgoing{Destination: queue, Body: []byte("committed")}); err != nil {
			t.Fatal(err)
		}
		short, cancelShort := context.WithTimeout(ctx, 500*time.Millisecond)
		if m, err := readNew(short, sub); err == nil {
			t.Errorf("received %q before COMMIT", m.Body)
		}
		cancelShort()
		if err := committed.Commit(ctx); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		m, err := readNew(ctx, sub)
		if err != nil || string(m.Body) != "committed" {
			t.Fatalf("after commit: %q, %v; want committed", m.Body, err)
		}
		if err := sub.AckWait(ctx, m); err != nil {
			t.Fatal(err)
		}
	})
}

func TestArtemisAutoAckBacklogLosesNothing(t *testing.T) {
	eachArtemisVersion(t, func(t *testing.T, c *stomp.Client) {
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()
		queue := fmt.Sprintf("/queue/it-backlog-%d", time.Now().UnixNano())
		anycast := stomp.Headers{{Name: "destination-type", Value: "ANYCAST"}}
		create, err := c.Subscribe(ctx, stomp.SubscribeRequest{Destination: queue, Headers: anycast})
		if err != nil {
			t.Fatal(err)
		}
		if err := create.Unsubscribe(ctx); err != nil {
			t.Fatal(err)
		}
		const backlog = 2000
		for i := range backlog {
			if err := c.Send(ctx, stomp.Outgoing{Destination: queue, Body: []byte(fmt.Sprint(i)), Headers: anycast}); err != nil {
				t.Fatal(err)
			}
		}
		sub, err := c.Subscribe(ctx, stomp.SubscribeRequest{Destination: queue, Headers: anycast})
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		for i := range backlog {
			m, err := readNew(ctx, sub)
			if err != nil {
				t.Fatalf("message %d of %d: %v", i, backlog, err)
			}
			if string(m.Body) != fmt.Sprint(i) {
				t.Fatalf("message %d = %q", i, m.Body)
			}
		}
	})
}

func TestArtemisDefaultWindowPacesAnUnacknowledgedBacklog(t *testing.T) {
	c := openBroker(t)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	queue := fmt.Sprintf("/queue/it-window-%d", time.Now().UnixNano())
	sub, err := c.Subscribe(ctx, stomp.SubscribeRequest{Destination: queue, Ack: stomp.AckClientIndividual})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 2000 {
		if err := c.Send(ctx, stomp.Outgoing{Destination: queue, Body: fmt.Appendf(nil, "%010d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	received := 0
	for {
		nctx, ncancel := context.WithTimeout(ctx, 2*time.Second)
		_, err := readNew(nctx, sub)
		ncancel()
		if errors.Is(err, context.DeadlineExceeded) {
			break
		}
		if err != nil {
			t.Fatalf("after %d unacknowledged messages: %v; want the broker to stop sending (SUB-8)", received, err)
		}
		received++
	}
	if received == 0 || received >= 256 {
		t.Errorf("Artemis delivered %d of 2000 unacknowledged messages; want between 1 and 255: the default window keeps the queue small (SUB-8, SUB-10)", received)
	}
	t.Logf("Artemis delivered %d unacknowledged messages", received)
}

func TestArtemisTopicKeepsEveryMessageBeyondCapacity(t *testing.T) {
	c := openBroker(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	topic := fmt.Sprintf("/topic/it-full-%d", time.Now().UnixNano())
	sub, err := c.Subscribe(ctx, stomp.SubscribeRequest{Destination: topic, Ack: stomp.AckClientIndividual, Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 10 {
		if err := c.Send(ctx, stomp.Outgoing{Destination: topic, Body: fmt.Appendf(nil, "%d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 10 {
		m, err := readNew(ctx, sub)
		if err != nil {
			t.Fatalf("ReadMessage after %d of 10: %v; want every message kept (SUB-8, SUB-9)", i, err)
		}
		if want := fmt.Sprint(i); string(m.Body) != want {
			t.Fatalf("message %d = %q; want %q", i, m.Body, want)
		}
		if err := sub.Ack(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
}

func TestArtemisLateMessageAfterOverflowUnsubscribeKeepsTheSession(t *testing.T) {
	var ended atomic.Int64
	c := openBroker(t, stomp.OnState(func(s stomp.State, err error) {
		if s == stomp.Waiting {
			ended.Add(1)
			t.Logf("session ended: %v", err)
		}
	}))
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	anycastSub := stomp.Headers{{Name: "subscription-type", Value: "ANYCAST"}}
	anycastSend := stomp.Headers{{Name: "destination-type", Value: "ANYCAST"}}
	for cycle := range 60 {
		queue := fmt.Sprintf("/queue/it-late-%d-%d", time.Now().UnixNano(), cycle)
		sub, err := c.Subscribe(ctx, stomp.SubscribeRequest{Destination: queue, Ack: stomp.AckClientIndividual,
			Capacity: 2, EndOnOverflow: true, Headers: anycastSub})
		if err != nil {
			t.Fatalf("cycle %d: Subscribe: %v", cycle, err)
		}
		for i := range 10 {
			if err := c.Send(ctx, stomp.Outgoing{Destination: queue, Body: fmt.Appendf(nil, "%d", i), Headers: anycastSend}); err != nil {
				t.Fatalf("cycle %d: Send %d: %v", cycle, i, err)
			}
		}
		for {
			nctx, ncancel := context.WithTimeout(ctx, 5*time.Second)
			_, err = readNew(nctx, sub)
			ncancel()
			if errors.Is(err, stomp.ErrOverflow) {
				break
			}
			if err != nil {
				t.Fatalf("cycle %d: ReadMessage: %v; want messages then ErrOverflow", cycle, err)
			}
		}
	}
	if n := ended.Load(); n != 0 {
		t.Errorf("%d sessions ended over 60 overflow UNSUBSCRIBE cycles; want 0 (SUB-2)", n)
	}
}
