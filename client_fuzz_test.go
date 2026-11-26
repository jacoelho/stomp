package stomp_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jacoelho/stomp"
	"github.com/jacoelho/stomp/stomptext"
)

// chaosBroker is a broker that follows the protocol but chooses at random
// whether to answer a receipt, ignore it, answer with ERROR, or drop the
// connection. It records what clients wrote so the test can check the
// client's guarantees afterwards.
type chaosBroker struct {
	t   *testing.T
	ver string

	mu        sync.Mutex
	rng       *rand.Rand
	conns     []*chaosConn
	sends     map[string]int  // SEND body to the number of SEND frames carrying it
	confirmed map[string]bool // SEND bodies whose RECEIPT the broker wrote
	acked     map[string]bool // message ids named by ACK or NACK
	nextMsg   int
}

type chaosConn struct {
	idx  int
	conn net.Conn
	wmu  sync.Mutex

	subs         map[string]bool   // guarded by chaosBroker.mu
	sent         map[string]string // message id to subscription id, guarded by chaosBroker.mu
	disconnected bool              // DISCONNECT was read, guarded by chaosBroker.mu
}

func newChaosBroker(t *testing.T, ver string, seed uint64) *chaosBroker {
	return &chaosBroker{
		t: t, ver: ver, rng: rand.New(rand.NewPCG(seed, 7)),
		sends: map[string]int{}, confirmed: map[string]bool{}, acked: map[string]bool{},
	}
}

func (b *chaosBroker) dial(ctx context.Context, _ int) (net.Conn, error) {
	client, server := net.Pipe()
	b.mu.Lock()
	cc := &chaosConn{idx: len(b.conns), conn: server, subs: map[string]bool{}, sent: map[string]string{}}
	b.conns = append(b.conns, cc)
	b.mu.Unlock()
	go b.serve(cc)
	return client, nil
}

func (b *chaosBroker) write(cc *chaosConn, raw string) {
	cc.wmu.Lock()
	defer cc.wmu.Unlock()
	io.WriteString(cc.conn, raw)
}

func (b *chaosBroker) serve(cc *chaosConn) {
	defer cc.conn.Close()
	dec := stomptext.NewDecoder(cc.conn)
	if f, err := dec.ReadFrame(); err != nil || f.Command() != stomptext.Connect {
		return
	}
	b.write(cc, "CONNECTED\nversion:"+b.ver+"\nheart-beat:0,0\n\n\x00")
	v, _ := stomptext.ParseVersion(b.ver)
	dec.SetVersion(v)
	for {
		f, err := dec.ReadFrame()
		if err != nil {
			var serr *stomptext.SyntacticError
			if errors.As(err, &serr) {
				b.t.Errorf("connection %d: client wrote an invalid frame: %v", cc.idx, err)
			}
			return
		}
		if !b.handle(cc, f) {
			return
		}
	}
}

// handle records f and answers it, reporting false when the broker closes the connection.
func (b *chaosBroker) handle(cc *chaosConn, f stomptext.Frame) bool {
	header := func(name string) string { v, _ := f.Header(name); return string(v) }
	b.mu.Lock()
	if cc.disconnected {
		b.t.Errorf("connection %d: %s after DISCONNECT", cc.idx, f.Command())
	}
	switch f.Command() {
	case stomptext.Send:
		if header("transaction") == "" {
			b.sends[string(f.Body())]++
		}
	case stomptext.Subscribe:
		cc.subs[header("id")] = true
	case stomptext.Unsubscribe:
		delete(cc.subs, header("id"))
	case stomptext.Ack, stomptext.Nack:
		id := header("id")
		if b.ver == "1.1" {
			id = header("message-id")
		}
		sub, ok := cc.sent[id]
		if !ok {
			b.t.Errorf("connection %d: %s for %q, not delivered on this connection", cc.idx, f.Command(), id)
		}
		if b.ver == "1.1" && header("subscription") != sub {
			b.t.Errorf("connection %d: 1.1 %s subscription %q; message went to %q", cc.idx, f.Command(), header("subscription"), sub)
		}
		b.acked[id] = true
	case stomptext.Disconnect:
		cc.disconnected = true
	}
	receipt := header("receipt")
	if receipt == "" {
		b.mu.Unlock()
		return true
	}
	roll := b.rng.IntN(100)
	if f.Command() == stomptext.Disconnect {
		roll = 0
	}
	if roll < 85 && f.Command() == stomptext.Send {
		b.confirmed[string(f.Body())] = true
	}
	b.mu.Unlock()
	switch {
	case roll < 85:
		b.write(cc, "RECEIPT\nreceipt-id:"+receipt+"\n\n\x00")
	case roll < 90:
		b.write(cc, "ERROR\nreceipt-id:"+receipt+"\nmessage:refused\n\n\x00")
		return false
	case roll < 95:
		return false
	}
	return true
}

// inject delivers n messages to subscriptions of the newest connection.
func (b *chaosBroker) inject(n int) {
	b.mu.Lock()
	if len(b.conns) == 0 {
		b.mu.Unlock()
		return
	}
	cc := b.conns[len(b.conns)-1]
	subs := slices.Sorted(func(yield func(string) bool) {
		for id := range cc.subs {
			if !yield(id) {
				return
			}
		}
	})
	var raw strings.Builder
	for range n {
		if len(subs) == 0 || cc.disconnected {
			break
		}
		sub := subs[b.rng.IntN(len(subs))]
		b.nextMsg++
		id := "m" + strconv.Itoa(b.nextMsg)
		cc.sent[id] = sub
		raw.WriteString("MESSAGE\nsubscription:" + sub + "\nmessage-id:" + id + "\ndestination:/q\n")
		if b.ver == "1.2" {
			raw.WriteString("ack:" + id + "\n")
		}
		raw.WriteString("\n" + id + "\x00")
	}
	b.mu.Unlock()
	if raw.Len() > 0 {
		b.write(cc, raw.String())
	}
}

func (b *chaosBroker) drop() {
	b.mu.Lock()
	var cc *chaosConn
	if len(b.conns) > 0 {
		cc = b.conns[len(b.conns)-1]
	}
	b.mu.Unlock()
	if cc != nil {
		cc.conn.Close()
	}
}

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// clientRun holds what the application observed.
type clientRun struct {
	t    *testing.T
	mu   sync.Mutex
	subs []*stomp.Subscription
	// order serialises consumers of one subscription, so the order in which
	// they record messages is the order ReadMessage returned them. It is a channel
	// because synctest counts a goroutine blocked on a channel as idle, and
	// one blocked on sync.Mutex as running.
	order      map[*stomp.Subscription]chan struct{}
	ends       map[*stomp.Subscription]bool
	lastStored map[*stomp.Subscription]int
	storedIDs  map[string]bool
	sent       map[string]error // Send body to its result
}

func (r *clientRun) allowed(what string, err error, extra ...error) {
	var serr *stomp.ServerError
	switch {
	case err == nil, errors.As(err, &serr), errors.Is(err, stomp.ErrOutcomeUnknown), errors.Is(err, stomp.ErrClosed),
		errors.Is(err, context.DeadlineExceeded):
		return
	}
	for _, e := range extra {
		if errors.Is(err, e) {
			return
		}
	}
	r.t.Errorf("%s: unexpected error %v", what, err)
}

func (r *clientRun) pickSub(x int) *stomp.Subscription {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.subs) == 0 {
		return nil
	}
	return r.subs[x%len(r.subs)]
}

func (r *clientRun) consume(ctx context.Context, s *stomp.Subscription, how byte) {
	r.mu.Lock()
	order, ends := r.order[s], r.ends[s]
	r.mu.Unlock()
	order <- struct{}{}
	defer func() { <-order }()
	m, err := readNew(ctx, s)
	if err != nil {
		if ends {
			r.allowed("ReadMessage", err, stomp.ErrOverflow, stomp.ErrUnsubscribed, context.Canceled)
		} else {
			r.allowed("ReadMessage", err, stomp.ErrUnsubscribed, context.Canceled)
		}
		return
	}
	n, convErr := strconv.Atoi(strings.TrimPrefix(m.ID, "m"))
	if convErr != nil || string(m.Body) != m.ID {
		r.t.Errorf("message %q with body %q", m.ID, m.Body)
	}
	if v, _ := m.Headers.Get("subscription"); v != s.ID() {
		r.t.Errorf("message %s for subscription %q returned by %s", m.ID, v, s.ID())
	}
	r.mu.Lock()
	if r.storedIDs[m.ID] {
		r.t.Errorf("message %s returned twice", m.ID)
	}
	r.storedIDs[m.ID] = true
	if n <= r.lastStored[s] {
		r.t.Errorf("%s returned message %d after %d", s.ID(), n, r.lastStored[s])
	}
	r.lastStored[s] = n
	r.mu.Unlock()
	var ackErr error
	switch how % 5 {
	case 0:
		ackErr = s.Ack(ctx, m)
	case 1:
		ackErr = s.AckWait(ctx, m)
	case 2:
		ackErr = s.Nack(ctx, m)
	case 3:
		ackErr = s.NackWait(ctx, m)
	}
	r.allowed("ack", ackErr, stomp.ErrStaleAck, stomp.ErrAutoAck)
}

// runClientChaos drives one Client with ops against a chaosBroker and checks
// the client's guarantees.
func runClientChaos(t *testing.T, ver string, seed uint64, ops []byte) {
	synctest.Test(t, func(t *testing.T) {
		b := newChaosBroker(t, ver, seed)
		c, err := stomp.Open(t.Context(), b.dial, stomp.Logger(discardLogger()), stomp.Backoff(10*time.Millisecond, time.Second))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		r := &clientRun{t: t, order: map[*stomp.Subscription]chan struct{}{}, ends: map[*stomp.Subscription]bool{}, lastStored: map[*stomp.Subscription]int{}, storedIDs: map[string]bool{}, sent: map[string]error{}}
		var wg sync.WaitGroup
		for i := 0; i+1 < len(ops) && i < 128; i += 2 {
			op, arg := ops[i]%9, ops[i+1]
			timeout := time.Duration(1+arg%4) * time.Second
			body := fmt.Sprintf("send-%d", i)
			switch op {
			case 0:
				wg.Go(func() {
					ctx, cancel := context.WithTimeout(t.Context(), timeout)
					defer cancel()
					err := c.Send(ctx, stomp.Outgoing{Destination: "/q", Body: []byte(body)})
					r.allowed("Send", err)
					r.mu.Lock()
					r.sent[body] = err
					r.mu.Unlock()
				})
			case 1:
				ack := stomp.AckMode(arg % 3)
				capacity := 0
				if arg&8 != 0 {
					capacity = 2
				}
				ctx, cancel := context.WithTimeout(t.Context(), timeout)
				s, err := c.Subscribe(ctx, stomp.SubscribeRequest{Destination: "/q", Ack: ack, Capacity: capacity, EndOnOverflow: arg&16 != 0})
				cancel()
				r.allowed("Subscribe", err)
				if err == nil {
					r.mu.Lock()
					r.subs = append(r.subs, s)
					r.order[s] = make(chan struct{}, 1)
					r.ends[s] = arg&16 != 0
					r.mu.Unlock()
				}
			case 2:
				if s := r.pickSub(int(arg)); s != nil {
					wg.Go(func() {
						ctx, cancel := context.WithTimeout(t.Context(), timeout)
						defer cancel()
						r.allowed("Unsubscribe", s.Unsubscribe(ctx))
					})
				}
			case 3, 4:
				if s := r.pickSub(int(arg)); s != nil {
					wg.Go(func() {
						ctx, cancel := context.WithTimeout(t.Context(), timeout)
						defer cancel()
						r.consume(ctx, s, arg>>3)
					})
				}
			case 5:
				b.inject(1 + int(arg)%6)
			case 6:
				if arg%4 == 0 {
					b.drop()
				}
			case 7:
				time.Sleep(time.Duration(arg%5) * 100 * time.Millisecond)
			case 8:
				wg.Go(func() {
					ctx, cancel := context.WithTimeout(t.Context(), timeout)
					defer cancel()
					tx, err := c.Begin(ctx)
					if err != nil {
						r.allowed("Begin", err)
						return
					}
					r.allowed("Tx.Send", tx.Send(ctx, stomp.Outgoing{Destination: "/q", Body: []byte(body)}), stomp.ErrTxAborted)
					if arg&1 == 0 {
						r.allowed("Commit", tx.Commit(ctx), stomp.ErrTxAborted)
					} else {
						r.allowed("Abort", tx.Abort(ctx), stomp.ErrTxAborted)
					}
				})
			}
		}
		wg.Wait()
		for _, s := range r.subs {
			drainCtx, cancel := context.WithTimeout(t.Context(), time.Second)
			for {
				m, err := readNew(drainCtx, s)
				if err != nil {
					break
				}
				if r.storedIDs[m.ID] {
					t.Errorf("message %s returned twice", m.ID)
				}
				r.storedIDs[m.ID] = true
			}
			cancel()
		}
		closeCtx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		var serr *stomp.ServerError
		if err := c.Close(closeCtx); err != nil && !errors.Is(err, stomp.ErrOutcomeUnknown) && !errors.As(err, &serr) {
			t.Errorf("Close: %v; want nil, ErrOutcomeUnknown or a ServerError (CON-28, CON-29)", err)
		}
		b.mu.Lock()
		defer b.mu.Unlock()
		okSends := 0
		for _, err := range r.sent {
			if err == nil {
				okSends++
			}
		}
		t.Logf("%d ops, %d connections, %d subscriptions, %d sends (%d confirmed), %d messages returned, %d acknowledged",
			len(ops)/2, len(b.conns), len(r.subs), len(r.sent), okSends, len(r.storedIDs), len(b.acked))
		for body, n := range b.sends {
			if n > 1 {
				t.Errorf("SEND %q written %d times", body, n)
			}
		}
		for body, err := range r.sent {
			if err == nil && !b.confirmed[body] {
				t.Errorf("Send %q returned nil without a RECEIPT", body)
			}
		}
		for id := range b.acked {
			if !r.storedIDs[id] {
				t.Errorf("message %s acknowledged before ReadMessage returned it", id)
			}
		}
	})
}

func TestClientProperties(t *testing.T) {
	for _, ver := range []string{"1.1", "1.2"} {
		for seed := range uint64(150) {
			rng := rand.New(rand.NewPCG(seed, 99))
			ops := make([]byte, 2*(10+rng.IntN(50)))
			for i := range ops {
				ops[i] = byte(rng.IntN(256))
			}
			t.Run(fmt.Sprintf("%s/%d", ver, seed), func(t *testing.T) { runClientChaos(t, ver, seed, ops) })
		}
	}
}

func FuzzClient(f *testing.F) {
	f.Add(false, uint64(1), []byte{1, 0, 5, 3, 3, 0, 0, 1, 6, 0, 0, 2})
	f.Add(true, uint64(2), []byte{1, 9, 5, 5, 3, 8, 3, 8, 3, 8, 8, 0, 2, 0})
	f.Fuzz(func(t *testing.T, v11 bool, seed uint64, ops []byte) {
		ver := "1.2"
		if v11 {
			ver = "1.1"
		}
		runClientChaos(t, ver, seed, ops)
	})
}
