package stomp

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/jacoelho/stomp/internal/session"
)

// Subscription receives the messages of one STOMP subscription. It is safe
// for concurrent use; concurrent ReadMessage calls receive distinct messages.
type Subscription struct {
	c    *Client
	id   string
	req  SubscribeRequest
	mode session.Mode

	confirmed bool // guarded by c.mu; set by a SUBSCRIBE receipt or a delivery (SUB-48)

	mu     sync.Mutex
	queue  []queued // queue[head:] are waiting; queue[:head] are cleared
	head   int
	arena  []byte        // bodies of queue[head:], from queue[head].off
	bytes  int           // octets of the waiting bodies
	peak   int           // most octets waiting in the current window (SUB-56)
	flow   int           // octets pushed in the current window
	signal chan struct{} // closed when a message arrives or the subscription ends
	err    error         // why it ended; nil while active
}

// queued is a waiting message whose body is arena[off : off+n].
type queued struct {
	m      Message
	off, n int
}

// minArena is the arena capacity a subscription may keep regardless of the
// bodies it holds (SUB-56).
const minArena = 64 << 10

// arenaWindow is the number of arena capacities of pushed octets over which
// the subscription measures its peak before shrinking (SUB-56). Shorter windows
// reallocate repeatedly under streaming backlogs of varying depth.
const arenaWindow = 8

const minArenaGrowth = 4 << 10

func newSubscription(c *Client, r SubscribeRequest, mode session.Mode) *Subscription {
	return &Subscription{c: c, id: r.ID, req: r, mode: mode}
}

// ID returns the STOMP subscription id.
func (s *Subscription) ID() string { return s.id }

// push queues m with a copy of body, reporting false when EndOnOverflow is
// set and m does not fit (specs/subscribe.md SUB-8, SUB-45).
func (s *Subscription) push(m Message, body []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.req.EndOnOverflow && (len(s.queue)-s.head >= s.req.Capacity || s.bytes+len(body) > s.req.CapacityBytes) {
		return false
	}
	s.makeRoomLocked(len(body))
	if len(s.queue) == cap(s.queue) && s.head >= len(s.queue)/2 && s.head > 0 {
		live := s.queue[s.head:]
		if keep := 2 * max(len(live), s.req.Capacity); cap(s.queue) > 2*keep {
			s.queue = append(make([]queued, 0, keep), live...)
		} else {
			n := copy(s.queue, live)
			clear(s.queue[n:])
			s.queue = s.queue[:n]
		}
		s.head = 0
	}
	s.queue = append(s.queue, queued{m: m, off: len(s.arena), n: len(body)})
	s.arena = append(s.arena, body...)
	s.bytes += len(body)
	s.peak = max(s.peak, s.bytes)
	s.notifyLocked()
	return true
}

// end records why the subscription ended. Queued messages stay for
// ReadMessage (SUB-14).
func (s *Subscription) end(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
	}
	s.notifyLocked()
}

func (s *Subscription) endedWith(err error) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err == err
}

func (s *Subscription) notifyLocked() {
	if s.signal != nil {
		close(s.signal)
		s.signal = nil
	}
}

// ReadMessage stores the next message in *m, waiting until one arrives, the
// subscription ends, or ctx ends. After the subscription ends, queued messages
// are still stored before the error is returned. The body is copied into the
// storage of m.Body when it is large enough, so a caller that reuses m
// receives bodies without allocating, and every copy of *m sharing that
// storage sees the new body; to keep a message, read the next into a new
// Message. m.Headers is replaced, not reused. On error *m is unchanged. In
// AckAuto mode ReadMessage acknowledges the message it stores.
func (s *Subscription) ReadMessage(ctx context.Context, m *Message) error {
	for {
		s.mu.Lock()
		if s.head < len(s.queue) {
			q := s.queue[s.head]
			s.queue[s.head] = queued{}
			s.head++
			body := append(m.Body[:0], s.arena[q.off:q.off+q.n]...)
			s.bytes -= q.n
			if s.head == len(s.queue) {
				s.drainedLocked()
			}
			s.mu.Unlock()
			q.m.Body = body
			*m = q.m
			if m.auto {
				s.autoAck(*m)
			}
			return nil
		}
		if s.err != nil {
			err := s.err
			s.mu.Unlock()
			return err
		}
		if s.signal == nil {
			s.signal = make(chan struct{})
		}
		ch := s.signal
		s.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// makeRoomLocked prepares the arena for a body of n octets: it drops the
// bodies already read once they outnumber those waiting or block growth
// (SUB-55), and reallocates when the waiting bodies outgrow it, fill it so
// that compacting frees under a quarter, or stayed small for a window of
// arenaWindow capacities of pushed octets (SUB-56, SUB-59).
func (s *Subscription) makeRoomLocked(n int) {
	dead := len(s.arena) - s.bytes
	need := s.bytes + n
	shrink := false
	if s.flow += n; s.flow >= arenaWindow*cap(s.arena) {
		shrink = cap(s.arena) > max(minArena, 4*s.peak)
		s.flow, s.peak = 0, s.bytes
	}
	if !shrink && need <= cap(s.arena) && dead <= s.bytes && len(s.arena)+n <= cap(s.arena) {
		return
	}
	live := s.arena[dead:]
	full := len(s.arena)+n > cap(s.arena) && 4*dead < cap(s.arena)
	if size := s.arenaSize(need); (shrink || full || need > cap(s.arena)) && size != cap(s.arena) {
		s.arena = append(make([]byte, 0, size), live...)
	} else {
		s.arena = s.arena[:copy(s.arena, live)]
	}
	for i := s.head; i < len(s.queue); i++ {
		s.queue[i].off -= dead
	}
}

// arenaSize is the capacity to allocate for need waiting octets: twice need,
// and with EndOnOverflow no more than 1.25 times CapacityBytes (SUB-59).
func (s *Subscription) arenaSize(need int) int {
	size := max(2*need, minArenaGrowth)
	if s.req.EndOnOverflow {
		size = min(size, max(need, s.req.CapacityBytes+s.req.CapacityBytes/4))
	}
	return size
}

// drainedLocked empties the queue, releasing a queue slice grown for a
// backlog.
func (s *Subscription) drainedLocked() {
	s.queue, s.head = s.queue[:0], 0
	if cap(s.queue) > 4*s.req.Capacity {
		s.queue = nil
	}
	s.arena = s.arena[:0]
}

// Ack acknowledges m without waiting for the broker. In AckClient mode it
// also acknowledges every earlier message of the subscription.
func (s *Subscription) Ack(ctx context.Context, m Message) error { return s.ack(ctx, m, false, false) }

// AckWait acknowledges m and waits for the broker's receipt. In STOMP 1.2 the
// receipt also confirms the broker received every frame sent before it.
func (s *Subscription) AckWait(ctx context.Context, m Message) error {
	return s.ack(ctx, m, false, true)
}

// Nack rejects m without waiting for the broker.
func (s *Subscription) Nack(ctx context.Context, m Message) error { return s.ack(ctx, m, true, false) }

// NackWait rejects m and waits for the broker's receipt.
func (s *Subscription) NackWait(ctx context.Context, m Message) error {
	return s.ack(ctx, m, true, true)
}

// autoAck buffers the acknowledgement of a message ReadMessage is storing,
// without waiting: not for ctx, because a message stored but never
// acknowledged would hold broker credit for the rest of the session (SUB-32),
// and not for WriteBufferLimit, so ReadMessage returns when its ctx ends. When the session that
// delivered m ended, or the Client closed, m stays unacknowledged and the
// broker redelivers it, which at-least-once delivery permits (SUB-29).
func (s *Subscription) autoAck(m Message) {
	c := s.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.life != running || c.sess == nil {
		return
	}
	f, _, err := c.sess.Ack(session.AckHandle{Gen: m.gen, Sub: s.id, Seq: m.seq, ID: m.ack}, false, false)
	switch {
	case err == nil:
		c.mustAppendLocked(f)
	case !errors.Is(err, ErrStaleAck):
		panic(fmt.Sprintf("stomp: internal: automatic acknowledgement of a message ReadMessage stored: %v", err))
	}
}

func (s *Subscription) ack(ctx context.Context, m Message, nack, wait bool) error {
	if s.req.Ack == AckAuto {
		return ErrAutoAck
	}
	if m.owner != s.c || m.sub != s.id {
		return ErrWrongSubscription
	}
	h := session.AckHandle{Gen: m.gen, Sub: s.id, Seq: m.seq, ID: m.ack}
	_, w, err := enqueue(s.c, ctx, 0, ErrStaleAck, func(sess *session.Session) (session.Frame, string, struct{}, error) {
		f, r, err := sess.Ack(h, nack, wait)
		return f, r, struct{}{}, err
	})
	if err != nil {
		return err
	}
	return w.wait(ctx)
}

// Unsubscribe ends the subscription and waits for the broker's receipt;
// ReadMessage still stores the messages already queued (specs/subscribe.md SUB-14). The
// subscription is not re-established after a reconnect even when the outcome
// is unknown.
func (s *Subscription) Unsubscribe(ctx context.Context) error {
	c := s.c
	c.mu.Lock()
	if c.life != running {
		c.mu.Unlock()
		return ErrClosed
	}
	if c.subs[s.id] != s {
		c.mu.Unlock()
		return nil
	}
	r, err := c.unsubscribeLocked(s, ErrUnsubscribed)
	if err != nil || r == "" {
		c.mu.Unlock()
		return cmp.Or(err, fmt.Errorf("%w: not connected; the broker may keep a durable subscription", ErrOutcomeUnknown))
	}
	w := c.waitLocked(r)
	c.mu.Unlock()
	return w.wait(ctx)
}
