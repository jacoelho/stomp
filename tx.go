package stomp

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/jacoelho/stomp/internal/session"
)

// Tx is a transaction begun by Client.Begin. Its sends and acknowledgements
// take effect when Commit returns nil. A Tx belongs to the session that began
// it: the broker aborts it when that session ends, and later calls return
// ErrTxAborted (specs/transaction.md).
//
// A Tx is safe for concurrent use.
type Tx struct {
	c        *Client
	h        session.TxHandle
	finished atomic.Bool // COMMIT or ABORT was buffered
}

// Begin starts a transaction and waits for the broker's receipt.
func (c *Client) Begin(ctx context.Context) (*Tx, error) {
	h, wait, err := enqueue(c, ctx, 0, nil, func(s *session.Session) (session.Frame, string, session.TxHandle, error) {
		c.nextTxID++
		f, h, r, err := s.Begin(fmt.Sprintf("tx-%d", c.nextTxID))
		return f, r, h, err
	})
	if err != nil {
		return nil, err
	}
	if err := wait.wait(ctx); err != nil {
		c.abandon(h)
		return nil, err
	}
	return &Tx{c: c, h: h}, nil
}

// abandon aborts a transaction whose Begin failed after BEGIN was buffered,
// so the broker does not hold it open for the rest of the session.
func (c *Client) abandon(h session.TxHandle) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sess == nil {
		return
	}
	f, _, err := c.sess.Finish(h, true)
	if err != nil {
		// The session already ended the transaction or is disconnecting: nothing to abort.
		return
	}
	c.mustAppendLocked(f)
}

// ID returns the transaction id sent to the broker.
func (t *Tx) ID() string { return t.h.ID }

// Send sends m in the transaction. It returns once the frame is buffered;
// Commit confirms it.
func (t *Tx) Send(ctx context.Context, m Outgoing) error {
	out := session.Outgoing{Destination: m.Destination, ContentType: m.ContentType, Headers: m.Headers, Body: m.Body, Text: m.Text}
	return t.buffer(ctx, len(m.Body), func(s *session.Session) (session.Frame, error) { return s.SendTx(t.h, out) })
}

// Ack acknowledges m in the transaction. It returns once the frame is buffered.
func (t *Tx) Ack(ctx context.Context, m Message) error { return t.ack(ctx, m, false) }

// Nack rejects m in the transaction. It returns once the frame is buffered.
func (t *Tx) Nack(ctx context.Context, m Message) error { return t.ack(ctx, m, true) }

func (t *Tx) ack(ctx context.Context, m Message, nack bool) error {
	if m.owner != t.c {
		return ErrWrongSubscription
	}
	if m.auto {
		return ErrAutoAck
	}
	h := session.AckHandle{Gen: m.gen, Sub: m.sub, Seq: m.seq, ID: m.ack}
	return t.buffer(ctx, 0, func(s *session.Session) (session.Frame, error) { return s.AckTx(t.h, h, nack) })
}

func (t *Tx) buffer(ctx context.Context, size int, build func(*session.Session) (session.Frame, error)) error {
	if t.finished.Load() {
		return ErrTxDone
	}
	_, _, err := enqueue(t.c, ctx, size, ErrTxAborted, func(s *session.Session) (session.Frame, string, struct{}, error) {
		f, err := build(s)
		return f, "", struct{}{}, err
	})
	return err
}

// Commit commits the transaction and waits for the broker's receipt. When the
// session ends after COMMIT was buffered it returns ErrOutcomeUnknown.
func (t *Tx) Commit(ctx context.Context) error { return t.finish(ctx, false) }

// Abort aborts the transaction and waits for the broker's receipt.
func (t *Tx) Abort(ctx context.Context) error { return t.finish(ctx, true) }

func (t *Tx) finish(ctx context.Context, abort bool) error {
	if t.finished.Load() {
		return ErrTxDone
	}
	_, wait, err := enqueue(t.c, ctx, 0, ErrTxAborted, func(s *session.Session) (session.Frame, string, struct{}, error) {
		f, r, err := s.Finish(t.h, abort)
		if err == nil {
			t.finished.Store(true)
		}
		return f, r, struct{}{}, err
	})
	if err != nil {
		return err
	}
	return wait.wait(ctx)
}
