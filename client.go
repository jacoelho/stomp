package stomp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jacoelho/stomp/internal/opts"
	"github.com/jacoelho/stomp/internal/session"
	"github.com/jacoelho/stomp/stomptext"
)

// Client is a STOMP connection that reconnects until Close. It is safe for
// concurrent use.
type Client struct {
	dial DialFunc
	o    opts.Struct
	log  *slog.Logger

	stop context.CancelFunc
	wg   sync.WaitGroup
	done chan struct{}

	mu        sync.Mutex
	state     State
	sess      *session.Session // nil between sessions
	wbuf      []byte           // frames buffered for the current session's writer
	wake      chan struct{}    // signals the writer; capacity 1
	drained   chan struct{}    // closed when the writer takes wbuf
	up        chan struct{}    // closed while a session runs
	waiters   map[string]chan error
	subs      map[string]*Subscription // active subscriptions, replayed on reconnect
	nextSubID uint64
	nextTxID  uint64
	gen       uint64
	life      lifecycle
	err       error
}

// lifecycle is whether the Client accepts work. It moves only forward:
// running, then closing when Close begins, then closed when the supervisor
// finishes. A Client that ends on its own (MaxAttempts, a fatal broker error)
// goes from running straight to closed. State reports the connection within
// it.
type lifecycle uint8

const (
	running lifecycle = iota
	closing
	closed
)

// Open connects and returns once the first session is established, retrying
// with backoff until ctx ends or MaxAttempts is reached. A broker ERROR during
// the handshake is returned at once as a *ServerError.
func Open(ctx context.Context, dial DialFunc, options ...Options) (*Client, error) {
	o := opts.Resolve(options)
	c := &Client{
		dial:    dial,
		o:       o,
		log:     o.Logger,
		done:    make(chan struct{}),
		wake:    make(chan struct{}, 1),
		drained: make(chan struct{}),
		up:      make(chan struct{}),
		waiters: map[string]chan error{},
		subs:    map[string]*Subscription{},
	}
	if c.log == nil {
		c.log = slog.Default()
	}
	var r retry
	l, err := c.establish(ctx, &r)
	if err != nil {
		return nil, err
	}
	sess := c.beginSession(l) // no Close can begin before Open returns c
	sctx, stop := context.WithCancel(context.Background())
	c.stop = stop
	c.wg.Add(1)
	go c.supervise(sctx, l, sess, r)
	return c, nil
}

// Done is closed when the Client has stopped for good.
func (c *Client) Done() <-chan struct{} { return c.done }

// Err returns nil while the Client runs, ErrClosed after Close, or the error
// that stopped it.
func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// State returns the current connection state.
func (c *Client) State() State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

// Close sends DISCONNECT and waits for its receipt until ctx ends, then closes
// the transport and waits for every goroutine of the Client. It returns nil
// once the broker confirmed DISCONNECT, which in STOMP 1.2 also confirms every
// earlier frame, or when no session was running. It returns ErrOutcomeUnknown
// when ctx or the session ended first, and a *ServerError when the broker
// answered with ERROR. Calls after Close return ErrClosed; a second Close
// returns nil.
func (c *Client) Close(ctx context.Context) error {
	c.mu.Lock()
	switch c.life {
	case closing:
		c.mu.Unlock()
		return nil
	case closed:
		c.mu.Unlock()
		c.stop()
		c.wg.Wait()
		return nil
	}
	c.life = closing
	var wait receipt
	if c.sess != nil {
		f, r, err := c.sess.Disconnect()
		if err != nil {
			panic(fmt.Sprintf("stomp: DISCONNECT built twice: %v", err))
		}
		c.mustAppendLocked(f)
		wait = c.waitLocked(r)
	}
	c.mu.Unlock()
	err := wait.wait(ctx)
	c.stop()
	c.wg.Wait()
	return err
}

// Version returns the protocol version of the current session, or 0 between sessions.
func (c *Client) Version() Version {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sess == nil {
		return 0
	}
	return c.sess.Version()
}

// Send sends m and waits for the broker's receipt.
func (c *Client) Send(ctx context.Context, m Outgoing) error {
	out := session.Outgoing{Destination: m.Destination, ContentType: m.ContentType, Headers: m.Headers, Body: m.Body, Text: m.Text}
	_, wait, err := enqueue(c, ctx, len(m.Body), nil, func(s *session.Session) (session.Frame, string, struct{}, error) {
		f, r, err := s.Send(out)
		return f, r, struct{}{}, err
	})
	if err != nil {
		return err
	}
	return wait.wait(ctx)
}

// enqueue builds a frame against the current session and buffers it for the
// writer, returning the value build made with the frame and the frame's
// receipt. An error means the frame was not buffered. It waits for a session,
// and for buffer space, bounded by ctx. A frame bound to an earlier session
// passes the error to report while disconnected as noSession; with nil,
// enqueue waits for a session.
func enqueue[T any](c *Client, ctx context.Context, size int, noSession error, build func(*session.Session) (session.Frame, string, T, error)) (T, receipt, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, nil, err
	}
	c.mu.Lock()
	for {
		if c.life != running {
			c.mu.Unlock()
			return zero, nil, ErrClosed
		}
		var ch chan struct{}
		switch {
		case c.sess == nil && noSession != nil:
			c.mu.Unlock()
			return zero, nil, noSession
		case c.sess == nil:
			ch = c.up
		case len(c.wbuf) > 0 && len(c.wbuf)+size+frameOverhead > c.o.WriteBufferLimit:
			ch = c.drained
		}
		if ch == nil {
			break
		}
		c.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return zero, nil, ctx.Err()
		case <-c.done:
			return zero, nil, ErrClosed
		}
		c.mu.Lock()
	}
	f, r, v, err := build(c.sess)
	if err != nil {
		c.mu.Unlock()
		return zero, nil, err
	}
	c.mustAppendLocked(f)
	var wait receipt
	if r != "" {
		wait = c.waitLocked(r)
	}
	c.mu.Unlock()
	return v, wait, nil
}

// frameOverhead approximates a frame's octets beyond its body when checking
// WriteBufferLimit before the frame is built.
const frameOverhead = 512

// mustAppendLocked buffers a frame for the writer. The session builds frames
// only from input it validated, and the session state a frame records is
// already committed, so a frame that fails to encode is a bug in this module
// (TestFramesBuiltFromAcceptedHeadersEncode) and panics at its caller.
func (c *Client) mustAppendLocked(f session.Frame) {
	b, err := f.Append(c.wbuf, c.sess.Version())
	if err != nil {
		panic(fmt.Sprintf("stomp: internal: %s frame not encodable: %v", f.Command, err))
	}
	c.wbuf = b
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// receipt delivers the broker's answer to a buffered frame: nil for its
// RECEIPT, or the error that ended the wait. A nil receipt belongs to a frame
// that asked for none.
type receipt <-chan error

func (c *Client) waitLocked(id string) receipt {
	ch := make(chan error, 1)
	c.waiters[id] = ch
	return ch
}

func (c *Client) resolveLocked(receipt string, err error) {
	if ch, ok := c.waiters[receipt]; ok {
		delete(c.waiters, receipt)
		ch <- err
	}
}

// wait waits for the broker's answer. A context that ends first leaves the
// outcome unknown.
func (r receipt) wait(ctx context.Context) error {
	if r == nil {
		return nil
	}
	select {
	case err := <-r:
		return err
	case <-ctx.Done():
	}
	select {
	case err := <-r:
		return err
	default:
		return fmt.Errorf("%w: %w", ErrOutcomeUnknown, ctx.Err())
	}
}

// Subscribe opens a subscription and waits for the broker's receipt. A MESSAGE
// for the subscription also confirms it, so Subscribe returns the
// subscription when one arrived before the session or ctx ended
// (specs/subscribe.md SUB-48). The subscription is re-established after every
// reconnect until it ends.
func (c *Client) Subscribe(ctx context.Context, r SubscribeRequest) (*Subscription, error) {
	mode, ok := r.Ack.session()
	switch {
	case !ok:
		return nil, fmt.Errorf("%w: ack mode %d", ErrInvalidRequest, r.Ack)
	case r.Destination == "":
		return nil, fmt.Errorf("%w: empty destination", ErrInvalidRequest)
	case r.Capacity < 0 || r.CapacityBytes < 0:
		return nil, fmt.Errorf("%w: negative capacity", ErrInvalidRequest)
	}
	if err := session.CheckSubscribeHeaders(r.Headers); err != nil {
		return nil, err
	}
	if r.Capacity == 0 {
		r.Capacity = 256
	}
	if r.CapacityBytes == 0 {
		r.CapacityBytes = 16 << 20
	}
	r.Headers = append(Headers(nil), r.Headers...)
	if h := c.o.PrefetchHeader; h != "" {
		if _, set := r.Headers.Get(h); !set {
			r.Headers = append(r.Headers, Header{Name: h, Value: strconv.Itoa(r.Capacity)})
		}
	}
	s, wait, err := enqueue(c, ctx, 0, nil, func(sess *session.Session) (session.Frame, string, *Subscription, error) {
		f, rc, err := c.subscribeLocked(sess, &r, mode)
		if err != nil {
			return f, rc, nil, err
		}
		s := newSubscription(c, r, mode)
		c.subs[r.ID] = s
		return f, rc, s, nil
	})
	if err != nil {
		return nil, err
	}
	if err := wait.wait(ctx); err != nil {
		c.mu.Lock()
		if s.confirmed && (c.subs[s.id] == s || c.life == closed && s.endedWith(c.err)) {
			c.mu.Unlock()
			return s, nil
		}
		_, ierr := c.unsubscribeLocked(s, err)
		c.mu.Unlock()
		if ierr != nil {
			return nil, errors.Join(err, ierr)
		}
		return nil, err
	}
	return s, nil
}

// subscribeLocked registers r in sess, generating `sub-N` for an empty ID and
// skipping ids still in use (specs/subscribe.md SUB-3, SUB-33). Every Client
// subscription is registered in the current session, so the session alone
// decides which ids are in use.
func (c *Client) subscribeLocked(sess *session.Session, r *SubscribeRequest, mode session.Mode) (session.Frame, string, error) {
	if r.ID != "" {
		return sess.Subscribe(r.ID, r.Destination, mode, r.Headers)
	}
	for {
		c.nextSubID++
		id := fmt.Sprintf("sub-%d", c.nextSubID)
		f, rc, err := sess.Subscribe(id, r.Destination, mode, r.Headers)
		if errors.Is(err, ErrDuplicateID) {
			continue
		}
		if err == nil {
			r.ID = id
		}
		return f, rc, err
	}
}

// Ending a subscription: every path removes it from c.subs, the set replayed
// on reconnect, ends the Subscription, and stops the current session from
// delivering to it, so the session never delivers to an id c.subs lacks.

// removeLocked removes s from c.subs and ends it with err, reporting false
// when s had already ended.
func (c *Client) removeLocked(s *Subscription, err error) bool {
	if c.subs[s.id] != s {
		return false
	}
	delete(c.subs, s.id)
	s.end(err)
	return true
}

// unsubscribeLocked ends s and buffers UNSUBSCRIBE when the session may still
// write one (SUB-15); after DISCONNECT the session discards its messages
// instead. It returns the receipt of the UNSUBSCRIBE, or "" when none was
// buffered.
func (c *Client) unsubscribeLocked(s *Subscription, err error) (string, error) {
	if !c.removeLocked(s, err) || c.sess == nil {
		return "", nil
	}
	f, r, uerr := c.sess.Unsubscribe(s.id)
	switch {
	case uerr == nil:
		c.mustAppendLocked(f)
		return r, nil
	case errors.Is(uerr, session.ErrDisconnecting):
		c.sess.End(s.id)
		return "", nil
	}
	return "", fmt.Errorf("stomp: internal: subscription %q held by the client but not active in its session: %w", s.id, uerr)
}

func (c *Client) setStateLocked(st State, err error) {
	if c.state == st {
		return
	}
	c.state = st
	c.log.Info("stomp state", "state", st, "err", err)
	if c.o.OnState != nil {
		c.o.OnState(int(st), err)
	}
}

// retry is the reconnect progress that establish and the supervisor share,
// so short-lived sessions also back off and count as failed attempts. Both
// reset once a session stays up for MaxBackoff (RCN-9, RCN-17).
type retry struct {
	exp      int // backoff exponent
	failures int // consecutive failed attempts
}

// failed records a failed attempt, returning the Client's terminal error once
// MaxAttempts is reached (RCN-10).
func (c *Client) failed(r *retry, err error) error {
	r.failures++
	if c.o.MaxAttempts > 0 && r.failures >= c.o.MaxAttempts {
		return fmt.Errorf("stomp: %d connection attempts failed: %w", r.failures, err)
	}
	return nil
}

// establish dials and handshakes, retrying with backoff.
func (c *Client) establish(ctx context.Context, r *retry) (link, error) {
	for {
		c.mu.Lock()
		if c.life != running {
			c.mu.Unlock()
			return link{}, ErrClosed
		}
		c.setStateLocked(Connecting, nil)
		c.mu.Unlock()
		l, err := c.connect(ctx, r.failures)
		if err == nil {
			return l, nil
		}
		if ctx.Err() != nil {
			return link{}, fmt.Errorf("stomp: connect: %w", errors.Join(ctx.Err(), err))
		}
		var serr *ServerError
		if errors.As(err, &serr) || errors.Is(err, ErrUnsupportedVersion) {
			return link{}, err
		}
		if gaveUp := c.failed(r, err); gaveUp != nil {
			return link{}, gaveUp
		}
		c.log.Warn("stomp connect failed", "attempt", r.failures, "err", err)
		if !c.backoff(ctx, &r.exp, err) {
			return link{}, fmt.Errorf("stomp: connect: %w", errors.Join(ctx.Err(), err))
		}
	}
}

// backoff waits before the next connection attempt, reporting false when ctx
// ended first or Close began, so no state is reported after Close (RCN-16).
func (c *Client) backoff(ctx context.Context, exp *int, cause error) bool {
	c.mu.Lock()
	if c.life != running {
		c.mu.Unlock()
		return false
	}
	c.setStateLocked(Waiting, cause)
	c.mu.Unlock()
	t := time.NewTimer(backoffDelay(*exp, c.o.BaseBackoff, c.o.MaxBackoff, rand.Float64()))
	*exp++
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// backoffDelay is the full-jitter delay before attempt n (specs/reconnect.md
// RCN-7): u·min(max, base·2ⁿ) with u uniform in [0, 1).
func backoffDelay(n int, base, max time.Duration, u float64) time.Duration {
	ceil := max
	if base <= max>>n {
		ceil = base << n
	}
	return time.Duration(u * float64(ceil))
}

// link is an established transport, positioned after CONNECTED.
type link struct {
	conn net.Conn
	dec  *stomptext.Decoder
	act  *activityReader
	acc  session.Accepted
}

// connect dials and performs the CONNECT/CONNECTED exchange within HandshakeTimeout.
func (c *Client) connect(ctx context.Context, attempt int) (link, error) {
	hctx, cancel := context.WithTimeout(ctx, c.o.HandshakeTimeout)
	defer cancel()
	conn, err := c.dial(hctx, attempt)
	if err != nil {
		return link{}, fmt.Errorf("stomp: dial: %w", err)
	}
	stopClose := context.AfterFunc(hctx, func() { conn.Close() })
	l, err := c.handshake(conn)
	if !stopClose() && err == nil {
		err = fmt.Errorf("stomp: handshake: %w", hctx.Err())
	}
	if err != nil {
		conn.Close()
		return link{}, err
	}
	return l, nil
}

func (c *Client) handshake(conn net.Conn) (link, error) {
	versions := make([]stomptext.Version, len(c.o.Versions))
	for i, v := range c.o.Versions {
		versions[i] = stomptext.Version(v)
	}
	f := session.Connect(versions, c.o.Host, c.o.Login, c.o.Passcode, c.o.ClientID, c.o.HeartbeatSend, c.o.HeartbeatExpect)
	b, err := f.Append(nil, stomptext.Version12)
	if err != nil {
		return link{}, fmt.Errorf("stomp: CONNECT: %w", err)
	}
	if _, err := conn.Write(b); err != nil {
		return link{}, fmt.Errorf("stomp: write CONNECT: %w", err)
	}
	act := &activityReader{r: conn}
	dec := stomptext.NewDecoder(act, frameLimits(c.o)...)
	reply, err := dec.ReadFrame()
	if err != nil {
		return link{}, fmt.Errorf("stomp: read CONNECTED: %w", err)
	}
	acc, ok, err := session.Accept(reply, versions, c.o.HeartbeatSend, c.o.HeartbeatExpect)
	if err != nil {
		return link{}, err
	}
	if !ok {
		return link{}, serverError(reply)
	}
	dec.SetVersion(acc.Version)
	return link{conn: conn, dec: dec, act: act, acc: acc}, nil
}

func frameLimits(o opts.Struct) []stomptext.Options {
	return []stomptext.Options{
		stomptext.MaxFrameSize(o.MaxFrameSize),
		stomptext.MaxHeaderCount(o.MaxHeaderCount),
		stomptext.MaxHeaderBytes(o.MaxHeaderBytes),
	}
}

// activityReader records when octets last arrived, for receive liveness
// (specs/connection.md CON-11).
type activityReader struct {
	r    io.Reader
	last atomic.Int64
}

func (a *activityReader) Read(p []byte) (int, error) {
	n, err := a.r.Read(p)
	if n > 0 {
		a.last.Store(time.Now().UnixNano())
	}
	return n, err
}

func serverError(f stomptext.Frame) *ServerError {
	e := &ServerError{Headers: ownHeaders(f), Body: append([]byte(nil), f.Body()...)}
	e.Message, _ = e.Headers.Get("message")
	return e
}

// ownHeaders copies a frame's headers into one string allocation.
func ownHeaders(f stomptext.Frame) Headers {
	n := 0
	for k, v := range f.All() {
		n += len(k) + len(v)
	}
	var sb strings.Builder
	sb.Grow(n)
	for k, v := range f.All() {
		sb.Write(k)
		sb.Write(v)
	}
	text := sb.String()
	hs := make(Headers, 0, f.HeaderCount())
	pos := 0
	for k, v := range f.All() {
		name := text[pos : pos+len(k)]
		pos += len(k)
		hs = append(hs, Header{Name: name, Value: text[pos : pos+len(v)]})
		pos += len(v)
	}
	return hs
}

// supervise runs sessions until Close or a terminal error, then finalises the Client.
func (c *Client) supervise(ctx context.Context, l link, sess *session.Session, r retry) {
	defer c.wg.Done()
	var cause error
	for {
		started := time.Now()
		cause = c.runSession(ctx, l, sess)
		if ctx.Err() != nil || !c.isRunning() {
			break
		}
		c.log.Warn("stomp session ended", "err", cause)
		if time.Since(started) >= c.o.MaxBackoff {
			r = retry{}
		} else if gaveUp := c.failed(&r, cause); gaveUp != nil {
			cause = gaveUp
			break
		}
		if !c.backoff(ctx, &r.exp, cause) {
			break
		}
		var err error
		if l, err = c.establish(ctx, &r); err != nil {
			cause = err
			break
		}
		if sess = c.beginSession(l); sess == nil {
			break
		}
	}
	c.finish(cause)
}

func (c *Client) isRunning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.life == running
}

// finish ends the Client with cause, or with ErrClosed when Close stopped it:
// ctx of supervise ends only through Close (CON-17).
func (c *Client) finish(cause error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.life == closing {
		cause = ErrClosed
	}
	c.life = closed
	c.err = cause
	for _, s := range c.subs {
		c.removeLocked(s, cause)
	}
	c.setStateLocked(Closed, cause)
	close(c.done)
}

// beginSession installs a new session over l and replays subscriptions, so
// calls made once it returns use the session. It closes l and returns nil
// when Close began during the handshake: Close found no session to
// DISCONNECT, so none may start (specs/connection.md CON-30).
func (c *Client) beginSession(l link) *session.Session {
	c.mu.Lock()
	if c.life != running {
		c.mu.Unlock()
		l.conn.Close()
		return nil
	}
	defer c.mu.Unlock()
	c.gen++
	sess := session.New(c.gen, l.acc)
	c.sess = sess
	c.wbuf = c.wbuf[:0]
	for _, s := range c.subs {
		f, _, err := sess.Subscribe(s.id, s.req.Destination, s.mode, s.req.Headers)
		if err != nil {
			panic(fmt.Sprintf("stomp: replaying %q into a new session: %v", s.id, err))
		}
		c.mustAppendLocked(f)
	}
	close(c.up)
	c.setStateLocked(Connected, nil)
	return sess
}

// runSession runs the session begun over l and returns why it ended.
func (c *Client) runSession(ctx context.Context, l link, sess *session.Session) error {

	l.act.last.Store(time.Now().UnixNano())
	errs := make(chan error, 2)
	stopWriter := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); errs <- c.readLoop(sess, l.dec) }()
	go func() { defer wg.Done(); errs <- c.writeLoop(l.conn, l.acc.Heartbeat, l.act, stopWriter) }()

	var cause error
	select {
	case cause = <-errs:
	case <-ctx.Done():
		cause = ctx.Err()
	}
	close(stopWriter)
	l.conn.Close()
	wg.Wait()

	c.mu.Lock()
	defer c.mu.Unlock()
	c.sess = nil
	c.up = make(chan struct{})
	for r, ch := range c.waiters {
		delete(c.waiters, r)
		ch <- fmt.Errorf("%w: %w", ErrOutcomeUnknown, cause)
	}
	for _, s := range c.subs {
		if !s.confirmed {
			c.removeLocked(s, fmt.Errorf("%w: %w", ErrOutcomeUnknown, cause))
		}
	}
	close(c.drained)
	c.drained = make(chan struct{})
	if c.life == running {
		c.setStateLocked(Waiting, cause)
	}
	return cause
}

func (c *Client) readLoop(sess *session.Session, dec *stomptext.Decoder) error {
	for {
		f, err := dec.ReadFrame()
		if err != nil {
			return fmt.Errorf("stomp: read: %w", err)
		}
		var msg Message
		if f.Command() == stomptext.Message {
			msg = Message{Headers: ownHeaders(f)}
			msg.Destination, _ = msg.Headers.Get("destination")
			msg.ID, _ = msg.Headers.Get("message-id")
		}
		if err := c.receive(sess, f, msg); err != nil {
			return err
		}
	}
}

func (c *Client) receive(sess *session.Session, f stomptext.Frame, msg Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	ev, err := sess.Receive(f)
	if err != nil {
		return err
	}
	switch ev.Kind {
	case session.EventDeliver:
		s := c.subs[ev.Sub]
		if s == nil {
			return fmt.Errorf("stomp: internal: the session delivered to subscription %q, which the client no longer holds", ev.Sub)
		}
		msg.owner, msg.auto, msg.sub, msg.seq, msg.ack, msg.gen = c, s.req.Ack == AckAuto, ev.Sub, ev.Seq, ev.AckID, sess.Gen()
		s.confirmed = true
		if !s.push(msg, f.Body()) {
			if _, err := c.unsubscribeLocked(s, ErrOverflow); err != nil {
				return err
			}
		}
	case session.EventDiscard:
		c.log.Debug("stomp MESSAGE discarded unacknowledged", "subscription", ev.Sub, "message-id", msg.ID)
	case session.EventReceipt:
		if ev.Pending.Kind == session.KindSubscribe {
			if s := c.subs[ev.Pending.Sub]; s != nil {
				s.confirmed = true
			}
		}
		c.resolveLocked(ev.Receipt, nil)
	case session.EventError:
		serr := serverError(f)
		if ev.Known {
			c.resolveLocked(ev.Receipt, serr)
			if s := c.subs[ev.Pending.Sub]; s != nil && ev.Pending.Kind == session.KindSubscribe {
				c.removeLocked(s, serr)
			}
		}
		return serr
	}
	return nil
}

// writeLoop writes buffered frames in batches and keeps heart-beats in both directions.
func (c *Client) writeLoop(conn net.Conn, hb session.Heartbeat, act *activityReader, stop <-chan struct{}) error {
	var spare []byte
	lastWrite := time.Now()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	write := func(b []byte) error {
		if err := conn.SetWriteDeadline(time.Now().Add(c.o.WriteTimeout)); err != nil {
			return err
		}
		if _, err := conn.Write(b); err != nil {
			return fmt.Errorf("stomp: write: %w", err)
		}
		lastWrite = time.Now()
		return nil
	}
	for {
		c.mu.Lock()
		batch := c.wbuf
		c.wbuf = spare[:0]
		if len(batch) > 0 {
			close(c.drained)
			c.drained = make(chan struct{})
		}
		c.mu.Unlock()
		if len(batch) > 0 {
			if err := write(batch); err != nil {
				return err
			}
		}
		spare = batch
		if cap(spare) > 4*c.o.WriteBufferLimit {
			spare = nil
		}

		now := time.Now()
		next := time.Hour
		if hb.Recv > 0 {
			silent := now.Sub(time.Unix(0, act.last.Load()))
			if silent >= 2*hb.Recv {
				return ErrHeartbeatTimeout
			}
			next = min(next, 2*hb.Recv-silent)
		}
		if hb.Send > 0 {
			idle := now.Sub(lastWrite)
			if idle >= hb.Send {
				if err := write([]byte{'\n'}); err != nil {
					return err
				}
				idle = 0
			}
			next = min(next, hb.Send-idle)
		}
		timer.Reset(next)
		select {
		case <-c.wake:
		case <-timer.C:
		case <-stop:
			return nil
		}
	}
}
