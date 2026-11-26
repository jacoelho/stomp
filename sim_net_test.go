package stomp_test

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
)

// simNet is an in-memory network for deterministic simulation: every
// connection is a pair of simConn endpoints joined by two simPipes, one per
// direction. Faults are applied through its methods by the injector. It must
// be created inside the synctest bubble.
type simNet struct {
	logf func(format string, args ...any)

	mu          sync.Mutex
	rng         *rand.Rand
	window      int
	links       map[int]*simLink
	nextLink    int
	accept      chan *simConn
	listening   bool
	partitioned int // partition-all depth; dials hang while positive
	allHeld     map[int]bool
	spike       time.Duration
	refuseUntil time.Time
	refuseDials bool
	slowDials   time.Duration // upper bound of the delay before a dial completes

	stopped chan struct{}
	pumps   sync.WaitGroup
}

func newSimNet(seed uint64, window int, logf func(string, ...any)) *simNet {
	return &simNet{
		logf:    logf,
		rng:     rand.New(rand.NewPCG(seed, 0x6e6574)),
		window:  window,
		links:   map[int]*simLink{},
		accept:  make(chan *simConn, 64),
		allHeld: map[int]bool{},
		stopped: make(chan struct{}),
	}
}

type simLink struct {
	id       int
	c2s, s2c *simPipe
	client   *simConn
	server   *simConn
	held     int // single-link partition depth, guarded by simNet.mu
}

type simAddr string

func (a simAddr) Network() string { return "sim" }
func (a simAddr) String() string  { return string(a) }

func (n *simNet) listen() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.listening = true
}

func (n *simNet) closeListener() []*simConn {
	n.mu.Lock()
	n.listening = false
	n.mu.Unlock()
	var pending []*simConn
	for {
		select {
		case c := <-n.accept:
			pending = append(pending, c)
		default:
			return pending
		}
	}
}

func (n *simNet) dial(ctx context.Context, _ int) (net.Conn, error) {
	n.mu.Lock()
	now := time.Now()
	switch {
	case !n.listening, n.refuseDials, now.Before(n.refuseUntil):
		n.mu.Unlock()
		n.logf("dial refused")
		return nil, &net.OpError{Op: "dial", Net: "sim", Err: syscall.ECONNREFUSED}
	case n.partitioned > 0:
		n.mu.Unlock()
		n.logf("dial hangs: partition")
		<-ctx.Done()
		return nil, &net.OpError{Op: "dial", Net: "sim", Err: ctx.Err()}
	}
	var delay time.Duration
	if n.slowDials > 0 {
		delay = time.Duration(n.rng.Int64N(int64(n.slowDials)))
	}
	n.mu.Unlock()
	if delay > 0 {
		n.logf("dial delayed %v", delay)
		t := time.NewTimer(delay)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return nil, &net.OpError{Op: "dial", Net: "sim", Err: ctx.Err()}
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.listening || n.partitioned > 0 || n.refuseDials || time.Now().Before(n.refuseUntil) {
		return nil, &net.OpError{Op: "dial", Net: "sim", Err: syscall.ECONNREFUSED}
	}
	n.nextLink++
	id := n.nextLink
	l := &simLink{
		id:  id,
		c2s: newSimPipe(n, fmt.Sprintf("c%d>", id), n.rng.Uint64()),
		s2c: newSimPipe(n, fmt.Sprintf("c%d<", id), n.rng.Uint64()),
	}
	l.client = &simConn{link: l, in: l.s2c, out: l.c2s, local: simAddr(fmt.Sprintf("client:%d", id)), remote: "broker"}
	l.server = &simConn{link: l, in: l.c2s, out: l.s2c, local: "broker", remote: simAddr(fmt.Sprintf("client:%d", id))}
	select {
	case n.accept <- l.server:
	default:
		return nil, &net.OpError{Op: "dial", Net: "sim", Err: syscall.ECONNREFUSED}
	}
	n.links[id] = l
	for _, p := range []*simPipe{l.c2s, l.s2c} {
		n.pumps.Add(1)
		go func() { defer n.pumps.Done(); p.pump() }()
	}
	return l.client, nil
}

func (n *simNet) liveLinks() []*simLink {
	n.mu.Lock()
	all := make([]*simLink, 0, len(n.links))
	for _, l := range n.links {
		all = append(all, l)
	}
	n.mu.Unlock()
	var ls []*simLink
	for _, l := range all {
		if !l.c2s.finished() || !l.s2c.finished() {
			ls = append(ls, l)
		}
	}
	slices.SortFunc(ls, func(a, b *simLink) int { return a.id - b.id })
	return ls
}

func (n *simNet) pick(x uint64) *simLink {
	ls := n.liveLinks()
	if len(ls) == 0 {
		return nil
	}
	return ls[x%uint64(len(ls))]
}

func (n *simNet) latency(r *rand.Rand) time.Duration {
	n.mu.Lock()
	spike := n.spike
	n.mu.Unlock()
	return time.Millisecond + time.Duration(r.Int64N(int64(15*time.Millisecond))) + spike
}

func (n *simNet) partitionLink(l *simLink) {
	n.mu.Lock()
	l.held++
	n.mu.Unlock()
	l.c2s.hold()
	l.s2c.hold()
}

func (n *simNet) healLink(l *simLink) {
	n.mu.Lock()
	l.held--
	n.mu.Unlock()
	l.c2s.release()
	l.s2c.release()
}

func (n *simNet) partitionAll() {
	n.mu.Lock()
	n.partitioned++
	var ls []*simLink
	if n.partitioned == 1 {
		for id, l := range n.links {
			n.allHeld[id] = true
			ls = append(ls, l)
		}
	}
	n.mu.Unlock()
	for _, l := range ls {
		l.c2s.hold()
		l.s2c.hold()
	}
}

func (n *simNet) healAll() {
	n.mu.Lock()
	n.partitioned--
	var ls []*simLink
	if n.partitioned == 0 {
		for id := range n.allHeld {
			ls = append(ls, n.links[id])
		}
		clear(n.allHeld)
	}
	n.mu.Unlock()
	for _, l := range ls {
		l.c2s.release()
		l.s2c.release()
	}
}

// reset fails both endpoints of l at once, dropping undelivered octets.
func (n *simNet) reset(l *simLink) {
	l.c2s.reset()
	l.s2c.reset()
}

func (n *simNet) setSpike(d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.spike = d
}

func (n *simNet) setDialFaults(refuse bool, slow time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.refuseDials, n.slowDials = refuse, slow
}

func (n *simNet) refuseFor(d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.refuseUntil = time.Now().Add(d)
}

func (n *simNet) heal() {
	n.mu.Lock()
	n.spike, n.refuseDials, n.slowDials, n.refuseUntil = 0, false, 0, time.Time{}
	var pipes []*simPipe
	for _, l := range n.links {
		l.held = 0
		pipes = append(pipes, l.c2s, l.s2c)
	}
	n.partitioned = 0
	clear(n.allHeld)
	n.mu.Unlock()
	for _, p := range pipes {
		p.releaseAll()
	}
}

func (n *simNet) stop() {
	n.mu.Lock()
	ls := make([]*simLink, 0, len(n.links))
	for _, l := range n.links {
		ls = append(ls, l)
	}
	n.mu.Unlock()
	for _, l := range ls {
		n.reset(l)
	}
	close(n.stopped)
	n.pumps.Wait()
}

type simSegment struct {
	at   time.Time
	data []byte
	fin  bool
	rst  func() // resets the link once data is delivered
}

// simPipe carries octets in one direction. Written segments are queued with
// a delivery time and moved to the reader's buffer by pump; queued plus
// unread octets are bounded by the window, like a TCP receive window.
type simPipe struct {
	net  *simNet
	name string

	mu          sync.Mutex
	rng         *rand.Rand
	queued      []simSegment
	queuedBytes int
	buf         []byte
	lastAt      time.Time
	holds       int
	finQueued   bool
	finRead     bool
	rdClosed    bool
	wrClosed    bool
	isReset     bool
	rdDeadline  time.Time
	wrDeadline  time.Time
	resetAfter  int // octets until the segment that carries an armed reset; 0 disables
	onReset     func()
	changed     chan struct{}
}

func newSimPipe(n *simNet, name string, seed uint64) *simPipe {
	return &simPipe{net: n, name: name, rng: rand.New(rand.NewPCG(seed, 0x70697065)), changed: make(chan struct{})}
}

func (p *simPipe) notifyLocked() {
	close(p.changed)
	p.changed = make(chan struct{})
}

func (p *simPipe) waitLocked(deadline time.Time) {
	ch := p.changed
	p.mu.Unlock()
	defer p.mu.Lock()
	if deadline.IsZero() {
		<-ch
		return
	}
	d := time.Until(deadline)
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ch:
	case <-t.C:
	}
}

func expired(deadline time.Time) bool { return !deadline.IsZero() && !time.Now().Before(deadline) }

func (p *simPipe) finished() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.doneLocked()
}

func (p *simPipe) doneLocked() bool {
	return p.isReset || p.finRead || (p.rdClosed && p.wrClosed && len(p.queued) == 0)
}

func (p *simPipe) pump() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for !p.doneLocked() {
		if p.holds > 0 || len(p.queued) == 0 {
			ch := p.changed
			p.mu.Unlock()
			select {
			case <-ch:
			case <-p.net.stopped:
			}
			p.mu.Lock()
			if isClosed(p.net.stopped) {
				return
			}
			continue
		}
		if wait := time.Until(p.queued[0].at); wait > 0 {
			ch := p.changed
			p.mu.Unlock()
			t := time.NewTimer(wait)
			select {
			case <-ch:
			case <-t.C:
			case <-p.net.stopped:
			}
			t.Stop()
			p.mu.Lock()
			if isClosed(p.net.stopped) {
				return
			}
			continue
		}
		now := time.Now()
		for len(p.queued) > 0 && !p.queued[0].at.After(now) {
			s := p.queued[0]
			p.queued = p.queued[1:]
			p.queuedBytes -= len(s.data)
			switch {
			case s.fin:
				p.finRead = true
			case !p.rdClosed:
				p.buf = append(p.buf, s.data...)
			}
			if s.rst != nil {
				p.notifyLocked()
				p.mu.Unlock()
				s.rst()
				p.mu.Lock()
				break
			}
		}
		p.notifyLocked()
	}
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func (p *simPipe) hold() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.holds++
	p.notifyLocked()
}

func (p *simPipe) release() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.holds > 0 {
		p.holds--
	}
	p.retimeLocked()
}

func (p *simPipe) releaseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.holds = 0
	p.retimeLocked()
}

// retimeLocked delays segments held by a partition so they arrive after it
// heals, in order.
func (p *simPipe) retimeLocked() {
	if p.holds == 0 {
		earliest := time.Now().Add(p.net.latency(p.rng))
		for i := range p.queued {
			if p.queued[i].at.Before(earliest) {
				p.queued[i].at = earliest
			}
		}
		if p.lastAt.Before(earliest) && len(p.queued) > 0 {
			p.lastAt = earliest
		}
	}
	p.notifyLocked()
}

func (p *simPipe) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.isReset {
		return
	}
	p.isReset = true
	p.queued, p.queuedBytes = nil, 0
	p.notifyLocked()
}

// armReset makes the link reset when the octet n octets from now is
// delivered, so the reader sees the frame it was part of truncated.
func (p *simPipe) armReset(n int, onReset func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resetAfter, p.onReset = n, onReset
}

func (p *simPipe) enqueueLocked(data []byte, fin bool, rst func()) {
	at := time.Now().Add(p.net.latency(p.rng))
	if at.Before(p.lastAt) {
		at = p.lastAt
	}
	p.lastAt = at
	p.queued = append(p.queued, simSegment{at: at, data: data, fin: fin, rst: rst})
	p.queuedBytes += len(data)
	p.notifyLocked()
}

func (p *simPipe) read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		switch {
		case p.rdClosed:
			return 0, &net.OpError{Op: "read", Net: "sim", Err: net.ErrClosed}
		case len(p.buf) > 0:
			n := 1 + p.rng.IntN(min(len(b), len(p.buf)))
			copy(b, p.buf[:n])
			p.buf = p.buf[n:]
			p.notifyLocked()
			return n, nil
		case p.isReset:
			return 0, &net.OpError{Op: "read", Net: "sim", Err: syscall.ECONNRESET}
		case p.finRead:
			return 0, io.EOF
		case expired(p.rdDeadline):
			return 0, &net.OpError{Op: "read", Net: "sim", Err: os.ErrDeadlineExceeded}
		}
		p.waitLocked(p.rdDeadline)
	}
}

func (p *simPipe) write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	written := 0
	for written < len(b) {
		switch {
		case p.wrClosed:
			return written, &net.OpError{Op: "write", Net: "sim", Err: net.ErrClosed}
		case p.isReset:
			return written, &net.OpError{Op: "write", Net: "sim", Err: syscall.ECONNRESET}
		case expired(p.wrDeadline):
			return written, &net.OpError{Op: "write", Net: "sim", Err: os.ErrDeadlineExceeded}
		}
		space := p.net.window - p.queuedBytes - len(p.buf)
		if p.rdClosed {
			space = p.net.window - p.queuedBytes
		}
		if space <= 0 {
			p.waitLocked(p.wrDeadline)
			continue
		}
		n := min(len(b)-written, space, 1+p.rng.IntN(1500))
		var rst func()
		if p.resetAfter > 0 {
			if n >= p.resetAfter {
				n, rst, p.onReset = p.resetAfter, p.onReset, nil
				p.resetAfter = 0
			} else {
				p.resetAfter -= n
			}
		}
		p.enqueueLocked(slices.Clone(b[written:written+n]), false, rst)
		written += n
	}
	return written, nil
}

type simConn struct {
	link          *simLink
	in, out       *simPipe
	local, remote simAddr
	closeOnce     sync.Once
}

func (c *simConn) Read(b []byte) (int, error)  { return c.in.read(b) }
func (c *simConn) Write(b []byte) (int, error) { return c.out.write(b) }
func (c *simConn) LocalAddr() net.Addr         { return c.local }
func (c *simConn) RemoteAddr() net.Addr        { return c.remote }

// Close is graceful: octets in flight are delivered, then FIN.
func (c *simConn) Close() error {
	closed := false
	c.closeOnce.Do(func() {
		closed = true
		c.in.mu.Lock()
		c.in.rdClosed = true
		c.in.buf = nil
		c.in.notifyLocked()
		c.in.mu.Unlock()
		c.out.mu.Lock()
		c.out.wrClosed = true
		if !c.out.isReset && !c.out.finQueued {
			c.out.finQueued = true
			c.out.enqueueLocked(nil, true, nil)
		}
		c.out.notifyLocked()
		c.out.mu.Unlock()
	})
	if !closed {
		return &net.OpError{Op: "close", Net: "sim", Err: net.ErrClosed}
	}
	return nil
}

func (c *simConn) SetDeadline(t time.Time) error {
	c.SetReadDeadline(t)
	return c.SetWriteDeadline(t)
}

func (c *simConn) SetReadDeadline(t time.Time) error {
	c.in.mu.Lock()
	defer c.in.mu.Unlock()
	c.in.rdDeadline = t
	c.in.notifyLocked()
	return nil
}

func (c *simConn) SetWriteDeadline(t time.Time) error {
	c.out.mu.Lock()
	defer c.out.mu.Unlock()
	c.out.wrDeadline = t
	c.out.notifyLocked()
	return nil
}

func (l *simLink) String() string { return strings.TrimSuffix(l.c2s.name, ">") }
