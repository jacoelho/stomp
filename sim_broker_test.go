package stomp_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jacoelho/stomp/stomptext"
)

const (
	simQueue    = "/queue/q"
	simPrefetch = 4
)

// simKey is the identity of a message body: the producer's id before any padding.
func simKey(body []byte) string {
	s := string(body)
	if i := strings.IndexByte(s, '|'); i >= 0 {
		return s[:i]
	}
	return s
}

type simViolation struct {
	prop, spec, evidence string
}

// simBroker is a STOMP 1.1 and 1.2 broker with one durable queue, simQueue.
// All state is under mu; each connection's reader and writer goroutines hold
// it only to apply a frame or take queued output.
type simBroker struct {
	net              *simNet
	logf             func(string, ...any)
	msgBeforeReceipt bool
	// returned reports whether the application got a delivery from ReadMessage.
	returned func(delivery string) bool

	mu        sync.Mutex
	rng       *rand.Rand
	conns     map[*simBrokerConn]bool
	queue     []*simMsg
	rr        []*simBrokerSub
	rrNext    int
	nextMsg   int
	nextDeliv int
	nextConn  int
	slowUntil time.Time
	slowMax   time.Duration
	stopped   bool

	accepted   map[string]string // message key to broker message id
	sends      map[string]int    // message key to SEND frames carrying it
	acks       map[string]int    // message id to ACKs that consumed it
	violations []simViolation
	stats      simBrokerStats

	wg sync.WaitGroup
}

type simBrokerStats struct {
	conns, deliveries, redeliveries, acks, nacks, receipts, truncated int
}

type simMsg struct {
	id, key     string
	body        []byte
	redelivered bool
}

type simBrokerConn struct {
	id   int
	conn net.Conn
	ver  stomptext.Version

	subs         map[string]*simBrokerSub
	deliveries   map[string]*simDelivery // every delivery made on this connection, by ack id (1.2)
	ever11       map[[2]string]bool      // 1.1: every (message-id, subscription) delivered
	open11       map[[2]string]*simDelivery
	pending      []*simDelivery // unsettled, in delivery order
	txs          map[string]*simTx
	disconnected bool
	dead         bool
	hbSend       time.Duration
	out          [][]byte
	closeAfter   bool
	wake         chan struct{}
}

type simBrokerSub struct {
	id      string
	conn    *simBrokerConn
	mode    string
	unacked int
	active  bool
}

type simDelivery struct {
	ackID, deliveryID string
	msg               *simMsg
	sub               *simBrokerSub
	settled           bool
}

type simTx struct {
	sends []*simMsg
	acks  []simTxAck
}

type simTxAck struct {
	d    *simDelivery
	nack bool
}

func newSimBroker(n *simNet, seed uint64, msgBeforeReceipt bool, returned func(string) bool, logf func(string, ...any)) *simBroker {
	return &simBroker{
		net: n, logf: logf, msgBeforeReceipt: msgBeforeReceipt, returned: returned,
		rng:      rand.New(rand.NewPCG(seed, 0x62726f6b)),
		conns:    map[*simBrokerConn]bool{},
		accepted: map[string]string{},
		sends:    map[string]int{},
		acks:     map[string]int{},
	}
}

func (b *simBroker) violate(prop, spec, format string, args ...any) {
	b.violations = append(b.violations, simViolation{prop, spec, fmt.Sprintf(format, args...)})
}

func (b *simBroker) start() {
	b.net.listen()
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for {
			select {
			case c := <-b.net.accept:
				b.serve(c)
			case <-b.net.stopped:
				return
			}
		}
	}()
}

func (b *simBroker) stop() {
	b.mu.Lock()
	b.stopped = true
	var cs []*simBrokerConn
	for c := range b.conns {
		cs = append(cs, c)
	}
	b.mu.Unlock()
	for _, c := range b.net.closeListener() {
		c.Close()
	}
	for _, c := range cs {
		c.conn.Close()
	}
}

func (b *simBroker) serve(nc *simConn) {
	b.mu.Lock()
	if b.stopped {
		b.mu.Unlock()
		nc.Close()
		return
	}
	b.nextConn++
	b.stats.conns++
	c := &simBrokerConn{
		id: b.nextConn, conn: nc,
		subs: map[string]*simBrokerSub{}, deliveries: map[string]*simDelivery{},
		ever11: map[[2]string]bool{}, open11: map[[2]string]*simDelivery{},
		txs: map[string]*simTx{}, wake: make(chan struct{}, 1),
	}
	b.conns[c] = true
	b.mu.Unlock()
	b.wg.Add(2)
	go func() { defer b.wg.Done(); b.read(c) }()
	go func() { defer b.wg.Done(); b.write(c) }()
}

// simIdleReader extends the read deadline before every read, so a client
// silent for the timeout ends the connection however frames are split.
type simIdleReader struct {
	conn    net.Conn
	timeout time.Duration
	total   int64
	tail    []byte // octets read past the last decoded frame
}

func (r *simIdleReader) Read(p []byte) (int, error) {
	var deadline time.Time
	if r.timeout > 0 {
		deadline = time.Now().Add(r.timeout)
	}
	r.conn.SetReadDeadline(deadline)
	n, err := r.conn.Read(p)
	r.total += int64(n)
	r.tail = append(r.tail, p[:n]...)
	return n, err
}

func (r *simIdleReader) framed(offset int64) {
	r.tail = r.tail[len(r.tail)-int(r.total-offset):]
}

func (r *simIdleReader) truncated() bool { return len(bytes.Trim(r.tail, "\r\n")) > 0 }

func (b *simBroker) read(c *simBrokerConn) {
	idle := &simIdleReader{conn: c.conn, timeout: 10 * time.Second}
	dec := stomptext.NewDecoder(idle)
	reason := b.handshake(c, dec, idle)
	for reason == "" {
		f, err := dec.ReadFrame()
		if err != nil {
			reason = "read: " + err.Error()
			var serr *stomptext.SyntacticError
			b.mu.Lock()
			switch {
			case errors.As(err, &serr):
				b.violate("a", "FRM/CON-15", "connection %d: client wrote an invalid frame: %v", c.id, err)
			case idle.truncated():
				b.stats.truncated++
			}
			b.mu.Unlock()
			break
		}
		idle.framed(dec.InputOffset())
		reason = b.handle(c, f)
	}
	b.end(c, reason)
}

func (b *simBroker) handshake(c *simBrokerConn, dec *stomptext.Decoder, idle *simIdleReader) string {
	f, err := dec.ReadFrame()
	if err != nil {
		var serr *stomptext.SyntacticError
		if errors.As(err, &serr) {
			b.mu.Lock()
			b.violate("a", "FRM/CON-15", "connection %d: invalid CONNECT: %v", c.id, err)
			b.mu.Unlock()
		}
		return "handshake: " + err.Error()
	}
	if f.Command() != stomptext.Connect && f.Command() != stomptext.Stomp {
		b.mu.Lock()
		b.violate("a", "CON-2", "connection %d: first frame %s", c.id, f.Command())
		b.mu.Unlock()
		return "handshake: not CONNECT"
	}
	offered, _ := f.Header("accept-version")
	ver := stomptext.Version11
	if slices.Contains(strings.Split(string(offered), ","), "1.2") {
		ver = stomptext.Version12
	}
	cx, cy := 0, 0
	if hb, ok := f.Header("heart-beat"); ok {
		x, y, _ := strings.Cut(string(hb), ",")
		cx, _ = strconv.Atoi(x)
		cy, _ = strconv.Atoi(y)
	}
	const sx, sy = 1000, 1000
	if cx > 0 {
		idle.timeout = 3 * time.Duration(max(cx, sy)) * time.Millisecond
	} else {
		idle.timeout = 0
	}
	b.mu.Lock()
	c.ver = ver
	if cy > 0 {
		c.hbSend = time.Duration(max(cy, sx)) * time.Millisecond
	}
	b.outLocked(c, stomptext.Connected, stomptext.Headers{
		{Name: "version", Value: ver.String()},
		{Name: "heart-beat", Value: fmt.Sprintf("%d,%d", sx, sy)},
		{Name: "server", Value: "sim"},
	}, nil)
	b.mu.Unlock()
	dec.SetVersion(ver)
	idle.framed(dec.InputOffset())
	return ""
}

func (b *simBroker) outLocked(c *simBrokerConn, cmd stomptext.Command, hs stomptext.Headers, body []byte) {
	v := c.ver
	if cmd == stomptext.Connected {
		v = stomptext.Version12
	}
	raw, err := stomptext.AppendFrame(nil, v, cmd, hs, body)
	if err != nil {
		panic(fmt.Sprintf("sim broker: encode %s: %v", cmd, err))
	}
	c.out = append(c.out, raw)
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (b *simBroker) errorLocked(c *simBrokerConn, msg, receipt string) {
	hs := stomptext.Headers{{Name: "message", Value: msg}}
	if receipt != "" {
		hs = append(hs, stomptext.Header{Name: "receipt-id", Value: receipt})
	}
	b.outLocked(c, stomptext.Error, hs, nil)
	c.closeAfter = true
}

func (b *simBroker) write(c *simBrokerConn) {
	last := time.Now()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		b.mu.Lock()
		out, closeAfter, dead, hb := c.out, c.closeAfter, c.dead, c.hbSend
		c.out = nil
		var delay time.Duration
		if len(out) > 0 && time.Now().Before(b.slowUntil) && b.slowMax > 0 {
			delay = time.Duration(b.rng.Int64N(int64(b.slowMax)))
		}
		b.mu.Unlock()
		if dead && !closeAfter {
			return
		}
		if delay > 0 {
			time.Sleep(delay)
		}
		for _, raw := range out {
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if _, err := c.conn.Write(raw); err != nil {
				c.conn.Close()
				return
			}
			last = time.Now()
		}
		if closeAfter {
			c.conn.Close()
			return
		}
		next := time.Hour
		if hb > 0 {
			if idle := time.Since(last); idle >= hb {
				c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if _, err := c.conn.Write([]byte{'\n'}); err != nil {
					c.conn.Close()
					return
				}
				last = time.Now()
				next = hb
			} else {
				next = hb - idle
			}
		}
		timer.Reset(next)
		select {
		case <-c.wake:
		case <-timer.C:
		}
	}
}

// end leaves closing to the writer after an ERROR, so the ERROR is written first.
func (b *simBroker) end(c *simBrokerConn, reason string) {
	b.mu.Lock()
	b.dropLocked(c, reason)
	closeAfter := c.closeAfter
	b.mu.Unlock()
	if !closeAfter {
		c.conn.Close()
	}
}

func (b *simBroker) dropLocked(c *simBrokerConn, reason string) {
	if c.dead {
		return
	}
	c.dead = true
	b.logf("broker: connection %d ended: %s", c.id, reason)
	b.requeueLocked(c.pending)
	c.pending = nil
	for _, s := range c.subs {
		s.active = false
	}
	b.rr = slices.DeleteFunc(b.rr, func(s *simBrokerSub) bool { return s.conn == c })
	c.txs = nil
	delete(b.conns, c)
	select {
	case c.wake <- struct{}{}:
	default:
	}
	b.dispatchLocked()
}

// requeueLocked puts the unsettled deliveries back at the front of the queue,
// in delivery order, marked redelivered.
func (b *simBroker) requeueLocked(ds []*simDelivery) {
	var front []*simMsg
	for _, d := range slices.Clone(ds) {
		if d.settled {
			continue
		}
		b.settleLocked(d)
		d.msg.redelivered = true
		front = append(front, d.msg)
	}
	b.queue = append(front, b.queue...)
}

func (b *simBroker) settleLocked(d *simDelivery) {
	d.settled = true
	d.sub.unacked--
	c := d.sub.conn
	c.pending = slices.DeleteFunc(c.pending, func(x *simDelivery) bool { return x == d })
	if c.ver == stomptext.Version11 {
		k := [2]string{d.msg.id, d.sub.id}
		if c.open11[k] == d {
			delete(c.open11, k)
		}
	}
}

// restart resets every connection; the queue survives.
func (b *simBroker) restart(down time.Duration) {
	b.mu.Lock()
	var links []*simLink
	for c := range b.conns {
		links = append(links, c.conn.(*simConn).link)
		b.dropLocked(c, "broker restart")
	}
	b.mu.Unlock()
	b.net.refuseFor(down)
	for _, l := range links {
		b.net.reset(l)
	}
}

func (b *simBroker) slow(until time.Time, maxDelay time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.slowUntil, b.slowMax = until, maxDelay
}

func (b *simBroker) dispatchLocked() {
	for len(b.queue) > 0 && len(b.rr) > 0 {
		var s *simBrokerSub
		for range len(b.rr) {
			cand := b.rr[b.rrNext%len(b.rr)]
			b.rrNext++
			if cand.active && !cand.conn.dead && !cand.conn.disconnected && cand.unacked < simPrefetch {
				s = cand
				break
			}
		}
		if s == nil {
			return
		}
		m := b.queue[0]
		b.queue = b.queue[1:]
		b.deliverLocked(s, m)
	}
}

func (b *simBroker) deliverLocked(s *simBrokerSub, m *simMsg) {
	c := s.conn
	b.nextDeliv++
	b.stats.deliveries++
	if m.redelivered {
		b.stats.redeliveries++
	}
	d := &simDelivery{deliveryID: "d" + strconv.Itoa(b.nextDeliv), msg: m, sub: s}
	hs := stomptext.Headers{
		{Name: "subscription", Value: s.id},
		{Name: "message-id", Value: m.id},
		{Name: "destination", Value: simQueue},
	}
	if c.ver == stomptext.Version12 {
		d.ackID = d.deliveryID
		hs = append(hs, stomptext.Header{Name: "ack", Value: d.ackID})
	} else {
		d.ackID = m.id
	}
	hs = append(hs,
		stomptext.Header{Name: "x-delivery", Value: d.deliveryID},
		stomptext.Header{Name: stomptext.HeaderContentLength, Value: strconv.Itoa(len(m.body))},
	)
	if m.redelivered {
		hs = append(hs, stomptext.Header{Name: "redelivered", Value: "true"})
	}
	b.outLocked(c, stomptext.Message, hs, m.body)
	if s.mode == "auto" {
		b.acks[m.id]++
		return
	}
	s.unacked++
	c.pending = append(c.pending, d)
	if c.ver == stomptext.Version12 {
		c.deliveries[d.ackID] = d
	} else {
		k := [2]string{m.id, s.id}
		c.ever11[k] = true
		c.open11[k] = d
	}
}

func (b *simBroker) handle(c *simBrokerConn, f stomptext.Frame) string {
	h := func(name string) string { v, _ := f.Header(name); return string(v) }
	receipt := h("receipt")
	b.mu.Lock()
	defer b.mu.Unlock()
	if c.dead {
		return "dead"
	}
	if c.disconnected {
		b.violate("b", "CON-25", "connection %d: %s after DISCONNECT", c.id, f.Command())
	}
	switch f.Command() {
	case stomptext.Send:
		key := simKey(f.Body())
		b.sends[key]++
		if b.sends[key] > 1 {
			b.violate("c", "SND-9", "connection %d: SEND %q reached the broker %d times", c.id, key, b.sends[key])
		}
		if h("destination") != simQueue {
			b.errorLocked(c, "unknown destination", receipt)
			return "sent ERROR"
		}
		b.nextMsg++
		m := &simMsg{id: "m" + strconv.Itoa(b.nextMsg), key: key, body: slices.Clone(f.Body())}
		if tx := h("transaction"); tx != "" {
			t := c.txs[tx]
			if t == nil {
				b.violate("f", "TX-2", "connection %d: SEND %q in unknown transaction %q", c.id, key, tx)
				b.errorLocked(c, "unknown transaction", receipt)
				return "sent ERROR"
			}
			t.sends = append(t.sends, m)
		} else {
			b.enqueueLocked(m)
		}
	case stomptext.Subscribe:
		id := h("id")
		if _, dup := c.subs[id]; dup {
			b.violate("e", "SUB-4", "connection %d: SUBSCRIBE reuses active id %q", c.id, id)
			b.errorLocked(c, "duplicate subscription", receipt)
			return "sent ERROR"
		}
		s := &simBrokerSub{id: id, conn: c, mode: h("ack"), active: true}
		if s.mode == "" {
			s.mode = "auto"
		}
		c.subs[id] = s
		b.rr = append(b.rr, s)
		if b.msgBeforeReceipt {
			b.dispatchLocked()
		}
	case stomptext.Unsubscribe:
		id := h("id")
		s := c.subs[id]
		if s == nil {
			b.errorLocked(c, "unknown subscription", receipt)
			return "sent ERROR"
		}
		delete(c.subs, id)
		s.active = false
		b.rr = slices.DeleteFunc(b.rr, func(x *simBrokerSub) bool { return x == s })
		var mine []*simDelivery
		for _, d := range c.pending {
			if d.sub == s {
				mine = append(mine, d)
			}
		}
		b.requeueLocked(mine)
	case stomptext.Ack, stomptext.Nack:
		nack := f.Command() == stomptext.Nack
		d, ok := b.resolveAckLocked(c, f, h)
		if !ok {
			b.errorLocked(c, "acknowledgement of a delivery not made on this connection", receipt)
			return "sent ERROR"
		}
		if tx := h("transaction"); tx != "" {
			t := c.txs[tx]
			if t == nil {
				b.violate("f", "TX-2", "connection %d: %s in unknown transaction %q", c.id, f.Command(), tx)
				b.errorLocked(c, "unknown transaction", receipt)
				return "sent ERROR"
			}
			if d != nil && !nack {
				b.checkReturnedLocked(c, d, true)
			}
			t.acks = append(t.acks, simTxAck{d: d, nack: nack})
		} else if d != nil {
			if !nack {
				b.checkReturnedLocked(c, d, false)
			}
			b.ackLocked(d, nack)
		}
	case stomptext.Begin:
		tx := h("transaction")
		if _, dup := c.txs[tx]; dup || tx == "" {
			b.violate("f", "TX-1", "connection %d: BEGIN of open transaction %q", c.id, tx)
			b.errorLocked(c, "transaction already open", receipt)
			return "sent ERROR"
		}
		c.txs[tx] = &simTx{}
	case stomptext.Commit, stomptext.Abort:
		tx := h("transaction")
		t := c.txs[tx]
		if t == nil {
			b.violate("f", "TX-4", "connection %d: %s of unknown transaction %q", c.id, f.Command(), tx)
			b.errorLocked(c, "unknown transaction", receipt)
			return "sent ERROR"
		}
		delete(c.txs, tx)
		if f.Command() == stomptext.Commit {
			for _, m := range t.sends {
				b.enqueueLocked(m)
			}
			for _, a := range t.acks {
				if a.d != nil && !a.d.settled {
					b.ackLocked(a.d, a.nack)
				}
			}
		}
	case stomptext.Disconnect:
		c.disconnected = true
	default:
		b.violate("a", "CON-23", "connection %d: unexpected %s from client", c.id, f.Command())
		b.errorLocked(c, "unexpected frame", receipt)
		return "sent ERROR"
	}
	if receipt != "" {
		b.stats.receipts++
		b.outLocked(c, stomptext.Receipt, stomptext.Headers{{Name: "receipt-id", Value: receipt}}, nil)
	}
	b.dispatchLocked()
	return ""
}

func (b *simBroker) enqueueLocked(m *simMsg) {
	b.accepted[m.key] = m.id
	b.queue = append(b.queue, m)
}

// resolveAckLocked finds the delivery an ACK or NACK names. ok is false when
// the connection never made that delivery (SUB-21); d is nil when it was made
// but is already settled.
func (b *simBroker) resolveAckLocked(c *simBrokerConn, f stomptext.Frame, h func(string) string) (d *simDelivery, ok bool) {
	if c.ver == stomptext.Version12 {
		id := h("id")
		d = c.deliveries[id]
		if d == nil {
			b.violate("e", "SUB-21", "connection %d: %s id %q names no delivery made on this connection", c.id, f.Command(), id)
			return nil, false
		}
	} else {
		k := [2]string{h("message-id"), h("subscription")}
		if !c.ever11[k] {
			b.violate("e", "SUB-21/SUB-27", "connection %d: %s message-id %q subscription %q names no delivery made on this connection", c.id, f.Command(), k[0], k[1])
			return nil, false
		}
		d = c.open11[k]
		if d == nil {
			return nil, true
		}
	}
	if !d.sub.active {
		b.violate("e", "SUB-16/SUB-21", "connection %d: %s of delivery %s after its subscription %q was unsubscribed", c.id, f.Command(), d.deliveryID, d.sub.id)
		return nil, true
	}
	if d.settled {
		return nil, true
	}
	return d, true
}

// checkReturnedLocked records an ACK, of d or cumulatively of earlier
// deliveries, that reached the broker before ReadMessage returned the delivery.
func (b *simBroker) checkReturnedLocked(c *simBrokerConn, d *simDelivery, inTx bool) {
	covered := []*simDelivery{d}
	if d.sub.mode == "client" {
		covered = covered[:0]
		for _, x := range c.pending {
			if x.sub == d.sub {
				covered = append(covered, x)
			}
			if x == d {
				break
			}
		}
	}
	for _, x := range covered {
		if !b.returned(x.deliveryID) {
			b.violate("e", "SUB-9/SUB-28", "connection %d: ACK (tx %v) acknowledges delivery %s of %s (%s mode, subscription %s) before ReadMessage returned it",
				c.id, inTx, x.deliveryID, x.msg.key, d.sub.mode, d.sub.id)
		}
	}
}

func (b *simBroker) ackLocked(d *simDelivery, nack bool) {
	if nack {
		b.stats.nacks++
		b.requeueLocked([]*simDelivery{d})
		return
	}
	ds := []*simDelivery{d}
	if d.sub.mode == "client" {
		ds = ds[:0]
		for _, x := range d.sub.conn.pending {
			if x.sub == d.sub {
				ds = append(ds, x)
			}
			if x == d {
				break
			}
		}
	}
	for _, x := range slices.Clone(ds) {
		b.stats.acks++
		b.acks[x.msg.id]++
		b.settleLocked(x)
	}
}

func (b *simBroker) liveSubscriptions() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var ids []string
	for c := range b.conns {
		if c.dead || c.disconnected {
			continue
		}
		for id := range c.subs {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}

type simPending struct {
	deliveryID, key, sub string
	conn                 int
}

func (b *simBroker) pendingLive() []simPending {
	b.mu.Lock()
	defer b.mu.Unlock()
	var ps []simPending
	for c := range b.conns {
		if c.dead || c.disconnected {
			continue
		}
		for _, d := range c.pending {
			ps = append(ps, simPending{d.deliveryID, d.msg.key, d.sub.id, c.id})
		}
	}
	return ps
}

func (b *simBroker) describe() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var sb strings.Builder
	fmt.Fprintf(&sb, "queue %d messages; %d connections", len(b.queue), len(b.conns))
	for c := range b.conns {
		fmt.Fprintf(&sb, "; conn %d (v%s, disconnected %v) subs", c.id, c.ver, c.disconnected)
		for id, s := range c.subs {
			fmt.Fprintf(&sb, " %s[%s unacked %d]", id, s.mode, s.unacked)
		}
	}
	return sb.String()
}
