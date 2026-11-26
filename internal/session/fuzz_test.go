package session_test

import (
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/jacoelho/stomp/internal/session"
	"github.com/jacoelho/stomp/stomptext"
)

// model is the session as the specs describe it, in sets: which subscription
// ids are active or draining (SUB-4, SUB-16), which receipts are pending
// (SND-5), which transaction handles are open (TX-5, TX-7), and whether
// DISCONNECT was built (CON-25).
type model struct {
	v             stomptext.Version
	active        map[string]bool
	seqs          map[string]uint64 // subscription id to the sequence number of its latest Subscribe
	nextSeq       uint64
	draining      map[string]bool
	pending       map[string]session.Pending
	issued        map[string]bool
	open          map[string]int // transaction id to the index of its open handle
	handles       []session.TxHandle
	disconnecting bool
}

var (
	fuzzSubIDs = []string{"a", "b", "c"}
	fuzzTxIDs  = []string{"t1", "t2"}
)

func FuzzSession(f *testing.F) {
	f.Add([]byte{0, 0, 2, 2, 0, 1, 1, 0, 0, 3, 1, 1, 0, 0, 0})
	f.Add([]byte{6, 0, 0, 7, 0, 0, 6, 0, 0, 8, 0, 0, 10, 0, 0, 0, 1, 1, 9, 0, 0})
	f.Add([]byte{1, 0, 0, 0, 1, 1, 5, 1, 3, 11, 1, 0, 2, 1, 1, 4, 0, 1})
	f.Fuzz(func(t *testing.T, ops []byte) {
		runSessionTrace(t, ops)
	})
}

func TestSessionProperties(t *testing.T) {
	for seed := range 500 {
		ops := make([]byte, 0, 120)
		x := uint32(seed*2654435761 + 1)
		for range cap(ops) {
			x ^= x << 13
			x ^= x >> 17
			x ^= x << 5
			ops = append(ops, byte(x))
		}
		runSessionTrace(t, ops)
		if t.Failed() {
			t.Fatalf("seed %d: ops %v", seed, ops)
		}
	}
}

func runSessionTrace(t *testing.T, ops []byte) {
	t.Helper()
	m := &model{
		v: stomptext.Version12, active: map[string]bool{}, seqs: map[string]uint64{}, draining: map[string]bool{},
		pending: map[string]session.Pending{}, issued: map[string]bool{}, open: map[string]int{},
	}
	if len(ops) > 0 && ops[0]&0x80 != 0 {
		m.v = stomptext.Version11
	}
	s := session.New(1, session.Accepted{Version: m.v})
	for i := 0; i+2 < len(ops); i += 3 {
		op, a, b := ops[i]%12, ops[i+1], ops[i+2]
		step := m.apply(t, s, op, a, b)
		if got := slices.Sorted(s.Outstanding()); !slices.Equal(got, slices.Sorted(maps.Keys(m.pending))) {
			t.Fatalf("step %d (%s): pending receipts %v; model %v", i/3, step, got, slices.Sorted(maps.Keys(m.pending)))
		}
		if t.Failed() {
			t.Fatalf("step %d (%s) failed; ops %v", i/3, step, ops)
		}
	}
}

// issue records a receipt id returned by the session, which must be fresh.
func (m *model) issue(t *testing.T, r string, p session.Pending) {
	t.Helper()
	if r == "" || m.issued[r] {
		t.Errorf("receipt id %q empty or issued twice", r)
	}
	m.issued[r] = true
	m.pending[r] = p
}

func want(t *testing.T, what string, err, expected error) {
	t.Helper()
	if expected == nil && err != nil || expected != nil && !errors.Is(err, expected) {
		t.Errorf("%s: error %v; want %v", what, err, expected)
	}
}

func (m *model) apply(t *testing.T, s *session.Session, op, a, b byte) string {
	t.Helper()
	sub := fuzzSubIDs[int(a)%len(fuzzSubIDs)]
	switch op {
	case 0:
		mode := session.Client + session.Mode(b%2)
		_, r, err := s.Subscribe(sub, "/q", mode, nil)
		_, isActive := m.active[sub]
		switch {
		case m.disconnecting:
			want(t, "subscribe", err, session.ErrDisconnecting)
		case isActive || m.draining[sub]:
			want(t, "subscribe "+sub+" in use", err, session.ErrDuplicateID)
		default:
			want(t, "subscribe", err, nil)
			m.active[sub] = true
			m.nextSeq++
			m.seqs[sub] = m.nextSeq
			m.issue(t, r, session.Pending{Kind: session.KindSubscribe, Sub: sub})
		}
		return "subscribe " + sub
	case 1:
		_, r, err := s.Unsubscribe(sub)
		_, isActive := m.active[sub]
		switch {
		case m.disconnecting:
			want(t, "unsubscribe", err, session.ErrDisconnecting)
		case !isActive:
			want(t, "unsubscribe "+sub, err, session.ErrNotSubscribed)
		default:
			want(t, "unsubscribe", err, nil)
			delete(m.active, sub)
			m.draining[sub] = true
			m.issue(t, r, session.Pending{Kind: session.KindUnsubscribe, Sub: sub})
		}
		return "unsubscribe " + sub
	case 2:
		raw := "MESSAGE\nsubscription:" + sub + "\n"
		if b&1 != 0 {
			raw += "ack:K\n"
		}
		if b&2 != 0 {
			raw += "message-id:M\n"
		}
		ev, err := s.Receive(decode(t, raw+"\n\x00"))
		isActive := m.active[sub]
		ackHeader, ackID := b&1 != 0, "K"
		if m.v == stomptext.Version11 {
			ackHeader, ackID = b&2 != 0, "M"
		}
		switch {
		case m.draining[sub] || !isActive:
			if err != nil || ev.Kind != session.EventDiscard || ev.Sub != sub {
				t.Errorf("MESSAGE for draining or unknown %s = %+v, %v; want discard (SUB-2, SUB-16)", sub, ev, err)
			}
		case !ackHeader:
			want(t, "MESSAGE without ack id", err, session.ErrProtocol)
		default:
			if err != nil || ev.Kind != session.EventDeliver || ev.Sub != sub || ev.Seq != m.seqs[sub] || ev.AckID != ackID {
				t.Errorf("MESSAGE for active %s = %+v, %v; want deliver with ack id %q", sub, ev, err, ackID)
			}
		}
		return "message " + sub
	case 3, 4:
		r := m.pick(a, b)
		cmd := "RECEIPT"
		if op == 4 {
			cmd = "ERROR"
		}
		ev, err := s.Receive(decode(t, cmd+"\nreceipt-id:"+r+"\n\n\x00"))
		p, known := m.pending[r]
		switch {
		case op == 4:
			if err != nil || ev.Kind != session.EventError || ev.Known != known || (known && ev.Pending != p) {
				t.Errorf("ERROR for %q = %+v, %v; want known=%v %+v", r, ev, err, known, p)
			}
		case !known:
			want(t, "RECEIPT for "+r, err, session.ErrProtocol)
		default:
			if err != nil || ev.Kind != session.EventReceipt || ev.Pending != p {
				t.Errorf("RECEIPT for %q = %+v, %v; want %+v", r, ev, err, p)
			}
			if p.Kind == session.KindUnsubscribe {
				delete(m.draining, p.Sub)
			}
		}
		delete(m.pending, r)
		return cmd + " " + r
	case 5:
		gen := uint64(1 + b&1)
		nack, wait := b&2 != 0, b&4 != 0
		seq := m.seqs[sub]
		if b&8 != 0 {
			seq--
		}
		f, r, err := s.Ack(session.AckHandle{Gen: gen, Sub: sub, Seq: seq, ID: "K"}, nack, wait)
		_, isActive := m.active[sub]
		switch {
		case m.disconnecting:
			want(t, "ack", err, session.ErrDisconnecting)
		case gen != 1 || !isActive || seq != m.seqs[sub]:
			want(t, "ack of stale or inactive "+sub, err, session.ErrStaleAck)
		default:
			want(t, "ack", err, nil)
			if wait {
				m.issue(t, r, session.Pending{Kind: session.KindAck, Sub: sub})
			} else if r != "" {
				t.Errorf("Ack without wait returned receipt %q", r)
			}
			if f.Command != stomptext.Ack && !nack || f.Command != stomptext.Nack && nack {
				t.Errorf("ack frame %s; nack %v", f.Command, nack)
			}
		}
		return "ack " + sub
	case 6:
		id := fuzzTxIDs[int(a)%len(fuzzTxIDs)]
		_, h, r, err := s.Begin(id)
		_, isOpen := m.open[id]
		switch {
		case m.disconnecting:
			want(t, "begin", err, session.ErrDisconnecting)
		case isOpen:
			want(t, "begin open "+id, err, session.ErrInvalidRequest)
		default:
			want(t, "begin", err, nil)
			m.handles = append(m.handles, h)
			m.open[id] = len(m.handles) - 1
			m.issue(t, r, session.Pending{Kind: session.KindBegin, Tx: id})
		}
		return "begin " + id
	case 7, 8:
		if len(m.handles) == 0 {
			return "no handle"
		}
		idx := int(a) % len(m.handles)
		h := m.handles[idx]
		if b&8 != 0 {
			h.Gen = 2
		}
		var r string
		var err error
		if op == 7 {
			_, r, err = s.Finish(h, b&1 != 0)
		} else {
			_, err = s.SendTx(h, session.Outgoing{Destination: "/q"})
		}
		cur, isOpen := m.open[h.ID]
		switch {
		case m.disconnecting:
			want(t, "tx op", err, session.ErrDisconnecting)
		case h.Gen != 1:
			want(t, "tx op from another session", err, session.ErrTxAborted)
		case !isOpen || cur != idx:
			want(t, "tx op on finished handle", err, session.ErrTxDone)
		default:
			want(t, "tx op", err, nil)
			if op == 7 {
				delete(m.open, h.ID)
				kind := session.KindCommit
				if b&1 != 0 {
					kind = session.KindAbort
				}
				m.issue(t, r, session.Pending{Kind: kind, Tx: h.ID})
			}
		}
		return "tx op"
	case 9:
		dest := "/q"
		if b&1 != 0 {
			dest = ""
		}
		_, r, err := s.Send(session.Outgoing{Destination: dest})
		switch {
		case m.disconnecting:
			want(t, "send", err, session.ErrDisconnecting)
		case dest == "":
			want(t, "send without destination", err, session.ErrInvalidRequest)
		default:
			want(t, "send", err, nil)
			m.issue(t, r, session.Pending{Kind: session.KindSend})
		}
		return "send"
	case 10:
		_, r, err := s.Disconnect()
		if m.disconnecting {
			want(t, "second disconnect", err, session.ErrDisconnecting)
		} else {
			want(t, "disconnect", err, nil)
			m.disconnecting = true
			m.issue(t, r, session.Pending{Kind: session.KindDisconnect})
		}
		return "disconnect"
	default:
		s.End(sub)
		if _, ok := m.active[sub]; ok {
			delete(m.active, sub)
			m.draining[sub] = true
		}
		return "end " + sub
	}
}

// pick returns a pending receipt id when b asks for one, otherwise an id the
// session never issued.
func (m *model) pick(a, b byte) string {
	if b&1 != 0 && len(m.pending) > 0 {
		ids := slices.Sorted(maps.Keys(m.pending))
		return ids[int(a)%len(ids)]
	}
	return strings.Repeat("9", 1+int(a)%3) + "-x"
}
