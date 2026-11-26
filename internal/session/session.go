// Package session holds the protocol rules of one STOMP 1.1 or 1.2 session as plain
// data (specs/decisions/0002). It performs no I/O, reads no clock and starts
// no goroutine: callers pass frames and times in and write the frames it returns.
package session

import (
	"bytes"
	"errors"
	"fmt"
	"iter"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jacoelho/stomp/stomptext"
)

// Errors returned by Session methods. Package stomp re-exports them.
var (
	ErrUnsupportedVersion = errors.New("stomp: broker did not negotiate an offered STOMP version")
	ErrProtocol           = errors.New("stomp: protocol violation")
	ErrStaleAck           = errors.New("stomp: the message's subscription or session has ended")
	ErrReservedHeader     = errors.New("stomp: header is set by the client")
	ErrTextHasNUL         = errors.New("stomp: text body contains NUL")
	ErrInvalidRequest     = errors.New("stomp: invalid request")
	ErrDuplicateID        = errors.New("stomp: subscription id in use")
	ErrDisconnecting      = errors.New("stomp: session is disconnecting")
	ErrNotSubscribed      = errors.New("stomp: subscription not active in this session")
	ErrTxAborted          = errors.New("stomp: transaction aborted by the end of its session")
	ErrTxDone             = errors.New("stomp: transaction already committed or aborted")
)

// Mode is a subscription's acknowledgement mode. Every subscription is
// acknowledged on the wire: package stomp acknowledges AckAuto messages
// itself (specs/subscribe.md SUB-28).
type Mode uint8

// Acknowledgement modes (spec SUBSCRIBE "ack" header).
const (
	Client Mode = iota + 1
	ClientIndividual
)

func (m Mode) String() string {
	if m == Client {
		return "client"
	}
	return "client-individual"
}

// Frame is a frame to write. The Headers of a frame built by Send, SendTx,
// Ack or AckTx share storage with the Session and are valid until the next
// call that builds a frame; encode it before then.
type Frame struct {
	Command stomptext.Command
	Headers stomptext.Headers
	Body    []byte
}

// Append encodes f in protocol version v onto dst.
func (f Frame) Append(dst []byte, v stomptext.Version) ([]byte, error) {
	return stomptext.AppendFrame(dst, v, f.Command, f.Headers, f.Body)
}

// Connect builds the CONNECT frame offering versions, which must be non-empty
// and ascending (specs/connection.md CON-2). Encode it as 1.2, whose rules for
// unescaped frames are the stricter.
func Connect(versions []stomptext.Version, host, login, passcode, clientID string, send, expect time.Duration) Frame {
	offered := make([]string, len(versions))
	for i, v := range versions {
		offered[i] = v.String()
	}
	hs := stomptext.Headers{
		{Name: "accept-version", Value: strings.Join(offered, ",")},
		{Name: "host", Value: host},
		{Name: "heart-beat", Value: millis(send) + "," + millis(expect)},
	}
	if login != "" {
		hs = append(hs, stomptext.Header{Name: "login", Value: login})
	}
	if passcode != "" {
		hs = append(hs, stomptext.Header{Name: "passcode", Value: passcode})
	}
	if clientID != "" {
		hs = append(hs, stomptext.Header{Name: "client-id", Value: clientID})
	}
	return Frame{Command: stomptext.Connect, Headers: hs}
}

func millis(d time.Duration) string { return strconv.FormatInt(d.Milliseconds(), 10) }

// Heartbeat holds the negotiated intervals; zero disables a direction.
type Heartbeat struct {
	Send time.Duration // client to broker
	Recv time.Duration // broker to client
}

// Negotiate applies spec "Heart-beating": each direction is MAX of the two
// values, or none when either is 0. A missing header counts as "0,0".
func Negotiate(send, expect time.Duration, header []byte, present bool) (Heartbeat, error) {
	if !present {
		return Heartbeat{}, nil
	}
	sx, sy, ok := strings.Cut(string(header), ",")
	if !ok {
		return Heartbeat{}, fmt.Errorf("%w: heart-beat %q", ErrProtocol, header)
	}
	sxMs, err1 := strconv.ParseUint(sx, 10, 31)
	syMs, err2 := strconv.ParseUint(sy, 10, 31)
	if err1 != nil || err2 != nil {
		return Heartbeat{}, fmt.Errorf("%w: heart-beat %q", ErrProtocol, header)
	}
	server := func(ms uint64) time.Duration { return time.Duration(ms) * time.Millisecond }
	pick := func(mine, theirs time.Duration) time.Duration {
		if mine == 0 || theirs == 0 {
			return 0
		}
		return max(mine, theirs)
	}
	return Heartbeat{Send: pick(send, server(syMs)), Recv: pick(expect, server(sxMs))}, nil
}

// Accepted is the outcome of a successful handshake.
type Accepted struct {
	Version   stomptext.Version
	Heartbeat Heartbeat
}

// Accept validates the broker's reply to a CONNECT that offered versions. An
// ERROR reply returns ok false so the caller can build its error from the frame.
func Accept(f stomptext.Frame, versions []stomptext.Version, send, expect time.Duration) (a Accepted, ok bool, err error) {
	switch f.Command() {
	case stomptext.Error:
		return Accepted{}, false, nil
	case stomptext.Connected:
	default:
		return Accepted{}, false, fmt.Errorf("%w: %s in reply to CONNECT", ErrProtocol, f.Command())
	}
	raw, _ := f.Header("version")
	v, known := stomptext.ParseVersion(raw)
	if !known || !slices.Contains(versions, v) {
		return Accepted{}, false, fmt.Errorf("%w (version %q)", ErrUnsupportedVersion, raw)
	}
	h, present := f.Header("heart-beat")
	hb, err := Negotiate(send, expect, h, present)
	if err != nil {
		return Accepted{}, false, err
	}
	return Accepted{Version: v, Heartbeat: hb}, true, nil
}

// Kind says what a pending receipt confirms.
type Kind uint8

// Receipt kinds.
const (
	KindSend Kind = iota + 1
	KindSubscribe
	KindUnsubscribe
	KindAck
	KindDisconnect
	KindBegin
	KindCommit
	KindAbort
)

// Pending is a frame awaiting its RECEIPT.
type Pending struct {
	Kind Kind
	Sub  string // subscription id for subscribe, unsubscribe and ack
	Tx   string // transaction id for begin, commit and abort
}

type sub struct {
	id       string
	draining bool
	seq      uint64 // distinguishes subscriptions that reuse an id within a session (SUB-31)
}

// Session is the state of one connected session.
type Session struct {
	gen      uint64
	ver      stomptext.Version
	subs     map[string]*sub
	txs      map[string]uint64 // open transactions: id to the sequence number of the Begin that opened it
	receipts map[string]Pending
	next     uint64
	nextTx   uint64
	nextSub  uint64
	// disconnecting is set once DISCONNECT is built; no frame may follow it (spec DISCONNECT).
	disconnecting bool
	scratch       stomptext.Headers // Headers of the last SEND, ACK or NACK built (see Frame)
}

func (s *Session) headers() stomptext.Headers {
	clear(s.scratch)
	return s.scratch[:0]
}

// New returns the state of session number gen after the handshake a.
func New(gen uint64, a Accepted) *Session {
	return &Session{gen: gen, ver: a.Version, subs: map[string]*sub{}, txs: map[string]uint64{}, receipts: map[string]Pending{}}
}

// Gen returns the session number.
func (s *Session) Gen() uint64 { return s.gen }

// Version returns the negotiated protocol version.
func (s *Session) Version() stomptext.Version { return s.ver }

func (s *Session) receipt(p Pending) string {
	s.next++
	id := strconv.FormatUint(s.gen, 10) + "-" + strconv.FormatUint(s.next, 10)
	s.receipts[id] = p
	return id
}

// Outgoing is a message to send.
type Outgoing struct {
	Destination string
	ContentType string
	Headers     stomptext.Headers
	Body        []byte
	Text        bool
}

var reservedSend = []string{"destination", "content-length", "content-type", "receipt", "transaction"}

// Send builds a SEND frame with a receipt (specs/send.md).
func (s *Session) Send(m Outgoing) (Frame, string, error) {
	if s.disconnecting {
		return Frame{}, "", ErrDisconnecting
	}
	hs, err := sendHeaders(s.headers(), m)
	if err != nil {
		return Frame{}, "", err
	}
	id := s.receipt(Pending{Kind: KindSend})
	hs = append(hs, stomptext.Header{Name: "receipt", Value: id})
	hs = append(hs, m.Headers...)
	s.scratch = hs
	return Frame{Command: stomptext.Send, Headers: hs, Body: m.Body}, id, nil
}

// SendTx builds a SEND frame in transaction tx, without a receipt (specs/transaction.md TX-2).
func (s *Session) SendTx(tx TxHandle, m Outgoing) (Frame, error) {
	if err := s.checkTx(tx); err != nil {
		return Frame{}, err
	}
	hs, err := sendHeaders(s.headers(), m)
	if err != nil {
		return Frame{}, err
	}
	hs = append(hs, stomptext.Header{Name: "transaction", Value: tx.ID})
	hs = append(hs, m.Headers...)
	s.scratch = hs
	return Frame{Command: stomptext.Send, Headers: hs, Body: m.Body}, nil
}

// sendHeaders validates m and returns the client's own SEND headers.
func sendHeaders(hs stomptext.Headers, m Outgoing) (stomptext.Headers, error) {
	if m.Destination == "" {
		return nil, fmt.Errorf("%w: empty destination", ErrInvalidRequest)
	}
	if err := checkReserved(m.Headers, reservedSend); err != nil {
		return nil, err
	}
	if m.Text && bytes.IndexByte(m.Body, 0) >= 0 {
		return nil, ErrTextHasNUL
	}
	hs = append(hs, stomptext.Header{Name: "destination", Value: m.Destination})
	if m.ContentType != "" {
		hs = append(hs, stomptext.Header{Name: "content-type", Value: m.ContentType})
	}
	if !m.Text {
		hs = append(hs, stomptext.Header{Name: stomptext.HeaderContentLength, Value: strconv.Itoa(len(m.Body))})
	}
	return hs, nil
}

var reservedSubscribe = []string{"id", "destination", "ack", "receipt", stomptext.HeaderContentLength}

// CheckSubscribeHeaders reports a reserved header in a subscription request.
func CheckSubscribeHeaders(hs stomptext.Headers) error { return checkReserved(hs, reservedSubscribe) }

func checkReserved(hs stomptext.Headers, reserved []string) error {
	for _, h := range hs {
		if h.Name == "" {
			return fmt.Errorf("%w: empty header name", ErrInvalidRequest)
		}
		for _, r := range reserved {
			if h.Name == r {
				return fmt.Errorf("%w: %q", ErrReservedHeader, h.Name)
			}
		}
	}
	return nil
}

// Subscribe builds a SUBSCRIBE frame and registers the subscription. An id
// still draining an UNSUBSCRIBE is in use until its receipt (SUB-4).
func (s *Session) Subscribe(id, dest string, mode Mode, extra stomptext.Headers) (Frame, string, error) {
	if s.disconnecting {
		return Frame{}, "", ErrDisconnecting
	}
	if _, used := s.subs[id]; used {
		return Frame{}, "", fmt.Errorf("%w: %q", ErrDuplicateID, id)
	}
	if err := CheckSubscribeHeaders(extra); err != nil {
		return Frame{}, "", err
	}
	s.nextSub++
	s.subs[id] = &sub{id: id, seq: s.nextSub}
	r := s.receipt(Pending{Kind: KindSubscribe, Sub: id})
	hs := make(stomptext.Headers, 0, 4+len(extra))
	hs = append(hs,
		stomptext.Header{Name: "id", Value: id},
		stomptext.Header{Name: "destination", Value: dest},
		stomptext.Header{Name: "ack", Value: mode.String()},
		stomptext.Header{Name: "receipt", Value: r},
	)
	hs = append(hs, extra...)
	return Frame{Command: stomptext.Subscribe, Headers: hs}, r, nil
}

// Unsubscribe builds an UNSUBSCRIBE frame. The subscription drains: its
// MESSAGE frames are discarded until the receipt arrives (SUB-16).
func (s *Session) Unsubscribe(id string) (Frame, string, error) {
	if s.disconnecting {
		return Frame{}, "", ErrDisconnecting
	}
	st, ok := s.subs[id]
	if !ok || st.draining {
		return Frame{}, "", fmt.Errorf("%w: %q", ErrNotSubscribed, id)
	}
	st.draining = true
	r := s.receipt(Pending{Kind: KindUnsubscribe, Sub: id})
	return Frame{Command: stomptext.Unsubscribe, Headers: stomptext.Headers{{Name: "id", Value: id}, {Name: "receipt", Value: r}}}, r, nil
}

// AckHandle identifies an acknowledgeable message.
type AckHandle struct {
	Gen uint64
	Sub string
	Seq uint64 // Event.Seq of the delivery
	ID  string // the MESSAGE "ack" header in 1.2, "message-id" in 1.1 (SUB-27)
}

// Ack builds an ACK or NACK frame; with wait it carries a receipt.
func (s *Session) Ack(h AckHandle, nack, wait bool) (Frame, string, error) {
	hs, err := s.ackHeaders(h)
	if err != nil {
		return Frame{}, "", err
	}
	var r string
	if wait {
		r = s.receipt(Pending{Kind: KindAck, Sub: h.Sub})
		hs = append(hs, stomptext.Header{Name: "receipt", Value: r})
	}
	s.scratch = hs
	return Frame{Command: ackCommand(nack), Headers: hs}, r, nil
}

// AckTx builds an ACK or NACK frame in transaction tx, without a receipt.
func (s *Session) AckTx(tx TxHandle, h AckHandle, nack bool) (Frame, error) {
	if err := s.checkTx(tx); err != nil {
		return Frame{}, err
	}
	hs, err := s.ackHeaders(h)
	if err != nil {
		return Frame{}, err
	}
	hs = append(hs, stomptext.Header{Name: "transaction", Value: tx.ID})
	s.scratch = hs
	return Frame{Command: ackCommand(nack), Headers: hs}, nil
}

func ackCommand(nack bool) stomptext.Command {
	if nack {
		return stomptext.Nack
	}
	return stomptext.Ack
}

func (s *Session) ackHeaders(h AckHandle) (stomptext.Headers, error) {
	if s.disconnecting {
		return nil, ErrDisconnecting
	}
	if h.Gen != s.gen {
		return nil, ErrStaleAck
	}
	st, ok := s.subs[h.Sub]
	if !ok || st.draining || st.seq != h.Seq {
		return nil, ErrStaleAck
	}
	hs := s.headers()
	if s.ver == stomptext.Version11 {
		return append(hs, stomptext.Header{Name: "message-id", Value: h.ID}, stomptext.Header{Name: "subscription", Value: h.Sub}), nil
	}
	return append(hs, stomptext.Header{Name: "id", Value: h.ID}), nil
}

// TxHandle identifies one Begin in session Gen. Seq distinguishes a
// transaction from a later one that reuses its id.
type TxHandle struct {
	Gen uint64
	ID  string
	Seq uint64
}

func (s *Session) checkTx(tx TxHandle) error {
	if s.disconnecting {
		return ErrDisconnecting
	}
	if tx.Gen != s.gen {
		return ErrTxAborted
	}
	if seq, open := s.txs[tx.ID]; !open || seq != tx.Seq {
		return ErrTxDone
	}
	return nil
}

// Begin builds a BEGIN frame with a receipt and opens transaction id.
func (s *Session) Begin(id string) (Frame, TxHandle, string, error) {
	if s.disconnecting {
		return Frame{}, TxHandle{}, "", ErrDisconnecting
	}
	if _, open := s.txs[id]; open {
		return Frame{}, TxHandle{}, "", fmt.Errorf("%w: transaction %q already open", ErrInvalidRequest, id)
	}
	s.nextTx++
	s.txs[id] = s.nextTx
	r := s.receipt(Pending{Kind: KindBegin, Tx: id})
	return Frame{Command: stomptext.Begin, Headers: stomptext.Headers{{Name: "transaction", Value: id}, {Name: "receipt", Value: r}}}, TxHandle{Gen: s.gen, ID: id, Seq: s.nextTx}, r, nil
}

// Finish builds a COMMIT, or with abort an ABORT, frame with a receipt and
// closes the transaction: no later frame may name it.
func (s *Session) Finish(tx TxHandle, abort bool) (Frame, string, error) {
	if err := s.checkTx(tx); err != nil {
		return Frame{}, "", err
	}
	delete(s.txs, tx.ID)
	cmd, kind := stomptext.Commit, KindCommit
	if abort {
		cmd, kind = stomptext.Abort, KindAbort
	}
	r := s.receipt(Pending{Kind: kind, Tx: tx.ID})
	return Frame{Command: cmd, Headers: stomptext.Headers{{Name: "transaction", Value: tx.ID}, {Name: "receipt", Value: r}}}, r, nil
}

// Disconnect builds a DISCONNECT frame with a receipt. A session sends at
// most one, so a second call fails with ErrDisconnecting.
func (s *Session) Disconnect() (Frame, string, error) {
	if s.disconnecting {
		return Frame{}, "", ErrDisconnecting
	}
	s.disconnecting = true
	r := s.receipt(Pending{Kind: KindDisconnect})
	return Frame{Command: stomptext.Disconnect, Headers: stomptext.Headers{{Name: "receipt", Value: r}}}, r, nil
}

// End stops delivery to a subscription without a frame, for when no
// UNSUBSCRIBE may be written. Its MESSAGE frames are discarded from then on.
func (s *Session) End(id string) {
	if st, ok := s.subs[id]; ok {
		st.draining = true
	}
}

// Event is the outcome of a received frame.
type Event struct {
	Kind    EventKind
	Sub     string  // EventDeliver: subscription id
	Seq     uint64  // EventDeliver: the subscription's sequence number in this session (AckHandle.Seq)
	AckID   string  // EventDeliver: acknowledgement id (AckHandle.ID)
	Receipt string  // EventReceipt, EventError: receipt id
	Pending Pending // EventReceipt, EventError with a known receipt
	Known   bool    // EventError: Receipt matched a pending frame
}

// EventKind classifies Event.
type EventKind uint8

// Event kinds.
const (
	EventDeliver EventKind = iota + 1
	EventDiscard
	EventReceipt
	EventError
)

// Receive applies a frame from the broker. A returned error ends the session.
func (s *Session) Receive(f stomptext.Frame) (Event, error) {
	switch f.Command() {
	case stomptext.Message:
		id, ok := f.Header("subscription")
		if !ok {
			return Event{}, fmt.Errorf("%w: MESSAGE without subscription", ErrProtocol)
		}
		st, ok := s.subs[string(id)]
		if !ok || st.draining {
			return Event{Kind: EventDiscard, Sub: string(id)}, nil
		}
		name := "ack"
		if s.ver == stomptext.Version11 {
			name = "message-id"
		}
		ack, ok := f.Header(name)
		if !ok {
			return Event{}, fmt.Errorf("%w: MESSAGE without %s on subscription %q", ErrProtocol, name, id)
		}
		return Event{Kind: EventDeliver, Sub: st.id, Seq: st.seq, AckID: string(ack)}, nil
	case stomptext.Receipt:
		id, _ := f.Header("receipt-id")
		p, ok := s.receipts[string(id)]
		if !ok {
			return Event{}, fmt.Errorf("%w: RECEIPT for unknown receipt %q", ErrProtocol, id)
		}
		delete(s.receipts, string(id))
		if p.Kind == KindUnsubscribe {
			delete(s.subs, p.Sub)
		}
		return Event{Kind: EventReceipt, Receipt: string(id), Pending: p}, nil
	case stomptext.Error:
		id, _ := f.Header("receipt-id")
		p, ok := s.receipts[string(id)]
		if ok {
			delete(s.receipts, string(id))
		}
		return Event{Kind: EventError, Receipt: string(id), Pending: p, Known: ok}, nil
	default:
		return Event{}, fmt.Errorf("%w: unexpected %s from broker", ErrProtocol, f.Command())
	}
}

// Outstanding returns the ids of the receipts still pending.
func (s *Session) Outstanding() iter.Seq[string] { return maps.Keys(s.receipts) }
