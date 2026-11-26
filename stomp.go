// Package stomp is a STOMP 1.1 and 1.2 client that reconnects on its own.
//
// Open connects through a DialFunc and keeps a session running until Close:
// after a connection loss it dials again with backoff and re-subscribes every
// active subscription. Sends and acknowledgements are never repeated
// (specs/decisions/0006).
//
// Send and Subscribe return after the broker's RECEIPT; Subscribe also returns
// once a MESSAGE confirmed the subscription and the session or ctx then ended.
// A call interrupted by a connection loss after its frame was buffered returns
// ErrOutcomeUnknown: the broker may or may not have processed it.
//
// Messages are owned by the caller. The client never discards a message it
// received unless an option permits it, and never stops reading for a slow
// consumer: a Subscription queues every message, so its memory is bounded by
// the broker's flow-control window (specs/subscribe.md SUB-8, SUB-9;
// specs/decisions/0004). PrefetchHeader writes that window from Capacity, or
// set the broker's header in SubscribeRequest.Headers. EndOnOverflow instead
// ends a subscription whose queue reaches Capacity.
//
// Delivery is at least once: no message is acknowledged to the broker before
// ReadMessage has stored it.
//
// Behaviour is specified in specs/*.md.
package stomp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"time"

	"github.com/jacoelho/stomp/internal/opts"
	"github.com/jacoelho/stomp/internal/session"
	"github.com/jacoelho/stomp/stomptext"
)

// Errors reported by the client.
var (
	ErrClosed            = errors.New("stomp: client closed")
	ErrOutcomeUnknown    = errors.New("stomp: frame buffered or written, outcome unknown")
	ErrOverflow          = errors.New("stomp: subscription capacity exceeded")
	ErrUnsubscribed      = errors.New("stomp: unsubscribed")
	ErrHeartbeatTimeout  = errors.New("stomp: no data from broker within the heart-beat interval")
	ErrWrongSubscription = errors.New("stomp: message belongs to another subscription")
	ErrAutoAck           = errors.New("stomp: subscription uses auto acknowledgement")

	ErrStaleAck           = session.ErrStaleAck
	ErrUnsupportedVersion = session.ErrUnsupportedVersion
	ErrProtocol           = session.ErrProtocol
	ErrReservedHeader     = session.ErrReservedHeader
	ErrTextHasNUL         = session.ErrTextHasNUL
	ErrInvalidRequest     = session.ErrInvalidRequest
	ErrDuplicateID        = session.ErrDuplicateID
	ErrTxAborted          = session.ErrTxAborted
	ErrTxDone             = session.ErrTxDone
)

// Version is a STOMP protocol version.
type Version = stomptext.Version

// Protocol versions the client supports.
const (
	V11 = stomptext.Version11
	V12 = stomptext.Version12
)

// Versions sets the protocol versions offered to the broker; each session uses
// the one the broker chooses. The default offers 1.1 and 1.2. It panics when
// given no version or an unknown one.
func Versions(vs ...Version) Options {
	if len(vs) == 0 {
		panic("stomp: Versions with no version")
	}
	set := make([]uint8, 0, len(vs))
	for _, v := range vs {
		if v != V11 && v != V12 {
			panic(fmt.Sprintf("stomp: Versions(%v)", v))
		}
		if !slices.Contains(set, uint8(v)) {
			set = append(set, uint8(v))
		}
	}
	slices.Sort(set)
	return opts.Func(func(s *opts.Struct) { s.Versions = set })
}

// Header is one header entry.
type Header = stomptext.Header

// Headers is an ordered list of header entries; Get returns the first match.
type Headers = stomptext.Headers

// ServerError is an ERROR frame sent by the broker. The broker closes the
// connection after sending it.
type ServerError struct {
	Message string // the "message" header
	Headers Headers
	Body    []byte
}

func (e *ServerError) Error() string { return "stomp: broker error: " + e.Message }

// Options configures a Client. Frame limits from package stomptext are accepted
// too. Later options override earlier ones.
type Options = opts.Options

// Login sets the login and passcode headers of CONNECT.
func Login(login, passcode string) Options {
	return opts.Func(func(s *opts.Struct) { s.Login, s.Passcode = login, passcode })
}

// ClientID sets the client-id header of CONNECT, used by brokers to key durable subscriptions.
func ClientID(id string) Options { return opts.Func(func(s *opts.Struct) { s.ClientID = id }) }

// PrefetchHeader names the SUBSCRIBE header through which the broker limits a
// subscription's unacknowledged messages, counted in messages: RabbitMQ
// "prefetch-count" or ActiveMQ Classic "activemq.prefetchSize". Every SUBSCRIBE
// then carries it with the subscription's Capacity, unless
// SubscribeRequest.Headers sets that header itself. The broker then stops
// sending while Capacity messages wait for acknowledgement, so a slow consumer
// slows delivery and the queue stays near Capacity. The window counts
// messages, not bytes, and messages kept from an ended session stay queued
// while the next session opens a fresh window, so the queue can exceed
// Capacity after a reconnect (specs/subscribe.md SUB-35).
//
// Artemis counts its window, consumer-window-size, in bytes, so PrefetchHeader
// does not apply to it; set consumer-window-size in SubscribeRequest.Headers. An
// empty name, the default, writes no header. PrefetchHeader panics on a name
// the client writes itself, such as id or ack.
func PrefetchHeader(name string) Options {
	if name != "" {
		if err := session.CheckSubscribeHeaders(Headers{{Name: name}}); err != nil {
			panic(fmt.Sprintf("stomp: PrefetchHeader(%q): %v", name, err))
		}
	}
	return opts.Func(func(s *opts.Struct) { s.PrefetchHeader = name })
}

// VirtualHost sets the host header of CONNECT. The default is "/".
func VirtualHost(host string) Options { return opts.Func(func(s *opts.Struct) { s.Host = host }) }

// Heartbeat sets the heart-beat intervals offered to the broker: how often the
// client can send and how often it wants to receive. Zero disables a
// direction. The default is 10 seconds each. It panics on negative values.
func Heartbeat(send, expect time.Duration) Options {
	if send < 0 || expect < 0 {
		panic("stomp: Heartbeat with a negative interval")
	}
	return opts.Func(func(s *opts.Struct) { s.HeartbeatSend, s.HeartbeatExpect = send, expect })
}

// HandshakeTimeout bounds each dial and CONNECT exchange. The default is 10 seconds.
func HandshakeTimeout(d time.Duration) Options {
	mustPositive("HandshakeTimeout", d)
	return opts.Func(func(s *opts.Struct) { s.HandshakeTimeout = d })
}

// WriteTimeout bounds each write to the transport; exceeding it ends the
// session. The default is 10 seconds.
func WriteTimeout(d time.Duration) Options {
	mustPositive("WriteTimeout", d)
	return opts.Func(func(s *opts.Struct) { s.WriteTimeout = d })
}

// WriteBufferLimit bounds the octets buffered for the writer; calls wait,
// bounded by their context, while it is full. ReadMessage's automatic
// acknowledgements, one per message stored, do not wait and may exceed it.
// The default is 1 MiB.
func WriteBufferLimit(n int) Options {
	mustPositive("WriteBufferLimit", time.Duration(n))
	return opts.Func(func(s *opts.Struct) { s.WriteBufferLimit = n })
}

// Backoff sets the reconnection delay: attempt n waits a random duration up to
// min(max, base·2ⁿ). The defaults are 100 ms and 30 seconds.
func Backoff(base, max time.Duration) Options {
	mustPositive("Backoff base", base)
	mustPositive("Backoff max", max)
	return opts.Func(func(s *opts.Struct) { s.BaseBackoff, s.MaxBackoff = base, max })
}

// MaxAttempts ends the Client after n consecutive failed connection attempts.
// A session that ends before lasting MaxBackoff counts as a failed attempt;
// one that lasts longer starts the count over. The default, 0, retries until
// Close.
func MaxAttempts(n int) Options {
	if n < 0 {
		panic("stomp: MaxAttempts with a negative count")
	}
	return opts.Func(func(s *opts.Struct) { s.MaxAttempts = n })
}

// Logger sets the logger for session transitions. The default is slog.Default().
func Logger(l *slog.Logger) Options { return opts.Func(func(s *opts.Struct) { s.Logger = l }) }

// OnState registers a function called synchronously on every state change,
// while the Client holds its lock. It must not block or call the Client.
func OnState(f func(State, error)) Options {
	return opts.Func(func(s *opts.Struct) { s.OnState = func(st int, err error) { f(State(st), err) } })
}

func mustPositive(name string, d time.Duration) {
	if d < 1 {
		panic("stomp: " + name + " must be positive")
	}
}

// State is the Client's connection state.
type State int

// Client states.
const (
	Connecting State = iota // dialling or handshaking
	Connected               // a session is running
	Waiting                 // backing off before the next attempt
	Closed                  // terminal; see Client.Err
)

func (s State) String() string {
	switch s {
	case Connecting:
		return "connecting"
	case Connected:
		return "connected"
	case Waiting:
		return "waiting"
	case Closed:
		return "closed"
	}
	return fmt.Sprintf("State(%d)", int(s))
}

// DialFunc opens a transport to a broker. It is where TLS, failover between
// brokers and test transports plug in. attempt is the number of consecutive
// failed connection attempts before this one, counted as for MaxAttempts: 0
// for Open's first attempt and for the first attempt after a session that
// lasted MaxBackoff (specs/reconnect.md RCN-17).
type DialFunc func(ctx context.Context, attempt int) (net.Conn, error)

// TCP returns a DialFunc that dials addrs[attempt % len(addrs)]: the first
// address after a session that lasted, and the next address after each failed
// attempt. It never moves off a broker whose sessions last. To spread clients
// across brokers, shuffle addrs before calling TCP. It panics if addrs is
// empty.
func TCP(addrs ...string) DialFunc {
	if len(addrs) == 0 {
		panic("stomp: TCP with no addresses")
	}
	return func(ctx context.Context, attempt int) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addrs[attempt%len(addrs)])
	}
}

// Outgoing is a message to send.
type Outgoing struct {
	Destination string
	ContentType string
	// Headers are written after the client's own headers. destination,
	// content-length, content-type, receipt and transaction are reserved.
	Headers Headers
	// Body is sent unchanged. The caller must not modify it until Send returns.
	Body []byte
	// Text omits content-length, which Artemis and ActiveMQ map to a
	// TextMessage. The body must then not contain NUL.
	Text bool
}

// AckMode is a subscription's acknowledgement mode.
type AckMode int

// Acknowledgement modes.
const (
	// AckAuto acknowledges each message as ReadMessage stores it. On the wire the
	// subscription is client-individual, so a message is never consumed at
	// the broker before the application has it (specs/subscribe.md SUB-28).
	AckAuto             AckMode = iota
	AckClient                   // Ack acknowledges the message and every earlier one
	AckClientIndividual         // Ack acknowledges one message
)

func (m AckMode) session() (session.Mode, bool) {
	switch m {
	case AckAuto:
		return session.ClientIndividual, true
	case AckClient:
		return session.Client, true
	case AckClientIndividual:
		return session.ClientIndividual, true
	}
	return 0, false
}

// SubscribeRequest describes a subscription.
type SubscribeRequest struct {
	// ID is the subscription id; empty generates "sub-N". A stable ID keeps
	// durable subscriptions addressable across restarts. MESSAGE frames carry
	// only the id, so reusing an ID in the same session can deliver a late
	// message of the earlier subscription to the new one (specs/subscribe.md
	// SUB-49); generated ids are never reused.
	ID          string
	Destination string
	Ack         AckMode
	// Capacity is the number of queued messages at which EndOnOverflow ends
	// the subscription, and the window PrefetchHeader writes; the default is
	// 256. Without either option it bounds nothing.
	Capacity int
	// CapacityBytes is the total of queued bodies, in octets, at which
	// EndOnOverflow ends the subscription; the default is 16 MiB. With
	// EndOnOverflow the queue's body storage has a capacity of at most 1.25
	// times CapacityBytes, and while it reallocates the old storage is also
	// held (SUB-59); without it, it bounds nothing.
	CapacityBytes int
	// EndOnOverflow makes a MESSAGE that finds Capacity messages or
	// CapacityBytes queued end the subscription with ErrOverflow and send
	// UNSUBSCRIBE. Messages that arrive until the broker confirms the
	// UNSUBSCRIBE are discarded unacknowledged, which loses them on a
	// destination that keeps no copy, such as a non-durable topic. Without it
	// the client queues every message it receives, and memory is bounded only
	// by the broker's window (specs/subscribe.md SUB-8, SUB-45).
	EndOnOverflow bool
	// Headers are written after the client's own; id, destination, ack and
	// receipt are reserved. Broker flow control goes here, for example
	// Artemis "consumer-window-size" or RabbitMQ "prefetch-count".
	Headers Headers
}

// Message is a received message, owned by the caller.
type Message struct {
	Destination string
	ID          string // the message-id header
	Headers     Headers
	Body        []byte

	owner *Client
	auto  bool // acknowledged by ReadMessage
	sub   string
	seq   uint64
	ack   string
	gen   uint64
}
