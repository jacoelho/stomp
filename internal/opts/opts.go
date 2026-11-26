// Package opts holds the option set shared by stomptext and stomp.
package opts

import (
	"log/slog"
	"time"
)

// Struct is the resolved configuration. Fields are read by stomptext and stomp only.
type Struct struct {
	MaxFrameSize   int
	MaxHeaderCount int
	MaxHeaderBytes int

	Versions []uint8 // stomptext.Version values, ascending; opts cannot import stomptext
	Host     string
	Login    string
	Passcode string
	ClientID string

	PrefetchHeader string // SUBSCRIBE header carrying Capacity; empty writes none

	HeartbeatSend   time.Duration
	HeartbeatExpect time.Duration

	HandshakeTimeout time.Duration
	WriteTimeout     time.Duration
	WriteBufferLimit int

	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	MaxAttempts int

	Logger  *slog.Logger
	OnState func(state int, err error)
}

// Default returns the documented defaults (specs/frame.md FRM-19, specs/connection.md, specs/reconnect.md).
func Default() Struct {
	return Struct{
		MaxFrameSize:     4 << 20,
		MaxHeaderCount:   128,
		MaxHeaderBytes:   64 << 10,
		Versions:         []uint8{11, 12},
		Host:             "/",
		HeartbeatSend:    10 * time.Second,
		HeartbeatExpect:  10 * time.Second,
		HandshakeTimeout: 10 * time.Second,
		WriteTimeout:     10 * time.Second,
		WriteBufferLimit: 1 << 20,
		BaseBackoff:      100 * time.Millisecond,
		MaxBackoff:       30 * time.Second,
	}
}

// Options is implemented only by Func, so callers outside the module cannot forge options.
type Options interface {
	JoinInto(*Struct)
}

// Func adapts a setter to Options.
type Func func(*Struct)

// JoinInto applies f.
func (f Func) JoinInto(s *Struct) { f(s) }

// Resolve applies opts in order over the defaults; later options win.
func Resolve(opts []Options) Struct {
	s := Default()
	for _, o := range opts {
		o.JoinInto(&s)
	}
	return s
}
