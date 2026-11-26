// Package stomptext implements syntactic processing of STOMP 1.1 and 1.2
// frames as specified in https://stomp.github.io/stomp-specification-1.1.html
// and https://stomp.github.io/stomp-specification-1.2.html.
//
// A Decoder reads frames from a stream; AppendFrame encodes one. Neither
// interprets headers beyond what framing requires: content-length and escaping.
// Session semantics belong to package stomp.
//
// Decoding is strict: undefined escapes, unknown commands, bodies on frames
// that forbid them, and frames past the configured limits are errors.
package stomptext

import (
	"errors"
	"fmt"
	"iter"

	"github.com/jacoelho/stomp/internal/opts"
)

// Version is a STOMP protocol version. The versions differ in framing only in
// line endings (1.2 also accepts CRLF) and escaping (1.2 also escapes CR).
type Version uint8

// Supported protocol versions.
const (
	Version11 Version = 11
	Version12 Version = 12
)

func (v Version) String() string {
	switch v {
	case Version11:
		return "1.1"
	case Version12:
		return "1.2"
	}
	return fmt.Sprintf("Version(%d)", uint8(v))
}

// ParseVersion returns the Version named by s, such as "1.2".
func ParseVersion[T ~string | ~[]byte](s T) (Version, bool) {
	switch string(s) {
	case "1.1":
		return Version11, true
	case "1.2":
		return Version12, true
	}
	return 0, false
}

func (v Version) known() bool { return v == Version11 || v == Version12 }

// Command is a STOMP frame command.
type Command string

// STOMP commands.
const (
	Connect     Command = "CONNECT"
	Stomp       Command = "STOMP"
	Connected   Command = "CONNECTED"
	Send        Command = "SEND"
	Subscribe   Command = "SUBSCRIBE"
	Unsubscribe Command = "UNSUBSCRIBE"
	Ack         Command = "ACK"
	Nack        Command = "NACK"
	Begin       Command = "BEGIN"
	Commit      Command = "COMMIT"
	Abort       Command = "ABORT"
	Disconnect  Command = "DISCONNECT"
	Message     Command = "MESSAGE"
	Receipt     Command = "RECEIPT"
	Error       Command = "ERROR"
)

func lookupCommand(b []byte) (Command, bool) {
	switch string(b) {
	case "CONNECT":
		return Connect, true
	case "STOMP":
		return Stomp, true
	case "CONNECTED":
		return Connected, true
	case "SEND":
		return Send, true
	case "SUBSCRIBE":
		return Subscribe, true
	case "UNSUBSCRIBE":
		return Unsubscribe, true
	case "ACK":
		return Ack, true
	case "NACK":
		return Nack, true
	case "BEGIN":
		return Begin, true
	case "COMMIT":
		return Commit, true
	case "ABORT":
		return Abort, true
	case "DISCONNECT":
		return Disconnect, true
	case "MESSAGE":
		return Message, true
	case "RECEIPT":
		return Receipt, true
	case "ERROR":
		return Error, true
	}
	return "", false
}

func (c Command) known() bool {
	_, ok := lookupCommand([]byte(c))
	return ok
}

// escaped reports whether headers of c use the escape sequences; CONNECT and
// CONNECTED do not (spec "Value Encoding", both versions).
func (c Command) escaped() bool {
	return c != Connect && c != Connected
}

func (c Command) bodyAllowed() bool {
	return c == Send || c == Message || c == Error
}

// Header is one header entry.
type Header struct {
	Name  string
	Value string
}

// Headers is an ordered list of header entries. Repeated names are allowed.
type Headers []Header

// Get returns the value of the first entry named name.
func (h Headers) Get(name string) (string, bool) {
	for _, e := range h {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

// HeaderContentLength is the header that delimits a body by length.
const HeaderContentLength = "content-length"

type span struct{ lo, hi int }

type field struct{ name, value span }

// Frame is a decoded frame. A Frame returned by Decoder.ReadFrame borrows the
// decoder's buffer and is valid only until the next ReadFrame or Reset; use
// Clone to keep it. The zero Frame has no command.
type Frame struct {
	cmd    Command
	raw    []byte
	fields []field
	body   span
}

// Command returns the frame's command.
func (f Frame) Command() Command { return f.cmd }

// Header returns the unescaped value of the first header named name.
// The result shares the frame's lifetime.
func (f Frame) Header(name string) ([]byte, bool) {
	for _, h := range f.fields {
		if string(f.raw[h.name.lo:h.name.hi]) == name {
			return f.raw[h.value.lo:h.value.hi], true
		}
	}
	return nil, false
}

// HeaderCount returns the number of header entries, repeated ones included.
func (f Frame) HeaderCount() int { return len(f.fields) }

// All yields every header entry in order, repeated ones included.
// Yielded slices share the frame's lifetime.
func (f Frame) All() iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		for _, h := range f.fields {
			if !yield(f.raw[h.name.lo:h.name.hi], f.raw[h.value.lo:h.value.hi]) {
				return
			}
		}
	}
}

// Body returns the frame body. It shares the frame's lifetime.
func (f Frame) Body() []byte { return f.raw[f.body.lo:f.body.hi] }

// Clone returns a copy of f that does not share memory with the decoder.
func (f Frame) Clone() Frame {
	c := f
	c.raw = append([]byte(nil), f.raw...)
	c.fields = append([]field(nil), f.fields...)
	return c
}

// Options configures a Decoder. The type is shared with package stomp, which
// passes frame limits through to its decoder. Later options override earlier ones.
type Options = opts.Options

// MaxFrameSize limits a frame, from the first command octet to the
// terminating NUL, to n octets. The default is 4 MiB. It panics if n < 1.
func MaxFrameSize(n int) Options {
	mustPositive("MaxFrameSize", n)
	return opts.Func(func(s *opts.Struct) { s.MaxFrameSize = n })
}

// MaxHeaderCount limits the number of header entries in a frame. The default
// is 128. It panics if n < 1.
func MaxHeaderCount(n int) Options {
	mustPositive("MaxHeaderCount", n)
	return opts.Func(func(s *opts.Struct) { s.MaxHeaderCount = n })
}

// MaxHeaderBytes limits the command line and header section of a frame to n
// octets. The default is 64 KiB. It panics if n < 1.
func MaxHeaderBytes(n int) Options {
	mustPositive("MaxHeaderBytes", n)
	return opts.Func(func(s *opts.Struct) { s.MaxHeaderBytes = n })
}

func mustPositive(name string, n int) {
	if n < 1 {
		panic(fmt.Sprintf("stomptext: %s(%d): must be at least 1", name, n))
	}
}

// Errors wrapped by SyntacticError and returned by AppendFrame.
var (
	ErrUnknownCommand       = errors.New("unknown command")
	ErrInvalidEscape        = errors.New("invalid escape sequence")
	ErrInvalidHeader        = errors.New("invalid header")
	ErrInvalidContentLength = errors.New("invalid content-length")
	ErrMissingNUL           = errors.New("frame not terminated by NUL")
	ErrBodyNotAllowed       = errors.New("body not allowed for command")
	ErrFrameTooLarge        = errors.New("frame exceeds MaxFrameSize")
	ErrTooManyHeaders       = errors.New("header count exceeds MaxHeaderCount")
	ErrHeaderTooLarge       = errors.New("header section exceeds MaxHeaderBytes")
	ErrInvalidEOL           = errors.New("carriage return not followed by line feed")
	ErrNULInBody            = errors.New("body contains NUL without content-length")
)

// SyntacticError reports a decoding error and the input offset where it was detected.
type SyntacticError struct {
	// ByteOffset is the offset, from the start of the decoder's input, of the
	// octet at which the error was detected.
	ByteOffset int64
	// Err is one of the Err* values of this package.
	Err error
}

func (e *SyntacticError) Error() string {
	return fmt.Sprintf("stomptext: offset %d: %v", e.ByteOffset, e.Err)
}

func (e *SyntacticError) Unwrap() error { return e.Err }
