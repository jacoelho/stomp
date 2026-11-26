package stomptext

import (
	"bytes"
	"fmt"
)

// AppendFrame appends the encoding of a frame in protocol version v to dst and
// returns the extended buffer. It writes exactly the given headers, escaping
// them as the command and version require, and never adds content-length.
//
// It fails, returning dst unchanged, when the command is unknown, a body is
// given for a command that forbids one, a header cannot be represented, a
// content-length header disagrees with the body, or the body contains NUL
// without a content-length header. It panics on an unknown version.
func AppendFrame(dst []byte, v Version, cmd Command, headers []Header, body []byte) ([]byte, error) {
	if !v.known() {
		panic(fmt.Sprintf("stomptext: AppendFrame version %v", v))
	}
	if !cmd.known() {
		return dst, fmt.Errorf("stomptext: command %q: %w", cmd, ErrUnknownCommand)
	}
	if len(body) > 0 && !cmd.bodyAllowed() {
		return dst, fmt.Errorf("stomptext: %s: %w", cmd, ErrBodyNotAllowed)
	}
	escaped := cmd.escaped()
	special := &special12
	if v == Version11 {
		special = &special11
	}
	hasLength := false
	out := append(dst, cmd...)
	out = append(out, '\n')
	for _, h := range headers {
		if h.Name == "" {
			return dst, fmt.Errorf("stomptext: empty header name: %w", ErrInvalidHeader)
		}
		if h.Name == HeaderContentLength && !hasLength {
			hasLength = true
			if n, ok := parseLength(h.Value); !ok || n != len(body) || (len(h.Value) > 1 && h.Value[0] == '0') {
				return dst, fmt.Errorf("stomptext: content-length %q for a %d-octet body: %w", h.Value, len(body), ErrInvalidContentLength)
			}
		}
		var ok bool
		if escaped {
			out = appendEscaped(out, special, h.Name)
			out = append(out, ':')
			out = appendEscaped(out, special, h.Value)
			ok = true
		} else {
			out, ok = appendRaw(out, v, h.Name, true)
			out = append(out, ':')
			if ok {
				out, ok = appendRaw(out, v, h.Value, false)
			}
		}
		if !ok {
			return dst, fmt.Errorf("stomptext: %s header %q cannot be represented without escaping: %w", cmd, h.Name, ErrInvalidHeader)
		}
		out = append(out, '\n')
	}
	if !hasLength && bytes.IndexByte(body, 0) >= 0 {
		return dst, fmt.Errorf("stomptext: %w", ErrNULInBody)
	}
	out = append(out, '\n')
	out = append(out, body...)
	return append(out, 0), nil
}

// special12 and special11 mark the octets header escaping replaces; 1.1 has no
// CR escape and carries CR as an ordinary octet.
var (
	special12 = [256]bool{'\\': true, '\r': true, '\n': true, ':': true}
	special11 = [256]bool{'\\': true, '\n': true, ':': true}
)

// appendRaw copies s for a frame without escaping, reporting false if s holds
// an octet that would break the line or, for names, the colon separator.
func appendRaw(dst []byte, v Version, s string, name bool) ([]byte, bool) {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c == '\n' || (c == '\r' && v == Version12) || (name && c == ':') {
			return dst, false
		}
	}
	return append(dst, s...), true
}

// appendEscaped copies runs of ordinary octets in bulk and escapes the rest.
func appendEscaped(dst []byte, special *[256]bool, s string) []byte {
	run := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !special[c] {
			continue
		}
		dst = append(dst, s[run:i]...)
		run = i + 1
		switch c {
		case '\\':
			dst = append(dst, '\\', '\\')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\n':
			dst = append(dst, '\\', 'n')
		case ':':
			dst = append(dst, '\\', 'c')
		}
	}
	return append(dst, s[run:]...)
}
