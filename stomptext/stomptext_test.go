package stomptext_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/jacoelho/stomp/stomptext"
)

type decoded struct {
	Cmd     stomptext.Command
	Headers stomptext.Headers
	Body    string
}

func own(f stomptext.Frame) decoded {
	d := decoded{Cmd: f.Command(), Body: string(f.Body())}
	for k, v := range f.All() {
		d.Headers = append(d.Headers, stomptext.Header{Name: string(k), Value: string(v)})
	}
	return d
}

func decodeAll(t *testing.T, r io.Reader, o ...stomptext.Options) ([]decoded, error) {
	t.Helper()
	return decodeVersion(t, stomptext.Version12, r, o...)
}

func decodeVersion(t *testing.T, v stomptext.Version, r io.Reader, o ...stomptext.Options) ([]decoded, error) {
	t.Helper()
	dec := stomptext.NewDecoder(r, o...)
	dec.SetVersion(v)
	var out []decoded
	for {
		f, err := dec.ReadFrame()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		d := own(f)
		if f.HeaderCount() != len(d.Headers) {
			t.Errorf("%s frame: HeaderCount() = %d; All yielded %d entries", d.Cmd, f.HeaderCount(), len(d.Headers))
		}
		out = append(out, d)
	}
}

func h(kv ...string) stomptext.Headers {
	var hs stomptext.Headers
	for i := 0; i < len(kv); i += 2 {
		hs = append(hs, stomptext.Header{Name: kv[i], Value: kv[i+1]})
	}
	return hs
}

func TestDecodeSpecExamples(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []decoded
	}{
		{
			name: "SEND with content-type",
			in:   "SEND\ndestination:/queue/a\ncontent-type:text/plain\n\nhello queue a\x00",
			want: []decoded{{stomptext.Send, h("destination", "/queue/a", "content-type", "text/plain"), "hello queue a"}},
		},
		{
			name: "CRLF line endings",
			in:   "RECEIPT\r\nreceipt-id:77\r\n\r\n\x00",
			want: []decoded{{stomptext.Receipt, h("receipt-id", "77"), ""}},
		},
		{
			name: "three frames back to back",
			in:   "RECEIPT\nreceipt-id:1\n\n\x00RECEIPT\nreceipt-id:2\n\n\x00RECEIPT\nreceipt-id:3\n\n\x00",
			want: []decoded{
				{stomptext.Receipt, h("receipt-id", "1"), ""},
				{stomptext.Receipt, h("receipt-id", "2"), ""},
				{stomptext.Receipt, h("receipt-id", "3"), ""},
			},
		},
		{
			name: "heart-beats before, between and after frames",
			in:   "\n\r\n\nRECEIPT\nreceipt-id:1\n\n\x00\n\n\r\nRECEIPT\nreceipt-id:2\n\n\x00\n",
			want: []decoded{
				{stomptext.Receipt, h("receipt-id", "1"), ""},
				{stomptext.Receipt, h("receipt-id", "2"), ""},
			},
		},
		{
			name: "content-length body containing NUL",
			in:   "MESSAGE\nsubscription:s\nmessage-id:1\ndestination:/q\ncontent-length:3\n\na\x00b\x00",
			want: []decoded{{stomptext.Message, h("subscription", "s", "message-id", "1", "destination", "/q", "content-length", "3"), "a\x00b"}},
		},
		{
			name: "escapes decoded in names and values",
			in:   "MESSAGE\nk\\cey:a\\nb\\rc\\\\d\\ce\n\n\x00",
			want: []decoded{{stomptext.Message, h("k:ey", "a\nb\rc\\d:e"), ""}},
		},
		{
			name: "CONNECTED headers are not unescaped",
			in:   "CONNECTED\nversion:1.2\nserver:Artemis\\c2\n\n\x00",
			want: []decoded{{stomptext.Connected, h("version", "1.2", "server", "Artemis\\c2"), ""}},
		},
		{
			name: "repeated headers kept in order",
			in:   "MESSAGE\nfoo:1\nfoo:2\n\n\x00",
			want: []decoded{{stomptext.Message, h("foo", "1", "foo", "2"), ""}},
		},
		{
			name: "header whitespace kept",
			in:   "MESSAGE\n destination : /q \n\n\x00",
			want: []decoded{{stomptext.Message, h(" destination ", " /q "), ""}},
		},
		{
			name: "value containing colon",
			in:   "CONNECTED\nversion:1.2\nsession:a:b\n\n\x00",
			want: []decoded{{stomptext.Connected, h("version", "1.2", "session", "a:b"), ""}},
		},
		{
			name: "first content-length governs the body",
			in:   "MESSAGE\ncontent-length:0\ncontent-length:2\n\n\x00",
			want: []decoded{{stomptext.Message, h("content-length", "0", "content-length", "2"), ""}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeAll(t, strings.NewReader(tt.in))
			if err != nil {
				t.Fatalf("decode %q: %v", tt.in, err)
			}
			if !slices.EqualFunc(got, tt.want, equalDecoded) {
				t.Errorf("decode %q\n got  %+v\n want %+v", tt.in, got, tt.want)
			}
		})
	}
}

func equalDecoded(a, b decoded) bool {
	return a.Cmd == b.Cmd && a.Body == b.Body && slices.Equal(a.Headers, b.Headers)
}

func TestHeaderLookupReturnsFirst(t *testing.T) {
	f, err := stomptext.NewDecoder(strings.NewReader("MESSAGE\nfoo:1\nfoo:2\n\n\x00")).ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	v, ok := f.Header("foo")
	if !ok || string(v) != "1" {
		t.Errorf("Header(foo) = %q, %v; want 1, true", v, ok)
	}
	if _, ok := f.Header("bar"); ok {
		t.Error("Header(bar) found on a frame without it")
	}
	if n := f.HeaderCount(); n != 2 {
		t.Errorf("HeaderCount() = %d; want 2", n)
	}
}

func TestDecodeErrors(t *testing.T) {
	small := []stomptext.Options{stomptext.MaxFrameSize(64), stomptext.MaxHeaderCount(2), stomptext.MaxHeaderBytes(48)}
	tests := []struct {
		name   string
		in     string
		want   error
		offset int64
	}{
		{"unknown command", "HELLO\n\n\x00", stomptext.ErrUnknownCommand, 0},
		{"undefined escape", "MESSAGE\nk:a\\tb\n\n\x00", stomptext.ErrInvalidEscape, 11},
		{"trailing backslash", "MESSAGE\nk:a\\\n\n\x00", stomptext.ErrInvalidEscape, 11},
		{"header without colon", "MESSAGE\ndestination\n\n\x00", stomptext.ErrInvalidHeader, 8},
		{"empty header name", "MESSAGE\n:v\n\n\x00", stomptext.ErrInvalidHeader, 8},
		{"negative content-length", "MESSAGE\ncontent-length:-1\n\n\x00", stomptext.ErrInvalidContentLength, 23},
		{"hex content-length", "MESSAGE\ncontent-length:0x1\n\n\x00", stomptext.ErrInvalidContentLength, 23},
		{"empty content-length", "MESSAGE\ncontent-length:\n\n\x00", stomptext.ErrInvalidContentLength, 23},
		{"octet after counted body", "MESSAGE\ncontent-length:2\n\nabc\x00", stomptext.ErrMissingNUL, 28},
		{"body on ACK", "ACK\nid:1\n\nx\x00", stomptext.ErrBodyNotAllowed, 10},
		{"CR without LF between frames", "\rRECEIPT\n\n\x00", stomptext.ErrInvalidEOL, 1},
		{"too many headers", "MESSAGE\na:1\nb:2\nc:3\n\n\x00", stomptext.ErrTooManyHeaders, 16},
		{"header section too large", "MESSAGE\nk:" + strings.Repeat("v", 60) + "\n\n\x00", stomptext.ErrHeaderTooLarge, 48},
		{"counted frame too large", "MESSAGE\ncontent-length:100\n\n" + strings.Repeat("x", 100) + "\x00", stomptext.ErrFrameTooLarge, 0},
		{"ten-digit content-length too large", "MESSAGE\ncontent-length:1000000000\n\n\x00", stomptext.ErrFrameTooLarge, 0},
		{"content-length beyond any int", "MESSAGE\ncontent-length:99999999999999999999999\n\n\x00", stomptext.ErrFrameTooLarge, 0},
		{"content-length above 32 bits", "MESSAGE\ncontent-length:4294967299\n\nabc\x00", stomptext.ErrFrameTooLarge, 0},
		{"uncounted frame too large", "MESSAGE\n\n" + strings.Repeat("x", 100) + "\x00", stomptext.ErrFrameTooLarge, 0},
		{"offset counts earlier frames", "RECEIPT\n\n\x00\nHELLO\n\n\x00", stomptext.ErrUnknownCommand, 11},
		{"octet after zero-length counted body", "MESSAGE\ncontent-length:0\n\na\x00", stomptext.ErrMissingNUL, 26},
		{"unterminated header section too large after a heart-beat", "\nMESSAGE\nk:" + strings.Repeat("v", 60) + "\n", stomptext.ErrHeaderTooLarge, 49},
		{"undefined escape after an earlier frame", "RECEIPT\n\n\x00MESSAGE\na:\\t\n\n\x00", stomptext.ErrInvalidEscape, 20},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeAll(t, strings.NewReader(tt.in), small...)
			var se *stomptext.SyntacticError
			if !errors.As(err, &se) || !errors.Is(err, tt.want) {
				t.Fatalf("decode %q: err = %v; want SyntacticError wrapping %v", tt.in, err, tt.want)
			}
			if se.ByteOffset != tt.offset {
				t.Errorf("decode %q: ByteOffset = %d; want %d", tt.in, se.ByteOffset, tt.offset)
			}
			if msg := se.Error(); !strings.Contains(msg, fmt.Sprint(tt.offset)) || !strings.Contains(msg, tt.want.Error()) {
				t.Errorf("decode %q: Error() = %q; want it to name offset %d and %q", tt.in, msg, tt.offset, tt.want)
			}
		})
	}
}

func TestDecodeEndOfInput(t *testing.T) {
	tests := []struct {
		in   string
		want error
	}{
		{"", io.EOF},
		{"\n\n", io.EOF},
		{"RECEIPT\nreceipt-id:1\n\n\x00\n", io.EOF},
		{"MESSAGE\ndest", io.ErrUnexpectedEOF},
		{"MESSAGE\ncontent-length:5\n\nab", io.ErrUnexpectedEOF},
		{"\r", io.ErrUnexpectedEOF},
	}
	for _, tt := range tests {
		_, err := decodeAll(t, strings.NewReader(tt.in))
		if tt.want == io.EOF {
			if err != nil {
				t.Errorf("decode %q: err = %v; want clean end", tt.in, err)
			}
			continue
		}
		if !errors.Is(err, tt.want) {
			t.Errorf("decode %q: err = %v; want %v", tt.in, err, tt.want)
		}
	}
}

func TestDecoderErrorIsSticky(t *testing.T) {
	dec := stomptext.NewDecoder(strings.NewReader("HELLO\n\n\x00RECEIPT\n\n\x00"))
	_, first := dec.ReadFrame()
	_, second := dec.ReadFrame()
	if first == nil || first != second {
		t.Errorf("errors = %v then %v; want the same non-nil error", first, second)
	}
}

func TestCloneSurvivesNextRead(t *testing.T) {
	dec := stomptext.NewDecoder(strings.NewReader("MESSAGE\nk:1\n\nbody-1\x00MESSAGE\nk:2\n\nbody-2\x00"))
	f1, err := dec.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	kept := f1.Clone()
	if _, err := dec.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if got := own(kept); got.Body != "body-1" || !slices.Equal(got.Headers, h("k", "1")) {
		t.Errorf("clone after next read = %+v; want body-1 with k:1", got)
	}
}

func TestEncodeSpecExamples(t *testing.T) {
	tests := []struct {
		name    string
		cmd     stomptext.Command
		headers stomptext.Headers
		body    string
		want    string
	}{
		{"SEND", stomptext.Send, h("destination", "/queue/a", "content-type", "text/plain"), "hello queue a",
			"SEND\ndestination:/queue/a\ncontent-type:text/plain\n\nhello queue a\x00"},
		{"escaping", stomptext.Send, h("k:ey", "a\nb\rc\\d:e"), "", "SEND\nk\\cey:a\\nb\\rc\\\\d\\ce\n\n\x00"},
		{"CONNECT not escaped", stomptext.Connect, h("accept-version", "1.2", "passcode", "a:b\\c"), "",
			"CONNECT\naccept-version:1.2\npasscode:a:b\\c\n\n\x00"},
		{"NUL body with content-length", stomptext.Send, h("content-length", "3"), "a\x00b", "SEND\ncontent-length:3\n\na\x00b\x00"},
		{"no content-length added", stomptext.Send, nil, "hi", "SEND\n\nhi\x00"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stomptext.AppendFrame([]byte("prefix"), stomptext.Version12, tt.cmd, tt.headers, []byte(tt.body))
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != "prefix"+tt.want {
				t.Errorf("AppendFrame = %q; want %q", got, "prefix"+tt.want)
			}
		})
	}
}

func TestEncodeErrors(t *testing.T) {
	tests := []struct {
		name    string
		cmd     stomptext.Command
		headers stomptext.Headers
		body    string
		want    error
	}{
		{"unknown command", "HELLO", nil, "", stomptext.ErrUnknownCommand},
		{"body on ACK", stomptext.Ack, h("id", "1"), "x", stomptext.ErrBodyNotAllowed},
		{"empty header name", stomptext.Send, h("", "v"), "", stomptext.ErrInvalidHeader},
		{"LF in CONNECT value", stomptext.Connect, h("passcode", "a\nb"), "", stomptext.ErrInvalidHeader},
		{"colon in CONNECT name", stomptext.Connect, h("a:b", "v"), "", stomptext.ErrInvalidHeader},
		{"content-length disagrees", stomptext.Send, h("content-length", "5"), "abc", stomptext.ErrInvalidContentLength},
		{"content-length with sign", stomptext.Send, h("content-length", "+3"), "abc", stomptext.ErrInvalidContentLength},
		{"NUL without content-length", stomptext.Send, nil, "a\x00b", stomptext.ErrNULInBody},
		{"NUL first in body without content-length", stomptext.Send, nil, "\x00ab", stomptext.ErrNULInBody},
		{"colon first in CONNECT name", stomptext.Connect, h(":a", "v"), "", stomptext.ErrInvalidHeader},
		{"LF first in CONNECT value", stomptext.Connect, h("passcode", "\nab"), "", stomptext.ErrInvalidHeader},
		{"CR in CONNECT value", stomptext.Connect, h("passcode", "a\rb"), "", stomptext.ErrInvalidHeader},
		{"CR in CONNECT name", stomptext.Connect, h("a\rb", "v"), "", stomptext.ErrInvalidHeader},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := []byte("keep")
			got, err := stomptext.AppendFrame(dst, stomptext.Version12, tt.cmd, tt.headers, []byte(tt.body))
			if !errors.Is(err, tt.want) {
				t.Errorf("err = %v; want %v", err, tt.want)
			}
			if string(got) != "keep" {
				t.Errorf("dst = %q; want unchanged", got)
			}
		})
	}
}

func TestLimitOptionPanicsBelowOne(t *testing.T) {
	for name, f := range map[string]func(int) stomptext.Options{
		"MaxFrameSize": stomptext.MaxFrameSize, "MaxHeaderCount": stomptext.MaxHeaderCount, "MaxHeaderBytes": stomptext.MaxHeaderBytes,
	} {
		for _, n := range []int{-1, 0, 1} {
			func() {
				defer func() {
					if panicked := recover() != nil; panicked != (n < 1) {
						t.Errorf("%s(%d): panicked = %v; want %v", name, n, panicked, n < 1)
					}
				}()
				f(n)
			}()
		}
	}
}

// randomFrames builds frames whose encoding is valid, with headers that need
// escaping and bodies that contain NUL.
func randomFrames(r *rand.Rand, n int) []decoded {
	cmds := []stomptext.Command{stomptext.Send, stomptext.Message, stomptext.Error, stomptext.Receipt, stomptext.Ack}
	alphabet := []byte("ab:\\\r\n \x00z")
	str := func(n int, nul bool) string {
		b := make([]byte, n)
		for i := range b {
			b[i] = alphabet[r.IntN(len(alphabet))]
			if !nul && b[i] == 0 {
				b[i] = 'x'
			}
		}
		return string(b)
	}
	var out []decoded
	for range n {
		d := decoded{Cmd: cmds[r.IntN(len(cmds))]}
		for range r.IntN(4) {
			d.Headers = append(d.Headers, stomptext.Header{Name: "h" + str(r.IntN(4), false), Value: str(r.IntN(8), false)})
		}
		if d.Cmd == stomptext.Send || d.Cmd == stomptext.Message || d.Cmd == stomptext.Error {
			d.Body = str(r.IntN(64), true)
			d.Headers = append(d.Headers, stomptext.Header{Name: "content-length", Value: fmt.Sprint(len(d.Body))})
		}
		out = append(out, d)
	}
	return out
}

func encodeAll(t testing.TB, v stomptext.Version, frames []decoded, r *rand.Rand) []byte {
	eols := []string{"\n", "\r\n"}
	if v == stomptext.Version11 {
		eols = eols[:1]
	}
	var b []byte
	for _, f := range frames {
		var err error
		if b, err = stomptext.AppendFrame(b, v, f.Cmd, f.Headers, []byte(f.Body)); err != nil {
			t.Fatalf("AppendFrame(%v, %+v): %v", v, f, err)
		}
		for range r.IntN(3) {
			b = append(b, eols[r.IntN(len(eols))]...)
		}
	}
	return b
}

// chunked splits input at random points, so decoding must not depend on read sizes.
type chunked struct {
	b []byte
	r *rand.Rand
}

func (c *chunked) Read(p []byte) (int, error) {
	if len(c.b) == 0 {
		return 0, io.EOF
	}
	n := min(len(p), len(c.b), 1+c.r.IntN(17))
	copy(p, c.b[:n])
	c.b = c.b[n:]
	return n, nil
}

func TestRoundTripIndependentOfChunkingAndHeartbeats(t *testing.T) {
	for _, v := range []stomptext.Version{stomptext.Version11, stomptext.Version12} {
		for seed := range uint64(200) {
			r := rand.New(rand.NewPCG(seed, 1))
			frames := randomFrames(r, 1+r.IntN(8))
			wire := encodeAll(t, v, frames, r)
			for _, src := range []io.Reader{bytes.NewReader(wire), iotest.OneByteReader(bytes.NewReader(wire)), &chunked{wire, r}} {
				got, err := decodeVersion(t, v, src)
				if err != nil {
					t.Fatalf("%v seed %d: decode: %v\nwire %q", v, seed, err, wire)
				}
				if !slices.EqualFunc(got, frames, equalDecoded) {
					t.Fatalf("%v seed %d: round trip mismatch\n got  %+v\n want %+v", v, seed, got, frames)
				}
			}
		}
	}
}

func TestLargeFrameThenSmallFrames(t *testing.T) {
	big := strings.Repeat("x", 1<<20)
	in := "MESSAGE\ncontent-length:" + fmt.Sprint(len(big)) + "\n\n" + big + "\x00" + "RECEIPT\nreceipt-id:9\n\n\x00"
	got, err := decodeAll(t, &chunked{[]byte(in), rand.New(rand.NewPCG(1, 2))})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || len(got[0].Body) != len(big) || got[1].Cmd != stomptext.Receipt {
		t.Errorf("got %d frames; want 1 MiB MESSAGE then RECEIPT", len(got))
	}
}

func FuzzDecode(f *testing.F) {
	f.Add([]byte("MESSAGE\ncontent-length:3\n\na\x00b\x00\nRECEIPT\nreceipt-id:1\n\n\x00"), uint64(1))
	f.Add([]byte("SEND\nk\\cey:a\\nb\n\nhello\x00"), uint64(2))
	f.Add([]byte("MESSAGE\na:b\r\n\n\x00"), uint64(3))
	f.Fuzz(func(t *testing.T, in []byte, seed uint64) {
		v := stomptext.Version12
		if seed%2 == 1 {
			v = stomptext.Version11
		}
		whole, wholeErr := decodeVersion(t, v, bytes.NewReader(in))
		split, splitErr := decodeVersion(t, v, &chunked{in, rand.New(rand.NewPCG(seed, 0))})
		if (wholeErr == nil) != (splitErr == nil) || !slices.EqualFunc(whole, split, equalDecoded) {
			t.Fatalf("chunking changed the result: %v/%v\n%+v\n%+v", wholeErr, splitErr, whole, split)
		}
		for _, d := range whole {
			wire, err := stomptext.AppendFrame(nil, v, d.Cmd, d.Headers, []byte(d.Body))
			if err != nil {
				continue
			}
			again, err := decodeVersion(t, v, bytes.NewReader(wire))
			if err != nil || len(again) != 1 || !equalDecoded(again[0], d) {
				t.Fatalf("re-encode of %+v decoded to %+v, %v", d, again, err)
			}
		}
	})
}

func TestVersion11Framing(t *testing.T) {
	for _, tt := range []struct {
		name, in string
		want     []decoded
		err      error
	}{
		{"CR is part of the value", "MESSAGE\na:b\r\n\n\x00", []decoded{{Cmd: stomptext.Message, Headers: h("a", "b\r")}}, nil},
		{"CR escape is undefined", "MESSAGE\na:b\\r\n\n\x00", nil, stomptext.ErrInvalidEscape},
		{"CR ends no command", "MESSAGE\r\n\n\x00", nil, stomptext.ErrUnknownCommand},
		{"CR before a command is not a heart-beat", "\r\nRECEIPT\n\n\x00", nil, stomptext.ErrUnknownCommand},
		{"line of CR is a header without colon", "MESSAGE\n\r\n\n\x00", nil, stomptext.ErrInvalidHeader},
		{"escapes still decode", "MESSAGE\nk\\cey:a\\nb\\\\\n\n\x00", []decoded{{Cmd: stomptext.Message, Headers: h("k:ey", "a\nb\\")}}, nil},
		{"STOMP frame is escaped", "STOMP\nlogin:a\\cb\n\n\x00", []decoded{{Cmd: stomptext.Stomp, Headers: h("login", "a:b")}}, nil},
		{"header starting with CR does not end the header section", "MESSAGE\n\rk:v\nx:y\n\nbody\x00", []decoded{{Cmd: stomptext.Message, Headers: h("\rk", "v", "x", "y"), Body: "body"}}, nil},
	} {
		got, err := decodeVersion(t, stomptext.Version11, strings.NewReader(tt.in))
		if !errors.Is(err, tt.err) || !slices.EqualFunc(got, tt.want, equalDecoded) {
			t.Errorf("%s: decode 1.1 %q = %+v, %v; want %+v, %v", tt.name, tt.in, got, err, tt.want, tt.err)
		}
	}
}

func TestVersion11Encoding(t *testing.T) {
	got, err := stomptext.AppendFrame(nil, stomptext.Version11, stomptext.Send, h("a", "x\r:y\n"), nil)
	if want := "SEND\na:x\r\\cy\\n\n\n\x00"; err != nil || string(got) != want {
		t.Errorf("AppendFrame 1.1 = %q, %v; want %q", got, err, want)
	}
	got, err = stomptext.AppendFrame(nil, stomptext.Version11, stomptext.Connect, h("passcode", "a\rb"), nil)
	if want := "CONNECT\npasscode:a\rb\n\n\x00"; err != nil || string(got) != want {
		t.Errorf("AppendFrame 1.1 CONNECT = %q, %v; want %q", got, err, want)
	}
	if _, err := stomptext.AppendFrame(nil, stomptext.Version12, stomptext.Connect, h("passcode", "a\rb"), nil); !errors.Is(err, stomptext.ErrInvalidHeader) {
		t.Errorf("AppendFrame 1.2 CONNECT with CR: %v; want ErrInvalidHeader", err)
	}
}

func TestSetVersionAppliesToBufferedOctets(t *testing.T) {
	dec := stomptext.NewDecoder(strings.NewReader("CONNECTED\nversion:1.1\n\n\x00MESSAGE\na:b\r\n\n\x00"))
	if f, err := dec.ReadFrame(); err != nil || f.Command() != stomptext.Connected {
		t.Fatalf("first frame: %v", err)
	}
	dec.SetVersion(stomptext.Version11)
	f, err := dec.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := f.Header("a"); string(v) != "b\r" {
		t.Errorf("1.1 header a = %q; want b\\r", v)
	}
}

func TestVersion11CREscapeReportsBackslashOffset(t *testing.T) {
	_, err := decodeVersion(t, stomptext.Version11, strings.NewReader("RECEIPT\n\n\x00MESSAGE\na:b\\r\n\n\x00"))
	var se *stomptext.SyntacticError
	if !errors.As(err, &se) || !errors.Is(err, stomptext.ErrInvalidEscape) || se.ByteOffset != 21 {
		t.Errorf("err = %v; want SyntacticError wrapping ErrInvalidEscape at offset 21", err)
	}
}

func TestLimitAcceptsExactlyNAndRejectsOneLess(t *testing.T) {
	tests := []struct {
		name   string
		frame  string
		limit  func(int) stomptext.Options
		size   int
		want   error
		offset int64
	}{
		{"counted frame size", "MESSAGE\ncontent-length:3\n\nabc\x00", stomptext.MaxFrameSize, 30, stomptext.ErrFrameTooLarge, 1},
		{"uncounted frame size", "MESSAGE\n\nabc\x00", stomptext.MaxFrameSize, 13, stomptext.ErrFrameTooLarge, 1},
		{"header bytes", "MESSAGE\nk:v\n\nabc\x00", stomptext.MaxHeaderBytes, 13, stomptext.ErrHeaderTooLarge, 13},
		{"header count", "MESSAGE\na:1\nb:2\n\n\x00", stomptext.MaxHeaderCount, 2, stomptext.ErrTooManyHeaders, 13},
	}
	readers := map[string]func(string) io.Reader{
		"whole":     func(s string) io.Reader { return strings.NewReader(s) },
		"one octet": func(s string) io.Reader { return iotest.OneByteReader(strings.NewReader(s)) },
	}
	for _, tt := range tests {
		for rname, reader := range readers {
			t.Run(tt.name+"/"+rname, func(t *testing.T) {
				in := "\n" + tt.frame
				got, err := decodeAll(t, reader(in), tt.limit(tt.size))
				if err != nil || len(got) != 1 {
					t.Errorf("limit %d: decode %q = %d frames, %v; want one frame", tt.size, in, len(got), err)
				}
				_, err = decodeAll(t, reader(in), tt.limit(tt.size-1))
				var se *stomptext.SyntacticError
				if !errors.As(err, &se) || !errors.Is(err, tt.want) || se.ByteOffset != tt.offset {
					t.Errorf("limit %d: decode %q err = %v; want SyntacticError wrapping %v at offset %d", tt.size-1, in, err, tt.want, tt.offset)
				}
			})
		}
	}
}

var errReadPastLimit = errors.New("read past MaxFrameSize")

func TestFrameTooLargeReportedWithoutReadingPastLimit(t *testing.T) {
	for _, tt := range []struct {
		name, in string
		limit    int
	}{
		{"uncounted body", "MESSAGE\n\nabcd", 13},
		{"counted body", "MESSAGE\ncontent-length:100\n\n", 64},
		{"unterminated header section", "MESSAGE\nk:" + strings.Repeat("v", 1000), 16},
		{"complete header section", "MESSAGE\nk:" + strings.Repeat("v", 20) + "\n\n", 16},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := io.MultiReader(strings.NewReader(tt.in), iotest.ErrReader(errReadPastLimit))
			_, err := stomptext.NewDecoder(r, stomptext.MaxFrameSize(tt.limit)).ReadFrame()
			var se *stomptext.SyntacticError
			if !errors.As(err, &se) || !errors.Is(err, stomptext.ErrFrameTooLarge) || se.ByteOffset != 0 {
				t.Errorf("MaxFrameSize(%d) on %q: err = %v; want ErrFrameTooLarge at offset 0 before further reads", tt.limit, tt.in, err)
			}
		})
	}
}

func TestDefaultLimits(t *testing.T) {
	headers := func(n int) string { return strings.Repeat("k:v\n", n) }
	tests := []struct {
		name      string
		atLimit   string
		overLimit string
		want      error
	}{
		{
			"4 MiB frame",
			"MESSAGE\n\n" + strings.Repeat("x", 4<<20-10) + "\x00",
			"MESSAGE\n\n" + strings.Repeat("x", 4<<20-9) + "\x00",
			stomptext.ErrFrameTooLarge,
		},
		{"128 headers", "MESSAGE\n" + headers(128) + "\n\x00", "MESSAGE\n" + headers(129) + "\n\x00", stomptext.ErrTooManyHeaders},
		{
			"64 KiB header section",
			"MESSAGE\nk:" + strings.Repeat("v", 64<<10-12) + "\n\n\x00",
			"MESSAGE\nk:" + strings.Repeat("v", 64<<10-11) + "\n\n\x00",
			stomptext.ErrHeaderTooLarge,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := decodeAll(t, strings.NewReader(tt.atLimit)); err != nil || len(got) != 1 {
				t.Errorf("at limit: %d frames, %v; want one frame", len(got), err)
			}
			if _, err := decodeAll(t, strings.NewReader(tt.overLimit)); !errors.Is(err, tt.want) {
				t.Errorf("over limit: err = %v; want %v", err, tt.want)
			}
		})
	}
}

func TestInputOffsetIsNextUnreadOctet(t *testing.T) {
	dec := stomptext.NewDecoder(strings.NewReader("\nRECEIPT\n\n\x00\n\nMESSAGE\n\nab\x00"))
	want := []int64{0, 11, 25}
	if got := dec.InputOffset(); got != want[0] {
		t.Errorf("before reading: InputOffset() = %d; want %d", got, want[0])
	}
	for i, w := range want[1:] {
		if _, err := dec.ReadFrame(); err != nil {
			t.Fatal(err)
		}
		if got := dec.InputOffset(); got != w {
			t.Errorf("after frame %d: InputOffset() = %d; want %d", i+1, got, w)
		}
	}
}

func TestInputOffsetAcrossManyFrames(t *testing.T) {
	const n = 2000
	in := strings.Repeat("RECEIPT\n\n\x00", n)
	dec := stomptext.NewDecoder(&chunked{[]byte(in), rand.New(rand.NewPCG(3, 4))})
	for i := range n {
		if _, err := dec.ReadFrame(); err != nil {
			t.Fatalf("frame %d: %v", i+1, err)
		}
		if got, want := dec.InputOffset(), int64(10*(i+1)); got != want {
			t.Fatalf("after frame %d: InputOffset() = %d; want %d", i+1, got, want)
		}
	}
}

func TestParseVersion(t *testing.T) {
	for _, tt := range []struct {
		in   string
		want stomptext.Version
		ok   bool
	}{
		{"1.1", stomptext.Version11, true},
		{"1.2", stomptext.Version12, true},
		{"1.0", 0, false},
		{"", 0, false},
		{"1.2 ", 0, false},
	} {
		if v, ok := stomptext.ParseVersion(tt.in); v != tt.want || ok != tt.ok {
			t.Errorf("ParseVersion(%q) = %v, %v; want %v, %v", tt.in, v, ok, tt.want, tt.ok)
		}
		if v, ok := stomptext.ParseVersion([]byte(tt.in)); v != tt.want || ok != tt.ok {
			t.Errorf("ParseVersion([]byte(%q)) = %v, %v; want %v, %v", tt.in, v, ok, tt.want, tt.ok)
		}
	}
}

func TestUnknownVersionPanics(t *testing.T) {
	for name, call := range map[string]func(){
		"SetVersion(1.0)":  func() { stomptext.NewDecoder(strings.NewReader("")).SetVersion(10) },
		"SetVersion(0)":    func() { stomptext.NewDecoder(strings.NewReader("")).SetVersion(0) },
		"AppendFrame(1.3)": func() { _, _ = stomptext.AppendFrame(nil, 13, stomptext.Send, nil, nil) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s did not panic", name)
				}
			}()
			call()
		}()
	}
}

func TestHeadersGetReturnsFirst(t *testing.T) {
	hs := h("foo", "1", "bar", "", "foo", "2")
	for _, tt := range []struct {
		name, want string
		ok         bool
	}{
		{"foo", "1", true},
		{"bar", "", true},
		{"baz", "", false},
	} {
		if v, ok := hs.Get(tt.name); v != tt.want || ok != tt.ok {
			t.Errorf("Get(%q) = %q, %v; want %q, %v", tt.name, v, ok, tt.want, tt.ok)
		}
	}
}

func TestNULBeforeTheHeaderSectionEndsIsNotAFrame(t *testing.T) {
	_, err := stomptext.NewDecoder(strings.NewReader("SEND\n\x00")).ReadFrame()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("ReadFrame of %q = %v; want io.ErrUnexpectedEOF", "SEND\n\x00", err)
	}
}

type emptyReads struct{}

func (emptyReads) Read([]byte) (int, error) { return 0, nil }

func TestReaderThatNeverProgressesFailsWithErrNoProgress(t *testing.T) {
	if _, err := stomptext.NewDecoder(emptyReads{}).ReadFrame(); !errors.Is(err, io.ErrNoProgress) {
		t.Errorf("ReadFrame from a reader returning (0, nil) = %v; want io.ErrNoProgress", err)
	}
}

type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

func TestDecoderReadsAtMostMaxFrameSizeOfAFrame(t *testing.T) {
	const limit = 1<<20 + 100
	for _, tt := range []struct{ name, head string }{
		{"unterminated body", "\nMESSAGE\n\n"},
		{"unterminated header section", "\nMESSAGE\nk:"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := &countingReader{r: io.MultiReader(strings.NewReader(tt.head), strings.NewReader(strings.Repeat("x", 8*limit)))}
			_, err := stomptext.NewDecoder(r, stomptext.MaxFrameSize(limit), stomptext.MaxHeaderBytes(2*limit)).ReadFrame()
			if !errors.Is(err, stomptext.ErrFrameTooLarge) {
				t.Fatalf("err = %v; want ErrFrameTooLarge", err)
			}
			if max := 1 + limit; r.n > max {
				t.Errorf("decoder read %d octets; want at most %d, one EOL and MaxFrameSize (FRM-16)", r.n, max)
			}
		})
	}
}

func TestBothLimitsReportTheSameErrorHoweverTheInputIsSplit(t *testing.T) {
	in := "MESSAGE\nk:" + strings.Repeat("v", 1000)
	for _, tt := range []struct {
		name          string
		frame, header int
		want          error
	}{
		{"header limit below frame limit", 64, 32, stomptext.ErrHeaderTooLarge},
		{"frame limit below header limit", 32, 64, stomptext.ErrFrameTooLarge},
		{"equal limits", 32, 32, stomptext.ErrFrameTooLarge},
		{"limits of one", 1, 1, stomptext.ErrFrameTooLarge},
	} {
		for rname, r := range map[string]func() io.Reader{
			"whole":     func() io.Reader { return strings.NewReader(in) },
			"one octet": func() io.Reader { return iotest.OneByteReader(strings.NewReader(in)) },
		} {
			t.Run(tt.name+"/"+rname, func(t *testing.T) {
				_, err := stomptext.NewDecoder(r(), stomptext.MaxFrameSize(tt.frame), stomptext.MaxHeaderBytes(tt.header)).ReadFrame()
				if !errors.Is(err, tt.want) {
					t.Errorf("MaxFrameSize(%d) MaxHeaderBytes(%d): err = %v; want %v (FRM-4, FRM-34)", tt.frame, tt.header, err, tt.want)
				}
			})
		}
	}
}

func TestConnectInSTOMP11CarriesCRButNotLFOrColon(t *testing.T) {
	got, err := stomptext.AppendFrame(nil, stomptext.Version11, stomptext.Connect, h("passcode", "a\rb"), nil)
	if err != nil || string(got) != "CONNECT\npasscode:a\rb\n\n\x00" {
		t.Errorf("1.1 CONNECT with CR in a value = %q, %v; want the CR written as is (FRM-12, FRM-30)", got, err)
	}
	for _, hs := range []stomptext.Headers{h("passcode", "a\nb"), h("a:b", "v")} {
		if _, err := stomptext.AppendFrame(nil, stomptext.Version11, stomptext.Connect, hs, nil); !errors.Is(err, stomptext.ErrInvalidHeader) {
			t.Errorf("1.1 CONNECT with %v = %v; want ErrInvalidHeader (FRM-12)", hs, err)
		}
	}
}

type failOnceReader struct {
	failed bool
	reads  int
}

var errFailOnce = errors.New("connection reset")

func (r *failOnceReader) Read(p []byte) (int, error) {
	r.reads++
	if !r.failed {
		r.failed = true
		return 0, errFailOnce
	}
	return copy(p, "RECEIPT\n\n\x00"), nil
}

func TestDecoderReadErrorIsSticky(t *testing.T) {
	r := &failOnceReader{}
	dec := stomptext.NewDecoder(r)
	for i := range 3 {
		if _, err := dec.ReadFrame(); !errors.Is(err, errFailOnce) {
			t.Errorf("ReadFrame %d = %v; want the first read error again (FRM-22)", i+1, err)
		}
	}
	if r.reads != 1 {
		t.Errorf("decoder read %d times; want 1, no read after the error (FRM-22)", r.reads)
	}
}
