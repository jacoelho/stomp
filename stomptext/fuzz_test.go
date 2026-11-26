package stomptext_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"
	"testing"
	"testing/iotest"

	"github.com/jacoelho/stomp/stomptext"
)

var allCommands = []stomptext.Command{
	stomptext.Connect, stomptext.Stomp, stomptext.Connected, stomptext.Send, stomptext.Subscribe,
	stomptext.Unsubscribe, stomptext.Ack, stomptext.Nack, stomptext.Begin, stomptext.Commit,
	stomptext.Abort, stomptext.Disconnect, stomptext.Message, stomptext.Receipt, stomptext.Error,
}

func fuzzHeaders(raw []byte) stomptext.Headers {
	parts := bytes.Split(raw, []byte{0xff})
	var hs stomptext.Headers
	for i := 0; i+1 < len(parts); i += 2 {
		hs = append(hs, stomptext.Header{Name: string(parts[i]), Value: string(parts[i+1])})
	}
	return hs
}

func FuzzEncode(f *testing.F) {
	f.Add(uint8(3), false, []byte("a\xffb:c\\\r\n\xffcontent-length\xff3"), []byte("x\x00y"))
	f.Add(uint8(0), true, []byte("login\xffa\rb"), []byte(nil))
	f.Add(uint8(12), false, []byte("k\xff\xff\xffv"), []byte("body"))
	f.Fuzz(func(t *testing.T, cmd uint8, v11 bool, rawHeaders, body []byte) {
		v := stomptext.Version12
		if v11 {
			v = stomptext.Version11
		}
		want := decoded{Cmd: allCommands[int(cmd)%len(allCommands)], Headers: fuzzHeaders(rawHeaders), Body: string(body)}
		prefix := []byte("prefix")
		out, err := stomptext.AppendFrame(slices.Clip(prefix), v, want.Cmd, want.Headers, body)
		if err != nil {
			if !bytes.Equal(out, prefix) {
				t.Fatalf("failed AppendFrame changed dst: %q; error %v", out, err)
			}
			return
		}
		if !bytes.HasPrefix(out, prefix) {
			t.Fatalf("AppendFrame lost dst prefix: %q", out)
		}
		wire := out[len(prefix):]
		got, err := decodeVersion(t, v, bytes.NewReader(wire))
		if err != nil || len(got) != 1 || !equalDecoded(got[0], want) {
			t.Fatalf("%v: encoded %+v as %q, decoded %+v, %v", v, want, wire, got, err)
		}
	})
}

func FuzzDecodeErrorsAndLimits(f *testing.F) {
	f.Add([]byte("MESSAGE\ncontent-length:3\n\na\x00b\x00RECEIPT\nreceipt-id:1\n\n\x00"), false, uint16(8), uint8(2))
	f.Add([]byte("SEND\nk:\\t\n\n\x00"), true, uint16(100), uint8(1))
	f.Add([]byte("AB"), false, uint16(0), uint8(0))
	f.Fuzz(func(t *testing.T, in []byte, v11 bool, frameLimit uint16, headerLimit uint8) {
		v := stomptext.Version12
		if v11 {
			v = stomptext.Version11
		}
		full, fullErr := decodeVersion(t, v, bytes.NewReader(in))
		checkOffset(t, fullErr, len(in))
		limits := []stomptext.Options{stomptext.MaxFrameSize(1 + int(frameLimit)), stomptext.MaxHeaderBytes(1 + int(headerLimit)), stomptext.MaxHeaderCount(1 + int(headerLimit))}
		limited, limitedErr := decodeVersion(t, v, bytes.NewReader(in), limits...)
		checkOffset(t, limitedErr, len(in))
		split, splitErr := decodeVersion(t, v, iotest.OneByteReader(bytes.NewReader(in)), limits...)
		if !slices.EqualFunc(split, limited, equalDecoded) || fmt.Sprint(splitErr) != fmt.Sprint(limitedErr) {
			t.Fatalf("one octet at a time decoded %+v, %v; whole %+v, %v (FRM-4)", split, splitErr, limited, limitedErr)
		}
		if len(limited) > len(full) || !slices.EqualFunc(limited, full[:len(limited)], equalDecoded) {
			t.Fatalf("limits changed decoded frames:\n limited %+v\n full    %+v", limited, full)
		}
		if limitedErr == nil && (fullErr != nil || len(limited) != len(full)) {
			t.Fatalf("limits accepted input the unlimited decoder rejects: %v", fullErr)
		}
	})
}

func checkOffset(t *testing.T, err error, n int) {
	t.Helper()
	var serr *stomptext.SyntacticError
	switch {
	case err == nil, errors.Is(err, io.ErrUnexpectedEOF):
	case errors.As(err, &serr):
		if serr.ByteOffset < 0 || serr.ByteOffset > int64(n) {
			t.Fatalf("error offset %d outside input of %d octets: %v", serr.ByteOffset, n, err)
		}
	default:
		t.Fatalf("unexpected error type %T: %v", err, err)
	}
}
