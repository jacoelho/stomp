package stomptext

import (
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"testing"
)

type readFunc func([]byte) (int, error)

func (f readFunc) Read(p []byte) (int, error) { return f(p) }

func TestDecoderGrowsToADeclaredLengthWithoutPassingIt(t *testing.T) {
	body := strings.Repeat("x", 20<<20)
	in := strings.NewReader("SEND\ncontent-length:" + fmt.Sprint(len(body)) + "\n\n" + body + "\x00")
	d := NewDecoder(readFunc(func(p []byte) (int, error) { return in.Read(p[:min(len(p), 64<<10)]) }), MaxFrameSize(64<<20))
	want := int(in.Size())
	if _, err := d.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if len(d.buf) != want {
		t.Errorf("buffer holds %d octets after a %d-octet frame; want exactly the frame (FRM-36)", len(d.buf), want)
	}
}

func TestDecoderGrowsOnceToADeclaredLength(t *testing.T) {
	body := strings.Repeat("x", 1<<20)
	in := strings.NewReader("SEND\ncontent-length:" + fmt.Sprint(len(body)) + "\n\n" + body + "\x00")
	var d *Decoder
	sizes := map[int]bool{}
	d = NewDecoder(readFunc(func(p []byte) (int, error) {
		sizes[len(d.buf)] = true
		return in.Read(p[:min(len(p), 64<<10)])
	}))
	if _, err := d.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if len(sizes) != 2 {
		t.Errorf("buffer took sizes %v while reading a 1 MiB frame in 64 KiB reads; want the first read's size and one size that holds the frame (FRM-36)", sizes)
	}
}

func TestDecoderAllocatesAtMostGrowAheadBeyondTheOctetsReceived(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("a content-length of 10^18 saturates to MaxInt and fails as too large")
	}
	in := "SEND\ncontent-length:1000000000000000000\n\n" + strings.Repeat("x", 5000)
	d := NewDecoder(strings.NewReader(in), MaxFrameSize(math.MaxInt))
	if _, err := d.ReadFrame(); err != io.ErrUnexpectedEOF {
		t.Fatalf("ReadFrame = %v; want io.ErrUnexpectedEOF", err)
	}
	if limit := len(in) + 4<<20; len(d.buf) > limit {
		t.Errorf("buffer holds %d octets after %d received; want at most %d (FRM-36)", len(d.buf), len(in), limit)
	}
}
