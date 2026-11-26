package stomptext_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/jacoelho/stomp/stomptext"
)

// repeatReader serves the same octets forever, so a benchmark decodes a stream
// without rebuilding input.
type repeatReader struct {
	b   []byte
	pos int
}

func (r *repeatReader) Read(p []byte) (int, error) {
	n := 0
	for n < len(p) {
		c := copy(p[n:], r.b[r.pos:])
		n += c
		r.pos = (r.pos + c) % len(r.b)
	}
	return n, nil
}

func benchFrame(body int) []byte {
	b, err := stomptext.AppendFrame(nil, stomptext.Version12, stomptext.Message, stomptext.Headers{
		{Name: "subscription", Value: "sub-1"}, {Name: "message-id", Value: "ID:artemis-12345-1"},
		{Name: "destination", Value: "/queue/orders"}, {Name: "ack", Value: "12345"},
		{Name: "content-length", Value: fmt.Sprint(body)},
	}, bytes.Repeat([]byte("x"), body))
	if err != nil {
		panic(err)
	}
	return b
}

func BenchmarkReadFrame(b *testing.B) {
	for _, size := range []int{64, 4096, 1 << 20} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			f := benchFrame(size)
			dec := stomptext.NewDecoder(&repeatReader{b: f})
			b.SetBytes(int64(len(f)))
			b.ReportAllocs()
			for b.Loop() {
				if _, err := dec.ReadFrame(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// chunkReader returns at most max octets per Read, as a socket returns what
// its receive buffer holds rather than filling the caller's buffer.
type chunkReader struct {
	r   repeatReader
	max int
}

func (r *chunkReader) Read(p []byte) (int, error) { return r.r.Read(p[:min(len(p), r.max)]) }

func BenchmarkReadFrameChunked(b *testing.B) {
	for _, size := range []int{4096, 1 << 20} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			f := benchFrame(size)
			dec := stomptext.NewDecoder(&chunkReader{r: repeatReader{b: f}, max: 64 << 10})
			b.SetBytes(int64(len(f)))
			b.ReportAllocs()
			for b.Loop() {
				if _, err := dec.ReadFrame(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkAppendAck(b *testing.B) {
	hs := stomptext.Headers{{Name: "id", Value: "ID:artemis-12345-1"}}
	buf := make([]byte, 0, 4096)
	b.ReportAllocs()
	for b.Loop() {
		var err error
		if buf, err = stomptext.AppendFrame(buf[:0], stomptext.Version12, stomptext.Ack, hs, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func benchHeaders(b *testing.B, hs stomptext.Headers, body int) {
	frame, err := stomptext.AppendFrame(nil, stomptext.Version12, stomptext.Message, hs, bytes.Repeat([]byte("x"), body))
	if err != nil {
		b.Fatal(err)
	}
	dec := stomptext.NewDecoder(&repeatReader{b: frame})
	b.SetBytes(int64(len(frame)))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := dec.ReadFrame(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadFrameHeaders(b *testing.B) {
	b.Run("broker", func(b *testing.B) {
		benchHeaders(b, stomptext.Headers{
			{Name: "subscription", Value: "sub-1"}, {Name: "message-id", Value: "ID:5f3c1e2a-9b7d-11ef-8a4c-0242ac120002"},
			{Name: "destination", Value: "/queue/orders"}, {Name: "ack", Value: "ID:5f3c1e2a-9b7d-11ef-8a4c-0242ac120002:12345"},
			{Name: "expires", Value: "0"}, {Name: "redelivered", Value: "false"}, {Name: "priority", Value: "4"},
			{Name: "persistent", Value: "true"}, {Name: "timestamp", Value: "1732600000000"},
			{Name: "content-type", Value: "application/json"}, {Name: "correlation-id", Value: "c0ffee-1234"},
			{Name: "content-length", Value: "256"},
		}, 256)
	})
	b.Run("minimal", func(b *testing.B) {
		benchHeaders(b, stomptext.Headers{{Name: "subscription", Value: "s"}, {Name: "ack", Value: "1"}}, 0)
	})
	b.Run("escaped", func(b *testing.B) {
		benchHeaders(b, stomptext.Headers{
			{Name: "subscription", Value: "s"}, {Name: "ack", Value: "1"},
			{Name: "destination", Value: "/queue/a:b:c"}, {Name: "message-id", Value: "ID:host:1:2:3:4"},
			{Name: "note", Value: "line1\nline2\\x"},
		}, 0)
	})
}

func BenchmarkReadFrameLargeThenSmall(b *testing.B) {
	var in []byte
	in = append(in, benchFrame(1<<20)...)
	for range 1000 {
		in = append(in, benchFrame(64)...)
	}
	b.SetBytes(int64(len(in)))
	b.ReportAllocs()
	for b.Loop() {
		dec := stomptext.NewDecoder(bytes.NewReader(in))
		for {
			if _, err := dec.ReadFrame(); err != nil {
				break
			}
		}
	}
}
