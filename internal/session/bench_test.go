package session_test

import (
	"bytes"
	"testing"

	"github.com/jacoelho/stomp/internal/session"
	"github.com/jacoelho/stomp/stomptext"
)

func BenchmarkReceiveMessage(b *testing.B) {
	s := session.New(1, session.Accepted{Version: stomptext.Version12})
	if _, _, err := s.Subscribe("sub-1", "/queue/orders", session.ClientIndividual, nil); err != nil {
		b.Fatal(err)
	}
	raw, err := stomptext.AppendFrame(nil, stomptext.Version12, stomptext.Message, stomptext.Headers{
		{Name: "subscription", Value: "sub-1"}, {Name: "message-id", Value: "ID:artemis-12345-1"},
		{Name: "destination", Value: "/queue/orders"}, {Name: "ack", Value: "ID:artemis-12345-1-ack"},
	}, []byte("hello"))
	if err != nil {
		b.Fatal(err)
	}
	f, err := stomptext.NewDecoder(bytes.NewReader(raw)).ReadFrame()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		ev, err := s.Receive(f)
		if err != nil || ev.Kind != session.EventDeliver || ev.Sub != "sub-1" {
			b.Fatal(ev, err)
		}
	}
}

func BenchmarkSendAppend(b *testing.B) {
	s := session.New(1, session.Accepted{Version: stomptext.Version12})
	m := session.Outgoing{Destination: "/queue/orders", ContentType: "text/plain", Body: bytes.Repeat([]byte("x"), 256)}
	buf := make([]byte, 0, 4096)
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		if i%1024 == 0 {
			s = session.New(1, session.Accepted{Version: stomptext.Version12})
		}
		f, r, err := s.Send(m)
		if err != nil || r == "" {
			b.Fatal(err)
		}
		if buf, err = f.Append(buf[:0], stomptext.Version12); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSendRoundTrip(b *testing.B) {
	s := session.New(1, session.Accepted{Version: stomptext.Version12})
	m := session.Outgoing{Destination: "/queue/orders", ContentType: "text/plain", Body: bytes.Repeat([]byte("x"), 256)}
	buf := make([]byte, 0, 4096)
	reply := make([]byte, 0, 64)
	var dec stomptext.Decoder
	var in bytes.Reader
	b.ReportAllocs()
	for b.Loop() {
		f, r, err := s.Send(m)
		if err != nil || r == "" {
			b.Fatal(err)
		}
		if buf, err = f.Append(buf[:0], stomptext.Version12); err != nil {
			b.Fatal(err)
		}
		reply = append(append(append(reply[:0], "RECEIPT\nreceipt-id:"...), r...), "\n\n\x00"...)
		in.Reset(reply)
		dec.Reset(&in)
		rf, err := dec.ReadFrame()
		if err != nil {
			b.Fatal(err)
		}
		if _, err := s.Receive(rf); err != nil {
			b.Fatal(err)
		}
	}
}
