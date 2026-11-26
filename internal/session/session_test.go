package session_test

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jacoelho/stomp/internal/session"
	"github.com/jacoelho/stomp/stomptext"
)

var (
	v12    = session.Accepted{Version: stomptext.Version12}
	v11    = session.Accepted{Version: stomptext.Version11}
	both   = []stomptext.Version{stomptext.Version11, stomptext.Version12}
	only12 = []stomptext.Version{stomptext.Version12}
)

func decode(t *testing.T, raw string) stomptext.Frame {
	t.Helper()
	f, err := stomptext.NewDecoder(strings.NewReader(raw)).ReadFrame()
	if err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	return f
}

func TestNegotiateFollowsSpec(t *testing.T) {
	s := time.Second
	tests := []struct {
		name         string
		send, expect time.Duration
		header       string
		present      bool
		want         session.Heartbeat
	}{
		{"missing header is 0,0", 10 * s, 10 * s, "", false, session.Heartbeat{}},
		{"server 0,0 disables both", 10 * s, 10 * s, "0,0", true, session.Heartbeat{}},
		{"max of each pair", 4 * s, 6 * s, "5000,3000", true, session.Heartbeat{Send: 4 * s, Recv: 6 * s}},
		{"server slower wins", 1 * s, 1 * s, "5000,3000", true, session.Heartbeat{Send: 3 * s, Recv: 5 * s}},
		{"client cannot send", 0, 1 * s, "1000,1000", true, session.Heartbeat{Recv: 1 * s}},
		{"client wants none", 1 * s, 0, "1000,1000", true, session.Heartbeat{Send: 1 * s}},
		{"server wants none", 1 * s, 1 * s, "1000,0", true, session.Heartbeat{Recv: 1 * s}},
	}
	for _, tt := range tests {
		got, err := session.Negotiate(tt.send, tt.expect, []byte(tt.header), tt.present)
		if err != nil || got != tt.want {
			t.Errorf("%s: Negotiate(%v, %v, %q) = %+v, %v; want %+v", tt.name, tt.send, tt.expect, tt.header, got, err, tt.want)
		}
	}
	for _, bad := range []string{"1000", "a,b", "-1,0", "1000,1000,1000"} {
		if _, err := session.Negotiate(s, s, []byte(bad), true); !errors.Is(err, session.ErrProtocol) {
			t.Errorf("Negotiate(%q) error = %v; want ErrProtocol", bad, err)
		}
	}
}

func TestAccept(t *testing.T) {
	if _, ok, err := session.Accept(decode(t, "ERROR\nmessage:denied\n\n\x00"), both, 0, 0); ok || err != nil {
		t.Errorf("ERROR reply: ok %v err %v; want false, nil", ok, err)
	}
	if _, _, err := session.Accept(decode(t, "CONNECTED\nversion:1.1\n\n\x00"), only12, 0, 0); !errors.Is(err, session.ErrUnsupportedVersion) {
		t.Errorf("version 1.1: %v", err)
	}
	if _, _, err := session.Accept(decode(t, "CONNECTED\n\n\x00"), both, 0, 0); !errors.Is(err, session.ErrUnsupportedVersion) {
		t.Errorf("missing version: %v", err)
	}
	a, ok, err := session.Accept(decode(t, "CONNECTED\nversion:1.1\nheart-beat:0,0\n\n\x00"), both, 0, 0)
	if !ok || err != nil || a.Version != stomptext.Version11 {
		t.Errorf("1.1 offered and chosen: %+v %v %v", a, ok, err)
	}
	if _, _, err := session.Accept(decode(t, "CONNECTED\nversion:1.0\n\n\x00"), both, 0, 0); !errors.Is(err, session.ErrUnsupportedVersion) {
		t.Errorf("version 1.0: %v", err)
	}
	if _, _, err := session.Accept(decode(t, "RECEIPT\nreceipt-id:1\n\n\x00"), both, 0, 0); !errors.Is(err, session.ErrProtocol) {
		t.Errorf("RECEIPT reply: %v", err)
	}
}

func TestReceiveRoutesBySubscriptionHeader(t *testing.T) {
	s := session.New(1, v12)
	s.Subscribe("a", "/queue/x", session.Client, nil)
	s.Subscribe("b", "/queue/x", session.ClientIndividual, nil)
	ev, err := s.Receive(decode(t, "MESSAGE\nsubscription:b\ndestination:/queue/x\nmessage-id:1\nack:K1\n\n\x00"))
	if err != nil || ev.Kind != session.EventDeliver || ev.Sub != "b" || ev.AckID != "K1" {
		t.Errorf("Receive = %+v, %v; want deliver to b with ack K1", ev, err)
	}
	if ev, err := s.Receive(decode(t, "MESSAGE\nsubscription:zz\n\n\x00")); err != nil || ev.Kind != session.EventDiscard || ev.Sub != "zz" {
		t.Errorf("unknown subscription: %+v, %v; want discard for zz (SUB-2)", ev, err)
	}
	if _, err := s.Receive(decode(t, "MESSAGE\nsubscription:b\n\n\x00")); !errors.Is(err, session.ErrProtocol) {
		t.Errorf("missing ack on client-individual: %v; want ErrProtocol", err)
	}
}

func TestUnsubscribeDrainsUntilReceipt(t *testing.T) {
	s := session.New(1, v12)
	s.Subscribe("a", "/q", session.Client, nil)
	_, r, _ := s.Unsubscribe("a")
	ev, err := s.Receive(decode(t, "MESSAGE\nsubscription:a\n\n\x00"))
	if err != nil || ev.Kind != session.EventDiscard {
		t.Errorf("MESSAGE while draining = %+v, %v; want discard", ev, err)
	}
	ev, err = s.Receive(decode(t, "RECEIPT\nreceipt-id:"+r+"\n\n\x00"))
	if err != nil || ev.Kind != session.EventReceipt || ev.Pending.Kind != session.KindUnsubscribe {
		t.Errorf("RECEIPT = %+v, %v", ev, err)
	}
	if ev, err := s.Receive(decode(t, "MESSAGE\nsubscription:a\n\n\x00")); err != nil || ev.Kind != session.EventDiscard {
		t.Errorf("MESSAGE after UNSUBSCRIBE receipt = %+v, %v; want discard (SUB-2)", ev, err)
	}
}

func TestReceiptsResolveOnce(t *testing.T) {
	s := session.New(7, v12)
	_, r1, _ := s.Send(session.Outgoing{Destination: "/q"})
	_, r2, _ := s.Send(session.Outgoing{Destination: "/q"})
	if r1 == r2 {
		t.Fatalf("receipt ids collide: %q", r1)
	}
	if _, err := s.Receive(decode(t, "RECEIPT\nreceipt-id:"+r1+"\n\n\x00")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Receive(decode(t, "RECEIPT\nreceipt-id:"+r1+"\n\n\x00")); !errors.Is(err, session.ErrProtocol) {
		t.Errorf("second RECEIPT for %s: %v; want ErrProtocol", r1, err)
	}
	if got := slices.Collect(s.Outstanding()); !slices.Equal(got, []string{r2}) {
		t.Errorf("outstanding = %v; want only %s", got, r2)
	}
}

func TestErrorMatchesPendingReceipt(t *testing.T) {
	s := session.New(1, v12)
	_, r, _ := s.Subscribe("a", "/q", session.Client, nil)
	ev, err := s.Receive(decode(t, "ERROR\nreceipt-id:"+r+"\nmessage:no queue\n\n\x00"))
	if err != nil || ev.Kind != session.EventError || !ev.Known || ev.Pending.Kind != session.KindSubscribe || ev.Pending.Sub != "a" {
		t.Errorf("ERROR = %+v, %v; want known subscribe of a", ev, err)
	}
}

func TestAckRules(t *testing.T) {
	s := session.New(2, v12)
	s.Subscribe("a", "/q", session.Client, nil)
	f, r, err := s.Ack(session.AckHandle{Gen: 2, Sub: "a", Seq: 1, ID: "K"}, false, false)
	if err != nil || r != "" || f.Command != stomptext.Ack || len(f.Headers) != 1 || f.Headers[0] != (stomptext.Header{Name: "id", Value: "K"}) {
		t.Errorf("Ack = %+v, %q, %v; want ACK id:K without receipt", f, r, err)
	}
	f, r, err = s.Ack(session.AckHandle{Gen: 2, Sub: "a", Seq: 1, ID: "K"}, true, true)
	if err != nil || r == "" || f.Command != stomptext.Nack {
		t.Errorf("NackWait = %+v, %q, %v; want NACK with receipt", f, r, err)
	}
	if _, _, err := s.Ack(session.AckHandle{Gen: 1, Sub: "a", Seq: 1, ID: "K"}, false, false); !errors.Is(err, session.ErrStaleAck) {
		t.Errorf("earlier session: %v; want ErrStaleAck", err)
	}
}

func TestSendFrame(t *testing.T) {
	s := session.New(1, v12)
	f, r, err := s.Send(session.Outgoing{Destination: "/q", ContentType: "text/plain", Body: []byte("hi"), Headers: stomptext.Headers{{Name: "x", Value: "1"}}})
	want := stomptext.Headers{{Name: "destination", Value: "/q"}, {Name: "content-type", Value: "text/plain"}, {Name: "content-length", Value: "2"}, {Name: "receipt", Value: r}, {Name: "x", Value: "1"}}
	if err != nil || len(f.Headers) != len(want) {
		t.Fatalf("Send = %+v, %v", f, err)
	}
	for i := range want {
		if f.Headers[i] != want[i] {
			t.Errorf("header %d = %v; want %v", i, f.Headers[i], want[i])
		}
	}
	for _, bad := range []session.Outgoing{
		{},
		{Destination: "/q", Headers: stomptext.Headers{{Name: "", Value: "v"}}},
		{Destination: "/q", Headers: stomptext.Headers{{Name: "destination", Value: "/other"}}},
		{Destination: "/q", Text: true, Body: []byte{0}},
	} {
		if _, _, err := s.Send(bad); err == nil {
			t.Errorf("Send(%+v) succeeded", bad)
		}
	}
	if n := len(slices.Collect(s.Outstanding())); n != 1 {
		t.Errorf("rejected sends left %d receipts pending; want 1", n)
	}
}

func TestDrainingIDCannotBeReusedUntilReceipt(t *testing.T) {
	s := session.New(1, v12)
	s.Subscribe("x", "/q", session.Client, nil)
	_, r, _ := s.Unsubscribe("x")
	if _, _, err := s.Subscribe("x", "/q", session.Client, nil); !errors.Is(err, session.ErrDuplicateID) {
		t.Errorf("Subscribe x while draining: %v; want ErrDuplicateID", err)
	}
	if _, err := s.Receive(decode(t, "RECEIPT\nreceipt-id:"+r+"\n\n\x00")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Subscribe("x", "/q", session.Client, nil); err != nil {
		t.Errorf("Subscribe x after UNSUBSCRIBE receipt: %v", err)
	}
	ev, err := s.Receive(decode(t, "MESSAGE\nsubscription:x\nack:K\n\n\x00"))
	if err != nil || ev.Kind != session.EventDeliver {
		t.Errorf("MESSAGE for reused x = %+v, %v; want deliver", ev, err)
	}
}

func TestNoFrameAfterDisconnect(t *testing.T) {
	s := session.New(1, v12)
	s.Subscribe("a", "/q", session.Client, nil)
	s.Disconnect()
	if _, _, err := s.Send(session.Outgoing{Destination: "/q"}); !errors.Is(err, session.ErrDisconnecting) {
		t.Errorf("Send: %v", err)
	}
	if _, _, err := s.Subscribe("b", "/q", session.Client, nil); !errors.Is(err, session.ErrDisconnecting) {
		t.Errorf("Subscribe: %v", err)
	}
	if _, _, err := s.Unsubscribe("a"); !errors.Is(err, session.ErrDisconnecting) {
		t.Errorf("Unsubscribe: %v", err)
	}
	if _, _, err := s.Ack(session.AckHandle{Gen: 1, Sub: "a", Seq: 1, ID: "K"}, false, false); !errors.Is(err, session.ErrDisconnecting) {
		t.Errorf("Ack: %v", err)
	}
}

func TestConnectOffersVersions(t *testing.T) {
	f := session.Connect(both, "/", "", "", "", 0, 0)
	if v, _ := f.Headers.Get("accept-version"); v != "1.1,1.2" {
		t.Errorf("accept-version = %q; want 1.1,1.2", v)
	}
}

func TestVersion11Acknowledgement(t *testing.T) {
	s := session.New(1, v11)
	s.Subscribe("a", "/q", session.ClientIndividual, nil)
	ev, err := s.Receive(decode(t, "MESSAGE\nsubscription:a\nmessage-id:m7\n\n\x00"))
	if err != nil || ev.AckID != "m7" {
		t.Fatalf("1.1 MESSAGE = %+v, %v; want ack id m7", ev, err)
	}
	f, _, err := s.Ack(session.AckHandle{Gen: 1, Sub: "a", Seq: 1, ID: ev.AckID}, true, false)
	want := stomptext.Headers{{Name: "message-id", Value: "m7"}, {Name: "subscription", Value: "a"}}
	if err != nil || f.Command != stomptext.Nack || len(f.Headers) != 2 || f.Headers[0] != want[0] || f.Headers[1] != want[1] {
		t.Errorf("1.1 NACK = %+v, %v; want %v", f, err, want)
	}
	if _, err := s.Receive(decode(t, "MESSAGE\nsubscription:a\nack:x\n\n\x00")); !errors.Is(err, session.ErrProtocol) {
		t.Errorf("1.1 MESSAGE without message-id: %v; want ErrProtocol", err)
	}
}

func TestTransactionLifecycle(t *testing.T) {
	s := session.New(3, v12)
	s.Subscribe("a", "/q", session.Client, nil)
	b, tx, r, err := s.Begin("tx-1")
	if err != nil || b.Command != stomptext.Begin || r == "" {
		t.Fatalf("Begin = %+v, %v", b, err)
	}
	if _, _, _, err := s.Begin("tx-1"); !errors.Is(err, session.ErrInvalidRequest) {
		t.Errorf("second Begin tx-1: %v", err)
	}
	f, err := s.SendTx(tx, session.Outgoing{Destination: "/q", Body: []byte("x")})
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := f.Headers.Get("transaction"); v != "tx-1" {
		t.Errorf("SEND transaction = %q", v)
	}
	if _, ok := f.Headers.Get("receipt"); ok {
		t.Error("transactional SEND requests a receipt")
	}
	if _, err := s.SendTx(tx, session.Outgoing{Destination: "/q", Headers: stomptext.Headers{{Name: "transaction", Value: "other"}}}); !errors.Is(err, session.ErrReservedHeader) {
		t.Errorf("transaction header: %v; want ErrReservedHeader", err)
	}
	a, err := s.AckTx(tx, session.AckHandle{Gen: 3, Sub: "a", Seq: 1, ID: "K"}, false)
	if v, _ := a.Headers.Get("transaction"); err != nil || v != "tx-1" {
		t.Errorf("ACK in tx = %+v, %v", a, err)
	}
	if _, err := s.AckTx(tx, session.AckHandle{Gen: 2, Sub: "a", Seq: 1, ID: "K"}, false); !errors.Is(err, session.ErrStaleAck) {
		t.Errorf("tx ack of earlier session's message: %v; want ErrStaleAck", err)
	}
	c, _, err := s.Finish(tx, false)
	if err != nil || c.Command != stomptext.Commit {
		t.Fatalf("Commit = %+v, %v", c, err)
	}
	if _, err := s.SendTx(tx, session.Outgoing{Destination: "/q"}); !errors.Is(err, session.ErrTxDone) {
		t.Errorf("SendTx after COMMIT: %v; want ErrTxDone", err)
	}
	if _, _, err := s.Finish(tx, true); !errors.Is(err, session.ErrTxDone) {
		t.Errorf("Abort after COMMIT: %v; want ErrTxDone", err)
	}
	next := session.New(4, v12)
	if _, err := next.SendTx(tx, session.Outgoing{Destination: "/q"}); !errors.Is(err, session.ErrTxAborted) {
		t.Errorf("SendTx in a later session: %v; want ErrTxAborted", err)
	}
}

func TestDisconnectBuiltOnce(t *testing.T) {
	s := session.New(1, v12)
	if _, _, err := s.Disconnect(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Disconnect(); !errors.Is(err, session.ErrDisconnecting) {
		t.Errorf("second Disconnect: %v; want ErrDisconnecting", err)
	}
}

func TestReusedTransactionIDDoesNotReviveOldHandle(t *testing.T) {
	s := session.New(1, v12)
	_, old, _, _ := s.Begin("t1")
	if _, _, err := s.Finish(old, false); err != nil {
		t.Fatal(err)
	}
	_, current, _, err := s.Begin("t1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.SendTx(old, session.Outgoing{Destination: "/q"}); !errors.Is(err, session.ErrTxDone) {
		t.Errorf("SendTx with the committed handle: %v; want ErrTxDone", err)
	}
	if _, err := s.SendTx(current, session.Outgoing{Destination: "/q"}); err != nil {
		t.Errorf("SendTx with the new handle: %v", err)
	}
}

func TestAckForEarlierSubscriptionWithReusedIDIsStale(t *testing.T) {
	s := session.New(1, v12)
	s.Subscribe("a", "/q", session.ClientIndividual, nil)
	ev, err := s.Receive(decode(t, "MESSAGE\nsubscription:a\nmessage-id:M\nack:K\n\n\x00"))
	if err != nil {
		t.Fatal(err)
	}
	_, r, _ := s.Unsubscribe("a")
	if _, err := s.Receive(decode(t, "RECEIPT\nreceipt-id:"+r+"\n\n\x00")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Subscribe("a", "/q", session.ClientIndividual, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Ack(session.AckHandle{Gen: 1, Sub: "a", Seq: ev.Seq, ID: ev.AckID}, false, false); !errors.Is(err, session.ErrStaleAck) {
		t.Errorf("Ack of a delivery to the earlier subscription a = %v; want ErrStaleAck", err)
	}
}

func TestReceiveDeliveryAllocatesOnlyTheAckID(t *testing.T) {
	s := session.New(1, v12)
	s.Subscribe("sub-1", "/q", session.ClientIndividual, nil)
	f := decode(t, "MESSAGE\nsubscription:sub-1\nmessage-id:M\nack:ID:broker-1-42\n\n\x00")
	if n := testing.AllocsPerRun(1000, func() { s.Receive(f) }); n != 1 {
		t.Errorf("Receive of a client-individual MESSAGE allocates %v times; want 1, the ack id", n)
	}
}

func TestCheckSubscribeHeadersRejectsReservedNames(t *testing.T) {
	for _, name := range []string{"id", "destination", "ack", "receipt", "content-length"} {
		if err := session.CheckSubscribeHeaders(stomptext.Headers{{Name: name, Value: "v"}}); !errors.Is(err, session.ErrReservedHeader) {
			t.Errorf("%s: %v; want ErrReservedHeader", name, err)
		}
	}
	if err := session.CheckSubscribeHeaders(stomptext.Headers{{Name: "selector", Value: "a"}}); err != nil {
		t.Errorf("selector: %v", err)
	}
}

func TestFramesBuiltFromAcceptedHeadersEncode(t *testing.T) {
	names := []string{"", "content-length", "content-type", "x", "a:b", "a\nb", "a\rb", "a\\b", "transaction", "receipt", "id", "ack", "destination"}
	values := []string{"", "5", "0", "a:b", "a\nb", "a\rb", "a\\b", "\x00"}
	bodies := [][]byte{nil, []byte("abc"), []byte("a\x00b")}
	for _, v := range []stomptext.Version{stomptext.Version11, stomptext.Version12} {
		encodes := func(what string, f session.Frame) {
			t.Helper()
			if _, err := f.Append(nil, v); err != nil {
				t.Errorf("%v %s: accepted but not encodable: %v", v, what, err)
			}
		}
		txs := session.New(1, session.Accepted{Version: v})
		f, tx, _, err := txs.Begin("t")
		if err != nil {
			t.Fatal(err)
		}
		encodes("BEGIN", f)
		for _, n := range names {
			for _, val := range values {
				hs := stomptext.Headers{{Name: n, Value: val}}
				s := session.New(1, session.Accepted{Version: v})
				if f, _, err := s.Subscribe("s", "/q", session.ClientIndividual, hs); err == nil {
					encodes(fmt.Sprintf("SUBSCRIBE with %q", hs), f)
				}
				for _, body := range bodies {
					for _, text := range []bool{false, true} {
						m := session.Outgoing{Destination: "/q", Headers: hs, Body: body, Text: text}
						what := fmt.Sprintf("SEND with %q body %q text %v", hs, body, text)
						if f, _, err := s.Send(m); err == nil {
							encodes(what, f)
						}
						if f, err := txs.SendTx(tx, m); err == nil {
							encodes("transactional "+what, f)
						}
					}
				}
			}
		}
	}
}
