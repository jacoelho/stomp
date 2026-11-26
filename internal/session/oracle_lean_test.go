//go:build lean

package session_test

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/jacoelho/stomp/internal/session"
	"github.com/jacoelho/stomp/stomptext"
)

func leanOracle(t *testing.T) string {
	t.Helper()
	path := os.Getenv("STOMP_LEAN_ORACLE")
	if path == "" {
		path = filepath.Join("..", "..", "proofs", ".lake", "build", "bin", "stomp-oracle")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("Lean oracle %s: %v (run `lake build` in proofs/ or set STOMP_LEAN_ORACLE)", path, err)
	}
	return path
}

// askOracle sends one request per line and returns one response per request.
func askOracle(t *testing.T, requests []any) []json.RawMessage {
	t.Helper()
	var in bytes.Buffer
	enc := json.NewEncoder(&in)
	for _, r := range requests {
		if err := enc.Encode(r); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command(leanOracle(t))
	cmd.Stdin = &in
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("oracle: %v", err)
	}
	var resps []json.RawMessage
	sc := bufio.NewScanner(bytes.NewReader(out))
	sc.Buffer(nil, 64<<20)
	for sc.Scan() {
		resps = append(resps, json.RawMessage(slices.Clone(sc.Bytes())))
	}
	if len(resps) != len(requests) {
		t.Fatalf("oracle answered %d of %d requests", len(resps), len(requests))
	}
	return resps
}

var sessionErrors = map[string]error{
	"ErrDisconnecting":  session.ErrDisconnecting,
	"ErrDuplicateID":    session.ErrDuplicateID,
	"ErrNotSubscribed":  session.ErrNotSubscribed,
	"ErrStaleAck":       session.ErrStaleAck,
	"ErrTxAborted":      session.ErrTxAborted,
	"ErrTxDone":         session.ErrTxDone,
	"ErrInvalidRequest": session.ErrInvalidRequest,
	"ErrReservedHeader": session.ErrReservedHeader,
	"ErrTextHasNUL":     session.ErrTextHasNUL,
	"ErrProtocol":       session.ErrProtocol,
}

func errorKind(t *testing.T, err error) string {
	t.Helper()
	var found []string
	for _, name := range slices.Sorted(maps.Keys(sessionErrors)) {
		if errors.Is(err, sessionErrors[name]) {
			found = append(found, name)
		}
	}
	if len(found) != 1 {
		t.Fatalf("error %v matches %v", err, found)
	}
	return found[0]
}

type frameJSON struct {
	Command string      `json:"command"`
	Headers [][2]string `json:"headers"`
	Body    string      `json:"body"`
}

type txJSON struct {
	Gen uint64 `json:"gen"`
	ID  string `json:"id"`
	Seq uint64 `json:"seq"`
}

type pendingJSON struct {
	Kind string `json:"kind"`
	Sub  string `json:"sub"`
	Tx   string `json:"tx"`
}

type eventJSON struct {
	Kind    string       `json:"kind"`
	Sub     string       `json:"sub"`
	Seq     uint64       `json:"seq"`
	AckID   string       `json:"ackId"`
	Receipt string       `json:"receipt"`
	Pending *pendingJSON `json:"pending"`
	Known   bool         `json:"known"`
}

type stepJSON struct {
	Error   string     `json:"error"`
	Frame   *frameJSON `json:"frame"`
	Receipt string     `json:"receipt"`
	Tx      *txJSON    `json:"tx"`
	Event   *eventJSON `json:"event"`
}

func headersJSON(hs stomptext.Headers) [][2]string {
	out := [][2]string{}
	for _, h := range hs {
		out = append(out, [2]string{h.Name, h.Value})
	}
	return out
}

func toFrameJSON(f session.Frame) *frameJSON {
	return &frameJSON{Command: string(f.Command), Headers: headersJSON(f.Headers), Body: hex.EncodeToString(f.Body)}
}

var kindNames = map[session.Kind]string{
	session.KindSend: "send", session.KindSubscribe: "subscribe",
	session.KindUnsubscribe: "unsubscribe", session.KindAck: "ack", session.KindDisconnect: "disconnect",
	session.KindBegin: "begin", session.KindCommit: "commit", session.KindAbort: "abort",
}

var eventNames = map[session.EventKind]string{
	session.EventDeliver: "deliver", session.EventDiscard: "discard",
	session.EventReceipt: "receipt", session.EventError: "error",
}

func toEventJSON(e session.Event) *eventJSON {
	out := &eventJSON{Kind: eventNames[e.Kind], Sub: e.Sub, Seq: e.Seq, AckID: e.AckID, Receipt: e.Receipt, Known: e.Known}
	if e.Pending != (session.Pending{}) {
		out.Pending = &pendingJSON{Kind: kindNames[e.Pending.Kind], Sub: e.Pending.Sub, Tx: e.Pending.Tx}
	}
	return out
}

// step is one call on a session, in the oracle's trace format.
type step map[string]any

type traceGen struct {
	r          *rand.Rand
	gen        uint64
	v          stomptext.Version
	subscribes uint64
}

var (
	subIDs   = []string{"s1", "s2", "s3"}
	txIDs    = []string{"t1", "t2"}
	ackIDs   = []string{"m1", "m2", ""}
	modes    = []string{"client", "client-individual"}
	modeOf   = map[string]session.Mode{"client": session.Client, "client-individual": session.ClientIndividual}
	userKeys = []string{"x-a", "", "destination", "receipt", "content-length", "id", "ack", "transaction"}
)

func (g *traceGen) pick(xs []string) string { return xs[g.r.IntN(len(xs))] }

func (g *traceGen) handleGen() uint64 {
	if g.r.IntN(5) == 0 {
		return g.gen + 1
	}
	return g.gen
}

func (g *traceGen) headers(n int) [][2]string {
	hs := [][2]string{}
	for range g.r.IntN(n + 1) {
		hs = append(hs, [2]string{g.pick(userKeys), g.pick(ackIDs)})
	}
	return hs
}

// subSeq picks a subscription sequence number up to the number of Subscribe
// steps generated so far, so a handle is sometimes current and sometimes stale.
func (g *traceGen) subSeq() uint64 {
	return g.r.Uint64N(g.subscribes + 1)
}

func (g *traceGen) tx() map[string]any {
	return map[string]any{"gen": g.handleGen(), "id": g.pick(txIDs), "seq": 1 + g.r.Uint64N(2)}
}

func (g *traceGen) msg() map[string]any {
	body := []byte{}
	for range g.r.IntN(4) {
		body = append(body, []byte{'a', 0, 'b'}[g.r.IntN(3)])
	}
	dest := "/q"
	if g.r.IntN(10) == 0 {
		dest = ""
	}
	ct := ""
	if g.r.IntN(3) == 0 {
		ct = "text/plain"
	}
	return map[string]any{"destination": dest, "contentType": ct, "headers": g.headers(2), "body": hex.EncodeToString(body), "text": g.r.IntN(2) == 0}
}

func (g *traceGen) inbound() step {
	cmd := []string{"MESSAGE", "MESSAGE", "MESSAGE", "RECEIPT", "RECEIPT", "ERROR", "CONNECTED"}[g.r.IntN(7)]
	hs := [][2]string{}
	switch cmd {
	case "MESSAGE":
		if g.r.IntN(8) > 0 {
			hs = append(hs, [2]string{"subscription", g.pick(append(subIDs, "zz"))})
		}
		for _, name := range []string{"ack", "message-id"} {
			if g.r.IntN(4) > 0 {
				hs = append(hs, [2]string{name, g.pick(ackIDs)})
			}
		}
	case "RECEIPT", "ERROR":
		if g.r.IntN(8) > 0 {
			hs = append(hs, [2]string{"receipt-id", fmt.Sprintf("%d-%d", g.handleGen(), 1+g.r.IntN(8))})
		}
	}
	if g.r.IntN(4) == 0 {
		slices.Reverse(hs)
	}
	return step{"do": "receive", "command": cmd, "headers": hs}
}

func (g *traceGen) step() step {
	switch g.r.IntN(13) {
	case 0, 1:
		g.subscribes++
		return step{"do": "subscribe", "id": g.pick(subIDs), "dest": "/q", "mode": g.pick(modes), "extra": g.headers(1)}
	case 2:
		return step{"do": "unsubscribe", "id": g.pick(subIDs)}
	case 3:
		return step{"do": "ack", "gen": g.handleGen(), "sub": g.pick(subIDs), "seq": g.subSeq(), "ackId": g.pick(ackIDs), "nack": g.r.IntN(2) == 0, "wait": g.r.IntN(2) == 0}
	case 4:
		return step{"do": "ackTx", "tx": g.tx(), "gen": g.handleGen(), "sub": g.pick(subIDs), "seq": g.subSeq(), "ackId": g.pick(ackIDs), "nack": g.r.IntN(2) == 0}
	case 5:
		return step{"do": "send", "msg": g.msg()}
	case 6:
		return step{"do": "sendTx", "tx": g.tx(), "msg": g.msg()}
	case 7:
		return step{"do": "begin", "id": g.pick(txIDs)}
	case 8:
		return step{"do": "finish", "tx": g.tx(), "abort": g.r.IntN(2) == 0}
	case 9:
		if g.r.IntN(8) == 0 {
			return step{"do": "disconnect"}
		}
		return step{"do": "end", "id": g.pick(subIDs)}
	default:
		return g.inbound()
	}
}

func pairs(t *testing.T, v any) stomptext.Headers {
	t.Helper()
	var hs stomptext.Headers
	for _, p := range v.([][2]string) {
		hs = append(hs, stomptext.Header{Name: p[0], Value: p[1]})
	}
	return hs
}

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func outgoing(t *testing.T, m map[string]any) session.Outgoing {
	return session.Outgoing{
		Destination: m["destination"].(string), ContentType: m["contentType"].(string),
		Headers: pairs(t, m["headers"]), Body: unhex(t, m["body"].(string)), Text: m["text"].(bool),
	}
}

func ackHandle(s step) session.AckHandle {
	return session.AckHandle{Gen: s["gen"].(uint64), Sub: s["sub"].(string), Seq: s["seq"].(uint64), ID: s["ackId"].(string)}
}

func txHandle(m map[string]any) session.TxHandle {
	return session.TxHandle{Gen: m["gen"].(uint64), ID: m["id"].(string), Seq: m["seq"].(uint64)}
}

func inboundFrame(t *testing.T, v stomptext.Version, s step) stomptext.Frame {
	t.Helper()
	raw, err := stomptext.AppendFrame(nil, v, stomptext.Command(s["command"].(string)), pairs(t, s["headers"]), nil)
	if err != nil {
		t.Fatal(err)
	}
	dec := stomptext.NewDecoder(bytes.NewReader(raw))
	dec.SetVersion(v)
	f, err := dec.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	return f.Clone()
}

// apply runs s on the Go session in the oracle's result format.
func apply(t *testing.T, sess *session.Session, s step) stepJSON {
	t.Helper()
	var (
		f   session.Frame
		r   string
		err error
		out stepJSON
	)
	switch s["do"] {
	case "subscribe":
		f, r, err = sess.Subscribe(s["id"].(string), s["dest"].(string), modeOf[s["mode"].(string)], pairs(t, s["extra"]))
	case "unsubscribe":
		f, r, err = sess.Unsubscribe(s["id"].(string))
	case "ack":
		f, r, err = sess.Ack(ackHandle(s), s["nack"].(bool), s["wait"].(bool))
	case "ackTx":
		f, err = sess.AckTx(txHandle(s["tx"].(map[string]any)), ackHandle(s), s["nack"].(bool))
	case "send":
		f, r, err = sess.Send(outgoing(t, s["msg"].(map[string]any)))
	case "sendTx":
		f, err = sess.SendTx(txHandle(s["tx"].(map[string]any)), outgoing(t, s["msg"].(map[string]any)))
	case "begin":
		var th session.TxHandle
		f, th, r, err = sess.Begin(s["id"].(string))
		if err == nil {
			out.Tx = &txJSON{Gen: th.Gen, ID: th.ID, Seq: th.Seq}
		}
	case "finish":
		f, r, err = sess.Finish(txHandle(s["tx"].(map[string]any)), s["abort"].(bool))
	case "disconnect":
		f, r, err = sess.Disconnect()
	case "end":
		sess.End(s["id"].(string))
		return out
	case "receive":
		ev, err := sess.Receive(inboundFrame(t, sess.Version(), s))
		if err != nil {
			return stepJSON{Error: errorKind(t, err)}
		}
		out.Event = toEventJSON(ev)
		return out
	default:
		t.Fatalf("unknown step %v", s["do"])
	}
	if err != nil {
		return stepJSON{Error: errorKind(t, err)}
	}
	out.Frame = toFrameJSON(f)
	out.Receipt = r
	return out
}

func TestLeanOracleSessionTracesAgree(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 6))
	type trace struct {
		gen   uint64
		v     stomptext.Version
		steps []step
	}
	tx1 := map[string]any{"gen": uint64(1), "id": "t1", "seq": uint64(1)}
	msg := map[string]any{"destination": "/q", "contentType": "", "headers": [][2]string{}, "body": "", "text": true}
	// The fixed traces reach CON-25 (a second Disconnect), TX-7 (a finished
	// handle after its id is reused) and SUB-31 (an ACK of a delivery to an
	// earlier subscription whose id is reused).
	traces := []trace{
		{gen: 1, v: stomptext.Version12, steps: []step{{"do": "disconnect"}, {"do": "disconnect"}}},
		{gen: 1, v: stomptext.Version12, steps: []step{
			{"do": "begin", "id": "t1"}, {"do": "finish", "tx": tx1, "abort": false},
			{"do": "begin", "id": "t1"}, {"do": "sendTx", "tx": tx1, "msg": msg},
		}},
		{gen: 1, v: stomptext.Version12, steps: []step{
			{"do": "subscribe", "id": "s1", "dest": "/q", "mode": "client", "extra": [][2]string{}},
			{"do": "receive", "command": "MESSAGE", "headers": [][2]string{{"subscription", "s1"}, {"ack", "m1"}}},
			{"do": "unsubscribe", "id": "s1"},
			{"do": "receive", "command": "RECEIPT", "headers": [][2]string{{"receipt-id", "1-2"}}},
			{"do": "subscribe", "id": "s1", "dest": "/q", "mode": "client", "extra": [][2]string{}},
			{"do": "ack", "gen": uint64(1), "sub": "s1", "seq": uint64(1), "ackId": "m1", "nack": false, "wait": false},
			{"do": "ack", "gen": uint64(1), "sub": "s1", "seq": uint64(2), "ackId": "m1", "nack": false, "wait": false},
		}},
	}
	for len(traces) < 600 {
		g := traceGen{r: r, gen: 1 + r.Uint64N(5), v: stomptext.Version12}
		if r.IntN(2) == 0 {
			g.v = stomptext.Version11
		}
		tr := trace{gen: g.gen, v: g.v}
		for range 1 + r.IntN(30) {
			tr.steps = append(tr.steps, g.step())
		}
		traces = append(traces, tr)
	}
	reqs := make([]any, len(traces))
	for i, tr := range traces {
		reqs[i] = map[string]any{"op": "session", "version": tr.v.String(), "gen": tr.gen, "trace": tr.steps}
	}
	resps := askOracle(t, reqs)
	outcomes := map[string]int{}
	steps := 0
	for i, tr := range traces {
		var want struct {
			Steps []stepJSON `json:"steps"`
		}
		if err := json.Unmarshal(resps[i], &want); err != nil || len(want.Steps) != len(tr.steps) {
			t.Fatalf("oracle response %s: %v", resps[i], err)
		}
		sess := session.New(tr.gen, session.Accepted{Version: tr.v})
		for j, s := range tr.steps {
			got := apply(t, sess, s)
			if !reflect.DeepEqual(got, want.Steps[j]) {
				g, _ := json.Marshal(got)
				w, _ := json.Marshal(want.Steps[j])
				t.Fatalf("trace %d (gen %d, %v) step %d %v:\n go:   %s\n lean: %s\n trace: %v", i, tr.gen, tr.v, j, s, g, w, tr.steps[:j+1])
			}
			key := fmt.Sprint(s["do"])
			switch {
			case got.Error != "":
				key += ":" + got.Error
			case got.Event != nil:
				key += ":" + got.Event.Kind
			}
			outcomes[key]++
			steps++
		}
	}
	t.Logf("%d traces, %d steps agree; outcomes %v", len(traces), steps, outcomes)
}

func TestLeanOracleNegotiateAgrees(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 8))
	parts := []string{"0", "1", "10", "007", "2147483647", "2147483648", "99999999999", "", "x", "+1", "-1", " 1", "1_0"}
	durations := []time.Duration{0, time.Millisecond, 5 * time.Millisecond, time.Second, 1500 * time.Microsecond, -time.Second, math.MaxInt64}
	type tc struct {
		send, expect time.Duration
		header       []byte
		present      bool
	}
	cases := make([]tc, 2000)
	reqs := make([]any, len(cases))
	for i := range cases {
		c := tc{send: durations[r.IntN(len(durations))], expect: durations[r.IntN(len(durations))], present: r.IntN(8) > 0}
		req := map[string]any{"op": "negotiate", "send": int64(c.send), "expect": int64(c.expect), "header": nil}
		if c.present {
			h := parts[r.IntN(len(parts))]
			if r.IntN(10) > 0 {
				h += "," + parts[r.IntN(len(parts))]
			}
			if r.IntN(10) == 0 {
				h += "," + parts[r.IntN(len(parts))]
			}
			c.header = []byte(h)
			req["header"] = hex.EncodeToString(c.header)
		}
		cases[i] = c
		reqs[i] = req
	}
	resps := askOracle(t, reqs)
	type result struct {
		Send  int64  `json:"send"`
		Recv  int64  `json:"recv"`
		Error string `json:"error"`
	}
	outcomes := map[string]int{}
	for i, c := range cases {
		var want result
		if err := json.Unmarshal(resps[i], &want); err != nil {
			t.Fatalf("oracle response %s: %v", resps[i], err)
		}
		var got result
		hb, err := session.Negotiate(c.send, c.expect, c.header, c.present)
		if err != nil {
			got.Error = errorKind(t, err)
			outcomes[got.Error]++
		} else {
			got.Send, got.Recv = int64(hb.Send), int64(hb.Recv)
			outcomes["ok"]++
		}
		if got != want {
			t.Fatalf("Negotiate(%v, %v, %q, %v): go %+v, lean %+v", c.send, c.expect, c.header, c.present, got, want)
		}
	}
	t.Logf("%d negotiate cases agree; outcomes %v", len(cases), outcomes)
}
