//go:build lean

package stomptext_test

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"math"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"testing"

	"github.com/jacoelho/stomp/stomptext"
)

func leanOracle(t *testing.T) string {
	t.Helper()
	path := os.Getenv("STOMP_LEAN_ORACLE")
	if path == "" {
		path = filepath.Join("..", "proofs", ".lake", "build", "bin", "stomp-oracle")
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

var codecErrors = map[string]error{
	"ErrUnknownCommand":       stomptext.ErrUnknownCommand,
	"ErrInvalidEscape":        stomptext.ErrInvalidEscape,
	"ErrInvalidHeader":        stomptext.ErrInvalidHeader,
	"ErrInvalidContentLength": stomptext.ErrInvalidContentLength,
	"ErrMissingNUL":           stomptext.ErrMissingNUL,
	"ErrBodyNotAllowed":       stomptext.ErrBodyNotAllowed,
	"ErrInvalidEOL":           stomptext.ErrInvalidEOL,
	"ErrNULInBody":            stomptext.ErrNULInBody,
	"ErrFrameTooLarge":        stomptext.ErrFrameTooLarge,
	"ErrTooManyHeaders":       stomptext.ErrTooManyHeaders,
	"ErrHeaderTooLarge":       stomptext.ErrHeaderTooLarge,
	"eof":                     io.EOF,
	"unexpected-eof":          io.ErrUnexpectedEOF,
}

func errorKind(t *testing.T, err error) string {
	t.Helper()
	for _, name := range slices.Sorted(maps.Keys(codecErrors)) {
		if errors.Is(err, codecErrors[name]) {
			return name
		}
	}
	t.Fatalf("unclassified error %v", err)
	return ""
}

type oracleFrame struct {
	Command string      `json:"command"`
	Headers [][2]string `json:"headers"`
	Body    string      `json:"body"`
}

type decodeResult struct {
	Frames []oracleFrame `json:"frames"`
	Rest   string        `json:"rest"`
	Error  string        `json:"error"`
}

type encodeResult struct {
	Output string `json:"output,omitempty"`
	Error  string `json:"error,omitempty"`
}

func versionName(v stomptext.Version) string { return v.String() }

func goDecode(t *testing.T, v stomptext.Version, input []byte) decodeResult {
	t.Helper()
	dec := stomptext.NewDecoder(bytes.NewReader(input),
		stomptext.MaxFrameSize(math.MaxInt), stomptext.MaxHeaderCount(math.MaxInt), stomptext.MaxHeaderBytes(math.MaxInt))
	dec.SetVersion(v)
	res := decodeResult{Frames: []oracleFrame{}}
	off := int64(0)
	for {
		f, err := dec.ReadFrame()
		if err != nil {
			res.Error = errorKind(t, err)
			res.Rest = hex.EncodeToString(input[off:])
			return res
		}
		of := oracleFrame{Command: string(f.Command()), Headers: [][2]string{}, Body: hex.EncodeToString(f.Body())}
		for k, val := range f.All() {
			of.Headers = append(of.Headers, [2]string{hex.EncodeToString(k), hex.EncodeToString(val)})
		}
		res.Frames = append(res.Frames, of)
		off = dec.InputOffset()
	}
}

var (
	oracleCommands = []stomptext.Command{
		stomptext.Connect, stomptext.Stomp, stomptext.Connected, stomptext.Send, stomptext.Subscribe,
		stomptext.Unsubscribe, stomptext.Ack, stomptext.Nack, stomptext.Begin, stomptext.Commit,
		stomptext.Abort, stomptext.Disconnect, stomptext.Message, stomptext.Receipt, stomptext.Error,
	}
	// alphabet favours the octets framing and escaping give meaning to.
	alphabet = []byte("\n\r:\\\x00ncr0123ab-")
	tokens   = []string{"content-length", "content-length", "0", "00", "1", "3", "10", "\\n", "\\c", "\\r", "\\\\", "\\t", "\\", "\r\n", "\n\n", "MESSAGE\n", "SEND\n", "CONNECTED\n"}
)

func randomText(r *rand.Rand, max int) string {
	var b []byte
	for range r.IntN(max + 1) {
		if r.IntN(4) == 0 {
			b = append(b, tokens[r.IntN(len(tokens))]...)
		} else {
			b = append(b, alphabet[r.IntN(len(alphabet))])
		}
	}
	return string(b)
}

type encodeCase struct {
	v       stomptext.Version
	cmd     stomptext.Command
	headers stomptext.Headers
	body    []byte
}

func randomEncodeCase(r *rand.Rand) encodeCase {
	c := encodeCase{v: stomptext.Version12, cmd: oracleCommands[r.IntN(len(oracleCommands))]}
	if r.IntN(2) == 0 {
		c.v = stomptext.Version11
	}
	if r.IntN(40) == 0 {
		c.cmd = "BOGUS"
	}
	if r.IntN(3) > 0 {
		c.body = []byte(randomText(r, 6))
	}
	for range r.IntN(4) {
		h := stomptext.Header{Name: randomText(r, 4), Value: randomText(r, 4)}
		switch r.IntN(4) {
		case 0:
			h.Name = stomptext.HeaderContentLength
			h.Value = []string{"0", "00", "1", "01", "2", "3", "x", ""}[r.IntN(8)]
		case 1:
			i := r.IntN(6)
			h.Name = "abcdeh"[i : i+1]
		}
		c.headers = append(c.headers, h)
	}
	if r.IntN(3) == 0 {
		c.headers = append(c.headers, stomptext.Header{Name: stomptext.HeaderContentLength, Value: strconv.Itoa(len(c.body))})
		if r.IntN(2) == 0 {
			slices.Reverse(c.headers)
		}
	}
	return c
}

func (c encodeCase) request() map[string]any {
	hs := [][2]string{}
	for _, h := range c.headers {
		hs = append(hs, [2]string{hex.EncodeToString([]byte(h.Name)), hex.EncodeToString([]byte(h.Value))})
	}
	return map[string]any{"op": "encode", "version": versionName(c.v), "command": string(c.cmd), "headers": hs, "body": hex.EncodeToString(c.body)}
}

func (c encodeCase) goResult(t *testing.T) encodeResult {
	out, err := stomptext.AppendFrame(nil, c.v, c.cmd, c.headers, c.body)
	if err != nil {
		return encodeResult{Error: errorKind(t, err)}
	}
	return encodeResult{Output: hex.EncodeToString(out)}
}

func mutate(r *rand.Rand, b []byte) []byte {
	b = slices.Clone(b)
	for range 1 + r.IntN(3) {
		switch op := r.IntN(4); {
		case op == 0 && len(b) > 0:
			b[r.IntN(len(b))] = alphabet[r.IntN(len(alphabet))]
		case op == 1:
			b = slices.Insert(b, r.IntN(len(b)+1), alphabet[r.IntN(len(alphabet))])
		case op == 2 && len(b) > 0:
			i := r.IntN(len(b))
			b = slices.Delete(b, i, i+1)
		case op == 3:
			b = b[:r.IntN(len(b)+1)]
		}
	}
	return b
}

func heartbeats(r *rand.Rand, v stomptext.Version) []byte {
	var b []byte
	for range r.IntN(3) {
		if v == stomptext.Version12 && r.IntN(2) == 0 {
			b = append(b, '\r', '\n')
		} else {
			b = append(b, '\n')
		}
	}
	return b
}

type decodeCase struct {
	v     stomptext.Version
	input []byte
}

func randomDecodeCase(r *rand.Rand) decodeCase {
	c := decodeCase{v: stomptext.Version12}
	if r.IntN(2) == 0 {
		c.v = stomptext.Version11
	}
	if r.IntN(5) == 0 {
		c.input = []byte(randomText(r, 30))
		return c
	}
	for range 1 + r.IntN(3) {
		c.input = append(c.input, heartbeats(r, c.v)...)
		if r.IntN(3) == 0 {
			c.input = append(c.input, rawFrame(r)...)
			continue
		}
		e := randomEncodeCase(r)
		e.v = c.v
		if e.cmd == "BOGUS" || (r.IntN(8) > 0 && e.cmd != stomptext.Send && e.cmd != stomptext.Message && e.cmd != stomptext.Error) {
			e.body = nil
		}
		if out, err := stomptext.AppendFrame(nil, e.v, e.cmd, e.headers, e.body); err == nil {
			c.input = append(c.input, out...)
		}
	}
	c.input = append(c.input, heartbeats(r, c.v)...)
	if r.IntN(2) == 0 {
		c.input = mutate(r, c.input)
	}
	return c
}

// rawFrame writes a frame without the encoder's checks, so header lines carry
// arbitrary escapes, colons and content-length values.
func rawFrame(r *rand.Rand) []byte {
	b := []byte(oracleCommands[r.IntN(len(oracleCommands))])
	b = append(b, '\n')
	for range r.IntN(4) {
		b = append(b, randomText(r, 4)...)
		b = append(b, ':')
		b = append(b, randomText(r, 4)...)
		b = append(b, '\n')
	}
	if r.IntN(2) == 0 {
		cl := []string{"0", "1", "2", "3", "4", "5", "01", "", "x", "12345678901", "99999"}[r.IntN(11)]
		b = append(b, "content-length:"+cl+"\n"...)
	}
	b = append(b, '\n')
	b = append(b, randomText(r, 5)...)
	return append(b, 0)
}

func TestLeanOracleDecodeAgrees(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	cases := make([]decodeCase, 4000)
	reqs := make([]any, len(cases))
	for i := range cases {
		cases[i] = randomDecodeCase(r)
		reqs[i] = map[string]any{"op": "decode", "version": versionName(cases[i].v), "input": hex.EncodeToString(cases[i].input)}
	}
	resps := askOracle(t, reqs)
	outcomes := map[string]int{}
	for i, c := range cases {
		var want decodeResult
		if err := json.Unmarshal(resps[i], &want); err != nil || want.Frames == nil {
			t.Fatalf("oracle response %s: %v", resps[i], err)
		}
		got := goDecode(t, c.v, c.input)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("decode %v %q:\n go:   %+v\n lean: %+v", c.v, c.input, got, want)
		}
		outcomes[got.Error]++
		if len(got.Frames) > 0 {
			outcomes["frames>0"]++
		}
	}
	t.Logf("%d decode cases agree; outcomes %v", len(cases), outcomes)
}

func TestLeanOracleEncodeAgrees(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	cases := make([]encodeCase, 3000)
	reqs := make([]any, len(cases))
	for i := range cases {
		cases[i] = randomEncodeCase(r)
		reqs[i] = cases[i].request()
	}
	resps := askOracle(t, reqs)
	outcomes := map[string]int{}
	for i, c := range cases {
		var want encodeResult
		if err := json.Unmarshal(resps[i], &want); err != nil {
			t.Fatalf("oracle response %s: %v", resps[i], err)
		}
		got := c.goResult(t)
		if got != want {
			t.Fatalf("encode %v %s %q %q:\n go:   %+v\n lean: %+v", c.v, c.cmd, c.headers, c.body, got, want)
		}
		if got.Error == "" {
			outcomes["ok"]++
		} else {
			outcomes[got.Error]++
		}
	}
	t.Logf("%d encode cases agree; outcomes %v", len(cases), outcomes)
}
