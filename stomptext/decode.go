package stomptext

import (
	"bytes"
	"fmt"
	"io"
	"math"

	"github.com/jacoelho/stomp/internal/opts"
)

const minReadSize = 4096

// Decoder reads frames from a stream. Octets that follow a frame in the same
// read are kept for the next ReadFrame, and end-of-line octets between frames
// (heart-beats) are skipped.
//
// A Decoder is not safe for concurrent use.
type Decoder struct {
	r   io.Reader
	lim opts.Struct
	ver Version

	buf        []byte
	start, end int   // unread octets are buf[start:end]
	base       int64 // input offset of buf[0]
	rerr       error // error returned by r, reported once buffered octets are used
	err        error // sticky error returned by ReadFrame

	fields []field
	cur    pending
}

// pending is the progress on the frame starting at buf[start], so octets are
// scanned once however the input is split across reads.
type pending struct {
	headScan  int // next octet to examine for the end of the header section
	lineStart int
	headEnd   int // first body octet; 0 until the header section is complete
	cl        int // content-length, or -1 when absent
	bodyScan  int // next octet to examine for NUL when cl is -1
	cmd       Command
}

// NewDecoder returns a Decoder reading from r.
func NewDecoder(r io.Reader, options ...Options) *Decoder {
	d := new(Decoder)
	d.Reset(r, options...)
	return d
}

// Reset discards buffered state and reads STOMP 1.2 frames from r with the
// given options.
func (d *Decoder) Reset(r io.Reader, options ...Options) {
	*d = Decoder{r: r, lim: opts.Resolve(options), ver: Version12, buf: d.buf[:0], fields: d.fields[:0]}
	d.buf = d.buf[:cap(d.buf)]
}

// SetVersion sets the protocol version of the frames read after it, including
// octets already buffered. A new Decoder reads 1.2, whose rules also accept the
// CONNECTED frame of a 1.1 server. It panics on an unknown version.
func (d *Decoder) SetVersion(v Version) {
	if !v.known() {
		panic(fmt.Sprintf("stomptext: SetVersion(%v)", v))
	}
	d.ver = v
}

// InputOffset returns the input offset of the next unread octet.
func (d *Decoder) InputOffset() int64 { return d.base + int64(d.start) }

// ReadFrame reads the next frame. The returned Frame is valid until the next
// ReadFrame or Reset.
//
// It returns io.EOF when the input ends between frames and
// io.ErrUnexpectedEOF when it ends inside one. Malformed input yields a
// *SyntacticError. After any error, ReadFrame returns the same error again.
func (d *Decoder) ReadFrame() (Frame, error) {
	if d.err != nil {
		return Frame{}, d.err
	}
	f, err := d.readFrame()
	if err != nil {
		d.err = err
		return Frame{}, err
	}
	return f, nil
}

func (d *Decoder) readFrame() (Frame, error) {
	d.reclaim()
	if err := d.skipEOL(); err != nil {
		return Frame{}, err
	}
	d.cur = pending{cl: -1}
	for {
		done, err := d.advance()
		if err != nil {
			return Frame{}, err
		}
		if done > 0 {
			return d.finish(done)
		}
		if err := d.fill(); err != nil {
			return Frame{}, err
		}
	}
}

// reclaim drops octets of frames already returned. The buffer keeps its
// size, which fill bounds by MaxFrameSize.
func (d *Decoder) reclaim() {
	if d.start > 0 && d.start == d.end {
		d.base += int64(d.start)
		d.start, d.end = 0, 0
	}
}

func (d *Decoder) skipEOL() error {
	for {
		if d.start == d.end {
			if err := d.fill(); err != nil {
				return err
			}
			continue
		}
		switch d.buf[d.start] {
		case '\n':
			d.start++
		case '\r':
			if d.ver == Version11 {
				return nil
			}
			if d.end-d.start < 2 {
				if err := d.fill(); err != nil {
					return err
				}
				continue
			}
			if d.buf[d.start+1] != '\n' {
				return d.syntax(d.start+1, ErrInvalidEOL)
			}
			d.start += 2
		default:
			return nil
		}
	}
}

// fill reads more input. It returns io.EOF only when no frame octet is
// buffered, and io.ErrUnexpectedEOF when the input ends inside a frame. It
// never reads past MaxFrameSize octets of the current frame, so a frame is
// rejected with the same error however the input is split (FRM-4, FRM-16);
// the window is at least two octets so a CRLF heart-beat can be told from a
// lone CR, and advance looks at no more than MaxFrameSize of them.
func (d *Decoder) fill() error {
	if d.rerr != nil {
		return d.endErr()
	}
	if d.end == len(d.buf) || d.frameRunsPastBuffer() {
		d.makeRoom()
	}
	window := d.buf[d.end : d.start+min(len(d.buf)-d.start, max(d.lim.MaxFrameSize, 2))]
	for range 100 {
		n, err := d.r.Read(window)
		d.end += n
		if err != nil {
			d.rerr = err
		}
		if n > 0 {
			return nil
		}
		if err != nil {
			return d.endErr()
		}
	}
	return io.ErrNoProgress
}

// frameRunsPastBuffer reports whether the current frame's declared length
// reaches past the end of the buffer. Moving such a frame to the front early
// copies only the part already read, not nearly the whole frame.
func (d *Decoder) frameRunsPastBuffer() bool {
	p := &d.cur
	return d.start > 0 && p.headEnd > 0 && p.cl >= 0 && p.headEnd+p.cl+1 > len(d.buf)-d.start
}

func (d *Decoder) endErr() error {
	if d.rerr == io.EOF && d.start < d.end {
		return io.ErrUnexpectedEOF
	}
	return d.rerr
}

func (d *Decoder) makeRoom() {
	if d.start > 0 {
		n := copy(d.buf, d.buf[d.start:d.end])
		d.base += int64(d.start)
		d.start, d.end = 0, n
	}
	if d.end+minReadSize/2 <= len(d.buf) {
		return
	}
	size := min(max(2*len(d.buf), minReadSize), max(d.lim.MaxFrameSize, minReadSize))
	if size <= len(d.buf) {
		return
	}
	nb := make([]byte, size)
	copy(nb, d.buf[:d.end])
	d.buf = nb
}

// advance scans newly buffered octets. It returns the length of the complete
// frame including its NUL, or 0 when more input is needed. An unfinished frame
// that already fills MaxFrameSize fails, since it needs at least its NUL more.
func (d *Decoder) advance() (int, error) {
	b := d.buf[d.start : d.start+min(d.end-d.start, d.lim.MaxFrameSize)]
	if d.cur.headEnd == 0 {
		if done, err := d.scanHead(b); err != nil {
			return 0, err
		} else if !done {
			return 0, d.checkUnfinished(b)
		}
	}
	total, err := d.scanBody(b)
	if total == 0 && err == nil {
		err = d.checkUnfinished(b)
	}
	return total, err
}

func (d *Decoder) checkUnfinished(b []byte) error {
	if len(b) >= d.lim.MaxFrameSize {
		return d.syntax(d.start, ErrFrameTooLarge)
	}
	return nil
}

// scanHead looks for the blank line ending the header section of b, the
// frame's buffered octets, enforcing MaxHeaderBytes, and parses the section
// once complete.
func (d *Decoder) scanHead(b []byte) (bool, error) {
	p := &d.cur
	for i := p.headScan; i < len(b); i++ {
		j := bytes.IndexByte(b[i:], '\n')
		if j < 0 {
			break
		}
		i += j
		line := b[p.lineStart:i]
		if p.lineStart > 0 && (len(line) == 0 || (d.ver == Version12 && len(line) == 1 && line[0] == '\r')) {
			p.headEnd = i + 1
			break
		}
		p.lineStart = i + 1
	}
	if p.headEnd == 0 {
		p.headScan = len(b)
		if len(b) > d.lim.MaxHeaderBytes {
			return false, d.syntax(d.start+d.lim.MaxHeaderBytes, ErrHeaderTooLarge)
		}
		return false, nil
	}
	if p.headEnd > d.lim.MaxHeaderBytes {
		return false, d.syntax(d.start+d.lim.MaxHeaderBytes, ErrHeaderTooLarge)
	}
	if err := d.parseHead(b[:p.headEnd]); err != nil {
		return false, err
	}
	p.bodyScan = p.headEnd
	return true, nil
}

// scanBody finds the end of the body after a complete header section: after
// content-length octets with a length (FRM-5), at the first NUL without one
// (FRM-6). It returns the frame's length, or 0 when more input is needed.
func (d *Decoder) scanBody(b []byte) (int, error) {
	p := &d.cur
	if p.cl >= 0 {
		if p.cl >= d.lim.MaxFrameSize-p.headEnd {
			return 0, d.syntax(d.start, ErrFrameTooLarge)
		}
		total := p.headEnd + p.cl + 1
		if len(b) < total {
			return 0, nil
		}
		if b[total-1] != 0 {
			return 0, d.syntax(d.start+total-1, ErrMissingNUL)
		}
		return total, nil
	}
	if i := bytes.IndexByte(b[p.bodyScan:], 0); i >= 0 {
		total := p.bodyScan + i + 1
		if total > d.lim.MaxFrameSize {
			return 0, d.syntax(d.start, ErrFrameTooLarge)
		}
		return total, nil
	}
	p.bodyScan = len(b)
	return 0, nil
}

// parseHead validates the command and headers of head, records header spans
// relative to buf[start], unescapes in place, and finds content-length.
func (d *Decoder) parseHead(head []byte) error {
	p := &d.cur
	nl := bytes.IndexByte(head, '\n')
	cmdLine := d.trimCR(head[:nl])
	cmd, ok := lookupCommand(cmdLine)
	if !ok {
		return d.syntax(d.start, ErrUnknownCommand)
	}
	p.cmd = cmd
	d.fields = d.fields[:0]
	escaped := cmd.escaped()
	pos := nl + 1
	for pos < len(head) {
		end := pos + bytes.IndexByte(head[pos:], '\n')
		line := d.trimCR(head[pos:end])
		if len(line) == 0 {
			break
		}
		if len(d.fields) == d.lim.MaxHeaderCount {
			return d.syntax(d.start+pos, ErrTooManyHeaders)
		}
		colon := bytes.IndexByte(line, ':')
		if colon <= 0 {
			return d.syntax(d.start+pos, ErrInvalidHeader)
		}
		name := span{pos, pos + colon}
		value := span{pos + colon + 1, pos + len(line)}
		if escaped {
			var err error
			if name, err = d.unescape(head, name); err != nil {
				return err
			}
			if value, err = d.unescape(head, value); err != nil {
				return err
			}
		}
		d.fields = append(d.fields, field{name, value})
		if p.cl < 0 && string(head[name.lo:name.hi]) == HeaderContentLength {
			n, ok := parseLength(head[value.lo:value.hi])
			if !ok {
				return d.syntax(d.start+value.lo, ErrInvalidContentLength)
			}
			p.cl = n
		}
		pos = end + 1
	}
	return nil
}

func (d *Decoder) trimCR(b []byte) []byte {
	if n := len(b); d.ver == Version12 && n > 0 && b[n-1] == '\r' {
		return b[:n-1]
	}
	return b
}

// unescape decodes s in place. Decoding only shrinks, so the result fits.
func (d *Decoder) unescape(b []byte, s span) (span, error) {
	first := bytes.IndexByte(b[s.lo:s.hi], '\\')
	if first < 0 {
		return s, nil
	}
	w := s.lo + first
	for r := w; r < s.hi; r++ {
		c := b[r]
		if c == '\\' {
			if r+1 == s.hi {
				return s, d.syntax(d.start+r, ErrInvalidEscape)
			}
			r++
			switch b[r] {
			case 'r':
				if d.ver == Version11 {
					return s, d.syntax(d.start+r-1, ErrInvalidEscape)
				}
				c = '\r'
			case 'n':
				c = '\n'
			case 'c':
				c = ':'
			case '\\':
				c = '\\'
			default:
				return s, d.syntax(d.start+r-1, ErrInvalidEscape)
			}
		}
		b[w] = c
		w++
	}
	return span{s.lo, w}, nil
}

// parseLength parses a non-negative decimal integer of any length,
// saturating at math.MaxInt so that an oversized length fails the frame size
// limit instead of wrapping.
func parseLength[T ~string | ~[]byte](b T) (int, bool) {
	if len(b) == 0 {
		return 0, false
	}
	n := 0
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		if d := int(c - '0'); n > (math.MaxInt-d)/10 {
			n = math.MaxInt
		} else {
			n = n*10 + d
		}
	}
	return n, true
}

func (d *Decoder) finish(total int) (Frame, error) {
	p := d.cur
	raw := d.buf[d.start : d.start+total]
	body := span{p.headEnd, total - 1}
	if body.hi > body.lo && !p.cmd.bodyAllowed() {
		return Frame{}, d.syntax(d.start+body.lo, ErrBodyNotAllowed)
	}
	d.start += total
	return Frame{cmd: p.cmd, raw: raw, fields: d.fields, body: body}, nil
}

func (d *Decoder) syntax(i int, err error) error {
	return &SyntacticError{ByteOffset: d.base + int64(i), Err: err}
}
