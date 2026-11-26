# Frame syntax (`stomptext`)

The grammar is STOMP 1.2 (`specification.md`) or STOMP 1.1 (stomp.github.io/stomp-specification-1.1.html), chosen per Decoder and per AppendFrame call. Entries below record what the grammar leaves to the implementation. Architecture: [decisions/0001](decisions/0001-layering.md), [decisions/0003](decisions/0003-lent-frames.md).

## FRM-1 Protocol version

Rule: STOMP 1.1 and 1.2 framing and escaping are both implemented.
Case: a broker that negotiates `version:1.1`.

## FRM-2 Frames that arrive in one read

Rule: Every frame in a read is decoded in order; bytes after a frame's NUL stay buffered for the next ReadFrame.
Case: one TCP read delivers three MESSAGE frames back to back.

## FRM-3 End-of-line octets between frames

Rule: LF, and in 1.2 also CRLF, before a command is skipped as a heart-beat and never forms part of a frame.
Case: `\n\nMESSAGE\n...` arrives after two server heart-beats.

## FRM-4 Read chunking

Rule: Decoded frames are identical however the input is split across reads.
Case: the same three frames delivered whole, or one byte per Read.

## FRM-5 Body with content-length

Rule: With content-length, exactly that many body octets are read, NUL octets included, and the next octet must be NUL.
Case: `content-length:3` and body `a\0b` followed by NUL.

## FRM-6 Body without content-length

Rule: Without content-length, the body ends at the first NUL octet.
Case: a MESSAGE with body `hello` and no content-length.

## FRM-7 Invalid content-length value

Rule: A content-length that is not a non-negative decimal integer is a syntactic error.
Case: `content-length:-1`, `content-length:0x10`, `content-length:` .

## FRM-8 Octet after a content-length body

Rule: A non-NUL octet where the terminating NUL should be is a syntactic error.
Case: `content-length:2` followed by body `abc` and NUL.

## FRM-9 Repeated headers

Rule: All repeated entries are kept in order; lookup by name returns the first.
Case: a MESSAGE carrying `foo:1` then `foo:2`; Header("foo") returns `1`.

## FRM-10 Undefined escape sequence

Rule: An undefined escape, including a trailing lone backslash, is a syntactic error.
Case: header value `a\tb` or `a\` in a MESSAGE frame.

## FRM-11 CONNECT and CONNECTED header escaping

Rule: CONNECT and CONNECTED headers are neither escaped nor unescaped; STOMP frame headers are escaped.
Case: CONNECTED header `server:Artemis\c2` is read as the literal `Artemis\c2`.

## FRM-12 Values that CONNECT cannot carry

Rule: Encoding a CONNECT header fails when its name contains colon or LF, or its value LF; in 1.2, CR in either also fails.
Case: a passcode containing a line feed; a 1.1 passcode containing CR is written as is (TestConnectInSTOMP11CarriesCRButNotLFOrColon).

## FRM-13 Header line without a colon

Rule: A header line with no colon is a syntactic error.
Case: header line `destination` with no separator.

## FRM-14 Empty header name

Rule: A header line starting with a colon is a syntactic error.
Case: header line `:value`.

## FRM-15 Unknown command

Rule: A command outside the STOMP 1.2 set is a syntactic error.
Case: frame command `HELLO`.

## FRM-16 Frame size limit

Rule: A frame larger than MaxFrameSize octets, from command to NUL, is a syntactic error; the decoder reads at most max(MaxFrameSize, 2) octets of an unfinished frame.
Case: MaxFrameSize 1 MiB and a frame with a 2 MiB body.

## FRM-17 Header count limit

Rule: More than MaxHeaderCount header entries is a syntactic error.
Case: MaxHeaderCount 128 and a frame with 129 headers.

## FRM-18 Header block size limit

Rule: A header block longer than MaxHeaderBytes octets is a syntactic error.
Case: MaxHeaderBytes 64 KiB and a 70 KiB header value.

## FRM-19 Default limits

Rule: MaxFrameSize defaults to 4 MiB, MaxHeaderCount to 128, MaxHeaderBytes to 64 KiB.
Case: NewDecoder called with no options.

## FRM-20 Invalid limit option

Rule: A limit option given a value below 1 panics when the option is created.
Case: `stomptext.MaxFrameSize(0)`.

## FRM-21 Error position

Rule: Syntactic errors are *SyntacticError carrying an input offset: the frame's first octet for ErrFrameTooLarge, otherwise the octet or line where the error was detected.
Case: an undefined escape at input octet 1042 reports ByteOffset 1042.

## FRM-22 Decoder after an error

Rule: After a syntactic or read error, every later ReadFrame returns the same error.
Case: ReadFrame fails with an invalid escape, then is called again.

## FRM-23 End of input

Rule: EOF between frames returns io.EOF; EOF inside a frame returns io.ErrUnexpectedEOF.
Case: the stream ends after `MESSAGE\ndest`.

## FRM-24 Encoder adds no headers

Rule: AppendFrame writes exactly the given headers; it never adds content-length.
Case: appending a SEND with body `hi` and no headers.

## FRM-25 Encoding a body with NUL and no content-length

Rule: AppendFrame fails when the body contains NUL and no content-length header is given.
Case: body `a\0b` without content-length.

## FRM-26 Encoding a wrong content-length

Rule: AppendFrame fails when a content-length header is given and is not the body length in decimal without leading zeros.
Case: `content-length:5` with a 3-octet body.

## FRM-27 Body on a frame that forbids one

Rule: A body on a frame other than SEND, MESSAGE or ERROR is a syntactic error when decoding and ErrBodyNotAllowed when encoding.
Case: an ACK frame with body `x`.

## FRM-28 Lent frame lifetime

Rule: A Frame from ReadFrame is valid until the next ReadFrame or Reset; Clone returns an independent copy.
Case: a caller keeps frame 1 and reads frame 2.

## FRM-29 Header whitespace

Rule: Header names and values are never trimmed.
Case: header ` destination : /q ` keeps both spaces in name and value.

## FRM-30 STOMP 1.1 line endings

Rule: In 1.1 only LF ends a line; a CR is an ordinary octet of the command or header.
Case: 1.1 header line `a:b\r\n` has value `b\r`.

## FRM-31 STOMP 1.1 escapes

Rule: In 1.1 `\r` is an undefined escape, and the encoder writes CR in header values unescaped.
Case: a 1.1 MESSAGE header value `a\rb`.

## FRM-32 Changing the decoder version

Rule: Decoder.SetVersion applies from the next frame read, including octets already buffered.
Case: CONNECTED and a 1.1 MESSAGE with a literal CR arrive in one read.

## FRM-33 Content-length beyond the frame limit

Rule: A content-length whose body cannot fit in MaxFrameSize is ErrFrameTooLarge, however many digits it has.
Case: `content-length:99999999999999999999` with the default limits.

## FRM-34 Both limits exceeded by an unterminated header section

Rule: An unterminated header section exceeding both limits reports ErrHeaderTooLarge when MaxHeaderBytes is below MaxFrameSize, else ErrFrameTooLarge.
Case: MaxFrameSize 64, MaxHeaderBytes 48 and a 71-octet header section without its blank line; with the two limits swapped, ErrFrameTooLarge, read whole or one octet at a time (TestBothLimitsReportTheSameErrorHoweverTheInputIsSplit).

## FRM-35 Decoder buffer retention

Assumed: A Decoder keeps its largest read buffer, at most max(MaxFrameSize, 4096) octets, for its lifetime, including across Reset.
Case: one 1 MiB frame, then 64-octet frames: the buffer stays about 1 MiB and later large frames allocate nothing in the decoder.
