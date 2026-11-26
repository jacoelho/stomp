# 0001 Two public layers: syntax and client

Status: accepted

## Decision
`stomptext` implements STOMP 1.2 syntax only: a stream Decoder, AppendFrame, typed errors with byte offsets, and limit Options. `stomp` implements the client. One Options type is shared by both (the `jsontext`/`json/v2` pattern), so frame limits passed to `stomp.Open` reach the decoder.

## Why
Syntax has independent callers (proxies, test brokers, tooling), and the client needs a decoder whose correctness can be tested and fuzzed without a network. Sharing Options removes a second configuration path.

## Rejected
- One package: framing bugs and session bugs become untestable in isolation; the previous implementation lost frames for this reason.
- Public token level (command, header, body chunk) as in `jsontext.ReadToken`: STOMP bodies are length-delimited and brokers deliver whole messages, so tokens add API without enabling streaming of a single message.
- `stomptext.Encoder` over an io.Writer: the client batches writes itself (0005); AppendFrame serves every known caller.
