# 0003 Frames are lent; messages are owned

Status: accepted; the body copy was revised by SUB-53

## Decision
`stomptext.Decoder.ReadFrame` returns a Frame that borrows the decoder's buffer until the next read, as `jsontext.Decoder.ReadValue` does; `Frame.Clone` copies. The client copies each MESSAGE's header text into one allocation and its body into its subscription's queue storage. `Subscription.ReadMessage` copies the body again, into the storage of a `stomp.Message` the caller owns and passes in (SUB-53). The caller owns the result.

## Why
Lending keeps the decoder allocation-free; ownership at the client boundary removes lifetime rules from ordinary users, who hand messages to other goroutines. A caller that reuses its own Message receives bodies without allocating, and the client writes that storage only during the caller's own ReadMessage call.

## Rejected
- Owned frames from the decoder: every syntax-layer caller pays for copies it may not need.
- Lent messages: the earlier implementation's pooled-buffer aliasing corrupted bodies; consumers cannot be expected to track lifetimes across goroutines.
- `sync.Pool` for frames or messages: ownership passes to the caller, so nothing returns to a pool.
- Copying each body once by swapping buffers with the caller: the reader writes a buffer the caller gave up at an unpredictable later time, so a caller still holding it sees corruption from another goroutine.
