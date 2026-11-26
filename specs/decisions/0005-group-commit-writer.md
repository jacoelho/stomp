# 0005 One writer with group commit

Status: accepted

## Decision
Callers append encoded frames to the session's write buffer under the client mutex; one writer goroutine swaps buffers and writes each batch with one Write. Ack and Nack return once buffered. A frame that would grow a non-empty buffer past WriteBufferLimit waits, bounded by ctx. Bodies are copied into the buffer.

## Why
ACK frames are about 50 octets; one flush per frame makes throughput syscall-bound. Appending under the same mutex that guards session state makes two orderings structural: a receipt is registered before its frame can be written, and an ack for an old session cannot enter a new session's buffer.

## Rejected
- A channel of frames to the writer: one handoff per frame and no batching without extra logic.
- Writing from the caller under a lock: one syscall per frame and callers stall on slow sockets.
- Zero-copy large bodies with net.Buffers: deferred until a benchmark shows the copy matters.
