# 0002 Session rules in a deterministic core

Status: accepted

## Decision
`internal/session` holds the protocol state of one session as plain data: subscriptions, pending receipts, ack handles, heart-beat intervals. Its methods take inputs (frames, the current time) and return outputs (frames to write, deliveries, resolved receipts, errors). It performs no I/O, reads no clock and starts no goroutine. `stomp` is the shell: transport, goroutines, waiting.

## Why
Every defect class found in the earlier implementation (routing by destination, heart-beat negotiation, receipts reported as success on close) was a protocol decision mixed with I/O. A deterministic core is testable with tables and is the model a Lean proof will mirror.

## Rejected
- Event/effect sum types for every interaction: same determinism, more types, no extra checkable property.
- Exporting the core: no caller outside this module needs it yet; `internal/` keeps it free to change.
