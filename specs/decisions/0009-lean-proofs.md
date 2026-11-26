# 0009 Lean proofs of the codec and session core

Status: accepted

## Decision
`proofs/` holds a Lean 4 model, using core Lean only, of the pure parts: header escaping, whole-input frame decoding and encoding for both versions, heart-beat negotiation, backoff bounds, and the session core (`internal/session`). It proves the theorems below. A Lean executable built from the same definitions serves as an oracle: build-tagged Go tests (`-tags lean`) feed it random inputs and session traces and require identical results from the Go code.

| Theorem | Spec |
|---|---|
| unescape v (escape v s) = s, and escaped text holds no LF, no `:`, and in 1.2 no CR | FRM-10, FRM-11, FRM-31 |
| decoding an encoded valid frame followed by any octets yields that frame and exactly those octets | FRM-2, FRM-5, FRM-6 |
| heart-beat EOLs before a frame do not change what is decoded | FRM-3, FRM-30 |
| each negotiated heart-beat direction is 0 when either side offers 0, otherwise the larger value | CON-7, CON-9 |
| backoff attempt n waits at most min(max, base·2ⁿ) | RCN-7 |
| a subscription id cannot be subscribed while active or draining | SUB-4 |
| a MESSAGE for a draining subscription is never delivered | SUB-16 |
| a MESSAGE for a subscription the session does not hold is discarded and the session goes on | SUB-2 |
| a receipt id is issued once and resolves at most once | SND-5, SUB-15 |
| no frame can be built after DISCONNECT | CON-25 |
| an acknowledgement or transaction from another session, or a finished transaction, builds no frame | SUB-21, TX-5, TX-7, TX-10 |
| an acknowledgement of a delivery to an earlier subscription whose id was reused builds no frame, and a delivery carries its subscription's sequence number | SUB-31 |
| a delivery in client modes carries the version's acknowledgement id | SUB-23, SUB-27 |
| the client's lifecycle only moves forward, reports Connected only while a session runs, starts no session and reports no new state once Close began, and stays closed | CON-17, CON-30, RCN-15, RCN-16 |

## Why
These are the properties the reviews found violated or easiest to break, and they are decided by deterministic functions (decisions/0002). Proving them on a model is only as good as the model's fidelity, so the differential oracle ties the model to the Go code on generated samples; the existing fuzz test ties the whole-input decoder to the streaming one (FRM-4).

## Limits
Not proven: the streaming buffer machine in `stomptext.Decoder`, and the client's goroutines, locking, timing and reconnection (`client.go`) beyond its lifecycle. The lifecycle model has no Go function for the oracle to compare, so it is tied to the code by review and by the TLA+ model of the same transitions. The client's interleavings are model-checked in TLA+ within small bounds (decisions/0010); timing relies on the race detector, `testing/synctest` tests, the network simulation and the Artemis integration tests. SUB-9 (no drops) is a property of the client, not of the session core, so it is checked by the client property test (`TestClientProperties`, `FuzzClient`) rather than proven.

## Rejected
- Proving the Go code directly: no maintained Go verifier covers this code's generics and concurrency.
- Mathlib: its build cost buys nothing these list and natural-number proofs need.
- Proofs without an oracle: a model nobody checks against the code proves properties of different software.
