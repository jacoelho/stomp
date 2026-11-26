# Lean proofs and differential oracle

This directory holds the Lean 4 model and proofs described in [decision 0009](../specs/decisions/0009-lean-proofs.md). It models header escaping, whole-input frame decoding and encoding for STOMP 1.1 and 1.2, heart-beat negotiation, the backoff ceiling, the session core and the client's lifecycle. The `stomp-oracle` executable is built from the same definitions, and build-tagged Go tests compare it with the Go code on generated inputs.

The model uses core Lean only: no Mathlib and no Batteries. The session proofs import `Std.Data.String.ToNat`, which ships with the Lean toolchain, for `Nat.repr_inj`.

## Build and run

```sh
cd proofs && lake build
cd .. && go test -count=1 -tags lean ./stomptext ./internal/session
```

`lake build` checks every proof, and `Axioms.lean` checks that each headline theorem depends on no axiom beyond `propext`, `Classical.choice` and `Quot.sound`. The build fails if an axiom list changes, including the appearance of `sorryAx`.

The Go tests find the oracle at `proofs/.lake/build/bin/stomp-oracle`, or at the path in `STOMP_LEAN_ORACLE`. They fail when the binary is missing. Use `-v` to print the case counts per outcome.

| Test | Cases |
|---|---|
| `TestLeanOracleDecodeAgrees` | 4000 inputs: encoded frames with heart-beats, hand-built frames, mutations and random octets, in both versions |
| `TestLeanOracleEncodeAgrees` | 3000 `AppendFrame` calls, valid and invalid |
| `TestLeanOracleSessionTracesAgree` | 600 traces, about 9500 steps: two fixed traces (a second `Disconnect`; a finished handle after `Begin` reuses its id) and random traces of 1 to 30 calls |
| `TestLeanOracleNegotiateAgrees` | 2000 `Negotiate` calls |

The Go decoder runs with every frame limit at `math.MaxInt`, because the model leaves the limits out.

## Layout

| File | Contents |
|---|---|
| `Stomp/Frame.lean` | Versions, commands, headers, frames, error kinds |
| `Stomp/Escape.lean` | `escape`, `unescape` |
| `Stomp/Decode.lean` | `decodeFrame`, one `ReadFrame` on a whole input; `decodeAll` |
| `Stomp/Encode.lean` | `encode`, the model of `AppendFrame` |
| `Stomp/Negotiate.lean` | `negotiate`, the model of `session.Negotiate` |
| `Stomp/Backoff.lean` | `backoffCeil`, the ceiling of `backoffDelay` |
| `Stomp/Session.lean` | The session state machine, `State.apply` and `State.run` |
| `Stomp/Lifecycle.lean` | The client's lifecycle under `c.mu`: `life`, the reported `State`, the session and a pending handshake |
| `Stomp/Proofs/*.lean` | The theorems |
| `Oracle/Main.lean` | The oracle executable |
| `Axioms.lean` | The `#print axioms` checks |

## Theorems

| Theorem | Spec | Guarantee |
|---|---|---|
| `unescape_escape` | FRM-10, FRM-11, FRM-31 | `unescape v (escape v s) = ok s` |
| `escape_no_special` | FRM-11, FRM-31 | Escaped text holds no LF and no `:`, and in 1.2 no CR |
| `unescape_v11_cr_escape` | FRM-31 | `\r` is an undefined escape in 1.1 |
| `encode_ok_iff` | FRM-2, FRM-5, FRM-6 | `encode` succeeds exactly on `ValidFrame` |
| `decode_encode` | FRM-2, FRM-5, FRM-6 | `encode v f = ok b → decodeFrame v (b ++ rest) = ok (f, rest)` |
| `decode_encode_valid` | FRM-2, FRM-5, FRM-6 | The same, starting from `ValidFrame v f` |
| `decodeFrame_heartbeats` | FRM-3, FRM-30 | LF, and in 1.2 CRLF, before a frame changes nothing decoded |
| `skipEOL_v11_cr` | FRM-30 | In 1.1 a CR before a frame is not skipped |
| `negotiate_absent` | CON-9 | A missing `heart-beat` gives `0,0` |
| `negotiate_some_iff` | CON-7 | The header forms `negotiate` accepts, and its result |
| `negotiate_directions`, `pick_eq_zero`, `pick_eq_max` | CON-7 | Each direction is 0 if either side offers 0, otherwise the larger offer |
| `backoffCeil_eq_min` | RCN-7 | The ceiling is `min(max, base·2ⁿ)` at every attempt |
| `subscribe_in_use` | SUB-4 | An id in the table, active or draining, cannot be subscribed |
| `unsubscribe_drains`, `apply_draining` | SUB-4, SUB-16 | A draining id stays draining until the RECEIPT of its UNSUBSCRIBE |
| `receive_draining_discards`, `receive_deliver` | SUB-16 | A MESSAGE for a draining subscription is discarded, never delivered |
| `receive_unknown_discards` | SUB-2 | A MESSAGE for a subscription the session does not hold is discarded without ending the session |
| `receipts_issued_once_resolved_at_most_once` | SND-5, SUB-15 | Over any trace, receipt ids are issued once and resolved at most once, after issue |
| `run_disconnecting`, `disconnect_disconnecting` | CON-25 | Once disconnecting, no call builds a frame, a second `Disconnect` included |
| `ack_stale`, `stale_ack_builds_no_frame` | SUB-21, TX-10 | An acknowledgement from another session builds no frame |
| `ackTx_stale` | TX-10 | In an open transaction it fails with `ErrStaleAck` |
| `ack_earlier_subscription_stale`, `reused_subscription_ack_builds_no_frame` | SUB-31 | Once a subscription id is subscribed again, acknowledging a delivery to the earlier subscription builds no frame, on any trace |
| `tx_other_session_builds_no_frame` | TX-5 | A transaction of another session builds no frame |
| `tx_finished_builds_no_frame`, `finish_closes`, `finished_tx_builds_no_frame` | TX-7 | After COMMIT or ABORT, no later call on that handle builds a frame, on any trace, even after a `Begin` reuses its id |
| `step_life_forward` | CON-17 | The lifecycle only moves from running to closing to closed |
| `connected_means_session` | RCN-15 | While the client runs, it reports Connected only when a session runs, in any reachable state |
| `no_session_after_close` | CON-30 | Once Close began, no event starts a session |
| `state_frozen_after_close` | RCN-16 | Once Close began, the reported state changes only to Closed |
| `closed_is_final` | CON-17 | No event changes a closed client, which holds no session and reports Closed |
| `receive_deliver`, `receive_missing_ack_id`, `ack_of_delivery` | SUB-23, SUB-27, SUB-31 | A client-mode delivery carries `ack` (1.2) or `message-id` (1.1) and its subscription's sequence number; without the id the session ends; ACK carries it under the version's header names |

## Findings

The oracle tests found no disagreement between the model and the Go code. Every row of decision 0009 is proven as stated.

## Oracle protocol

The oracle reads one JSON request per line on stdin and writes one JSON line per request on stdout. Byte strings are lower-case hex. A malformed request gets `{"badRequest": "<reason>"}`.

Codec error kinds are the Go sentinel names: `ErrUnknownCommand`, `ErrInvalidEscape`, `ErrInvalidHeader`, `ErrInvalidContentLength`, `ErrMissingNUL`, `ErrBodyNotAllowed`, `ErrInvalidEOL` and `ErrNULInBody`. The two end-of-input kinds are `eof`, for `io.EOF` between frames, and `unexpected-eof`, for `io.ErrUnexpectedEOF`.

### decode

```json
{"op":"decode","version":"1.2","input":"<hex>"}
```

The oracle reads frames until the first error, as repeated `ReadFrame` calls do. It returns the frames read, the input after the last of them, and the error that stopped decoding: `eof` when the input ended cleanly. The response carries no byte offsets.

```json
{"frames":[{"command":"MESSAGE","headers":[["<name hex>","<value hex>"]],"body":"<hex>"}],
 "rest":"<hex>","error":"eof"}
```

### encode

```json
{"op":"encode","version":"1.1","command":"SEND","headers":[["<hex>","<hex>"]],"body":"<hex>"}
```

The response is `{"output":"<hex>"}` or `{"error":"<kind>"}`. An unknown command gives `ErrUnknownCommand`.

### negotiate

```json
{"op":"negotiate","send":10000000000,"expect":0,"header":"<hex>"}
```

`send` and `expect` are nanoseconds. A `null` header means the header is absent. The response is `{"send":<ns>,"recv":<ns>}` or `{"error":"ErrProtocol"}`.

### session

```json
{"op":"session","version":"1.2","gen":3,"trace":[<step>, ...]}
```

The oracle applies the steps in order to `session.New(gen, Accepted{Version: version})`. A failed step leaves the session unchanged, as in Go. Header lists are `[[name, value], ...]` as plain strings; message bodies are hex.

| Step | Go call |
|---|---|
| `{"do":"subscribe","id":s,"dest":s,"mode":"client"\|"client-individual","extra":[[n,v]]}` | `Subscribe` |
| `{"do":"unsubscribe","id":s}` | `Unsubscribe` |
| `{"do":"ack","gen":n,"sub":s,"seq":n,"ackId":s,"nack":b,"wait":b}` | `Ack(AckHandle{gen, sub, seq, ackId}, nack, wait)` |
| `{"do":"ackTx","tx":T,"gen":n,"sub":s,"seq":n,"ackId":s,"nack":b}` | `AckTx` |
| `{"do":"send","msg":M}` | `Send` |
| `{"do":"sendTx","tx":T,"msg":M}` | `SendTx` |
| `{"do":"begin","id":s}` | `Begin` |
| `{"do":"finish","tx":T,"abort":b}` | `Finish` |
| `{"do":"disconnect"}` | `Disconnect` |
| `{"do":"end","id":s}` | `End` |
| `{"do":"receive","command":"MESSAGE","headers":[[n,v]]}` | `Receive` of a frame with that command and those headers |

`T` is a transaction handle, `{"gen":n,"id":s,"seq":n}`, where `seq` is the `Seq` of the `Begin` that opened it. `M` is `{"destination":s,"contentType":s,"headers":[[n,v]],"body":"<hex>","text":b}`.

The response is `{"steps":[<result>, ...]}`, with one result per step. A failed step gives `{"error":"<name>"}`, using the `session` sentinel names: `ErrDisconnecting`, `ErrDuplicateID`, `ErrNotSubscribed`, `ErrStaleAck`, `ErrTxAborted`, `ErrTxDone`, `ErrInvalidRequest`, `ErrReservedHeader`, `ErrTextHasNUL` and `ErrProtocol`. A successful step gives:

```json
{"frame":{"command":"SEND","headers":[["destination","/q"]],"body":"<hex>"} | null,
 "receipt":"3-1",
 "tx":{"gen":3,"id":"t1","seq":1} | null,
 "event":<event> | null}
```

`receipt` is `""` when the call requested none, and `tx` is set only by `begin`. The event is one of:

```json
{"kind":"deliver","sub":"s1","seq":1,"ackId":"m1"}
{"kind":"discard","sub":"s1"}
{"kind":"receipt","receipt":"3-1","pending":{"kind":"subscribe","sub":"s1","tx":""}}
{"kind":"error","receipt":"3-1","known":true,"pending":{...} | null}
```

Pending kinds are `send`, `subscribe`, `unsubscribe`, `ack`, `disconnect`, `begin`, `commit` and `abort`.

## Modelling choices

- **Whole-input decoding.** `decodeFrame` takes the whole remaining input. It does not model the streaming buffer of `stomptext.Decoder`; the existing fuzz test checks that chunking does not change what is decoded (FRM-4).
- **No frame limits.** `MaxFrameSize`, `MaxHeaderCount` and `MaxHeaderBytes` are left out. The oracle tests run Go with every limit at `math.MaxInt`.
- **Session strings.** Session ids, headers and receipt ids are Lean `String` values, where Go has byte strings. The traces use ASCII only, where the two agree.
- **Received frames.** The session model takes a received frame as a command and a header list. The Go test builds each one with `AppendFrame` and the `Decoder`.
- **Durations.** `negotiate` uses `Int` nanoseconds, as `time.Duration` does. `backoffCeil` uses naturals; for `base ≥ 1` and `max < 2⁶³`, the range `stomp.Backoff` accepts, Go's shifts compute the same ceiling (see `Stomp/Backoff.lean`).
- **Client lifecycle.** `Stomp/Lifecycle.lean` is written from `client.go`, one event per critical section under `c.mu`. No Go function computes these transitions on its own, so the oracle does not compare it: its fidelity rests on review, on the Go tests of RCN-15, CON-28 and CON-30, and on the TLA+ model, which checks the same properties against interleavings within bounds. The Lean proofs hold for traces of any length.
- **Counters.** The session number `gen` and the counters `next` and `nextTx` are `Nat`, where Go has `uint64`. The receipt-id and transaction theorems assume `next` and `nextTx` do not wrap, which takes 2⁶⁴ calls in one session.

## Not proven

These limits come from decision 0009:

- the streaming buffer machine in `stomptext.Decoder`;
- the client's goroutines, locking, timing and reconnection in `client.go`, beyond the lifecycle model, which the race detector, the `testing/synctest` tests and the Artemis integration tests cover;
- SUB-9 (no message drops), a property of the client's queues that the TLA+ model checks (decision 0010).

The model also leaves out:

- the frame limits and their errors, `ErrFrameTooLarge`, `ErrTooManyHeaders` and `ErrHeaderTooLarge`;
- `SyntacticError.ByteOffset`;
- the jitter in `backoffDelay`: the `u·ceil` float multiply and its conversion back to `time.Duration`. Only the ceiling is proven. No committed Go test checks `backoffDelay` against the model, because `backoffDelay` is unexported in package `stomp`, outside the two oracle test packages.
- `session.Connect` and `session.Accept`. Only `Negotiate`, which `Accept` calls, is modelled.
