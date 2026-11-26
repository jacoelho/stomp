# 0010 TLA+ model of the client's concurrency

Status: accepted

## Decision
`proofs/tla/StompClient.tla` models the Client's goroutines (application calls, reader, writer, supervisor) against a broker with one queue, a credit window, and redelivery of unacknowledged messages on connection loss and on UNSUBSCRIBE. TLC checks it exhaustively within small bounds: two messages, two sessions, two incarnations of one subscription id, capacity one, a window of one or two, with and without EndOnOverflow, and with a broker that delivers before the SUBSCRIBE receipt. `make tla` runs `proofs/tla/check.sh`, which also requires five reachability probes and eleven seeded defects to fail, so a passing run shows the properties can fail.

Each action is one critical section of the code. Splitting at the points where the code releases `c.mu` is what lets TLC interleave other actions there:

| Action | Code |
|---|---|
| AppSubscribe, AppSubscribeDone, AppSubscribeTimeout | `Client.Subscribe`: `enqueue`, then `await`, then `unsubscribeLocked` |
| AppUnsubscribe | `Subscription.Unsubscribe`, `unsubscribeLocked` |
| AppNext, AppNextEnded | `Subscription.ReadMessage` under `s.mu` |
| AutoAck, AppAck | `Subscription.autoAck`, `Subscription.ack` through `enqueue` and `Session.Ack` |
| AppSend | `Client.Send` |
| Close | `Client.Close` up to the DISCONNECT receipt, which moves `life` from running to closing; the stop is ConnDrop |
| Writer | `writeLoop` |
| ClientRecv | `readLoop` and `receive` |
| ConnDrop | a transport failure or Close stopping the session's loops |
| SessionEnd | the locked cleanup at the end of `runSession`, which reports Waiting while running |
| Handshake | `establish` completing a dial and handshake |
| BeginSession | `beginSession`, which closes the connection instead when Close began |
| Finish | `finish`, after Close or when reconnecting gives up |

| Property | Spec |
|---|---|
| no message is acknowledged before ReadMessage stored it | SUB-9, SUB-28 |
| every delivery the reader took is queued or stored by ReadMessage, unless the application unsubscribed or gave up that subscription, closed the Client, or set EndOnOverflow | SUB-8, SUB-9, SUB-43 to SUB-47 |
| a subscription holding messages is confirmed, by its receipt or a delivery, so a session end keeps it | SUB-48 |
| every ACK names a delivery outstanding on its connection and subscription | SUB-21, SUB-27, SUB-31 |
| no frame follows DISCONNECT | CON-25 |
| Send returns nil only after the broker accepted the message | SND-5 |
| messages reach the subscription the broker delivered them to; no second SUBSCRIBE for a live id | SUB-4 |
| once faults stop, every message reaches the application | SUB-9, SUB-32 |
| the Client reports Connected only while a session runs, except while Close ends it | RCN-15 |
| no session begins once Close began | CON-30 |
| once Close began, the reported state changes only to Closed | RCN-16 |
| the lifecycle only moves forward (running, closing, closed), and closed has no session | CON-17 |

## Why
The Lean proofs (decisions/0009) cover the pure session core; the defects the core cannot see come from interleavings in the client. TLC found two before any fix: an ACK for an earlier subscription whose id was reused (now SUB-31, fixed by a per-session subscription sequence in `AckHandle`), and an AckAuto message never acknowledged when ReadMessage's ctx had ended (now SUB-32).

## Limits
Bounded: a defect needing three sessions or three messages in flight is not excluded. One subscription id and one destination; no transactions, AckClient's cumulative acknowledgement, heart-beats, timeouts or buffer limits. The broker sees connection loss at the same moment as the client, so half-open connections, where the broker keeps a stale connection's deliveries, are not modelled; the network simulation tests cover them. The Client's public State has no Closing value: during Close it keeps the state it had until Closed (RCN-16), so the model checks RCN-15 only while running. The broker requeues every unacknowledged delivery when a session ends, so destinations that keep no copy, such as non-durable topics, are not modelled; NoUnpermittedDiscard checks the client side of SUB-9 instead. With EarlyDeliver the broker may deliver before the SUBSCRIBE receipt (SUB-48); it never sends a MESSAGE after the UNSUBSCRIBE receipt, so SUB-2 and SUB-49 are not modelled. The model is hand-written from the code, so its fidelity rests on review and on the Go tests that reproduce each counterexample.

## Rejected
- Model-checking the Go code directly: no maintained tool explores goroutine interleavings with this code's channels and synctest.
- Apalache: symbolic checking would allow larger bounds, but TLC's explicit search with counterexample traces suffices for these bounds and needs only a jar.
