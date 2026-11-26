# Reconnection

Architecture: [decisions/0006](decisions/0006-reconnect-ownership.md).

## RCN-1 Reconnect is supported

Rule: The Client reconnects after a session ends for any cause other than Close or a terminal error.
Case: the broker restarts.

## RCN-2 Subscriptions are replayed

Rule: Every subscription active when a session ends is subscribed again on the next session with the same id and headers.
Case: a durable subscription survives a broker failover.

## RCN-3 Replay does not wait

Rule: Replayed SUBSCRIBE frames request receipts, but no caller waits for them.
Case: three subscriptions replayed after reconnect.

## RCN-4 Replay rejected by the broker

Rule: An ERROR whose receipt-id names a replayed SUBSCRIBE ends that subscription with the *ServerError and drops it from replay.
Case: a queue deleted while the client was disconnected.

## RCN-5 Overflowed and unsubscribed subscriptions

Rule: Subscriptions that ended, by overflow, Unsubscribe or broker rejection, are not replayed.
Case: a subscription ended with ErrOverflow before the reconnect.

## RCN-6 Queued messages at session loss

Rule: Queued messages of every mode are kept at session loss; acknowledging them in a client mode returns ErrStaleAck, and the broker may redeliver them.
Case: 10 unacknowledged queued messages when the connection resets.

## RCN-7 Backoff

Rule: Attempt n waits a uniformly random duration in [0, min(MaxBackoff, BaseBackoff·2^n)].
Case: BaseBackoff 100 ms, attempt 3 waits up to 800 ms.

## RCN-8 Backoff defaults

Rule: BaseBackoff defaults to 100 ms and MaxBackoff to 30 seconds.
Case: Open without Backoff.

## RCN-9 Backoff reset

Rule: The backoff range returns to its first step once a session has stayed up for MaxBackoff.
Case: a session that lasts 5 minutes, then drops.

## RCN-10 Attempt limit

Rule: MaxAttempts 0, the default, retries forever; otherwise the Client ends after that many consecutive failed attempts.
Case: MaxAttempts 5 and a broker that is down.

## RCN-11 Terminal errors

Rule: Only Close, a handshake ERROR, ErrUnsupportedVersion and the attempt limit stop reconnection.
Case: a wrong passcode stops the Client immediately.
Rejected: A handshake ERROR on a reconnect is retried with backoff.

## RCN-12 State reporting

Rule: Each state transition calls OnState synchronously, under the Client lock, so the callback must not block or call the Client.
Case: Connected to Waiting after a reset.

## RCN-13 Close during backoff

Rule: Close interrupts an in-progress dial or backoff wait and returns without waiting for it to expire.
Case: Close during a 30-second backoff.

## RCN-14 Transactions

Rule: Transactions are supported; specs/transaction.md owns their behaviour.
Case: a caller needs BEGIN and COMMIT.

## RCN-15 Connected means a session is running

Rule: State leaves Connected for Waiting when the session ends, before any backoff starts, unless Close is ending the session.
Case: the broker resets the connection; State reports Waiting while Version returns 0.

## RCN-16 No Closing state

Rule: State has no Closing value; once Close begins, State keeps the value it had until Closed.
Case: OnState reports Connected then Closed for a Close against a running session; StateFrozenAfterClose checks it in the TLA+ model.

## RCN-17 Short sessions and MaxAttempts

Rule: A session that ends before lasting MaxBackoff counts as one failed attempt toward MaxAttempts; a session lasting MaxBackoff starts the count over.
Case: MaxAttempts 3 and a broker that completes every handshake, then drops the connection at once: the Client ends after the third session (TestShortSessionsCountTowardMaxAttempts).
Rejected: Any successful handshake starts the count over, so MaxAttempts counts only failed dials and handshakes and a broker that keeps accepting and dropping is retried forever.

