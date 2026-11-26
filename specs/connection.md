# Connection and session (`stomp.Client`)

A Client owns one STOMP session at a time; [reconnect.md](reconnect.md) covers replacing it. Architecture: [decisions/0002](decisions/0002-pure-session-core.md), [decisions/0004](decisions/0004-reader-never-blocks.md), [decisions/0005](decisions/0005-group-commit-writer.md).

## CON-1 Transport

Rule: The Client obtains every transport from a caller-supplied DialFunc, passing it the number of consecutive failed attempts counted as for MaxAttempts (RCN-17).
Case: TLS is used by passing a DialFunc that returns a *tls.Conn.

## CON-31 Choosing an address

Rule: TCP(addrs...) dials addrs[attempt mod len(addrs)], so it returns to the first address after a session that lasted MaxBackoff and moves on after each failed attempt.
Case: broker-a and broker-b; broker-a refuses, broker-b serves for an hour, then drops: the next attempt dials broker-a.
Rejected: Round-robin over every attempt, which moves a reconnect off a working broker | A host-order option with a random policy; callers shuffle addrs before calling TCP to spread clients | Switching back to the first address while a session on another runs

## CON-2 CONNECT headers

Rule: CONNECT carries accept-version with the offered versions (default `1.1,1.2`), host, heart-beat, and login, passcode and client-id when set.
Case: Open with Login("app", "secret") and no ClientID.

## CON-3 Default virtual host

Rule: The host header defaults to `/`.
Case: Open without VirtualHost.

## CON-4 Negotiated version

Rule: A CONNECTED frame whose version was not offered, or that lacks version, ends the attempt with ErrUnsupportedVersion.
Case: a broker answers `version:1.0`.

## CON-5 ERROR in reply to CONNECT

Rule: An ERROR frame during the handshake ends the Client with a *ServerError and no retry.
Case: a broker rejects the passcode.
Rejected: A handshake ERROR on a reconnect is a failed attempt that backs off and counts toward MaxAttempts; only one during Open is terminal.

## CON-6 Handshake timeout

Rule: A handshake not completed within HandshakeTimeout, default 10 seconds, fails that attempt.
Case: the broker accepts TCP but never sends CONNECTED.

## CON-7 Heart-beat negotiation

Rule: Each direction uses MAX of the two values, and is disabled when either value is 0 (spec 367-371).
Case: client `heart-beat:10000,10000`, server `heart-beat:0,0` gives no heart-beats.

## CON-8 Default heart-beat

Rule: The client offers heart-beat 10 seconds in each direction; Heartbeat(0, 0) offers none.
Case: Open without the Heartbeat option.

## CON-9 Missing heart-beat header

Rule: A CONNECTED without heart-beat is treated as `0,0`.
Case: a broker omits heart-beat.

## CON-10 Receive liveness

Rule: The session ends with ErrHeartbeatTimeout when no octet arrives for twice the negotiated receive interval.
Case: negotiated receive interval 5 seconds and 10 seconds of silence.

## CON-11 What counts as received activity

Rule: Any received octet resets the receive timer, including partial frames and EOL heart-beats.
Case: a 3 MiB body arriving slowly over 20 seconds keeps the session alive.

## CON-12 Sending heart-beats

Rule: An EOL is written when nothing has been written for the negotiated send interval.
Case: negotiated send interval 10 seconds and an idle client.

## CON-13 Write timeout

Rule: A write that does not complete within WriteTimeout, default 10 seconds, ends the session.
Case: the broker stops reading and the TCP window fills.

## CON-14 ERROR frame after the handshake

Rule: An ERROR frame ends the session with a *ServerError, and the Client reconnects.
Case: a SEND to a forbidden destination makes the broker send ERROR and close.

## CON-15 Read or syntax error

Rule: Any read error or syntactic error ends the session; it is never skipped.
Case: the broker sends an undefined escape sequence.

## CON-16 Pending calls when a session ends

Rule: Calls waiting for a receipt fail with ErrOutcomeUnknown wrapping the session's cause.
Case: Send is waiting for RECEIPT when the connection resets.

## CON-17 Close

Rule: Close sends DISCONNECT with a receipt, waits for it until ctx ends, closes the transport and joins every goroutine.
Case: Close with a 2-second context against a broker that never answers DISCONNECT.

## CON-18 Close is idempotent

Rule: A second Close returns nil without effect.
Case: deferred Close after an explicit Close.

## CON-19 Calls after Close

Rule: After Close, a call that passes its own checks (arguments, ctx, Tx state) returns ErrClosed; Close returns nil (CON-18) and ReadMessage first drains (SUB-14).
Case: Send after Close.

## CON-20 Err after Close

Rule: Err returns ErrClosed once Close returns, or the terminal cause when the Client had already ended on its own.
Case: Close, then Err.

## CON-21 Concurrency

Rule: Client and Subscription methods are safe for concurrent use.
Case: 64 goroutines Send and Ack on one Client.

## CON-22 Logging

Rule: The Client logs session transitions to the Logger option, default slog.Default, and never writes to stdout.
Case: a reconnect after a network reset.

## CON-23 Unsupported frames from the broker

Rule: A frame the client never expects, such as SEND or CONNECT, ends the session with a protocol error.
Case: a broker sends SEND to the client.

## CON-24 Open

Rule: Open returns after the first successful handshake, retrying with backoff until ctx ends, MaxAttempts is reached, or a terminal error occurs (RCN-11).
Case: Open while the broker restarts for 3 seconds.

## CON-25 No frame after DISCONNECT

Rule: Once DISCONNECT is buffered the session writes no further frame; with EndOnOverflow, an overflow during Close discards without UNSUBSCRIBE.
Case: a subscription with EndOnOverflow overflows while Close waits for the DISCONNECT receipt.

## CON-26 Offered versions

Rule: The Versions option restricts the offered versions; each session uses the version the broker chose.
Case: Versions(stomp.V11) against Artemis negotiates 1.1.

## CON-27 Version after reconnect

Rule: Each session negotiates afresh, so a reconnect may change the version.
Case: failover from a 1.2 broker to a 1.1 broker.

## CON-28 Close result

Rule: Close returns ErrOutcomeUnknown when ctx or the session ends before the DISCONNECT receipt, and nil after the receipt.
Case: Close with a 2-second context against a broker that never answers DISCONNECT returns ErrOutcomeUnknown after 2 seconds.

## CON-29 Close answered with ERROR

Rule: Close returns the *ServerError when the broker answers DISCONNECT with an ERROR naming its receipt.
Case: the broker replies `ERROR receipt-id:<DISCONNECT receipt>`.

## CON-30 Handshake completing during Close

Rule: A reconnect whose handshake completes after Close began starts no session and writes no frame; its connection is closed.
Case: Close is called while the client reads CONNECTED on a reconnect.
