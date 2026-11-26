# 0006 Reconnection is owned by the Client supervisor

Status: accepted

## Decision
`stomp.Client` is the only public session type. One supervisor goroutine dials, handshakes, runs a session, and on its end decides whether to back off and reconnect. Subscriptions are desired state and are replayed on every new session; sends and acks are events and are never replayed. Messages carry the session number, and acks across sessions are refused locally.

## Why
In the earlier implementation both the connection and the client tried to reconnect, neither replayed subscriptions, and the client path could never succeed. One owner removes the competing writers.

## Rejected
- A single-session public Conn plus an optional reconnecting wrapper: two types to learn and the wrapper must re-implement ack staleness and replay; no caller needs the bare session.
- Replaying unconfirmed sends: the client cannot know whether the broker processed them; duplicates would be silent.
- Sending acks for old-session messages: brokers reject unknown ack ids with ERROR, which would end the new session and loop.
