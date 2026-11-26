# stomp

A STOMP 1.1 and 1.2 client for Go. It subscribes to destinations, sends messages and groups work in transactions, against brokers such as Apache Artemis, RabbitMQ and ActiveMQ Classic.

```sh
go get github.com/jacoelho/stomp
```

The module requires Go 1.27.

## Connect

`Open` returns once the first session is running. It retries with backoff until its context ends or `MaxAttempts` is reached.

```go
c, err := stomp.Open(ctx, stomp.TCP("broker-a:61613", "broker-b:61613"),
	stomp.Login("app", pass),
	stomp.ClientID("billing"),
	stomptext.MaxFrameSize(8<<20),
)
if err != nil {
	return err
}
```

The client holds one connection at a time. It calls its `DialFunc` once per connection attempt, passing the number of consecutive failed attempts before this one. `TCP` dials the address at that position in its list, wrapping round, so with the two addresses above:

- The client never connects to both. The first attempt dials `broker-a`.
- Each failed attempt moves on: after the client backs off, the next attempt dials `broker-b`, then `broker-a` again.
- A session shorter than `MaxBackoff` counts as a failed attempt, so a broker that accepts connections and drops them is left for the next one.
- After a session that lasted `MaxBackoff`, the count starts over and the next attempt dials `broker-a` again. The client does not leave a working broker to return to `broker-a` while a session runs.
- A failed attempt on either address counts toward `MaxAttempts`, and a broker ERROR during the handshake on either one stops the client.

To spread many clients across brokers, shuffle the addresses before passing them to `TCP`. Any `DialFunc` works, so TLS, other transports and other failover policies plug in there.

A broker ERROR during the handshake, such as a wrong passcode, is returned at once as a `*ServerError`. `ClientID` sets the `client-id` header, which brokers use to key durable subscriptions.

### Detect a dead connection

Heart-beats let each side notice that the other has gone silent, such as when a network path drops without closing the TCP connection. The client offers 10 seconds in each direction by default. `Heartbeat` changes the offer: how often the client can send, then how often it wants to receive.

```go
c, err := stomp.Open(ctx, stomp.TCP("broker-a:61613"),
	stomp.Login("app", pass),
	stomp.Heartbeat(5*time.Second, 5*time.Second),
)
```

The broker answers with its own offer, and each direction uses the larger of the two intervals. A direction is off when either side offers 0, so a broker that answers `0,0` disables heart-beats whatever the client offers, and `Heartbeat(0, 0)` disables them from the client's side.

When heart-beats are on, the client writes one after the send interval passes with nothing written. Any octet received resets the receive timer, so a large body that arrives slowly keeps the session alive. If nothing arrives for twice the negotiated receive interval, the session ends with `ErrHeartbeatTimeout` and the client reconnects.

## Consume messages

`Subscribe` returns after the broker's RECEIPT, or after a MESSAGE for the subscription if the connection or `ctx` ends before the RECEIPT arrives. The returned `Subscription` queues the messages it receives. `ReadMessage` stores them, one per call, in arrival order.

```go
sub, err := c.Subscribe(ctx, stomp.SubscribeRequest{
	ID:          "billing-orders",
	Destination: "/queue/orders",
	Ack:         stomp.AckClientIndividual,
	Headers:     stomp.Headers{{Name: "consumer-window-size", Value: "65536"}},
})
if err != nil {
	return err
}
var m stomp.Message
for {
	if err := sub.ReadMessage(ctx, &m); err != nil {
		return err
	}
	if err := handle(ctx, m); err != nil {
		if err := sub.Nack(ctx, m); err != nil && !errors.Is(err, stomp.ErrStaleAck) {
			return err
		}
		continue
	}
	if err := sub.Ack(ctx, m); err != nil && !errors.Is(err, stomp.ErrStaleAck) {
		return err
	}
}
```

`ID` is optional: an empty one generates `sub-N`. A stable ID keeps a durable subscription addressable across restarts.

`ReadMessage` returns an error when `ctx` ends, or when the subscription has ended and no queued message is left; reconnects do not interrupt it. A subscription ends after `Unsubscribe`, `Close`, `ErrOverflow`, or a broker ERROR for it.

`Unsubscribe` ends one subscription and waits for the broker's receipt. `ReadMessage` still stores the messages already queued. A subscription ended this way is not subscribed again after a reconnect, even when `Unsubscribe` returned `ErrOutcomeUnknown`.

`ErrOverflow` happens only with `EndOnOverflow`, described in [Let the broker pace delivery](#let-the-broker-pace-delivery).

### What a message carries

A `Message` has four fields, and the caller owns all of them:

| Field | Content |
|---|---|
| `Destination` | The `destination` header. |
| `ID` | The `message-id` header. |
| `Headers` | Every header of the MESSAGE frame, in the order received, repeated names included, with escaping already removed. `Headers.Get` returns the first. |
| `Body` | The body octets. With `content-length` they are exact; without it the body ends at the first NUL. |

`Headers` include the ones the client uses itself, such as `subscription`, `message-id` and, in STOMP 1.2, `ack`, and whatever the broker adds or copies from the SEND. Artemis adds `persistent`, `priority`, `expires`, `timestamp` and `redelivered`; RabbitMQ adds `redelivered`. Both copy the sender's headers, its `receipt` included. Acknowledge with `Ack` or `Nack`, not by reading `ack`.

`ReadMessage` copies each body into the storage of the `Message` it is passed, so a loop that reuses one `Message` receives bodies without allocating. The client writes that storage only during a `ReadMessage` call, including one passed a copy that shares its `Body`. To keep a message, for example to hand it to another goroutine, read the next one into a new `Message`:

```go
for {
	var m stomp.Message // new each time: the worker owns the body it receives
	if err := sub.ReadMessage(ctx, &m); err != nil {
		return err
	}
	work <- m
}
```

### Choose an acknowledgement mode

| Mode | What acknowledges a message |
|---|---|
| `AckClientIndividual` | `Ack` acknowledges that message only. |
| `AckClient` | `Ack` acknowledges that message and every earlier message of the subscription. |
| `AckAuto` | `ReadMessage` acknowledges each message as it stores it. `Ack` and `Nack` return `ErrAutoAck`. |

In every mode, a message is acknowledged to the broker only after `ReadMessage` stored it. `AckAuto` subscribes as client-individual on the wire, so the broker's flow control still applies.

`Ack` and `Nack` return once the frame is buffered. `AckWait` and `NackWait` also wait for the broker's receipt. In STOMP 1.2, that receipt confirms every earlier frame too.

`Nack` tells the broker the client did not consume the message. The broker decides what happens to the message next.

### Run handlers concurrently

A `Subscription` is safe for concurrent use. Concurrent `ReadMessage` calls each receive a different message, so a fixed set of workers can share one subscription:

```go
var wg sync.WaitGroup
for range 4 {
	wg.Go(func() {
		var m stomp.Message
		for {
			if err := sub.ReadMessage(ctx, &m); err != nil {
				return
			}
			ack := sub.Ack
			if err := handle(ctx, m); err != nil {
				ack = sub.Nack
			}
			if err := ack(ctx, m); err != nil && !errors.Is(err, stomp.ErrStaleAck) {
				log.Printf("acknowledge %s: %v", m.ID, err)
			}
		}
	})
}
wg.Wait()
```

With several workers, messages finish out of arrival order. Use `AckClientIndividual` there: under `AckClient`, one worker's `Ack` also acknowledges messages other workers are still handling.

### Let the broker pace delivery

Back pressure comes from the broker's window of unacknowledged messages. Every mode sends acknowledgements, so a slow handler acknowledges slowly and the broker waits. Even `AckAuto` subscribes as client-individual on the wire.

The client never discards a message it received, and never stops reading for a slow consumer, so a subscription queues every message the broker sends. **Its memory is bounded only by the broker's window.** Without a window, such as RabbitMQ's default, a handler that falls behind grows the queue until the process runs out of memory.

Brokers name and measure their window differently, so the client writes it only when you name the header:

| Broker | Window | Client setting |
|---|---|---|
| RabbitMQ | `prefetch-count`, in messages, unlimited by default | `stomp.PrefetchHeader("prefetch-count")` |
| ActiveMQ Classic | `activemq.prefetchSize`, in messages | `stomp.PrefetchHeader("activemq.prefetchSize")` |
| Artemis | `consumer-window-size`, in bytes, 10 KiB by default in client modes | `consumer-window-size` in `SubscribeRequest.Headers` |

With `PrefetchHeader`, every SUBSCRIBE carries that header with the subscription's `Capacity` (default 256), so the broker stops sending while `Capacity` messages wait for acknowledgement. A header of the same name in `SubscribeRequest.Headers` is written instead.

Artemis's default window held about 40 small messages in a test. Set `consumer-window-size` to bound the queue in bytes; `0` sends one message at a time.

```go
c, err := stomp.Open(ctx, dial, stomp.PrefetchHeader("prefetch-count"))
```

Messages queued when a session ends stay queued, and the broker opens a fresh window for the next session, so each reconnect can add up to one more window to the queue until the handler catches up.

The reader never waits for a slow subscription. A RECEIPT can sit behind a MESSAGE on the connection, so a blocked reader would deadlock a handler waiting in `Send` or `AckWait` ([decision 0004](specs/decisions/0004-reader-never-blocks.md)).

To trade messages for a hard memory bound, set `EndOnOverflow`. A MESSAGE that finds `Capacity` messages or `CapacityBytes` of bodies (default 16 MiB) queued then ends the subscription with `ErrOverflow`, after `ReadMessage` has stored every queued message. The queue's body storage then has a capacity of at most 1.25 times `CapacityBytes`, plus the old storage while it reallocates. The messages that arrive until the broker confirms the UNSUBSCRIBE are discarded unacknowledged. A queue redelivers them, but a non-durable topic does not, so they are lost. To keep consuming, subscribe again. The id stays in use until the broker confirms the UNSUBSCRIBE, and reusing it earlier returns `ErrDuplicateID`.

## Send messages

`Send` returns after the broker's receipt.

```go
err := c.Send(ctx, stomp.Outgoing{
	Destination: "/queue/invoices",
	ContentType: "application/json",
	Body:        invoice,
})
switch {
case errors.Is(err, stomp.ErrOutcomeUnknown):
	// The frame was buffered or written before the connection dropped. The
	// broker may or may not have the message: resend only if processing it
	// twice is safe.
case err != nil:
	return err
}
```

### Headers on a sent message

The client writes these SEND headers itself, then yours from `Outgoing.Headers`, in slice order:

| Header | Written |
|---|---|
| `destination` | Always, from `Outgoing.Destination`. |
| `content-type` | When `Outgoing.ContentType` is set. |
| `content-length` | Unless `Outgoing.Text` is set. Artemis and ActiveMQ map a SEND without it to a TextMessage, and a text body must not contain NUL. |
| `receipt` | Always, so `Send` can wait for the broker. |
| `transaction` | On `Tx.Send`. |

Setting any of these names in `Outgoing.Headers` fails with `ErrReservedHeader`; the client never overrides or drops a header you pass. The client sets nothing else, delivery headers included. Artemis delivered messages sent without one as `persistent:false`, and as `persistent:true` with it. On Artemis, set `persistent:true` when a message must survive a broker restart:

```go
err := c.Send(ctx, stomp.Outgoing{
	Destination: "/queue/invoices",
	Body:        invoice,
	Headers:     stomp.Headers{{Name: "persistent", Value: "true"}},
})
```

SUBSCRIBE works the same way. The client writes `id`, `destination`, `ack` and `receipt`, plus the `PrefetchHeader` window when that option is set and `SubscribeRequest.Headers` does not already name it. Then it writes `SubscribeRequest.Headers`. `id`, `destination`, `ack`, `receipt` and `content-length` are reserved there. Pass names and values unescaped; the client escapes them for the negotiated version.

When the write buffer is full (`WriteBufferLimit`, default 1 MiB), `Send` waits, bounded by its context.

## Group work in a transaction

`Begin` starts a transaction. Its `Send`, `Ack` and `Nack` return once buffered. `Commit` returns after the broker's receipt.

```go
tx, err := c.Begin(ctx)
if err != nil {
	return err
}
if err := tx.Send(ctx, stomp.Outgoing{Destination: "/queue/invoices", Body: invoice}); err != nil {
	return err
}
if err := tx.Ack(ctx, order); err != nil {
	return err
}
return tx.Commit(ctx)
```

A transaction belongs to the session that began it. When that session ends, the broker aborts the transaction. Later calls on it return `ErrTxAborted`, and a `Commit` already in flight returns `ErrOutcomeUnknown`.

## Reconnection

A `Client` reconnects on its own. When a session ends for any cause other than `Close` or a terminal error, the client backs off and dials again. Attempt n waits a random duration up to min(max, base·2ⁿ). The defaults are 100 ms and 30 seconds, and `Backoff` changes them. The attempt count resets after a session stays up for the maximum backoff.

| Question | Option | Default |
|---|---|---|
| How long one attempt may take before the client moves on | `HandshakeTimeout`, which bounds the dial and the CONNECT exchange | 10 seconds |
| How long to wait between attempts | `Backoff(base, max)` | 100 ms, 30 seconds |
| When to give up | `MaxAttempts`; `Open` is also bounded by its context | 0, retry until `Close` |
| How a silent broker is noticed | `Heartbeat`; see [Detect a dead connection](#detect-a-dead-connection) | 10 seconds each way |
| Which broker to dial | the `DialFunc`; `TCP` is described in [Connect](#connect) | |

On the new session, every active subscription is subscribed again with the same id and headers. Subscriptions that ended are not. Sends and acknowledgements are never repeated.

### What the application sees

- A `Send`, `Subscribe`, `Unsubscribe` or `AckWait` waiting for a receipt returns `ErrOutcomeUnknown`.
- Messages already queued stay queued, and `ReadMessage` still stores them.
- Acknowledging a message from an earlier session returns `ErrStaleAck` and writes nothing. The broker may redeliver that message.
- `Send`, `Subscribe` and `Begin` called while disconnected wait for the next session, bounded by their context.
- `Unsubscribe` called while disconnected returns `ErrOutcomeUnknown`: the broker may keep a durable subscription.
- A transaction open when the session ended was aborted by the broker. Later calls on it return `ErrTxAborted`.

Only these stop reconnection for good: `Close`, a broker ERROR during the handshake, `ErrUnsupportedVersion`, and `MaxAttempts` consecutive failed attempts. A session that ends before lasting `MaxBackoff` counts as a failed attempt. The default `MaxAttempts`, 0, retries until `Close`.

### Trade-offs

Reconnecting inside the client keeps subscriptions running across broker restarts and network failures. Each choice below leaves the application something to handle:

| Choice | Cost | Rejected alternative |
|---|---|---|
| Sends are not repeated on the next session. | A send whose receipt never arrived returns `ErrOutcomeUnknown`, and the application decides whether to resend. | Repeating unconfirmed sends: the client cannot know whether the broker processed them, so duplicates would be silent. |
| Acknowledgements from an earlier session are refused locally with `ErrStaleAck`. | The broker redelivers those messages, so delivery is at least once and the application can see a message twice. | Sending them on the new session: brokers answer unknown ack ids with ERROR, which ends the new session and repeats. |
| Queued messages are kept when a session ends. | The broker opens a fresh window for each session, so each reconnect can add up to one more window to the queue until the handler catches up. | Discarding them, which loses messages the application has not seen. |
| A broker ERROR during a reconnect's handshake, such as a changed passcode, stops the client. | The application must open a new `Client` once the broker accepts it again. | Retrying it with backoff. |
| A session shorter than `MaxBackoff` counts toward `MaxAttempts`. | A broker that accepts connections and then drops them ends the client. | Counting only failed dials and handshakes, which retries such a broker forever. |
| `Client` is the only connection type. | No type gives a single session without reconnection; `MaxAttempts` bounds the retries instead. | A single-session `Conn` with an optional reconnecting wrapper: two types to learn, and the wrapper would repeat acknowledgement staleness and subscription replay. |

Whether an unacknowledged message survives a connection loss depends on the destination's durability, which the broker decides. [Decision 0006](specs/decisions/0006-reconnect-ownership.md) and [`specs/reconnect.md`](specs/reconnect.md) record these rules.

### Watch the connection

`State` returns `Connecting`, `Connected`, `Waiting` or `Closed`. The client reports `Connected` only while a session runs. The `OnState` option registers a function the client calls on every change. The client calls it while holding its lock, so the function must not block or call the client:

```go
var connected atomic.Bool
c, err := stomp.Open(ctx, dial, stomp.OnState(func(st stomp.State, err error) {
	connected.Store(st == stomp.Connected)
	if err != nil {
		log.Printf("stomp %s: %v", st, err)
	}
}))
```

`Done` is closed when the client has stopped for good. `Err` then returns `ErrClosed` after `Close`, or the error that stopped it.

```go
select {
case <-c.Done():
	return c.Err()
case <-ctx.Done():
}
```

A broker that stops sending ends the session with `ErrHeartbeatTimeout`; [Detect a dead connection](#detect-a-dead-connection) explains the intervals.

## Shut down

Stop the consumers first, then close the client with a deadline:

```go
cancel() // ends every ReadMessage that uses ctx
wg.Wait()

closeCtx, done := context.WithTimeout(context.Background(), 5*time.Second)
defer done()
if err := c.Close(closeCtx); err != nil && !errors.Is(err, stomp.ErrOutcomeUnknown) {
	return err
}
```

`Close` sends DISCONNECT and waits for its receipt until its context ends. It then closes the connection and waits for every goroutine of the client.

`Close` returns:

- nil once the broker confirmed DISCONNECT, which in STOMP 1.2 also confirms every earlier frame, or when no session was running;
- `ErrOutcomeUnknown` when its context or the connection ended first;
- a `*ServerError` when the broker answered DISCONNECT with ERROR.

After `Close`, calls return `ErrClosed`. A second `Close` returns nil. Messages the application did not acknowledge stay unacknowledged at the broker.

## Design

A `Client` keeps one connection to a broker at a time. One supervisor goroutine dials, runs a session, and decides after each session ends whether to back off and dial again ([decision 0006](specs/decisions/0006-reconnect-ownership.md)).

Subscriptions are state the application wants, so every new session subscribes them again; sends and acknowledgements are events, never repeated. Each message carries the number of the session that delivered it, which is how the client refuses an acknowledgement from an earlier session with `ErrStaleAck`. [Reconnection](#reconnection) lists what this costs.

Calls buffer their encoded frames, and one writer goroutine writes each batch with a single write. That is why `Ack` and `Nack` can return once buffered ([decision 0005](specs/decisions/0005-group-commit-writer.md)).

A single reader goroutine never waits for a consumer. Each subscription has its own queue, and the client discards no message unless `EndOnOverflow` or `Unsubscribe` permits it ([decision 0004](specs/decisions/0004-reader-never-blocks.md)). Flow control is the broker's, and `PrefetchHeader` writes its window for brokers that count messages.

Each session negotiates 1.1 or 1.2, and `Versions` restricts the offer. Escaping and acknowledgement headers follow the negotiated version.

The code is in three packages. Two are public layers, syntax and client ([decision 0001](specs/decisions/0001-layering.md)), and the session rules sit between them:

| Package | Role |
|---|---|
| `stomp` | The client: transport, goroutines, reconnection, subscriptions, waiting |
| `internal/session` | The protocol rules of one session as plain data, with no I/O, clock or goroutines ([decision 0002](specs/decisions/0002-pure-session-core.md)) |
| `stomptext` | STOMP 1.1 and 1.2 syntax: streaming `Decoder`, `AppendFrame`, typed errors with byte offsets |

## Specifications

Behaviour is specified in [`specs/`](specs/), one entry per decision. Architecture decisions and their rejected alternatives are in [`specs/decisions/`](specs/decisions/). The protocol text is [`specification.md`](specification.md).

## Proofs

[`proofs/`](proofs/) holds a Lean 4 model of the codec, the session core and the client's lifecycle. Its proofs cover the properties listed in [decision 0009](specs/decisions/0009-lean-proofs.md):

- escaping and frame round trips in both versions;
- heart-beat negotiation and the backoff bound;
- session invariants: subscription ids, receipts, no frame after DISCONNECT, stale acknowledgements and transactions;
- the lifecycle: it only moves forward, reports Connected only while a session runs, and starts no session once Close began.

Build-tagged tests (`-tags lean`) run the Go codec and session core against the Lean model on generated inputs and traces.

[`proofs/tla/`](proofs/tla/) holds a TLA+ model of the client's goroutines against a broker that redelivers unacknowledged messages. TLC checks it within small bounds ([decision 0010](specs/decisions/0010-tla-client-model.md)). It checks that no message is acknowledged before `ReadMessage` stores it, that every ACK names an outstanding delivery, and that once faults stop every message reaches the application. Race-detector, deterministic-time and broker tests cover timing and larger configurations.

## Testing

```sh
go test -race ./...                                   # unit, fake broker, deterministic time
make fuzz FUZZTIME=1m                                 # codec, session-core and client fuzzers
docker compose -f docker-compose.test.yml up -d       # Apache Artemis
go test -race -tags=integration ./...                 # against Artemis (STOMP_TEST_SERVER overrides localhost:61613)
make proofs                                           # Lean proofs, then Go checked against the Lean model
make tla                                              # TLC model check of the client (needs tla2tools.jar)
```
