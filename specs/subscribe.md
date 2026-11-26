# Subscribing, consuming and acknowledging

Architecture: [decisions/0004](decisions/0004-reader-never-blocks.md).

## SUB-1 Routing

Rule: A MESSAGE goes only to the subscription named by its subscription header; its destination is never used for routing.
Case: two subscriptions to `/queue/a` and a MESSAGE with `subscription:sub-1`.

## SUB-2 MESSAGE for a subscription the session does not hold

Rule: A MESSAGE naming a subscription the session does not hold is discarded unacknowledged and logged at debug level; the session continues.
Case: Artemis 2.44 sent RECEIPT for an UNSUBSCRIBE and then a MESSAGE for that subscription, in about one of three cycles under a logging proxy.

## SUB-3 Subscription id

Rule: SubscribeRequest.ID is used as the STOMP id; an empty ID becomes `sub-N` with N unique per Client.
Case: two Subscribe calls without ID get `sub-1` and `sub-2`.

## SUB-33 Generated id already in use

Rule: A generated `sub-N` that is already in use is skipped for the next N.
Case: Subscribe with ID `sub-1`, then Subscribe without ID gets `sub-2`.

## SUB-4 Duplicate subscription id

Rule: Subscribe fails with ErrDuplicateID while the ID is active or awaiting its UNSUBSCRIBE receipt.
Case: two Subscribe calls with ID `orders`.

## SUB-5 Subscribe receipt

Rule: Subscribe requests a receipt and returns after the matching RECEIPT arrives, or after a MESSAGE for it when the session, ctx or Client then ends (SUB-48, SUB-52).
Case: Subscribe on a healthy broker.

## SUB-6 Reserved subscribe headers

Rule: Subscribe fails with ErrReservedHeader if Headers sets id, destination, ack, receipt or content-length.
Case: Headers containing `ack:auto`.

## SUB-7 Capacity defaults

Rule: Capacity defaults to 256 messages and CapacityBytes to 16 MiB of bodies.
Case: SubscribeRequest without Capacity.

## SUB-8 Full subscription queue

Rule: The client queues the message beyond Capacity, so memory is bounded only by the broker's window, and without one, such as RabbitMQ's unlimited default prefetch, grows until the process fails and loses everything it holds. EndOnOverflow replaces this with ending the subscription (SUB-45).
Case: Artemis non-durable topic, Capacity 2, 10 messages published, no ReadMessage calls: ending the subscription kept 2 and lost 8, even after a new connection.
Rejected: The client stops reading the connection until ReadMessage frees space, which stalls every subscription and every receipt wait on that Client, and deadlocks a handler that calls Send or AckWait on the same Client until its context ends

## SUB-9 No discards

Rule: The client never discards a received MESSAGE unless the application set an option that permits it.
Case: a slow consumer on a non-durable topic, where the broker keeps no copy of a delivered message.

## SUB-10 Broker flow control

Rule: Without PrefetchHeader the client writes no flow-control header; callers set the broker's own header in SubscribeRequest.Headers.
Case: Artemis `consumer-window-size:65536`.

## SUB-11 Consumption order

Rule: ReadMessage stores messages in arrival order.
Case: MESSAGE m1 then m2 arrive; the first ReadMessage stores m1.

## SUB-12 Several consumers

Rule: Concurrent ReadMessage calls each receive distinct messages.
Case: 4 goroutines calling ReadMessage on one Subscription.

## SUB-13 Reconnect during ReadMessage

Rule: ReadMessage returns an error only when the subscription or its ctx ends; a reconnect returns nothing.
Case: a reconnect happens while ReadMessage waits, and it keeps waiting for the next message.

## SUB-14 Messages left after the subscription ends

Rule: ReadMessage stores queued messages before reporting why the subscription ended, Unsubscribe and overflow included.
Case: with EndOnOverflow, an overflow with 256 messages queued.

## SUB-15 Unsubscribe

Rule: Unsubscribe removes the subscription from the replay set, sends UNSUBSCRIBE with a receipt and waits for it.
Case: Unsubscribe on a healthy broker.

## SUB-16 MESSAGE after UNSUBSCRIBE

Rule: MESSAGE frames for a subscription the application unsubscribed are left unacknowledged and not queued; only a destination that keeps unacknowledged messages redelivers them.
Case: two MESSAGE frames in flight when Unsubscribe writes UNSUBSCRIBE.

## SUB-17 Unsubscribe outcome unknown

Rule: When the session ends before the UNSUBSCRIBE receipt, Unsubscribe returns ErrOutcomeUnknown and the subscription stays removed.
Case: connection reset after writing UNSUBSCRIBE on a durable subscription.

## SUB-18 Ack modes

Rule: Ack and Nack fail with ErrAutoAck on an auto-mode subscription, whatever the session state.
Case: Ack on an AckAuto subscription.

## SUB-19 Ack without waiting

Rule: Ack and Nack return once the frame is in the write buffer.
Case: Ack on a healthy session returns without a network round trip.

## SUB-20 Confirmed acknowledgement

Rule: AckWait and NackWait request a receipt and return after the matching RECEIPT; only in 1.2 does that also confirm earlier frames.
Case: Ack 99 messages, then AckWait the hundredth.

## SUB-21 Stale acknowledgement

Rule: Ack, Nack and their Wait forms fail with ErrStaleAck, writing nothing, when the message came from an earlier session.
Case: a message received before a reconnect is acknowledged after it.

## SUB-22 Acknowledging another subscription's message

Rule: Ack fails with ErrWrongSubscription for a message of another subscription or Client; Tx.Ack, for a message of another Client.
Case: sub A acknowledges a message from sub B.

## SUB-23 Missing ack header

Rule: A MESSAGE for any held subscription, AckAuto included since it acknowledges on the wire (SUB-28), that lacks its acknowledgement id ends the session with a protocol error: `ack` in 1.2, `message-id` in 1.1.
Case: a 1.2 broker omits `ack` on a client-individual subscription.

## SUB-24 Message ownership

Rule: Messages ReadMessage stores are owned by the caller; the client writes a body's storage only during a ReadMessage call passed a Message that shares it.
Case: a caller keeps a message body and reads the next message into a new Message.

## SUB-25 Message headers

Rule: Message.Headers keeps every received header in order, repeated ones included.
Case: a MESSAGE with two `foo` headers.

## SUB-26 Unsubscribe while disconnected

Rule: Unsubscribe with no session removes the subscription locally and returns ErrOutcomeUnknown, since no UNSUBSCRIBE was sent.
Case: a durable subscription unsubscribed during backoff.

## SUB-27 STOMP 1.1 acknowledgement

Rule: In 1.1, ACK and NACK carry `message-id` and `subscription` instead of `id`.
Case: Ack of message-id `m7` on subscription `sub-1` in a 1.1 session.

## SUB-28 Auto mode on the wire

Rule: AckAuto subscribes as client-individual and acknowledges each message when ReadMessage stores it, so the broker's window applies.
Case: Artemis delivered a 2000-message auto-mode backlog; the subscription received 256 and then ErrOverflow, losing 1744.
Rejected: Auto mode keeps broker auto ack and an unbounded queue | Auto mode keeps the current bounded queue and documents the loss

## SUB-29 Automatic acknowledgement failure

Rule: When ReadMessage cannot buffer an automatic ACK because the session ended or the Client closed, it still stores the message, which the broker may redeliver.
Case: the connection drops between a MESSAGE arriving and ReadMessage storing it.

## SUB-30 Delivery guarantee

Rule: Delivery is at least once; whether an unacknowledged message survives a connection loss is the destination's durability, decided by the broker.
Case: a non-durable topic subscription across a broker restart.

## SUB-31 Acknowledging after the id was reused

Rule: Acknowledging a message delivered to an earlier subscription whose id was reused in the same session returns ErrStaleAck and writes nothing.
Case: subscription x receives K1, is unsubscribed and confirmed, x is subscribed again, then K1 is acknowledged.

## SUB-32 Automatic acknowledgement and an ended context

Rule: ReadMessage in AckAuto mode buffers the acknowledgement of the message it stores without waiting, even when its ctx has ended or the write buffer is full.
Case: a message is queued and ReadMessage is called with a cancelled ctx under a broker credit window of one.

## SUB-34 Broker window from Capacity

Rule: The broker's window paces a slow consumer: PrefetchHeader names a message-counted window header, and the client writes it on every SUBSCRIBE with the subscription's Capacity.
Case: Capacity 10, RabbitMQ, 100 unacknowledged messages: without PrefetchHeader the subscription ends with ErrOverflow after 10; with PrefetchHeader("prefetch-count") the broker stops at 10.

## SUB-40 No broker detection

Rule: The client writes no window header unless PrefetchHeader names one, whatever the CONNECTED server header says.
Case: CONNECTED carries server:ActiveMQ-Artemis/2.44.0.

## SUB-35 Messages kept from an ended session

Rule: Messages queued when a session ends stay queued and count toward Capacity; a MESSAGE that then finds the queue full follows SUB-8, or SUB-45 with EndOnOverflow.
Case: Capacity 2, two messages queued, the connection drops, and the next session redelivers one (TestKeptMessagesStayQueuedAfterReconnect).

## SUB-36 PrefetchHeader value

Rule: The header's value is Capacity after its default, and a header of the same name in SubscribeRequest.Headers is written instead.
Case: PrefetchHeader("prefetch-count") and Capacity 0 writes prefetch-count:256.

## SUB-37 PrefetchHeader names

Rule: PrefetchHeader panics on a header name Subscribe reserves (SUB-6); an empty name writes no header.
Case: PrefetchHeader("ack") panics.

## SUB-42 MESSAGE larger than MaxFrameSize

Rule: The session ends with ErrFrameTooLarge, and the default limit counts as an option the application set
Case: a 5 MiB MESSAGE on a non-durable topic; ending the session loses it and every other message in flight on the connection.
Rejected: MaxFrameSize has no default, so the session never ends on frame size unless the application set a limit

## SUB-43 Discarding after Unsubscribe

Rule: Unsubscribe permits the client to discard the subscription's MESSAGE frames, before the UNSUBSCRIBE receipt and after it (SUB-2).
Case: two MESSAGE frames in flight when Unsubscribe writes UNSUBSCRIBE, on a non-durable topic.

## SUB-44 Discarding after Close

Rule: Close permits the client to discard MESSAGE frames that arrive after DISCONNECT is buffered.
Case: a MESSAGE arrives while Close waits for the DISCONNECT receipt.

## SUB-45 Overflow option

Rule: SubscribeRequest.EndOnOverflow makes a MESSAGE that finds the queue full end the subscription with ErrOverflow and send UNSUBSCRIBE instead.
Case: EndOnOverflow, Capacity 2, three MESSAGE frames and no ReadMessage calls.

## SUB-46 Capacity without the overflow option

Rule: Without EndOnOverflow, Capacity and CapacityBytes limit nothing in the client; Capacity is still the value PrefetchHeader writes.
Case: Capacity 2 with PrefetchHeader("prefetch-count") on RabbitMQ writes prefetch-count:2, and a third redelivered MESSAGE after a reconnect is queued.

## SUB-47 Subscribe whose context ends

Rule: When Subscribe's ctx ends before the receipt, Subscribe returns the subscription if a MESSAGE already confirmed it, and otherwise ErrOutcomeUnknown with UNSUBSCRIBE.
Case: SUBSCRIBE written, one MESSAGE queued, then the caller's ctx ends: Subscribe returns the subscription and ReadMessage stores the message.

## SUB-48 Session ending before the SUBSCRIBE receipt

Rule: A MESSAGE for the subscription confirms it like its receipt, so when the session ends after a delivery but before the receipt, Subscribe returns the subscription, which is kept and replayed on reconnect.
Case: Artemis 2.44 and RabbitMQ 4.4 sent the SUBSCRIBE receipt before any MESSAGE in 40 of 40 subscriptions with a 20-message backlog, so only a broker that delivers first reaches this; TestMessageBeforeSubscribeReceiptConfirmsTheSubscription covers it.
Rejected: Subscribe returns ErrOutcomeUnknown and the delivered messages are discarded unacknowledged, an exception to SUB-9 | Subscribe returns the Subscription together with ErrOutcomeUnknown, so ReadMessage still stores the delivered messages

## SUB-49 Late MESSAGE for a reused id

Rule: A late MESSAGE for an earlier subscription whose explicit id was reused in the same session is delivered to the subscription that now holds the id.
Case: Unsubscribe x is confirmed, Subscribe x again, then Artemis delivers a late MESSAGE of the first x; MESSAGE carries only the id. That MESSAGE also confirms the second x (SUB-48), which the replay or its own receipt then settles.

## SUB-50 MESSAGE that breaks the protocol

Rule: A MESSAGE without a subscription header, or for a held subscription without its acknowledgement header, ends the session; an exception to SUB-9.
Case: a 1.2 broker omits `ack` on a client-individual subscription (SUB-23); a MESSAGE without `subscription` (TestMessageWithoutSubscriptionHeaderEndsTheSession).

## SUB-51 MESSAGE, then ERROR for the SUBSCRIBE

Rule: When the broker delivers to a subscription and then answers its SUBSCRIBE with ERROR, Subscribe returns the *ServerError and the delivered messages are discarded, an exception to SUB-9.
Case: SUBSCRIBE, one MESSAGE, then ERROR naming the SUBSCRIBE receipt.
Rejected: Subscribe returns the ended Subscription together with the *ServerError, so ReadMessage stores the delivered messages and then returns the error.

## SUB-52 Client ending while Subscribe waits

Rule: When Close or a terminal failure ends the Client while Subscribe waits, Subscribe returns a subscription a MESSAGE confirmed, unless a broker ERROR or an overflow ended it first, and ReadMessage stores its messages before returning ErrClosed or the cause.
Case: SUBSCRIBE, one MESSAGE, no receipt, then Close (TestCloseAfterAMessageConfirmedAPendingSubscribe); an ERROR for the SUBSCRIBE during Close returns the *ServerError instead (TestSubscribeRejectedWhileClosingReturnsTheServerError).

## SUB-53 Reading into the caller's message

Rule: ReadMessage, the only call that receives messages, stores the next message in the caller's Message, copying the body into the storage of its Body.
Case: a loop calling ReadMessage(ctx, &m) with one m receives 4 KiB bodies without allocating them.
Rejected: Next and an All iterator beside ReadMessage, each returning a message in a new Message | An opt-in SubscribeRequest field, so subscriptions that never reuse a Message keep one copy per body | The subscription swaps buffers with the caller, copying once but writing a buffer the caller gave up later, from the reader goroutine | A Release method on Message returning its body to a pool

## SUB-54 ReadMessage that returns an error

Assumed: When ReadMessage returns an error, the caller's Message is unchanged.
Case: ReadMessage on an unsubscribed subscription with nothing queued returns ErrUnsubscribed, and m keeps its ID and Body.

## SUB-55 Read bodies in queued body storage

Assumed: The subscription drops the bodies already read from its storage once they outnumber the bodies still waiting.
Case: a consumer always one 1000-octet message behind holds at most about 3000 octets of bodies.

## SUB-59 Body storage growth

Assumed: Body storage grows to twice the waiting octets, with EndOnOverflow to a capacity of at most 1.25 times CapacityBytes.
Case: CapacityBytes 1 MiB and a queue held near full: storage peaks at no more than 1.25 MiB.

## SUB-56 Shrinking queued body storage

Assumed: Body storage above 64 KiB is reallocated smaller within sixteen times its capacity in arriving bodies once under a quarter of it waits.
Case: after a 16 MiB burst, one waiting 4 KiB body at a time: storage shrinks after at most about 512 MiB more arrives; idle, it stays.

## SUB-57 Where the reader copies a body

Assumed: The reader copies each body into its queue under the Client's lock, sometimes moving all waiting bodies, so a long backlog can stall Send.
Case: a backlog of 16 MiB that reaches its growth limit makes one MESSAGE copy 16 MiB while a concurrent Send waits to buffer its frame.

## SUB-60 Consumer copy under the subscription lock

Assumed: ReadMessage copies its body while holding the subscription's lock, so a MESSAGE for that subscription waits for one body copy.
Case: a consumer copying a 1 MiB body delays the reader's next MESSAGE for that subscription by one 1 MiB copy.

## SUB-58 Headers in ReadMessage

Assumed: ReadMessage replaces the Message's Headers with newly allocated ones rather than reusing their storage.
Case: two ReadMessage calls into one m allocate header storage twice.
