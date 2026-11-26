# 0004 The reader never waits for a consumer

Status: accepted; the overflow outcome was revised by SUB-8 and SUB-9

## Decision
The single reader goroutine never blocks on a subscription. A MESSAGE that finds its subscription's queue full is queued anyway, because the client discards nothing the application did not permit (SUB-9). The broker's window bounds the queue; without one, memory grows with the broker's send rate. Ending the subscription with ErrOverflow at Capacity is an option the application sets (SUB-45). Flow control is the broker's, configured with the broker's own SUBSCRIBE header, which PrefetchHeader writes from Capacity for brokers that count messages.

## Why
A RECEIPT can sit behind a MESSAGE in the stream. If the reader blocked on a full queue while its consumer waited for that RECEIPT (in Send, Subscribe or AckWait), the client would deadlock, and every other subscription and heart-beat would stall with it.

## Rejected
- Blocking the reader (TCP backpressure): deadlocks as above.
- Dropping messages: silent loss; with cumulative ack it acknowledges messages nobody processed.
- Ending the subscription with ErrOverflow by default: it discards the MESSAGE and every one until the UNSUBSCRIBE receipt, which loses them on a destination that keeps no copy. Against Artemis, a non-durable topic with Capacity 2 kept 2 of 10 messages.
- A generic prefetch option mapped per broker: the headers differ in name and unit (Artemis bytes, ActiveMQ Classic and RabbitMQ messages). PrefetchHeader instead takes the header's name and writes Capacity, for brokers that count messages (SUB-34). Artemis paces client acknowledgement modes with a 10 KiB window by default; measured against Artemis 2.44.0, about 37 messages with 10-byte bodies arrive before any acknowledgement. With Capacity 10, RabbitMQ 4 and ActiveMQ Classic overflow without the header and stop at 10 with it.
