# Sending (`Client.Send`)

## SND-1 Binary bodies

Rule: Bodies are arbitrary octets, including NUL, and are delivered unchanged.
Case: sending a protobuf payload that contains NUL octets.

## SND-2 Default content-length

Rule: Send writes content-length equal to the body length unless Outgoing.Text is true.
Case: Outgoing{Body: []byte("hi")} carries `content-length:2`.

## SND-3 Text bodies

Rule: With Outgoing.Text, Send omits content-length so Artemis and ActiveMQ create a TextMessage.
Case: a JSON body sent to a JMS consumer that expects TextMessage.

## SND-4 NUL in a text body

Rule: Send with Outgoing.Text and a body containing NUL fails with ErrTextHasNUL before writing.
Case: Text true and body `a\0b`.

## SND-5 Receipt

Rule: Send always requests a receipt and returns nil only after the matching RECEIPT arrives.
Case: Send to a queue on a healthy broker.

## SND-6 Outcome when the session ends before the receipt

Rule: Send returns ErrOutcomeUnknown when the session ends after the frame entered the write buffer.
Case: the connection resets between writing SEND and receiving RECEIPT.

## SND-7 Context ends before the frame is buffered

Rule: Send returns ctx.Err() and nothing is written when ctx ends before the frame enters the write buffer.
Case: Send called with an already cancelled context.

## SND-8 Context ends while waiting for the receipt

Rule: Send returns ErrOutcomeUnknown wrapping ctx.Err() when ctx ends after the frame was buffered.
Case: a 1 ms deadline and a broker that takes 10 ms to send RECEIPT.

## SND-9 Never resent

Rule: A Send is never resent after a reconnect.
Case: the connection resets while Send waits for RECEIPT.

## SND-10 Reserved headers

Rule: Send fails with ErrReservedHeader if Outgoing.Headers sets destination, content-length, content-type, receipt or transaction.
Case: Headers containing `content-length:99`.

## SND-11 Headers order

Rule: Outgoing.Headers are written in slice order after the headers the client sets.
Case: Headers {a:1, b:2} appear as `a:1` then `b:2`.

## SND-12 Empty destination

Rule: Send with an empty Destination fails with ErrInvalidRequest before writing.
Case: Outgoing{Body: []byte("x")}.

## SND-13 Send while disconnected

Rule: Send waits for the next session, bounded by ctx; it is never queued without a caller waiting.
Case: Send during a 2-second reconnect backoff with a 5-second context.

## SND-14 Body ownership

Rule: The caller must not modify Outgoing.Body until Send returns; the client does not retain it afterwards.
Case: a caller reuses its buffer after Send returns.
