# Transactions

STOMP BEGIN, COMMIT and ABORT. Architecture: [decisions/0008](decisions/0008-transactions.md).

## TX-1 Begin

Rule: Begin sends BEGIN with a generated id `tx-N`, unique per Client, and returns after its receipt.
Case: two concurrent Begin calls.

## TX-2 Transactional frames

Rule: Tx.Send, Tx.Ack and Tx.Nack return once buffered, without a receipt; Commit's receipt confirms them.
Case: a transaction of 100 sends and one commit.

## TX-3 Transactional send validation

Rule: Tx.Send validates its message as Send does (SND-3, SND-4, SND-10, SND-12) and rejects `transaction` as a reserved header.
Case: Tx.Send with header `transaction:other`.

## TX-4 Commit and Abort

Rule: Commit and Abort send their frame with a receipt and return after it.
Case: Commit after three sends.

## TX-5 Session lost before COMMIT

Rule: When the session ends before COMMIT is buffered, every later call on the Tx returns ErrTxAborted.
Case: the connection resets between two Tx.Send calls.

## TX-6 Session lost after COMMIT

Rule: When the session ends after COMMIT is buffered but before its receipt, Commit returns ErrOutcomeUnknown.
Case: the broker restarts while COMMIT is in flight.

## TX-7 Finished transaction

Rule: Calls on a Tx after Commit or Abort returned return ErrTxDone, writing nothing.
Case: Tx.Send after Commit.

## TX-8 Not replayed

Rule: Transactions are never re-begun after a reconnect.
Case: a session with an open transaction resets.

## TX-9 Close with open transactions

Rule: Close does not abort open transactions explicitly; the broker aborts them at DISCONNECT, and later Tx calls return ErrClosed.
Case: Close while a Tx holds two sends.

## TX-10 Acknowledging another session's message

Rule: Tx.Ack of a message received in an earlier session returns ErrStaleAck, writing nothing.
Case: a message received before a reconnect is acknowledged in a transaction begun after it.

## TX-11 Concurrent use

Rule: A Tx is safe for concurrent use; its frames are written in the order the calls buffered them.
Case: two goroutines call Tx.Send on one Tx.
