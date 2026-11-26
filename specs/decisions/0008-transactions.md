# 0008 Transactions in the session core

Status: accepted

## Decision
Open transactions are part of the session core's state. The core refuses frames for a transaction that is not open in the current session, so a Tx from an earlier session cannot write. A Tx holds its session generation, like an acknowledgement handle.

## Why
The broker aborts every transaction when the connection fails (STOMP 1.1 and 1.2), so a transaction never outlives its session. Keeping the open set in the core makes "no frame for a dead transaction" a checkable property of one deterministic function, and part of the Lean model.

## Rejected
- Replaying transactions after reconnect: the broker discarded their frames, and resending them would repeat sends (SND-9).
- Transactions tracked only in the client: splits the session's protocol state across two owners.
- Receipts on every transactional frame: COMMIT's receipt already confirms the transaction; per-frame receipts double the round trips.
