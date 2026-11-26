# 0007 STOMP 1.1 and 1.2

Status: accepted

## Decision
The client offers 1.1 and 1.2 and uses the version the broker chooses for each session. `stomptext` takes the version as a parameter: the Decoder switches with SetVersion after CONNECTED, and AppendFrame takes it per call. The session core holds the negotiated version and builds version-specific ACK and NACK frames.

## Why
The owner requires both versions. The differences are confined to line endings, the `\r` escape and the acknowledgement headers, so one codec with a version parameter covers both without a second implementation.

## Rejected
- 1.2 only: excludes 1.1-only brokers such as ActiveMQ Classic's documented STOMP support.
- 1.0: the owner asked for 1.1 and 1.2; 1.0 has no NACK, no heart-beating and no escaping.
- A separate decoder per version: duplicates the incremental scanner for three differences.
