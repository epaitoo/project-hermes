# ADR-0005: A hand-rolled write-ahead log instead of an embedded database

## Status

Accepted

## Context

Everything in the broker lives in memory: the queues, the leases, the DLQ. A broker restart, whether a deploy or a crash, would lose all of it. The system needs durable state that survives process death.

The project guide named two paths: integrate an embedded database such as BoltDB or BadgerDB, or implement a write-ahead log. Both give durability. They differ sharply in what they teach and what they hide.

## Decision

Hand-roll a write-ahead log. The broker records each state mutation as a framed, checksummed record appended to an on-disk log, fsync'd for durability, and replayed on startup to reconstruct state.

## Alternatives considered

- **BoltDB or BadgerDB**: mature, crash-safe, less code to write. But a B+tree key-value store hides precisely the mechanisms worth understanding: how a durable append works, how framing and checksums detect corruption, how replay reconstructs state, how a torn write is handled. The durability story would be "the library handles it", which is not a story that can be defended in depth.
- **A relational or client-server database**: far too heavy, and it reintroduces the external-dependency problem ADR-0001 exists to avoid.

## Consequences

- The durability path is fully owned and can be explained end to end: framing, CRC32 checksums, fsync, replay, torn-tail handling.
- The workload is a good fit for an append-only log: state changes are a sequential stream of mutations, which is exactly what a WAL is.
- There are no secondary indexes and no random-access reads. Recovery is a full replay from the start of the log, which is acceptable at this scale but would need compaction or checkpointing to scale further.
- The explicit tradeoff: BoltDB would have meant less code and crash-safe random access for free. That convenience was traded for understanding and a transparent durability story.
