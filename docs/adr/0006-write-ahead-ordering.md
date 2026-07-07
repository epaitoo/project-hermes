# ADR-0006: Write before you mutate: write-ahead ordering

## Status

Accepted

## Context

A write-ahead log is only useful if the log is guaranteed to be at least as up to date as the in-memory state it protects. If the broker changed its state first and then wrote the log, a crash in the window between the two would leave a change that happened but was never recorded. Recovery would silently lose it.

## Decision

At every mutation point, append the log record before mutating in-memory state. The log write is the commit point; the memory change is a consequence of a change that is already durable.

## Alternatives considered

- **Mutate then log**: simpler to write, since the in-memory operation naturally comes first in the code. But it inverts the durability guarantee and makes silent loss possible on crash. Rejected outright.

## Consequences

- A durable log entry is the source of truth. Recovery reconstructs exactly and only what was logged.
- There is an fsync on the hot path of every mutation, which is a real latency cost accepted in exchange for correctness.
- The ordering discipline must hold at all six mutation points. A single place that mutates before logging reopens the loss window, so this is a rule that is only as strong as its least careful application.
