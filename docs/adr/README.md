# Architecture Decision Records

This directory records the significant design decisions made while building Hermes, and the reasoning behind each one. The point of an ADR is not to document what the code does; it is to capture *why* a path was chosen over the alternatives, so that the decision can be revisited later with its original context intact.

Each record is self-contained and dated by its status. They are numbered in roughly the order the decisions were made, which tracks the project's phases: broker and workers, then fault tolerance, then persistence, then observability.

## Index

| ADR | Title | Phase |
|-----|-------|-------|
| [0001](0001-no-external-message-broker.md) | Build from first principles: no external message broker | Foundational |
| [0002](0002-lease-based-locking.md) | Lease-based job locking with anonymous, poll-based workers | Fault tolerance |
| [0003](0003-lease-expiry-as-failed-attempt.md) | Lease expiry is counted as a failed attempt | Fault tolerance |
| [0004](0004-bounded-retries-and-dlq.md) | Bounded retries with a dead letter queue | Fault tolerance |
| [0005](0005-hand-rolled-wal.md) | A hand-rolled write-ahead log instead of an embedded database | Persistence |
| [0006](0006-write-ahead-ordering.md) | Write before you mutate: write-ahead ordering | Persistence |
| [0007](0007-wal-framing-and-checksums.md) | WAL record framing and checksum placement | Persistence |
| [0008](0008-torn-tail-assumption.md) | Assume only the tail of the log can be torn | Persistence |
| [0009](0009-recovery-semantics.md) | Recovery discards in-flight leases but preserves retry counts | Persistence |
| [0010](0010-counters-versus-gauges.md) | Counters versus gauges, and restoring gauges on recovery | Observability |
| [0011](0011-split-failure-counter.md) | Split the failure counter into two named counters | Observability |
| [0012](0012-metric-placement.md) | Increment metrics at the WAL commit boundary | Observability |
| [0013](0013-shared-models-package.md) | A shared models package to decouple broker and worker | Foundational |

## Format

Records follow a lightweight template: context, the decision, the alternatives that were weighed, and the consequences accepted. See [template.md](template.md).
