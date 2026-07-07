# ADR-0012: Increment metrics at the WAL commit boundary

## Status

Accepted

## Context

Where in the code a counter is incremented determines whether it counts intentions or facts. If a metric is bumped before the operation is durable, a subsequent failure leaves a count that claims something happened when it did not.

## Decision

Increment a counter only past the last point where the operation can still fail. For state mutations, that point is the WAL append: the commit boundary established in ADR-0006. A job is not counted as submitted until it is durably logged.

## Alternatives considered

- **Increment at the start of the handler**: counts attempts, not commits, and overcounts whenever an append or validation fails afterward.

## Consequences

- Counts reflect committed operations only. The submitted counter matches what recovery would replay.
- The placement rule is uniform across mutation points, which keeps the reasoning simple: find the commit point, increment after it.
