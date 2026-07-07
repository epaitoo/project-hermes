# ADR-0004: Bounded retries with a dead letter queue

## Status

Accepted

## Context

Failures must be retried, or transient problems would permanently lose work. But unbounded retries create a worse failure mode: a poison-pill job that fails every time will be retried forever, consuming worker capacity that healthy jobs need.

## Decision

Every job carries a retry count and a `max_retries` ceiling. On a failed attempt, if retries remain, the count is incremented and the job returns to pending. If retries are exhausted, the job is moved to a dead letter queue instead of being retried again. The DLQ supports three operations: inspect, redrive (return a job to its queue, typically after a fix), and discard.

## Alternatives considered

- **Unbounded retries**: simplest to implement, but lets one bad job starve the system.
- **Drop failed jobs after N attempts**: bounds the blast radius but destroys evidence and offers no recovery path. The DLQ keeps the job for inspection and manual redrive.

## Consequences

- A single failing job has a bounded blast radius.
- There is a human-in-the-loop recovery path: inspect the DLQ, fix the underlying cause, redrive.
- The DLQ size becomes a meaningful health signal in its own right (see ADR-0011).
