# ADR-0003: Lease expiry is counted as a failed attempt

## Status

Accepted

## Context

When a lease expires, the broker faces genuine ambiguity. The worker might have crashed, might be alive but wedged, or might have hit a real execution error and simply failed to report it. From the broker's vantage point, over the network, these are indistinguishable. All it knows is that a job was handed out and never confirmed.

## Decision

Treat lease expiry as a failed attempt, routed through the same retry path as an explicitly reported failure. There is no separate "lease expired" outcome or transition; expiry increments the attempt-failure count and, subject to the retry ceiling, returns the job to pending or dead-letters it.

## Alternatives considered

- **A distinct lease-expired state and metric**: this would pretend the broker knows the cause was a crash rather than a failure. It does not know that, so the distinction would be a fiction that complicates the state machine without adding real information.

## Consequences

- The state machine stays small: a leased job either succeeds or fails, and expiry is just one way to fail.
- Poison pills and crash loops are both bounded by the same retry ceiling, which is the operationally honest outcome: a job that repeatedly cannot be confirmed should not be retried forever regardless of the reason.
- A dedicated LeaseExpired transition method was considered and dropped for this reason.
