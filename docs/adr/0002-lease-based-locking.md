# ADR-0002: Lease-based job locking with anonymous, poll-based workers

## Status

Accepted

## Context

Multiple workers pull from the same queues. Two things must hold. First, no two workers should execute the same job concurrently. Second, if a worker dies mid-job, that job must eventually be handed to another worker rather than lost.

A common approach is a worker registry: workers register with the broker, send heartbeats, and the broker detects a dead worker when its heartbeats stop, then reassigns its jobs. That works, but it makes the broker responsible for tracking worker liveness, which is a second failure-detection problem layered on top of the first.

## Decision

When a worker polls for work, the broker leases the job to it: the job is stamped with a lease and becomes invisible to every other worker until the lease expires. Workers are anonymous. They do not register and the broker keeps no roster of them. A background lease checker inside the broker reclaims expired leases back to the pending pool. Long-running jobs send heartbeats that renew their lease so they are not reclaimed while still legitimately running.

## Alternatives considered

- **Worker registration with heartbeats to a registry**: the broker tracks each worker's liveness and reassigns on heartbeat timeout. More moving parts, more state to keep consistent, and more failure modes, for no benefit at this scale.

## Consequences

- The broker tracks lease liveness, not worker liveness. There is only one failure signal to reason about: did the lease get renewed or not.
- There is no concept of "the set of currently live workers", so a live-worker-count metric is architecturally infeasible. This is revisited in ADR-0010.
- Lease duration becomes a tuning knob: too short and healthy long jobs get reclaimed, too long and dead work is slow to recover.
