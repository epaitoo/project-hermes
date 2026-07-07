# ADR-0009: Recovery discards in-flight leases but preserves retry counts

## Status

Accepted

## Context

When the broker restarts and replays the log, some jobs were leased and in flight at the moment of the crash. Recovery has to decide what state those jobs return in. Two fields matter: the lease, and the retry count (with its scheduling timestamp).

## Decision

On recovery, reset in-flight (leased) jobs to pending, discarding the lease. Preserve each job's retry count and its next-run timestamp.

## Alternatives considered

- **Preserve the lease across restart**: meaningless. The worker that held the lease is gone with the old process. Keeping the lease would make the job invisible until an expiry that no longer corresponds to anything.
- **Reset retry counts on recovery**: this would hand every in-flight job a fresh full set of retries after every restart. A poison pill that crashes the broker, or simply happens to be in flight during an unrelated restart, would escape its ceiling forever. Preserving the count keeps ADR-0004's protection intact across restarts.

## Consequences

- A job that was mid-execution at the crash gets a fresh attempt, correctly counted against its remaining retries.
- Backoff scheduling survives, because the next-run timestamp is preserved.
- Recovery reconstructs the pending and DLQ populations from the persisted jobs, which also feeds gauge restoration (ADR-0010).
