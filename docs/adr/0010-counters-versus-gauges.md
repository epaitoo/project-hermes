# ADR-0010: Counters versus gauges, and restoring gauges on recovery

## Status

Accepted

## Context

The metrics fall into two shapes, and they behave differently across a restart. Getting the distinction wrong produces numbers that lie after every recovery.

## Decision

Classify each metric by a single test: can this number go down?

- **Counters** (submitted, completed, dead-lettered, attempt-failures) only ever increase. They reset to zero on restart and are not restored, because the meaningful thing about a counter is its rate over time, and a monitoring system computes that rate from the counter's movement, not its absolute value.
- **Gauges** (pending, leased, dlq_size) can go up and down. They are restored during recovery, because a gauge must reflect the true current state of the system, and recovery reconstructs that state from the log.

## Alternatives considered

- **Restore counters too**: pointless and misleading. There is no correct pre-restart total to restore to, and the rate calculation does not need one.
- **Recompute gauges lazily instead of restoring**: possible, but restoring them explicitly during `Recover` keeps the snapshot correct from the first scrape after startup.

## Consequences

- After recovery, gauges immediately reflect reality via setters such as `SetPending` and `SetDLQSize`.
- This path is guarded by a recovery test, because it is exactly where a gauge miscount previously lived: a `SetPending` off-by-one that only a restart would reveal.
