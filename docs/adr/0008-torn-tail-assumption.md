# ADR-0008: Assume only the tail of the log can be torn

## Status

Accepted

## Context

A crash can interrupt an append partway through, leaving a partial record on disk. The recovery reader has to decide what to do when it hits a record it cannot fully read or whose checksum fails. The safe scope of that decision depends on where corruption can occur.

Because every append is a single fsync'd write appended to the end of the file, and the process shuts down cleanly in the normal case, the only record that can be partially written is the last one. Records in the middle of the log were fully written and fsync'd before the next one began.

## Decision

Assume only the tail of the log can be torn. On replay, distinguish three cases:

- **Clean EOF**: the log ended exactly on a record boundary. Recovery is complete.
- **Torn tail**: a truncated read (`io.ErrUnexpectedEOF`) or a checksum mismatch (`ErrChecksumMismatch`) on the final record. Stop, and keep everything read so far.
- **A real error**: anything else is surfaced, not swallowed.

## Alternatives considered

- **Attempt mid-log repair**: if corruption could appear anywhere, recovery would have to scan past bad records and resynchronize. Under the torn-tail assumption this complexity is unnecessary, and repairing corruption the system cannot reason about risks reconstructing wrong state.
- **Truncate on any error**: discarding valid earlier records because the tail is torn would lose committed work. Stopping and keeping is correct.

## Consequences

- Recovery is simple and correct under the stated assumption.
- The assumption is documented so it can be re-examined if the write path ever changes (for example, if appends stop being single fsync'd writes).
