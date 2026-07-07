# ADR-0007: WAL record framing and checksum placement

## Status

Accepted

## Context

The log is a flat byte stream. On replay, the reader has to split that stream back into discrete records and detect any record that was corrupted, whether by a torn write or bit rot. That requires a self-describing frame around each record and a way to verify integrity.

## Decision

Each record is framed as:

```
[length][checksum][type][payload]
```

- `length` and `checksum` are fixed 4-byte, big-endian headers.
- The checksum is CRC32 (IEEE polynomial), computed over the content bytes `[type][payload]`.
- `length` counts those same content bytes.

The checksum sits immediately after the length and immediately before the content it protects, which is the order the reader needs: read the length, read the checksum, read `length` content bytes, recompute the checksum over them, compare.

## Alternatives considered

- **Checksum at the end of the record**: forces the reader to buffer the whole record before it can know where the checksum is. Putting fixed-size headers first makes framing streamable.
- **Checksum covering the length too**: the length is needed to know how much to read before the checksum can be verified, so it cannot itself be under the checksum without a chicken-and-egg problem. Covering content only is the clean choice.

## Consequences

- Corruption and torn writes are detectable on replay via checksum mismatch.
- The header is a fixed 8 bytes per record.
- The reader logic is a simple, streamable loop: header, then content, then verify.
