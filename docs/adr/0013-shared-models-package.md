# ADR-0013: A shared models package to decouple broker and worker

## Status

Accepted

## Context

The worker needs the `Job` type. The direct route is for the worker package to import the broker package and use `broker.Job`. That creates a dependency from worker to broker, which quietly contradicts the system's real shape: in a real deployment the broker and the worker are separate processes on separate machines. They do not share a codebase. They share a contract over HTTP, and each deserializes the wire format into its own local type.

## Decision

Put the shared types (`Job`, `JobStatus`) in a neutral `internal/models` package. Both `broker` and `worker` import `models`; neither imports the other.

## Alternatives considered

- **Worker imports broker**: works in a single binary, but couples two components that are conceptually separate processes and inverts the intended dependency direction.
- **Duplicate the types in each package**: honest to the process boundary, but drifts out of sync and adds friction for a project that runs as one binary today.

## Consequences

- The dependency graph is clean: two leaf packages depending on a shared neutral one.
- The structure mirrors the real HTTP-contract boundary, which is a useful point to be able to articulate when asked about the design.
