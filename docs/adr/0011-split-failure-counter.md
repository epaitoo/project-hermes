# ADR-0011: Split the failure counter into two named counters

## Status

Accepted

## Context

The obvious first design is a single `failed` counter. It turns out to hide the exact distinction an operator needs during an incident.

Consider two systems. One is thrashing: jobs fail, retry, fail, retry, but eventually succeed. The other is quietly giving up: jobs fail their way through the retry ceiling and land in the DLQ. Under a single `failed` counter these look identical, yet they are completely different incidents with different responses.

## Decision

Use two counters instead of one:

- `job_attempt_failures_total`: every failed attempt, including ones that will be retried.
- `jobs_dead_lettered_total`: only the terminal failures that exhausted retries.

## Alternatives considered

- **A single `failed` counter**: fewer metrics, but it collapses retry churn and terminal give-up into one number and creates an operational blind spot.

## Consequences

- Retry churn and terminal failure are separately visible. A high attempt-failure rate with a low dead-letter rate is a retrying-but-recovering system; a rising dead-letter rate is a system losing work.
- Slightly more instrumentation, which is a small price for removing the blind spot.
