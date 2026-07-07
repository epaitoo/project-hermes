# ADR-0001: Build from first principles, no external message broker

## Status

Accepted

## Context

Hermes is a distributed task queue. Production systems that solve this problem already exist: Redis, RabbitMQ, Kafka, and cloud queue services. Reaching for one of them would make Hermes work faster, but it would move every interesting problem out of this codebase and into someone else's.

The purpose of Hermes is to demonstrate genuine understanding of distributed systems, not to ship the fastest possible queue. That purpose shapes the constraint.

## Decision

Build the queuing, failure detection, durability, and crash recovery directly on top of Go's standard library and a hand-rolled write-ahead log. Use no external message broker of any kind.

## Alternatives considered

- **Use Kafka or RabbitMQ as the transport**: proven and scalable, but it hides exactly the mechanisms Hermes exists to teach: delivery guarantees, lease coordination, durability. Wiring up Kafka demonstrates integration skill, not systems understanding.
- **Use Redis as the queue store**: lighter than Kafka, but still delegates persistence and atomicity to another process.

## Consequences

- The hard problems (exactly-once-ish delivery, worker failure detection, crash recovery) are solved in code that can be read and defended in an interview.
- There is more to own and test, since nothing is delegated.
- Hermes is not production scale, and that is an accepted tradeoff for a portfolio project.
- Studying how Kafka and RabbitMQ solved the same problems remains valuable as comparison material, precisely because Hermes solves them independently.
