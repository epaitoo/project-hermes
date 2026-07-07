# Benchmarks

This document defines what Hermes measures, why each measurement matters, and how to run the benchmarks. The numbers below are real, measured on the hardware named in the Environment section. Re-run them on your own machine before quoting them elsewhere — a benchmark is only meaningful on the machine and code it was run against, and the fsync-bound figures are dominated by the disk.

## What to measure and why

The goal is not a single headline throughput number. It is to characterize the parts of the system where a design choice could plausibly hurt, and to have real evidence for the tradeoffs the ADRs claim.

- **Submission throughput**: submissions per second through the broker. This is the hot path where the write-ahead ordering rule (ADR-0006) puts an fsync in front of every mutation, so it is the clearest place to see the cost of durability.
- **Poll and lease throughput**: lease operations per second. This is the other high-frequency broker path, guarded by the same mutex as submission.
- **WAL append cost, fsync versus no fsync**: the per-append latency with and without the durable sync. This isolates and quantifies exactly what durability costs, which is the concrete evidence behind the ADR-0005 tradeoff.
- **End-to-end job latency**: time from submission to completion under a fixed worker count. This is what a user of the queue actually experiences.
- **Recovery time versus log size**: how long `Recover` takes as the log grows. Because recovery is a full replay (ADR-0005), this is expected to grow linearly with log length, and measuring it tells you when compaction or checkpointing would become necessary.

## How to run

The benchmarks live next to the code they exercise: broker throughput and recovery in [internal/broker/queue_bench_test.go](../internal/broker/queue_bench_test.go), the WAL append comparison in [internal/wal/wal_bench_test.go](../internal/wal/wal_bench_test.go), and the end-to-end latency measurement in [internal/broker/e2e_latency_test.go](../internal/broker/e2e_latency_test.go). 

Broker throughput. `PollLease` rebuilds a single-job queue each iteration, so its per-op setup makes the default auto-scaling run for minutes; pin the iteration count:

```bash
go test -bench='BenchmarkSubmit|BenchmarkPollLease' -benchmem -run=^$ -benchtime=200x ./internal/broker/...
```

WAL append, fsync versus no fsync:

```bash
go test -bench=BenchmarkWAL -benchmem -run=^$ ./internal/wal/...
```

Recovery (replay is read/CPU-bound, so a few iterations suffice):

```bash
go test -bench=BenchmarkRecover -benchmem -run=^$ -benchtime=5x ./internal/broker/...
```

End-to-end latency (a test, not a benchmark — it prints a median/p99 distribution):

```bash
go test -run TestE2ELatency -v ./internal/broker/
```

**Run against a real disk.** `b.TempDir()` and `t.TempDir()` resolve under `$TMPDIR` (default `/tmp`), which on many Linux systems is a `tmpfs` RAM disk where `fsync` is a no-op — that makes the durable and non-durable paths look identical and hides the entire point of these numbers. Point the temp dir at the disk you actually care about before trusting any fsync-bound result:

```bash
mkdir -p .benchtmp
TMPDIR="$PWD/.benchtmp" go test -bench=BenchmarkWAL -benchmem -run=^$ ./internal/wal/...
```

Record the environment alongside the numbers. A throughput figure is meaningless without the CPU, disk type, and Go version it was produced on — and, as above, the filesystem the temp dir landed on.

## Environment

All numbers below were produced on this machine. The fsync-bound results (Submit, WAL append with fsync, end-to-end latency) were run against the ext4 disk via the `TMPDIR` override above, not the default `/tmp` tmpfs.

- **Machine**: Intel Core i5-8350U @ 1.70GHz, 4 cores / 8 threads, 8 GB RAM
- **Disk**: SATA SSD (Samsung MZNLN256), ext4
- **OS**: Linux 6.18.37-1-lts (x86-64)
- **Go version**: go1.26.4

## Results

Measured on the environment above. Re-run on your own hardware — the fsync-bound rows in particular are entirely a property of the disk.

### Broker throughput

| Operation | Ops/sec | ns/op | Allocs/op |
|-----------|---------|-------|-----------|
| Submit | ~133 | 7,538,949 | 13 |
| Poll and lease | ~172,000 | 5,816 | 0 |

Submit is entirely fsync-bound: ~7.5 ms per op is the disk flush, not the ~5 µs of encoding and in-memory work around it (compare the WAL table below). Poll-and-lease touches no disk — it is a mutex acquisition plus a slice scan — and is ~1,300× faster with zero allocations.

### WAL append: the cost of durability

| Mode | ns/op | Notes |
|------|-------|-------|
| Append with fsync | 9,186,284 | Durable. The real hot-path cost. |
| Append without fsync | 5,277 | Not durable. Isolates fsync overhead. |

The gap between these two rows is the concrete price of the durability guarantee: **~9.18 ms of fsync on top of ~5 µs of actual work, roughly 1,700×.** That is the whole story of ADR-0005 — a durable append is not "a bit slower," it is dominated by a single disk-flush syscall, and everything else the WAL does is rounding error. On a `tmpfs` mount these two rows are identical (both ~5 µs), which is exactly why the run instructions insist on a real disk.

### Recovery

| Log size (records) | Recover time | ns per record |
|--------------------|--------------|---------------|
| 1,000 | 19.3 ms | ~19,300 |
| 10,000 | 222 ms | ~22,200 |
| 100,000 | 1.29 s | ~12,900 |

The shape is roughly linear, as expected for a full replay: 10× the records is ~11× the time from 1k to 10k. Recovery reads and decodes rather than fsyncs, so it is CPU- and allocation-bound (~1.4 M allocations at 100k records) rather than disk-flush-bound — these numbers barely move between tmpfs and SSD. The takeaway stands: at ~1.3 s per 100k records, a multi-million-record log makes startup slow, which is the point at which compaction or checkpointing stops being optional.

### End-to-end latency

Measured in-process (submit → lease → complete, carrying the real WAL fsync and queue mutex; no HTTP), draining a backlog of 800 jobs on the SATA SSD.

| Workers | Median latency | p99 latency |
|---------|----------------|-------------|
| 1 | 88.1 ms | 279.3 ms |
| 4 | 47.2 ms | 63.1 ms |
| 16 | 42.7 ms | 63.1 ms |

Note the diminishing returns: going from 1 to 4 workers roughly halves median latency, but 4 to 16 barely moves it. That is expected here — both the submit and the complete paths fsync while holding the same queue mutex, so throughput is serialized on disk flushes regardless of worker count. Past a handful of workers you are latency-bound by the disk, not by concurrency. On tmpfs (fsync free) the same test runs sub-millisecond, which again shows the disk, not the code, sets the ceiling.

## Reading the results

Two things are worth calling out explicitly when you present these:

- The fsync cost is real and it is the point, not a flaw. It is what you bought durability with, and being able to quote the number shows you understand the tradeoff rather than having stumbled into it.
- Recovery time growing with log size is the known limit of a replay-only design. Naming it, and naming compaction or checkpointing as the fix, is a stronger answer than pretending the design has no limit.
