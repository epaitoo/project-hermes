package broker

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/epaitoo/hermes/internal/metrics"
	"github.com/epaitoo/hermes/internal/models"
	"github.com/epaitoo/hermes/internal/wal"
	"github.com/google/uuid"
)

// newBenchQueue builds a queue backed by a real temp-dir WAL, so the
// benchmarks pay the same fsync'd append cost the production path pays.
func newBenchQueue(b *testing.B) *Queue {
	b.Helper()
	w, err := wal.Open(filepath.Join(b.TempDir(), "bench.wal"))
	if err != nil {
		b.Fatalf("open wal: %v", err)
	}
	b.Cleanup(func() { w.Close() })
	return NewQueue(w, &metrics.Metrics{})
}

func benchJob() models.Job {
	return models.Job{
		Id:         uuid.New(),
		Name:       "bench-job",
		TaskType:   "email",
		Status:     models.StatusPending,
		MaxRetries: 3,
	}
}

// BenchmarkSubmit measures the hot submission path: an fsync'd WAL append
// in front of the in-memory append under the queue mutex.
func BenchmarkSubmit(b *testing.B) {
	q := newBenchQueue(b)
	const queueName = "email"

	for b.Loop() {
		if err := q.AddJob(queueName, benchJob()); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPollLease measures leasing one pending job in isolation. A leased
// job stays in the slice as InProgress, so RequestJob's scan grows with the
// backlog; to measure the lease itself and not that O(n) scan, each iteration
// runs against a fresh single-job queue (built with the timer stopped).
func BenchmarkPollLease(b *testing.B) {
	w, err := wal.Open(filepath.Join(b.TempDir(), "bench.wal"))
	if err != nil {
		b.Fatalf("open wal: %v", err)
	}
	b.Cleanup(func() { w.Close() })
	const queueName = "email"

	for b.Loop() {
		b.StopTimer()
		q := NewQueue(w, &metrics.Metrics{})
		if err := q.AddJob(queueName, benchJob()); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		if _, err := q.RequestJob(queueName); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRecover measures a full crash recovery: replaying a WAL of the
// given size and rebuilding in-memory state. Recovery is a full replay
// (ADR-0005), so cost is expected to grow linearly with the log length.
func BenchmarkRecover(b *testing.B) {
	const queueName = "email"

	for _, size := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("records=%d", size), func(b *testing.B) {
			// Prime a WAL on disk with `size` created-job records, once.
			path := filepath.Join(b.TempDir(), "recover.wal")
			w, err := wal.Open(path)
			if err != nil {
				b.Fatalf("open wal: %v", err)
			}
			seed := NewQueue(w, &metrics.Metrics{})
			for i := 0; i < size; i++ {
				if err := seed.AddJob(queueName, benchJob()); err != nil {
					b.Fatalf("seed: %v", err)
				}
			}
			w.Close()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				rw, err := wal.Open(path)
				if err != nil {
					b.Fatalf("reopen wal: %v", err)
				}
				q := NewQueue(rw, &metrics.Metrics{})
				b.StartTimer()

				if err := q.Recover(); err != nil {
					b.Fatalf("recover: %v", err)
				}

				b.StopTimer()
				rw.Close()
				b.StartTimer()
			}
		})
	}
}
