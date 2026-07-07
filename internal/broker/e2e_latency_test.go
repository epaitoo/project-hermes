package broker

import (
	"errors"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/epaitoo/hermes/internal/metrics"
	"github.com/epaitoo/hermes/internal/models"
	"github.com/epaitoo/hermes/internal/wal"
)

// TestE2ELatency measures end-to-end job latency (submit -> lease -> complete)
// at the queue+WAL level, under a fixed worker count. It is not a Go
// benchmark: it reports a median and p99 latency distribution, which a
// single ns/op figure cannot express. It is skipped in -short mode; run it
// explicitly with:
//
//	go test -run TestE2ELatency -v ./internal/broker/
func TestE2ELatency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end latency measurement in -short mode")
	}

	const (
		queueName = "email"
		totalJobs = 800
	)

	for _, workers := range []int{1, 4, 16} {
		w, err := wal.Open(filepath.Join(t.TempDir(), "e2e.wal"))
		if err != nil {
			t.Fatalf("open wal: %v", err)
		}
		q := NewQueue(w, &metrics.Metrics{})

		var submitAt sync.Map // uuid.UUID -> time.Time
		latencies := make(chan time.Duration, totalJobs)

		var wg sync.WaitGroup
		done := make(chan struct{})
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-done:
						return
					default:
					}
					job, err := q.RequestJob(queueName)
					if errors.Is(err, ErrNoJobAvailable) {
						time.Sleep(50 * time.Microsecond)
						continue
					}
					if err != nil {
						return
					}
					// simulate an instantaneous no-op handler, then complete.
					job.Status = models.StatusCompleted
					job.CompletedAt = time.Now()
					if _, err := q.UpdateJob(queueName, job); err != nil {
						return
					}
					start, _ := submitAt.Load(job.Id)
					latencies <- time.Since(start.(time.Time))
				}
			}()
		}

		// Producer: submit all jobs as fast as possible, creating a backlog
		// the workers drain. Latency is measured from each submit.
		for i := 0; i < totalJobs; i++ {
			j := benchJob()
			submitAt.Store(j.Id, time.Now())
			if err := q.AddJob(queueName, j); err != nil {
				t.Fatalf("submit: %v", err)
			}
		}

		collected := make([]time.Duration, 0, totalJobs)
		for range totalJobs {
			collected = append(collected, <-latencies)
		}
		close(done)
		wg.Wait()
		w.Close()

		sort.Slice(collected, func(i, j int) bool { return collected[i] < collected[j] })
		median := collected[len(collected)*50/100]
		p99 := collected[len(collected)*99/100]
		t.Logf("workers=%-2d  median=%-12v  p99=%-12v  (n=%d)", workers, median, p99, len(collected))
	}
}
