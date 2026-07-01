package broker

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/epaitoo/hermes/internal/metrics"
	"github.com/epaitoo/hermes/internal/models"
	"github.com/epaitoo/hermes/internal/wal"
	"github.com/google/uuid"
)

// newMeteredQueue builds a queue backed by a real temp-dir WAL and its own
// metrics instance, and hands back both so tests can inspect the snapshot.
func newMeteredQueue(t *testing.T) (*Queue, *metrics.Metrics) {
	t.Helper()
	w, err := wal.Open(filepath.Join(t.TempDir(), "test.wal"))
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	t.Cleanup(func() { w.Close() })

	m := &metrics.Metrics{}
	return NewQueue(w, m), m
}

// meteredJob builds a pending job ready to be leased. maxRetries controls
// whether a later failure retries or dead-letters.
func meteredJob(maxRetries int) models.Job {
	return models.Job{
		Id:         uuid.New(),
		Name:       "test-job",
		TaskType:   "email_job",
		Status:     models.StatusPending,
		MaxRetries: maxRetries,
	}
}

func TestMetricsWiring(t *testing.T) {
	const queueName = "email"

	tests := []struct {
		name string
		run  func(t *testing.T, q *Queue, jobID uuid.UUID)
		want metrics.Snapshot
	}{
		{
			name: "submit, lease, complete",
			run: func(t *testing.T, q *Queue, jobID uuid.UUID) {
				leaseOne(t, q, queueName)
				completeOne(t, q, queueName, jobID)
			},
			want: metrics.Snapshot{Submitted: 1, Completed: 1},
		},
		{
			name: "submit, lease, retry (retries remain)",
			run: func(t *testing.T, q *Queue, jobID uuid.UUID) {
				leaseOne(t, q, queueName)
				if _, err := q.FailOrRetry(jobID); err != nil {
					t.Fatalf("FailOrRetry: %v", err)
				}
			},
			// retry sends the job back to pending; one attempt failed.
			want: metrics.Snapshot{Submitted: 1, Pending: 1, JobAttemptFailure: 1},
		},
		{
			name: "submit, lease, dead-letter (out of retries)",
			run: func(t *testing.T, q *Queue, jobID uuid.UUID) {
				leaseOne(t, q, queueName)
				if _, err := q.FailOrRetry(jobID); err != nil {
					t.Fatalf("FailOrRetry: %v", err)
				}
			},
			// terminal failure bumps BOTH failure counters.
			want: metrics.Snapshot{
				Submitted: 1, DLQSize: 1,
				JobAttemptFailure: 1, JobDeadLetter: 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, m := newMeteredQueue(t)

			// The dead-letter case needs a job with no retries left.
			maxRetries := 3
			if tt.name == "submit, lease, dead-letter (out of retries)" {
				maxRetries = 0
			}
			job := meteredJob(maxRetries)

			if err := q.AddJob(queueName, job); err != nil {
				t.Fatalf("AddJob: %v", err)
			}
			tt.run(t, q, job.Id)

			got := m.Snapshot()
			if got != tt.want {
				t.Errorf("snapshot mismatch\n got: %+v\nwant: %+v", got, tt.want)
			}

			// Live-pool invariant must hold no matter the path.
			if inSystem := got.Pending + got.Leased + got.DLQSize; inSystem < 0 {
				t.Errorf("invariant violated: pending+leased+dlqSize = %d", inSystem)
			}
		})
	}
}

// leaseOne requests a job and fails the test if none was available.
func leaseOne(t *testing.T, q *Queue, queueName string) {
	t.Helper()
	if _, err := q.RequestJob(queueName); err != nil {
		t.Fatalf("RequestJob: %v", err)
	}
}

// completeOne marks a leased job completed via UpdateJob.
func completeOne(t *testing.T, q *Queue, queueName string, jobID uuid.UUID) {
	t.Helper()
	done := models.Job{Id: jobID, Status: models.StatusCompleted}
	if _, err := q.UpdateJob(queueName, done); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
}

// TestRecoverRestoresGauges drives jobs on one queue, "crashes" by closing the
// WAL, then reopens the same WAL into a fresh Queue with a fresh Metrics and
// recovers. It asserts SetPending/SetDLQSize seeded the gauges from the true
// rebuilt counts — the path the live-transition tests never exercise, and where
// the original SetPending miscount lived.
func TestRecoverRestoresGauges(t *testing.T) {
	const queueName = "email"
	walPath := filepath.Join(t.TempDir(), "recover.wal")

	// First "process": add jobs, dead-letter one, then die.
	w1, err := wal.Open(walPath)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	q1 := NewQueue(w1, &metrics.Metrics{})

	// One job with no retries left goes in first so the next poll leases it;
	// lease it, fail it, and it dead-letters.
	dead := meteredJob(0)
	if err := q1.AddJob(queueName, dead); err != nil {
		t.Fatalf("AddJob dead: %v", err)
	}
	if _, err := q1.RequestJob(queueName); err != nil {
		t.Fatalf("RequestJob: %v", err)
	}
	if _, err := q1.FailOrRetry(dead.Id); err != nil {
		t.Fatalf("FailOrRetry: %v", err)
	}

	// Two jobs that stay pending.
	for i := 0; i < 2; i++ {
		if err := q1.AddJob(queueName, meteredJob(3)); err != nil {
			t.Fatalf("AddJob pending: %v", err)
		}
	}

	w1.Close() // simulate the process dying

	// Second "process": fresh queue and metrics, same WAL, recover.
	w2, err := wal.Open(walPath)
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	t.Cleanup(func() { w2.Close() })

	m2 := &metrics.Metrics{}
	q2 := NewQueue(w2, m2)
	if err := q2.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}

	// The gauges must match the counts actually rebuilt into the queue maps.
	var wantPending int64
	for _, jobs := range q2.q {
		wantPending += int64(len(jobs))
	}
	var wantDLQ int64
	for _, jobs := range q2.dlq {
		wantDLQ += int64(len(jobs))
	}

	got := m2.Snapshot()
	if got.Pending != wantPending {
		t.Errorf("Pending gauge = %d, want rebuilt count %d", got.Pending, wantPending)
	}
	if got.DLQSize != wantDLQ {
		t.Errorf("DLQSize gauge = %d, want rebuilt count %d", got.DLQSize, wantDLQ)
	}

	// Sanity-check the scenario itself: 2 pending survivors, 1 dead-lettered.
	if wantPending != 2 || wantDLQ != 1 {
		t.Fatalf("unexpected rebuilt state: pending=%d dlq=%d", wantPending, wantDLQ)
	}
}

// TestRequestJobEmptyDoesNotLease is the regression guard for the
// JobLeased-on-failed-poll bug: polling an empty queue must return
// ErrNoJobAvailable and touch neither the pending nor the leased gauge.
func TestRequestJobEmptyDoesNotLease(t *testing.T) {
	q, m := newMeteredQueue(t)

	if _, err := q.RequestJob("email"); !errors.Is(err, ErrNoJobAvailable) {
		t.Fatalf("RequestJob on empty queue: got %v, want %v", err, ErrNoJobAvailable)
	}

	got := m.Snapshot()
	if got.Pending != 0 || got.Leased != 0 {
		t.Errorf("empty poll moved gauges: pending=%d leased=%d, want 0 and 0", got.Pending, got.Leased)
	}
}
