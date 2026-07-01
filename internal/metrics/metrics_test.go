package metrics

import (
	"sync"
	"testing"
)

// op is one transition with no arguments, so we can list sequences in a table.
type op func(*Metrics)

func TestMetricsTransitions(t *testing.T) {
	tests := []struct {
		name string
		ops  []op
		want Snapshot
	}{
		{
			name: "zero value",
			want: Snapshot{},
		},
		{
			name: "happy path: submit, lease, complete",
			ops: []op{
				(*Metrics).JobSubmitted,
				(*Metrics).JobLeased,
				(*Metrics).JobCompleted,
			},
			want: Snapshot{Submitted: 1, Completed: 1},
		},
		{
			name: "retry then succeed",
			ops: []op{
				(*Metrics).JobSubmitted,
				(*Metrics).JobLeased,
				(*Metrics).JobRetried,
				(*Metrics).JobLeased,
				(*Metrics).JobCompleted,
			},
			want: Snapshot{Submitted: 1, Completed: 1, JobAttemptFailure: 1},
		},
		{
			name: "dead letter bumps both failure counters",
			ops: []op{
				(*Metrics).JobSubmitted,
				(*Metrics).JobLeased,
				(*Metrics).JobDeadLettered,
			},
			want: Snapshot{Submitted: 1, DLQSize: 1, JobAttemptFailure: 1, JobDeadLetter: 1},
		},
		{
			name: "dlq redrive",
			ops: []op{
				(*Metrics).JobSubmitted,
				(*Metrics).JobLeased,
				(*Metrics).JobDeadLettered,
				(*Metrics).DLQRedriven,
			},
			want: Snapshot{Submitted: 1, Pending: 1, JobAttemptFailure: 1, JobDeadLetter: 1},
		},
		{
			name: "dlq discard",
			ops: []op{
				(*Metrics).JobSubmitted,
				(*Metrics).JobLeased,
				(*Metrics).JobDeadLettered,
				(*Metrics).DLQDiscarded,
			},
			want: Snapshot{Submitted: 1, JobAttemptFailure: 1, JobDeadLetter: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var m Metrics
			for _, fn := range tt.ops {
				fn(&m)
			}
			got := m.Snapshot()
			if got != tt.want {
				t.Errorf("snapshot mismatch\n got: %+v\nwant: %+v", got, tt.want)
			}

			// Live-pool invariant: the count of jobs still in the system is
			// pending + leased + dlqSize, and it must never go negative.
			if inSystem := got.Pending + got.Leased + got.DLQSize; inSystem < 0 {
				t.Errorf("invariant violated: pending+leased+dlqSize = %d, want >= 0", inSystem)
			}
		})
	}
}

// SetPending/SetDLQSize must Store (overwrite), not Add. This proves it:
// a prior increment is discarded, not summed.
func TestRecoverySettersOverwrite(t *testing.T) {
	var m Metrics
	m.JobSubmitted() // pending is now 1
	m.SetPending(47) // must become 47, not 48
	m.SetDLQSize(5)

	got := m.Snapshot()
	want := Snapshot{Submitted: 1, Pending: 47, DLQSize: 5}
	if got != want {
		t.Errorf("after recovery setters\n got: %+v\nwant: %+v", got, want)
	}
}

// The reason atomics exist. With a plain int field this fails the count
// (and trips -race). With atomic.Int64 it passes cleanly under contention.
func TestConcurrentIncrements(t *testing.T) {
	var m Metrics
	const goroutines, perGoroutine = 50, 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				m.JobSubmitted()
			}
		}()
	}
	wg.Wait()

	want := int64(goroutines * perGoroutine)
	if got := m.Snapshot().Submitted; got != want {
		t.Errorf("concurrent submitted: got %d, want %d", got, want)
	}
}
