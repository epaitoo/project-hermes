package metrics

import (
	"fmt"
	"strings"
	"sync/atomic"
)

type Metrics struct {
	submitted         atomic.Int64
	completed         atomic.Int64
	jobDeadLetter     atomic.Int64
	jobAttemptFailure atomic.Int64
	pending           atomic.Int64
	leased            atomic.Int64
	dlqSize           atomic.Int64
}

type Snapshot struct {
	Submitted         int64
	Completed         int64
	JobDeadLetter     int64
	JobAttemptFailure int64
	Pending           int64
	Leased            int64
	DLQSize           int64
}

func (m *Metrics) JobSubmitted() {
	m.submitted.Add(1)
	m.pending.Add(1)
}

func (m *Metrics) JobCompleted() {
	m.leased.Add(-1)
	m.completed.Add(1)
}

func (m *Metrics) JobLeased() {
	m.pending.Add(-1)
	m.leased.Add(1)
}

func (m *Metrics) JobRetried() {
	m.leased.Add(-1)
	m.pending.Add(1)
	m.jobAttemptFailure.Add(1)
}

func (m *Metrics) JobDeadLettered() {
	m.leased.Add(-1)
	m.dlqSize.Add(1)
	m.jobAttemptFailure.Add(1)
	m.jobDeadLetter.Add(1)
}

func (m *Metrics) DLQRedriven() {
	m.dlqSize.Add(-1)
	m.pending.Add(1)
}

func (m *Metrics) DLQDiscarded() {
	m.dlqSize.Add(-1)
}

func (m *Metrics) SetPending(n int64) {
	m.pending.Store(n)
}

func (m *Metrics) SetDLQSize(n int64) {
	m.dlqSize.Store(n)
}

func (m *Metrics) Snapshot() Snapshot {
	return Snapshot{
		Submitted:         m.submitted.Load(),
		Completed:         m.completed.Load(),
		JobDeadLetter:     m.jobDeadLetter.Load(),
		JobAttemptFailure: m.jobAttemptFailure.Load(),
		Pending:           m.pending.Load(),
		Leased:            m.leased.Load(),
		DLQSize:           m.dlqSize.Load(),
	}
}

func (s Snapshot) Prometheus() string {
	var b strings.Builder

	fmt.Fprintf(&b, "# HELP hermes_jobs_submitted_total Total jobs durably accepted into the broker.\n")
	fmt.Fprintf(&b, "# TYPE hermes_jobs_submitted_total counter\n")
	fmt.Fprintf(&b, "hermes_jobs_submitted_total %d\n", s.Submitted)

	fmt.Fprintf(&b, "# HELP hermes_jobs_completed_total Total jobs completed.\n")
	fmt.Fprintf(&b, "# TYPE hermes_jobs_completed_total counter\n")
	fmt.Fprintf(&b, "hermes_jobs_completed_total %d\n", s.Completed)

	fmt.Fprintf(&b, "# HELP hermes_jobs_dead_letter_total Total jobs which are dead lettered.\n")
	fmt.Fprintf(&b, "# TYPE hermes_jobs_dead_letter_total counter\n")
	fmt.Fprintf(&b, "hermes_jobs_dead_letter_total %d\n", s.JobDeadLetter)

	fmt.Fprintf(&b, "# HELP hermes_jobs_attempt_failure_total Total jobs which failed.\n")
	fmt.Fprintf(&b, "# TYPE hermes_jobs_attempt_failure_total counter\n")
	fmt.Fprintf(&b, "hermes_jobs_attempt_failure_total %d\n", s.JobAttemptFailure)

	fmt.Fprintf(&b, "# HELP hermes_jobs_pending Jobs currently waiting to be leased.\n")
	fmt.Fprintf(&b, "# TYPE hermes_jobs_pending gauge\n")
	fmt.Fprintf(&b, "hermes_jobs_pending %d\n", s.Pending)

	fmt.Fprintf(&b, "# HELP hermes_jobs_leased Jobs currently leased.\n")
	fmt.Fprintf(&b, "# TYPE hermes_jobs_leased gauge\n")
	fmt.Fprintf(&b, "hermes_jobs_leased %d\n", s.Leased)

	fmt.Fprintf(&b, "# HELP hermes_jobs_dlq_size Jobs in dlq.\n")
	fmt.Fprintf(&b, "# TYPE hermes_jobs_dlq_size gauge\n")
	fmt.Fprintf(&b, "hermes_jobs_dlq_size %d\n", s.DLQSize)

	return b.String()
}
