package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/epaitoo/hermes/internal/metrics"
	"github.com/epaitoo/hermes/internal/models"
	"github.com/epaitoo/hermes/internal/wal"
	"github.com/google/uuid"
)

var ErrJobNotFound = errors.New("No Job found in Queue")
var ErrLeaseNotRenewable = errors.New("cannot renew lease not in progress")
var ErrEmptyQueue = errors.New("queue is empty")
var ErrJobNotInProgress = errors.New("job is not in progress")
var ErrNoJobAvailable = errors.New("No Jobs Available")

type Queue struct {
	q       map[string][]models.Job
	dlq     map[string][]models.Job
	mu      sync.RWMutex
	wal     *wal.WAL
	metrics *metrics.Metrics
}

func NewQueue(w *wal.WAL, m *metrics.Metrics) *Queue {
	q := make(map[string][]models.Job)
	dq := make(map[string][]models.Job)
	return &Queue{
		q:       q,
		dlq:     dq,
		wal:     w,
		metrics: m,
	}
}

// Add Job
func (qu *Queue) AddJob(queueName string, job models.Job) error {
	qu.mu.Lock()
	defer qu.mu.Unlock()

	rec, err := wal.NewRecord(wal.RecordCreated, wal.JobCreatedPayload{QueueName: queueName, Job: job})
	if err != nil {
		return err
	}

	err = qu.wal.Append(rec)
	if err != nil {
		return err
	}

	qu.metrics.JobSubmitted()

	qu.q[queueName] = append(qu.q[queueName], job)

	return nil
}

// read a job without modifying it
func (qu *Queue) ReadJobById(queueName string, jobId uuid.UUID) (models.Job, error) {
	qu.mu.RLock()
	defer qu.mu.RUnlock()

	jobs, ok := qu.q[queueName]
	var j models.Job
	if ok {
		for _, job := range jobs {
			if job.Id == jobId {
				return job, nil
			}
		}
	} else {
		return j, ErrEmptyQueue
	}

	return j, ErrEmptyQueue
}

// Request Job
func (qu *Queue) RequestJob(queueName string) (models.Job, error) {
	qu.mu.Lock()
	defer qu.mu.Unlock()
	jobs, ok := qu.q[queueName]
	var j models.Job
	if ok {
		if len(jobs) > 0 {
			for i := range jobs {
				if jobs[i].Status == models.StatusPending && time.Now().After(jobs[i].NextRunAt) {
					jobs[i].Status = models.StatusInProgress
					now := time.Now()
					jobs[i].StartedAt = now
					// check if there's a LeaseDuration
					if jobs[i].LeaseDuration == 0 {
						jobs[i].LeaseDuration = 30 * time.Second // this is a sample time
					}
					jobs[i].LeaseExpiresAt = now.Add(jobs[i].LeaseDuration)

					qu.metrics.JobLeased()

					return jobs[i], nil
				}
			}
		}

	}
	return j, ErrNoJobAvailable
}

// update job
func (qu *Queue) UpdateJob(queueName string, job models.Job) (models.Job, error) {
	qu.mu.Lock()
	defer qu.mu.Unlock()
	// find the job
	jobs, ok := qu.q[queueName]
	var j models.Job
	if ok {
		if len(jobs) == 0 {
			return j, ErrEmptyQueue
		} else {
			for i := range jobs {
				if jobs[i].Id == job.Id {
					// log jobs which are completed to WAL
					if job.Status == models.StatusCompleted {
						rec, err := wal.NewRecord(wal.RecordDone, wal.JobDonePayload{JobID: job.Id, QueueName: queueName})

						if err != nil {
							return j, err
						}

						err = qu.wal.Append(rec)
						if err != nil {
							return j, err
						}

						qu.metrics.JobCompleted()
					}

					// update the job
					jobs[i].Status = job.Status
					jobs[i].CompletedAt = job.CompletedAt
					jobs[i].RetryCount = job.RetryCount
					return jobs[i], nil
				}
			}
			return j, errors.New("Job with ID: " + job.Id.String() + " not found in Queue")
		}
	}

	return j, ErrJobNotFound
}

func applyFailOrRetry(job *models.Job) bool {
	if job.RetryCount >= job.MaxRetries {
		job.Status = models.StatusFailed
		return true
	} else {

		shift := min(job.RetryCount, 20)
		delay := time.Duration(1<<shift) * time.Second
		const maxDelay = 3 * time.Minute

		delay = min(delay, maxDelay)

		jitter := time.Duration(rand.Float64() * float64(delay) * 0.5)
		backoff := delay + jitter

		job.Status = models.StatusPending
		job.NextRunAt = time.Now().Add(backoff)
		job.RetryCount++
		job.LeaseExpiresAt = time.Time{}
		job.StartedAt = time.Time{}
		return false
	}
}

// method to check for expired leases
func (qu *Queue) CheckForExpiredLeases() {
	qu.mu.Lock()
	defer qu.mu.Unlock()

	for k, jobs := range qu.q {
		survivors := make([]models.Job, 0, len(jobs))
		for i := range jobs {
			if time.Now().After(jobs[i].LeaseExpiresAt) && jobs[i].Status == models.StatusInProgress {
				updated := jobs[i]
				dead := applyFailOrRetry(&updated)

				if dead {
					rec, err := wal.NewRecord(wal.RecordMovedToDLQ, wal.JobMovedToDLQPayload{JobID: updated.Id, QueueName: k})

					if err != nil {
						slog.Error("failed to create DLQ record",
							"error", err,
							"job_id", jobs[i].Id,
							"queue", k,
							"attempts", jobs[i].RetryCount)
						survivors = append(survivors, jobs[i])
						continue
					}

					if err := qu.wal.Append(rec); err != nil {
						slog.Error("failed to persist DLQ record to WAL",
							"error", err,
							"job_id", jobs[i].Id,
							"queue", k,
							"attempts", jobs[i].RetryCount)
						survivors = append(survivors, jobs[i])
						continue
					}

					qu.metrics.JobDeadLettered()

					jobs[i] = updated
					qu.dlq[k] = append(qu.dlq[k], jobs[i])
				} else {
					rec, err := wal.NewRecord(wal.RecordFailed, wal.JobFailedPayload{
						JobID:      updated.Id,
						RetryCount: updated.RetryCount,
						NextRunAt:  updated.NextRunAt,
					})

					if err != nil {
						slog.Error("failed to create lease expiry record",
							"error", err,
							"job_id", jobs[i].Id,
							"queue", k,
							"attempts", jobs[i].RetryCount)
						survivors = append(survivors, jobs[i])
						continue
					}

					if err := qu.wal.Append(rec); err != nil {
						slog.Error("failed to persist lease expiry to WAL",
							"error", err,
							"job_id", jobs[i].Id,
							"queue", k,
							"attempts", jobs[i].RetryCount)
						survivors = append(survivors, jobs[i])
						continue
					}

					qu.metrics.JobRetried()

					jobs[i] = updated
					survivors = append(survivors, jobs[i])
				}
			} else {
				survivors = append(survivors, jobs[i])
			}
		}

		qu.q[k] = survivors
	}
}

// lease renewal method
func (qu *Queue) LeaseRenewal(queueName string, jobID uuid.UUID) (models.Job, error) {
	qu.mu.Lock()
	defer qu.mu.Unlock()
	// find the job
	jobs, ok := qu.q[queueName]
	var j models.Job

	if ok {
		if len(jobs) == 0 {
			return j, ErrEmptyQueue
		} else {
			for i := range jobs {
				if jobs[i].Id == jobID {
					if jobs[i].Status == models.StatusInProgress {
						jobs[i].LeaseExpiresAt = time.Now().Add(jobs[i].LeaseDuration)
						return jobs[i], nil
					} else {
						return j, ErrLeaseNotRenewable

					}
				}
			}
			return j, errors.New("Job with ID: " + jobID.String() + " not found in Queue")
		}
	}

	return j, ErrJobNotFound

}

func (qu *Queue) FailOrRetry(jobID uuid.UUID) (*models.Job, error) {
	qu.mu.Lock()
	defer qu.mu.Unlock()

	for k, jobs := range qu.q {
		for i := range jobs {
			if jobs[i].Id == jobID {
				if jobs[i].Status != models.StatusInProgress {
					return nil, ErrJobNotInProgress
				}
				updated := jobs[i]
				dead := applyFailOrRetry(&updated)

				if dead {
					rec, err := wal.NewRecord(wal.RecordMovedToDLQ, wal.JobMovedToDLQPayload{JobID: jobID, QueueName: k})
					if err != nil {
						return nil, err
					}

					//Append to WAL
					err = qu.wal.Append(rec)
					if err != nil {
						return nil, err
					}

					qu.metrics.JobDeadLettered()

					deadJob := updated

					qu.q[k] = slices.Delete(jobs, i, i+1)
					qu.dlq[k] = append(qu.dlq[k], deadJob)

					return &deadJob, nil

				}

				rec, err := wal.NewRecord(wal.RecordFailed, wal.JobFailedPayload{
					JobID:      jobID,
					RetryCount: updated.RetryCount,
					NextRunAt:  updated.NextRunAt,
				})

				if err != nil {
					return nil, err
				}

				err = qu.wal.Append(rec)
				if err != nil {
					return nil, err
				}

				qu.metrics.JobRetried()

				jobs[i] = updated

				return &jobs[i], nil

			}
		}
	}

	return nil, ErrJobNotFound
}

// dlq Operations
func (qu *Queue) ListDeadLetter(queueName string) ([]models.Job, error) {
	qu.mu.RLock()
	defer qu.mu.RUnlock()

	jobs := qu.dlq[queueName]
	out := make([]models.Job, len(jobs))
	copy(out, jobs)
	return out, nil
}

func (qu *Queue) RedriveJob(queueName string, jobId uuid.UUID) (models.Job, error) {
	qu.mu.Lock()
	defer qu.mu.Unlock()

	var j models.Job

	// find the job
	dlqJobs := qu.dlq[queueName]

	for i := range dlqJobs {
		if dlqJobs[i].Id == jobId {

			rec, err := wal.NewRecord(wal.RecordRedrive, wal.JobRedrivePayload{JobID: jobId, QueueName: queueName})

			if err != nil {
				return j, err
			}

			if err := qu.wal.Append(rec); err != nil {
				return j, err
			}

			qu.metrics.DLQRedriven()

			dlqJobs[i].Status = models.StatusPending
			dlqJobs[i].RetryCount = 0
			dlqJobs[i].NextRunAt = time.Time{}

			redriveJob := dlqJobs[i]
			qu.q[queueName] = append(qu.q[queueName], redriveJob)
			qu.dlq[queueName] = slices.Delete(dlqJobs, i, i+1)
			return redriveJob, nil
		}
	}

	return j, ErrJobNotFound
}

func (qu *Queue) DiscardDeadJob(queueName string, jobId uuid.UUID) error {
	qu.mu.Lock()
	defer qu.mu.Unlock()
	// find the job
	dlqJobs := qu.dlq[queueName]

	for i := range dlqJobs {
		if dlqJobs[i].Id == jobId {
			rec, err := wal.NewRecord(wal.RecordDiscard, wal.JobDiscardPayload{JobID: jobId, QueueName: queueName})

			if err != nil {
				return err
			}

			err = qu.wal.Append(rec)

			if err != nil {
				return err
			}

			qu.metrics.DLQDiscarded()

			qu.dlq[queueName] = slices.Delete(dlqJobs, i, i+1)
			return nil
		}
	}

	return ErrJobNotFound
}

func (qu *Queue) findJobByID(jobID uuid.UUID) (string, int, bool) {
	for queueName, jobs := range qu.q {
		for i := range jobs {
			if jobs[i].Id == jobID {
				return queueName, i, true
			}
		}
	}

	return "", 0, false
}

func (qu *Queue) Recover() error {
	qu.mu.Lock()
	defer qu.mu.Unlock()

	records, err := qu.wal.Replay()

	if err != nil {
		return err
	}

	for _, rec := range records {
		switch rec.Type {

		case wal.RecordCreated:
			var p wal.JobCreatedPayload
			if err := json.Unmarshal(rec.Payload, &p); err != nil {
				return err
			}

			qu.q[p.QueueName] = append(qu.q[p.QueueName], p.Job)

		case wal.RecordFailed:
			var p wal.JobFailedPayload
			if err := json.Unmarshal(rec.Payload, &p); err != nil {
				return err
			}

			queueName, idx, ok := qu.findJobByID(p.JobID)
			if !ok {
				return fmt.Errorf("recover: job %s from RecordFailed not found", p.JobID)
			}
			qu.q[queueName][idx].RetryCount = p.RetryCount
			qu.q[queueName][idx].NextRunAt = p.NextRunAt
			qu.q[queueName][idx].Status = models.StatusPending

		case wal.RecordDone:
			var p wal.JobDonePayload
			if err := json.Unmarshal(rec.Payload, &p); err != nil {
				return err
			}

			qJobs, ok := qu.q[p.QueueName]

			if !ok {
				return fmt.Errorf("recover: queue %s missing for RecordDone", p.QueueName)
			}

			for i := range qJobs {
				if qJobs[i].Id == p.JobID {
					qu.q[p.QueueName] = slices.Delete(qJobs, i, i+1)
					break
				}
			}

		case wal.RecordMovedToDLQ:
			var p wal.JobMovedToDLQPayload
			if err := json.Unmarshal(rec.Payload, &p); err != nil {
				return err
			}

			qJobs, ok := qu.q[p.QueueName]

			if !ok {
				return fmt.Errorf("recover: queue %s missing for RecordMovedToDLQ", p.QueueName)
			}

			for i := range qJobs {
				if qJobs[i].Id == p.JobID {
					job := qJobs[i]
					qu.q[p.QueueName] = slices.Delete(qJobs, i, i+1)
					qu.dlq[p.QueueName] = append(qu.dlq[p.QueueName], job)
					break
				}
			}

		case wal.RecordRedrive:
			var p wal.JobRedrivePayload

			if err := json.Unmarshal(rec.Payload, &p); err != nil {
				return err
			}

			dlqJobs, ok := qu.dlq[p.QueueName]

			if !ok {
				return fmt.Errorf("recover: queue %s missing for RecordRedrive", p.QueueName)
			}

			for i := range dlqJobs {
				if dlqJobs[i].Id == p.JobID {
					job := dlqJobs[i]
					qu.q[p.QueueName] = append(qu.q[p.QueueName], job)
					qu.dlq[p.QueueName] = slices.Delete(dlqJobs, i, i+1)
					break
				}
			}

		case wal.RecordDiscard:
			var p wal.JobDiscardPayload
			if err := json.Unmarshal(rec.Payload, &p); err != nil {
				return err
			}

			dlqJobs, ok := qu.dlq[p.QueueName]

			if !ok {
				return fmt.Errorf("recover: queue %s missing for RecordDiscard", p.QueueName)
			}

			for i := range dlqJobs {
				if dlqJobs[i].Id == p.JobID {
					qu.dlq[p.QueueName] = slices.Delete(dlqJobs, i, i+1)
					break
				}
			}

		}

	}

	// reset loop
	for queueName, jobs := range qu.q {
		for i := range jobs {
			if jobs[i].Status == models.StatusInProgress {
				qu.q[queueName][i].Status = models.StatusPending
				qu.q[queueName][i].LeaseExpiresAt = time.Time{}
				qu.q[queueName][i].StartedAt = time.Time{}
			}
		}
	}

	// after the reset loop, every job in qu.q is pending
	var pending int64
	for _, jobs := range qu.q {
		pending += int64(len(jobs))
	}
	qu.metrics.SetPending(pending)

	var dlq int64
	for _, jobs := range qu.dlq {
		dlq += int64(len(jobs))
	}
	qu.metrics.SetDLQSize(dlq)

	return nil
}
