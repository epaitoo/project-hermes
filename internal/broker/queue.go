package broker

import (
	"errors"
	"sync"
	"time"

	"github.com/epaitoo/hermes/internal/models"
	"github.com/google/uuid"
)

type Queue struct {
	q  map[string][]models.Job
	mu sync.RWMutex
}

func NewQueue() *Queue {
	m := make(map[string][]models.Job)
	return &Queue{
		q: m,
	}
}

// Add Job
func (qu *Queue) AddJob(queueName string, job models.Job) {
	qu.mu.Lock()
	defer qu.mu.Unlock()
	qu.q[queueName] = append(qu.q[queueName], job)
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
		return j, errors.New("No Pending Jobs found in Queue")
	}

	return j, errors.New("No Pending Jobs found in Queue")
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
				if jobs[i].Status == models.StatusPending {
					jobs[i].Status = models.StatusInProgress
					now := time.Now()
					jobs[i].StartedAt = now
					// check if there's a LeaseDuration
					if jobs[i].LeaseDuration == 0 {
						jobs[i].LeaseDuration = 30 * time.Second // this is a sample time
					}
					jobs[i].LeaseExpiresAt = now.Add(jobs[i].LeaseDuration)

					return jobs[i], nil
				}
			}
		}

	}
	return j, errors.New("No Pending Jobs found in Queue")
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
			return j, errors.New("queue is empty")
		} else {
			for i := range jobs {
				if jobs[i].Id == job.Id {
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

	return j, errors.New("No Job found in Queue")
}

// method to check for expired leases
func (qu *Queue) CheckForExpiredLeases() {
	qu.mu.Lock()
	defer qu.mu.Unlock()

	for _, jobs := range qu.q {
		for i := range jobs {
			if time.Now().After(jobs[i].LeaseExpiresAt) && jobs[i].Status == models.StatusInProgress {
				// check for RetryCount
				if jobs[i].RetryCount >= jobs[i].MaxRetries {
					jobs[i].Status = models.StatusFailed
				} else {
					jobs[i].Status = models.StatusPending
					jobs[i].RetryCount++
					jobs[i].LeaseExpiresAt = time.Time{}
					jobs[i].StartedAt = time.Time{}
				}
			}
		}
	}
}
