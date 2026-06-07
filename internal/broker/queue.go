package broker

import (
	"errors"
	"sync"
	"time"

	"github.com/epaitoo/hermes/internal/models"
	"github.com/google/uuid"
)

var ErrJobNotFound = errors.New("No Job found in Queue")
var ErrLeaseNotRenewable = errors.New("cannot renew lease not in progress")
var ErrEmptyQueue = errors.New("queue is empty")
var ErrJobNotInProgress = errors.New("job is not in progress")

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
	return j, ErrEmptyQueue
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

// method to check for expired leases
func (qu *Queue) CheckForExpiredLeases() {
	qu.mu.Lock()
	defer qu.mu.Unlock()

	for _, jobs := range qu.q {
		for i := range jobs {
			if time.Now().After(jobs[i].LeaseExpiresAt) && jobs[i].Status == models.StatusInProgress {
				applyFailOrRetry(&jobs[i])
			}
		}
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

func applyFailOrRetry(job *models.Job) {
	if job.RetryCount >= job.MaxRetries {
		job.Status = models.StatusFailed
	} else {
		job.Status = models.StatusPending
		job.RetryCount++
		job.LeaseExpiresAt = time.Time{}
		job.StartedAt = time.Time{}
	}
}

func (qu *Queue) FailOrRetry(jobID uuid.UUID) (*models.Job, error) {
	qu.mu.Lock()
	defer qu.mu.Unlock()
	// find the job
	var j *models.Job

	for _, jobs := range qu.q {
		for i := range jobs {
			if jobs[i].Id == jobID {
				if jobs[i].Status == models.StatusInProgress {
					applyFailOrRetry(&jobs[i])
					return &jobs[i], nil
				} else {
					return j, ErrJobNotInProgress
				}
			}
		}
	}

	return j, ErrJobNotFound
}
