package broker

import (
	"errors"
	"sync"
	"time"
)

type Queue struct {
	q  map[string][]Job
	mu sync.RWMutex
}

func NewQueue() *Queue {
	m := make(map[string][]Job)
	return &Queue{
		q: m,
	}
}

// Add Job
func (qu *Queue) AddJob(queueName string, job Job) {
	qu.mu.Lock()
	defer qu.mu.Unlock()
	qu.q[queueName] = append(qu.q[queueName], job)
}

// Request Job
func (qu *Queue) RequestJob(queueName string) (Job, error) {
	qu.mu.Lock()
	defer qu.mu.Unlock()
	jobs, ok := qu.q[queueName]
	var j Job
	if ok {
		if len(jobs) > 0 {
			for i := range jobs {
				if jobs[i].Status == StatusPending {
					jobs[i].Status = StatusInProgress
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
func (qu *Queue) UpdateJob(queueName string, job Job) (Job, error) {
	qu.mu.Lock()
	defer qu.mu.Unlock()
	// find the job
	jobs, ok := qu.q[queueName]
	var j Job
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
