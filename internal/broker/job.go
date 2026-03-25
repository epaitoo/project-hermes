package broker

import (
	"time"

	"github.com/google/uuid"
)

type JobStatus string

const (
	StatusPending    JobStatus = "pending"
	StatusInProgress JobStatus = "in_progress"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
)

type Job struct {
	Id             uuid.UUID      `json:"id"`
	Name           string         `json:"name"`
	TaskType       string         `json:"task_type"`
	Payload        map[string]any `json:"payload"`
	Status         JobStatus      `json:"status"`
	CreatedAt      time.Time      `json:"created_at"`
	StartedAt      time.Time      `json:"started_at"`
	CompletedAt    time.Time      `json:"completed_at"`
	LeaseDuration  time.Duration  `json:"lease_duration"`
	LeaseExpiresAt time.Time      `json:"lease_expires_at"`
	MaxRetries     int            `json:"max_retries"`
	RetryCount     int            `json:"retry_count"`
}
