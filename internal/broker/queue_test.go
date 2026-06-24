package broker

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/epaitoo/hermes/internal/models"
	"github.com/epaitoo/hermes/internal/wal"
	"github.com/google/uuid"
)

func newTestQueue(t *testing.T) *Queue {
	t.Helper()
	w, err := wal.Open(filepath.Join(t.TempDir(), "test.wal"))

	if err != nil {
		t.Fatalf("opening test wal: %v", err)
	}
	return NewQueue(w)
}

func createJob() models.Job {
	return models.Job{
		Id:        uuid.New(),
		Name:      "test Job",
		Status:    models.StatusPending,
		CreatedAt: time.Now(),
	}
}

func TestAddJob(t *testing.T) {
	q := newTestQueue(t)
	j := createJob()

	if err := q.AddJob("email", j); err != nil {
		t.Fatalf("AddJob returned unexpected error: %v", err)
	}

	jobs, _ := q.q["email"]
	got := len(jobs)
	want := 1

	if got != want {
		t.Errorf("got %d, wanted %d", got, want)
	}

	if jobs[0].Id.String() != j.Id.String() {
		t.Errorf("got %q, wanted %q", jobs[0].Id.String(), j.Id.String())
	}

	if jobs[0].Name != j.Name {
		t.Errorf("got %q, wanted %q", jobs[0].Name, j.Name)
	}

	if jobs[0].Status != j.Status {
		t.Errorf("got %v, wanted %v", jobs[0].Status, j.Status)
	}

	if jobs[0].CreatedAt != j.CreatedAt {
		t.Errorf("got %v, wanted %v", jobs[0].CreatedAt, j.CreatedAt)
	}
}

func TestRequestJob(t *testing.T) {
	q := newTestQueue(t)
	j := createJob()
	if err := q.AddJob("email", j); err != nil {
		t.Fatalf("AddJob returned unexpected error: %v", err)
	}

	res, err := q.RequestJob("email")

	if res.Status != models.StatusInProgress {
		t.Errorf("got %v, wanted %v", res.Status, models.StatusInProgress)
	}

	if res.StartedAt.IsZero() {
		t.Errorf("got %v, wanted %v", res.StartedAt, time.Now())
	}

	if res.LeaseExpiresAt != res.StartedAt.Add(res.LeaseDuration) {
		t.Errorf("got %v, wanted %v", res.LeaseExpiresAt, res.StartedAt.Add(res.LeaseDuration))
	}

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRequestJobEmptyQueue(t *testing.T) {
	q := newTestQueue(t)
	expectedErrMsg := ErrEmptyQueue.Error()

	_, err := q.RequestJob("email")

	if err.Error() != expectedErrMsg {
		t.Errorf("got %v, wanted %v", err.Error(), expectedErrMsg)
	}
}

func TestUpdateJob(t *testing.T) {
	q := newTestQueue(t)
	j := createJob()
	j.Id = uuid.New()
	if err := q.AddJob("email", j); err != nil {
		t.Fatalf("AddJob returned unexpected error: %v", err)
	}

	res, _ := q.RequestJob("email")
	res.Status = models.StatusCompleted
	res.CompletedAt = time.Now()
	res.RetryCount = 1

	job, err := q.UpdateJob("email", res)

	if job.Id != res.Id {
		t.Errorf("got %v, wanted %v", job.Id, res.Id)
	}

	if job.Status != res.Status {
		t.Errorf("got %v, wanted %v", job.Status, res.Status)
	}

	if job.CompletedAt != res.CompletedAt {
		t.Errorf("got %v, wanted %v", job.CompletedAt, res.CompletedAt)
	}

	if job.RetryCount != res.RetryCount {
		t.Errorf("got %v, wanted %v", job.RetryCount, res.RetryCount)
	}

	if job.Name != j.Name {
		t.Errorf("Name was overwritten: got %v, wanted %v", job.Name, j.Name)
	}

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

}

func TestCheckForExpiredLeases(t *testing.T) {

	type expectedOutcomes struct {
		jobStatus      models.JobStatus
		retryCount     int
		leaseExpiresAt time.Time
		startedAt      time.Time
	}

	type jobInputs struct {
		jobStatus              models.JobStatus
		leaseExpiresAt         time.Time
		retryCount, maxRetries int
	}

	expiredLease := time.Now().Add(-1 * time.Hour)
	futureLease := time.Now().Add(1 * time.Hour)

	tests := []struct {
		name     string
		input    jobInputs
		expected expectedOutcomes
	}{
		{"StatusInProgress_and_lease expired",
			jobInputs{jobStatus: models.StatusInProgress, leaseExpiresAt: expiredLease, retryCount: 3, maxRetries: 3},
			expectedOutcomes{jobStatus: models.StatusFailed, retryCount: 3, leaseExpiresAt: expiredLease}},

		{"RetryCount_<_MaxRetries", jobInputs{jobStatus: models.StatusInProgress, leaseExpiresAt: expiredLease, retryCount: 0, maxRetries: 4},
			expectedOutcomes{jobStatus: models.StatusPending, retryCount: 1, leaseExpiresAt: time.Time{}, startedAt: time.Time{}}},

		{"Job_is_StatusInProgress_+_lease_not_expired", jobInputs{jobStatus: models.StatusInProgress, leaseExpiresAt: futureLease},
			expectedOutcomes{jobStatus: models.StatusInProgress, leaseExpiresAt: futureLease}},

		{"Job_is_StatusPending_+_lease_expired", jobInputs{jobStatus: models.StatusPending}, expectedOutcomes{jobStatus: models.StatusPending}},

		{"Job_is_StatusCompleted_+_lease_expired", jobInputs{jobStatus: models.StatusCompleted, leaseExpiresAt: expiredLease},
			expectedOutcomes{jobStatus: models.StatusCompleted, leaseExpiresAt: expiredLease}},
	}

	for _, tt := range tests {
		q := newTestQueue(t)
		j := createJob()
		j.Id = uuid.New()
		j.Status = tt.input.jobStatus
		j.LeaseExpiresAt = tt.input.leaseExpiresAt
		j.RetryCount = tt.input.retryCount
		j.MaxRetries = tt.input.maxRetries

		t.Run(tt.name, func(t *testing.T) {
			if err := q.AddJob("email", j); err != nil {
				t.Fatalf("AddJob returned unexpected error: %v", err)
			}

			q.CheckForExpiredLeases()
			res, _ := q.ReadJobById("email", j.Id)

			if res.Status != tt.expected.jobStatus {
				t.Errorf("got %v, wanted %v", tt.expected.jobStatus, res.Status)
			}

			if res.RetryCount != tt.expected.retryCount {
				t.Errorf("got %v, wanted %v", tt.expected.retryCount, res.RetryCount)
			}

			if res.LeaseExpiresAt != tt.expected.leaseExpiresAt {
				t.Errorf("got %v, wanted %v", tt.expected.leaseExpiresAt, res.LeaseExpiresAt)
			}

			if res.StartedAt != tt.expected.startedAt {
				t.Errorf("got %v, wanted %v", tt.expected.startedAt, res.StartedAt)
			}

		})
	}
}

func TestLeaseRenewal(t *testing.T) {
	const queueName = "email"

	tests := []struct {
		name        string
		setup       func(t *testing.T) (*Queue, uuid.UUID)
		wantErr     bool
		errContains string
	}{
		{
			name: "renews an in-progress job",
			setup: func(t *testing.T) (*Queue, uuid.UUID) {
				q := newTestQueue(t)
				j := createJob()
				j.Id = uuid.New()
				j.Status = models.StatusInProgress
				j.LeaseDuration = 30 * time.Second
				j.LeaseExpiresAt = time.Now().Add(-time.Hour)
				if err := q.AddJob(queueName, j); err != nil {
					t.Fatalf("AddJob returned unexpected error: %v", err)
				}
				return q, j.Id
			},
		},
		{
			name: "unknown job id",
			setup: func(t *testing.T) (*Queue, uuid.UUID) {
				q := newTestQueue(t)
				j := createJob()
				j.Id = uuid.New()
				if err := q.AddJob(queueName, j); err != nil {
					t.Fatalf("AddJob returned unexpected error: %v", err)
				}
				return q, uuid.New()
			},
			wantErr:     true,
			errContains: "not found",
		},
		{
			name: "job not in progress",
			setup: func(t *testing.T) (*Queue, uuid.UUID) {
				q := newTestQueue(t)
				j := createJob()
				j.Id = uuid.New()
				j.Status = models.StatusFailed
				if err := q.AddJob(queueName, j); err != nil {
					t.Fatalf("AddJob returned unexpected error: %v", err)
				}
				return q, j.Id
			},
			wantErr:     true,
			errContains: "not in progress",
		},
		{
			name: "renews target in a multi-job queue",
			setup: func(t *testing.T) (*Queue, uuid.UUID) {
				q := newTestQueue(t)

				for i := 0; i < 2; i++ {
					other := createJob()
					other.Id = uuid.New()
					if err := q.AddJob(queueName, other); err != nil {
						t.Fatalf("AddJob returned unexpected error: %v", err)
					}

				}

				target := createJob()
				target.Id = uuid.New()
				target.Status = models.StatusInProgress
				target.LeaseDuration = 30 * time.Second
				target.LeaseExpiresAt = time.Now().Add(-time.Hour)
				if err := q.AddJob(queueName, target); err != nil {
					t.Fatalf("AddJob returned unexpected error: %v", err)
				}

				return q, target.Id
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, id := tt.setup(t)

			before := time.Now()

			_, err := q.LeaseRenewal(queueName, id)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Fatalf("error %q does not contain %q", err.Error(), tt.errContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			res, err := q.ReadJobById(queueName, id)
			if err != nil {
				t.Fatalf("could not re-read job: %v", err)
			}
			if !res.LeaseExpiresAt.After(before) {
				t.Errorf("lease not renewed: expiry %v is not after %v", res.LeaseExpiresAt, before)
			}
		})
	}
}
