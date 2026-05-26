package broker

import (
	"testing"
	"time"

	"github.com/epaitoo/hermes/internal/models"
	"github.com/google/uuid"
)

func createJob() models.Job {
	return models.Job{
		Id:        uuid.New(),
		Name:      "test Job",
		Status:    models.StatusPending,
		CreatedAt: time.Now(),
	}
}

func TestAddJob(t *testing.T) {
	q := NewQueue()
	j := createJob()

	q.AddJob("email", j)
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
	q := NewQueue()
	j := createJob()
	q.AddJob("email", j)

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
	q := NewQueue()
	expectedErrMsg := "No Pending Jobs found in Queue"

	_, err := q.RequestJob("email")

	if err.Error() != expectedErrMsg {
		t.Errorf("got %v, wanted %v", err.Error(), expectedErrMsg)
	}
}

func TestUpdateJob(t *testing.T) {
	q := NewQueue()
	j := createJob()
	j.Id = uuid.New()
	q.AddJob("email", j)

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
		q := NewQueue()
		j := createJob()
		j.Id = uuid.New()
		j.Status = tt.input.jobStatus
		j.LeaseExpiresAt = tt.input.leaseExpiresAt
		j.RetryCount = tt.input.retryCount
		j.MaxRetries = tt.input.maxRetries

		t.Run(tt.name, func(t *testing.T) {
			q.AddJob("email", j)

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
