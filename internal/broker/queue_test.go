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
