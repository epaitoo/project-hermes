package worker

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/epaitoo/hermes/internal/models"
	"github.com/google/uuid"
)

// newTestJob builds a minimal job for exercising ProcessJob.
func newTestJob() models.Job {
	return models.Job{
		Id:       uuid.New(),
		Name:     "test-job",
		TaskType: "email_job",
		Status:   models.StatusInProgress,
	}
}

// TestProcessJob_Success verifies that when Process returns nil, the worker
// marks the job completed and sends a PUT update to the broker.
func TestProcessJob_Success(t *testing.T) {
	var gotUpdate bool

	// Fake broker: record the PUT update call.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			gotUpdate = true
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	process := func(j models.Job) error {
		return nil // pretend the work succeeded
	}

	// pollInterval is unused here (ProcessJob is called directly, not Start);
	// 0 falls back to the default in NewWorker.
	w := NewWorker(srv.URL, process, "email_job", 0)

	stopCh := make(chan struct{})
	w.ProcessJob(newTestJob(), stopCh) // blocks until the job finishes

	if !gotUpdate {
		t.Fatal("expected worker to PUT a job update to the broker")
	}
}

// TestProcessJob_LeaseLost verifies the worker abandons the job when a
// heartbeat comes back 409 Conflict (lease lost).
func TestProcessJob_LeaseLost(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Heartbeat endpoint returns 409 -> LeaseLostErr.
		w.WriteHeader(http.StatusConflict)
	}))
	defer srv.Close()

	// Slow process so the heartbeat ticker fires first.
	process := func(j models.Job) error {
		time.Sleep(30 * time.Second)
		return nil
	}

	// pollInterval is unused here (ProcessJob is called directly, not Start);
	// 0 falls back to the default in NewWorker.
	w := NewWorker(srv.URL, process, "email_job", 0)

	done := make(chan struct{})
	go func() {
		w.ProcessJob(newTestJob(), make(chan struct{}))
		close(done)
	}()

	select {
	case <-done:
		// good: worker returned after losing the lease
	case <-time.After(15 * time.Second):
		t.Fatal("ProcessJob did not abandon job after lease lost")
	}
}
