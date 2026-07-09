package broker

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"log/slog"
	"net/http"
	"time"

	"github.com/epaitoo/hermes/internal/metrics"
	"github.com/epaitoo/hermes/internal/models"
	"github.com/epaitoo/hermes/internal/wal"
	"github.com/google/uuid"
)

type BrokerServer struct {
	queue   *Queue
	metrics *metrics.Metrics
}

// POST /queues/{queueName}/jobs        - AddJob
// GET  /queues/{queueName}/jobs        - RequestJob
// PUT  /queues/{queueName}/jobs/{jobId} - UpdateJob

func NewBrokerServer(walPath string) (*BrokerServer, error) {
	w, err := wal.Open(walPath)

	if err != nil {
		return nil, err
	}

	m := &metrics.Metrics{}

	q := NewQueue(w, m)

	if err := q.Recover(); err != nil {
		return nil, err
	}

	return &BrokerServer{queue: q, metrics: m}, nil
}

func (bs *BrokerServer) AddJob(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("queueName")
	var job models.Job

	err := json.NewDecoder(r.Body).Decode(&job)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	newUUID := uuid.New()
	job.Id = newUUID
	job.Status = models.StatusPending
	job.CreatedAt = time.Now()

	err = bs.queue.AddJob(queueName, job)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(job)
}

func (bs *BrokerServer) RequestJob(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("queueName")

	job, err := bs.queue.RequestJob(queueName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(job)

}

func (bs *BrokerServer) UpdateJob(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("queueName")
	jobId := r.PathValue("jobId")

	var j models.Job

	err := json.NewDecoder(r.Body).Decode(&j)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if jobId != j.Id.String() {
		http.Error(w, "Job ID in URL does not match job ID in body", http.StatusBadRequest)
		return
	}

	job, err := bs.queue.UpdateJob(queueName, j)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(job)

}

func (bs *BrokerServer) LeaseRenewal(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("queueName")
	jobId := r.PathValue("jobId")

	var j models.Job

	err := json.NewDecoder(r.Body).Decode(&j)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	id, err := uuid.Parse(jobId)
	if err != nil {
		http.Error(w, "uuid could not parse id", http.StatusBadRequest)
		return
	}

	if id != j.Id {
		http.Error(w, "Job ID in URL does not match job ID in body", http.StatusNotFound)
		return
	}

	job, err := bs.queue.LeaseRenewal(queueName, j.Id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(job)
}

func (bs *BrokerServer) FailOrRetryHandler(w http.ResponseWriter, r *http.Request) {
	jobId := r.PathValue("jobId")

	var j models.Job

	err := json.NewDecoder(r.Body).Decode(&j)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	id, err := uuid.Parse(jobId)
	if err != nil {
		http.Error(w, "uuid could not parse id", http.StatusBadRequest)
		return
	}

	if id != j.Id {
		http.Error(w, "Job ID in URL does not match job ID in body", http.StatusNotFound)
		return
	}

	job, err := bs.queue.FailOrRetry(id)

	if err != nil {
		if errors.Is(err, ErrJobNotInProgress) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		} else {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(job)

}

// dlq
func (bs *BrokerServer) ListDeadLetterHandler(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("queueName")

	job, _ := bs.queue.ListDeadLetter(queueName)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(job)
}

func (bs *BrokerServer) RedriveJobHandler(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("queueName")
	jobId := r.PathValue("jobId")

	id, err := uuid.Parse(jobId)
	if err != nil {
		http.Error(w, "uuid could not parse id", http.StatusBadRequest)
		return
	}

	job, err := bs.queue.RedriveJob(queueName, id)
	if err != nil {
		if errors.Is(err, ErrJobNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(job)
}

func (bs *BrokerServer) DiscardDeadJobHandler(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("queueName")
	jobId := r.PathValue("jobId")

	id, err := uuid.Parse(jobId)
	if err != nil {
		http.Error(w, "uuid could not parse id", http.StatusBadRequest)
		return
	}

	err = bs.queue.DiscardDeadJob(queueName, id)
	if err != nil {
		if errors.Is(err, ErrJobNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNoContent)

}

func (bs *BrokerServer) Start(port string) error {
	http.HandleFunc("POST /queues/{queueName}/jobs", bs.AddJob)
	http.HandleFunc("GET /queues/{queueName}/jobs", bs.RequestJob)
	http.HandleFunc("PUT /queues/{queueName}/jobs/{jobId}", bs.UpdateJob)
	http.HandleFunc("POST /queues/{queueName}/jobs/{jobId}/heartbeat", bs.LeaseRenewal)

	http.HandleFunc("GET /queues/{queueName}/dlq", bs.ListDeadLetterHandler)
	http.HandleFunc("POST /queues/{queueName}/dlq/{jobId}/redrive", bs.RedriveJobHandler)
	http.HandleFunc("DELETE /queues/{queueName}/dlq/{jobId}", bs.DiscardDeadJobHandler)

	http.HandleFunc("POST /jobs/{jobId}/fail", bs.FailOrRetryHandler)
	http.HandleFunc("GET /metrics", bs.MetricsHandler)

	err := http.ListenAndServe(port, nil)

	if err != nil {
		return err
	}

	log.Printf("Server started on Port %s", port)

	return nil
}

func (bs *BrokerServer) MetricsHandler(w http.ResponseWriter, r *http.Request) {
	snap := bs.metrics.Snapshot()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.WriteHeader(http.StatusOK)

	if _, err := io.WriteString(w, snap.Prometheus()); err != nil {
		slog.Error("failed writing metrics response", "error", err)
	}
}

func (bs *BrokerServer) StartLeaseChecker() {
	bs.queue.CheckForExpiredLeases()
}

func (bs *BrokerServer) Close() error {
	return bs.queue.wal.Close()
}
