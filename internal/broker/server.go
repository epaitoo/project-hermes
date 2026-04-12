package broker

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/epaitoo/hermes/internal/models"
	"github.com/google/uuid"
)

type BrokerServer struct {
	queue *Queue
}

// POST /queues/{queueName}/jobs        - AddJob
// GET  /queues/{queueName}/jobs        - RequestJob
// PUT  /queues/{queueName}/jobs/{jobId} - UpdateJob

func NewBrokerServer() *BrokerServer {
	q := NewQueue()
	return &BrokerServer{
		queue: q,
	}
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

	bs.queue.AddJob(queueName, job)
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

func (bs *BrokerServer) Start(port string) error {
	http.HandleFunc("POST /queues/{queueName}/jobs", bs.AddJob)
	http.HandleFunc("GET /queues/{queueName}/jobs", bs.RequestJob)
	http.HandleFunc("PUT /queues/{queueName}/jobs/{jobId}", bs.UpdateJob)

	err := http.ListenAndServe(port, nil)

	if err != nil {
		return err
	}

	log.Printf("Server started on Port %s", port)

	return nil
}

func (bs *BrokerServer) StartLeaseCheck() {
	bs.queue.CheckForExpiredLeases()
}
