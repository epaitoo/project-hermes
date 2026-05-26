package worker

import (
	"github.com/epaitoo/hermes/internal/models"
	"github.com/google/uuid"
)

type WorkerPool struct {
	workers        map[uuid.UUID]*Worker
	stopCh         chan struct{}
	workerCount    int
	brokerEndpoint string
	jobType        string
	process        func(models.Job) error
}

func NewWorkerPool(workerCount int, brokerEndpoint string, jobType string, processFunc func(models.Job) error) *WorkerPool {
	w := make(map[uuid.UUID]*Worker)
	stopCh := make(chan struct{})
	return &WorkerPool{
		workers:        w,
		stopCh:         stopCh,
		workerCount:    workerCount,
		brokerEndpoint: brokerEndpoint,
		jobType:        jobType,
		process:        processFunc,
	}
}

func (w *WorkerPool) StartWorkerPool() {
	for i := 0; i < w.workerCount; i++ {
		worker := NewWorker(w.brokerEndpoint, w.process, w.jobType)

		go worker.Start(w.stopCh)

		w.workers[worker.Id] = worker
	}
}

func (w *WorkerPool) StopWorkerPool() {
	close(w.stopCh)
}
