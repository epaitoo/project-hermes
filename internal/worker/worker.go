package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/epaitoo/hermes/internal/models"
	"github.com/google/uuid"
)

type WorkerState string

const (
	Idle       WorkerState = "idle"
	Busy       WorkerState = "busy"
	Terminated WorkerState = "terminated"
)

type Worker struct {
	Id             uuid.UUID
	State          WorkerState
	BrokerEndpoint string
	Process        func(models.Job) error
	JobType        string
	logger         *slog.Logger
}

func NewWorker(brokerEndpoint string, proccesFunc func(models.Job) error, jobType string) *Worker {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	return &Worker{
		Id:             uuid.New(),
		State:          Idle,
		BrokerEndpoint: brokerEndpoint,
		Process:        proccesFunc,
		JobType:        jobType,
		logger:         logger,
	}

}

func (w *Worker) Start(stopCh <-chan struct{}) {
	ticker := time.NewTicker(30 * time.Second)

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			func() {
				// make http GET Request here
				endpoint := fmt.Sprintf("%s/queues/%s/jobs", w.BrokerEndpoint, w.JobType)
				resp, err := http.Get(endpoint)
				if err != nil {
					// handle error
					w.logger.Error("error making HTTP request", "error", err)
					return
				}

				defer resp.Body.Close()

				if resp.StatusCode == http.StatusNotFound {
					w.logger.Info("No Job Found")
					return
				}

				var job models.Job

				// Decode the JSON response
				if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
					w.logger.Error("error reading response body", "error", err)
					return
				}

				// print for now
				w.logger.Info("job info", "job", job)

				//else -> Process and Update the broker
				err = w.Process(job)
				if err != nil {
					job.Status = models.StatusPending
					w.logger.Error("error processing job", "error", err)

					return
				}

				job.Status = models.StatusCompleted
				job.CompletedAt = time.Now()
				// update
				updateErr := w.updateJobRequest(job)

				if updateErr != nil {
					w.logger.Error("worker updateJobRequest error", "error", updateErr)
				}
			}()
		}
	}
}

func (w *Worker) updateJobRequest(job models.Job) error {
	updateEndpoint := fmt.Sprintf("%s/queues/%s/jobs/%s", w.BrokerEndpoint, w.JobType, job.Id)

	bodyBytes, err := json.Marshal(job)

	if err != nil {
		return fmt.Errorf("error marshalling bodybytes: %w", err)
	}

	updateBody := bytes.NewBuffer(bodyBytes)
	req, err := http.NewRequest(http.MethodPut, updateEndpoint, updateBody)

	if err != nil {
		return fmt.Errorf("error making http put request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	res, err := client.Do(req)

	if err != nil {
		return fmt.Errorf("error reading response body: %w", err)
	}

	defer res.Body.Close()

	if res.StatusCode == http.StatusBadRequest {
		return fmt.Errorf("bad request")
	}

	if res.StatusCode == http.StatusNotFound {
		return fmt.Errorf("job Id not found")
	}

	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("error %w", err)
	}

	// print the updated job
	w.logger.Info("status", "code", res.Status)
	w.logger.Info("msg", "response", string(resBody))

	return nil
}
