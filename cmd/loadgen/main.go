// Command loadgen is demo tooling that continuously submits jobs to a running
// Hermes broker so the queue has visible depth during a crash-recovery demo.
//
// It POSTs jobs to the real submit endpoint (POST /queues/{queue}/jobs) at a
// configurable rate and logs each outcome. Ctrl-C stops it cleanly.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os/signal"
	"syscall"
	"time"
)

// submitBody is the subset of the broker's models.Job that a submitter needs to
// send. The broker fills in id/status/created_at itself, so we only provide the
// fields that describe the work. JSON tags match internal/models/job.go.
type submitBody struct {
	Name          string         `json:"name"`
	TaskType      string         `json:"task_type"`
	Payload       map[string]any `json:"payload"`
	LeaseDuration time.Duration  `json:"lease_duration"`
	MaxRetries    int            `json:"max_retries"`
}

func main() {
	rate := flag.Float64("rate", 5, "submit rate in jobs per second")
	brokerURL := flag.String("broker", "http://localhost:8080", "broker base URL")
	queue := flag.String("queue", "email_job", "target queue name")
	failRate := flag.Float64("failrate", 0, "fraction of jobs (0.0-1.0) to tag as poison so the worker fails them, exercising retry -> DLQ")
	flag.Parse()

	if *rate <= 0 {
		log.Fatalf("rate must be > 0, got %v", *rate)
	}
	if *failRate < 0 || *failRate > 1 {
		log.Fatalf("failrate must be between 0.0 and 1.0, got %v", *failRate)
	}

	submitURL := fmt.Sprintf("%s/queues/%s/jobs", *brokerURL, *queue)
	interval := time.Duration(float64(time.Second) / *rate)

	// Clean shutdown on Ctrl-C: cancel the context, which stops the loop.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client := &http.Client{Timeout: 5 * time.Second}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("loadgen: submitting to %s at %.2f jobs/sec, failrate=%.2f (Ctrl-C to stop)", submitURL, *rate, *failRate)

	var submitted, failed, poisoned int64
	var seq int64

	for {
		select {
		case <-ctx.Done():
			log.Printf("loadgen: shutting down — %d submitted (%d tagged-to-fail), %d send errors", submitted, poisoned, failed)
			return
		case <-ticker.C:
			seq++
			// Tag this job as poison for the demo's failrate fraction; the
			// broker's worker reads the tag and fails it -> retry -> DLQ.
			poison := *failRate > 0 && rand.Float64() < *failRate
			if err := submitJob(ctx, client, submitURL, seq, poison); err != nil {
				failed++
				log.Printf("job #%d FAILED to submit: %v", seq, err)
				continue
			}
			submitted++
			if poison {
				poisoned++
				log.Printf("job #%d submitted [POISON] (total ok=%d poison=%d send-err=%d)", seq, submitted, poisoned, failed)
			} else {
				log.Printf("job #%d submitted (total ok=%d poison=%d send-err=%d)", seq, submitted, poisoned, failed)
			}
		}
	}
}

func submitJob(ctx context.Context, client *http.Client, url string, seq int64, poison bool) error {
	payload := map[string]any{
		"to":      fmt.Sprintf("user%d@example.com", seq),
		"subject": "Hermes crash-recovery demo",
		"seq":     seq,
	}
	if poison {
		// The broker's demo worker fails any job carrying this tag.
		payload["_demo_outcome"] = "fail"
	}

	body := submitBody{
		Name:          fmt.Sprintf("demo-job-%d", seq),
		TaskType:      "email",
		Payload:       payload,
		LeaseDuration: 30 * time.Second,
		MaxRetries:    3,
	}

	buf, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("send: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("unexpected status %s", resp.Status)
	}
	return nil
}
