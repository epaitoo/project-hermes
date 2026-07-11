package main

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/epaitoo/hermes/internal/broker"
	"github.com/epaitoo/hermes/internal/models"
	"github.com/epaitoo/hermes/internal/worker"
	"github.com/joho/godotenv"
)

func startLeaseChecker(stopCh <-chan struct{}, bs *broker.BrokerServer) {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			bs.StartLeaseChecker()
		}
	}
}

func main() {
	stopCh := make(chan struct{})
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	if err := godotenv.Load(); err != nil {
		slog.Info("no .env file loaded; using process environment", "error", err)
	}

	val := os.Getenv("HERMES_WAL_PATH")
	slog.Info("wal path resolved", "path", val)

	if val == "" {
		val = "hermes.wal"
	}

	// Worker poll cadence. Defaults to 0 (NewWorker falls back to 30s); the demo
	// sets HERMES_WORKER_POLL_INTERVAL low so the queue drains visibly on camera.
	var pollInterval time.Duration
	if v := os.Getenv("HERMES_WORKER_POLL_INTERVAL"); v != "" {
		d, perr := time.ParseDuration(v)
		if perr != nil {
			log.Fatalf("invalid HERMES_WORKER_POLL_INTERVAL %q: %v", v, perr)
		}
		pollInterval = d
	}
	slog.Info("worker poll interval resolved", "interval", pollInterval)

	// Simulated work duration. Defaults to 0 (instant, real behavior). The demo
	// sets HERMES_DEMO_WORK_DURATION so jobs stay leased long enough for the
	// leased gauge to register a nonzero value between scrapes.
	var workDuration time.Duration
	if v := os.Getenv("HERMES_DEMO_WORK_DURATION"); v != "" {
		d, perr := time.ParseDuration(v)
		if perr != nil {
			log.Fatalf("invalid HERMES_DEMO_WORK_DURATION %q: %v", v, perr)
		}
		workDuration = d
	}
	slog.Info("demo work duration resolved", "duration", workDuration)

	brokerServer, err := broker.NewBrokerServer(val)

	if err != nil {
		log.Fatalf("failed to start broker: %v", err)
	}

	// Demo worker: honors a failure tag the load generator sets in the payload,
	// so the retry -> backoff -> DLQ path can be exercised on demand. A real
	// worker would fail here on genuinely bad input; this just simulates it.
	// workDuration keeps a job "in progress" so the leased gauge is visible.
	p := func(j models.Job) error {
		if workDuration > 0 {
			time.Sleep(workDuration)
		}
		if out, _ := j.Payload["_demo_outcome"].(string); out == "fail" {
			return fmt.Errorf("simulated failure for job %s", j.Id)
		}
		return nil
	}
	workerPool := worker.NewWorkerPool(3, "http://localhost:8080", "email_job", p, pollInterval)

	workerPool.StartWorkerPool()

	go startLeaseChecker(stopCh, brokerServer)

	go func() {

		err := brokerServer.Start(":8080")

		if err != nil {
			log.Printf("Error from Server Startup: %s", err)
			quit <- syscall.SIGTERM
		}
	}()

	<-quit
	workerPool.StopWorkerPool()
	close(stopCh)

	if err := brokerServer.Close(); err != nil {
		log.Printf("error closing broker: %v", err)
	}

}
