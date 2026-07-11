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

	brokerServer, err := broker.NewBrokerServer(val)

	if err != nil {
		log.Fatalf("failed to start broker: %v", err)
	}

	// Demo worker: honors a failure tag the load generator sets in the payload,
	// so the retry -> backoff -> DLQ path can be exercised on demand. A real
	// worker would fail here on genuinely bad input; this just simulates it.
	p := func(j models.Job) error {
		if out, _ := j.Payload["_demo_outcome"].(string); out == "fail" {
			return fmt.Errorf("simulated failure for job %s", j.Id)
		}
		return nil
	}
	workerPool := worker.NewWorkerPool(3, "http://localhost:8080", "email_job", p)

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
