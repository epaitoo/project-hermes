package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/epaitoo/hermes/internal/broker"
	"github.com/epaitoo/hermes/internal/models"
	"github.com/epaitoo/hermes/internal/worker"
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

	brokerServer := broker.NewBrokerServer()

	p := func(j models.Job) error {
		return fmt.Errorf("simulated failure")
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
}
