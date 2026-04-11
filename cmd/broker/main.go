package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/epaitoo/hermes/internal/broker"
)

func startLeaseChecker(stopCh <-chan struct{}, bs *broker.BrokerServer) {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			bs.StartLeaseCheck()
		}
	}
}

func main() {
	stopCh := make(chan struct{})
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	brokerServer := broker.NewBrokerServer()

	go startLeaseChecker(stopCh, brokerServer)

	go func() {

		err := brokerServer.Start(":8080")

		if err != nil {
			log.Printf("Error from Server Startup: %s", err)
			quit <- syscall.SIGTERM
		}
	}()

	<-quit
	close(stopCh)

}
