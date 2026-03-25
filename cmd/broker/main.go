package main

import (
	"log"
	"os"

	"github.com/epaitoo/hermes/internal/broker"
)

func main() {
	brokerServer := broker.NewBrokerServer()
	err := brokerServer.Start(":8080")

	if err != nil {
		log.Fatalf("Error from Startup: %s", err)
		os.Exit(1)
	}

}
