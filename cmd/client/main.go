package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"

	log "log/slog"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const amqpServerUrl = "amqp://guest:guest@localhost:5672/"
	fmt.Println("Starting Peril client...")

	conn, err := amqp.Dial(amqpServerUrl)
	if err != nil {
		log.Error("Failed to connect to the rabbitMQ", "error", err)
		os.Exit(1)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Error("Failed to register", "error", err)
		os.Exit(1)
	}

	queueName := strings.Join([]string{routing.PauseKey, username}, ".")

	_, _, err = pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, routing.PauseKey, queueName, pubsub.Transient)
	if err != nil {
		log.Error("Failed on create/bind queue", "error", err)
		os.Exit(1)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)
	<-sigs

	fmt.Println("\nShutting down...")
}
