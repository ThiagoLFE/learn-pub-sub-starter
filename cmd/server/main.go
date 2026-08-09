package main

import (
	"fmt"
	"os"
	"os/signal"

	log "log/slog"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const amqpServerUrl = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(amqpServerUrl)
	if err != nil {
		log.Error("failed to create connection with rabbitMQ", "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println("Starting Peril server...")
	fmt.Println("connection with rabbitmq successfuly")

	channel, err := conn.Channel()
	if err != nil {
		log.Error("failed to create a channel channel", "error", err)
		os.Exit(1)
	}

	firstMove := routing.PlayingState{IsPaused: true}
	pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, firstMove)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)
	<-sigs

	fmt.Println("\nShutting down...")
}
