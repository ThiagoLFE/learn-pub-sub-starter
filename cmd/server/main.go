package main

import (
	"fmt"
	"log"
	"os"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const rabbitConnString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game server connected to RabbitMQ!")

	key := fmt.Sprintf("%s.*", routing.GameLogSlug)
	publishCh, err := conn.Channel()
	if err != nil {

		fmt.Printf("Failed to create a channel to comunicate with rabbitMQ: %v", err)
		os.Exit(1)
	}
	err = pubsub.SubscribeGob(conn, routing.ExchangePerilTopic, routing.GameLogSlug, key, pubsub.Durable, handlerLogs())
	if err != nil {
		log.Fatalf("subscribe to the logs queue: %v", err)
	}

	gamelogic.PrintServerHelp()
	for {

		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "pause":
			fmt.Println("Publishing paused game state")
			err = pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				},
			)
			if err != nil {
				log.Printf("could not publish time: %v", err)
			}
		case "resume":
			fmt.Println("Publishing resumes game state")
			err = pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				},
			)
			if err != nil {
				log.Printf("could not publish time: %v", err)
			}
		case "quit":
			log.Println("goodbye")
			return
		case "help":
			gamelogic.PrintServerHelp()
		default:
			fmt.Println("unknown command")
		}
	}
}

func handlerLogs() func(routing.GameLog) pubsub.Acktype {
	return func(log routing.GameLog) pubsub.Acktype {
		defer fmt.Print("> ")

		if err := gamelogic.WriteLog(log); err != nil {
			fmt.Printf("error writing log: %v\n", err)
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}
