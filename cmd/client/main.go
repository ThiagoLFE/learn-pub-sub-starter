package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	const rabbitConnString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game client connected to RabbitMQ!")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("could not get username: %v", err)
	}

	gs := gamelogic.NewGameState(username)

	publishCh, err := conn.Channel()
	if err != nil {
		fmt.Printf("Failed to create the publish Channel: %v", err)
		return
	}

	// subscribing on the pause/resume routing key
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		strings.Join([]string{routing.PauseKey, username}, ","),
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gs),
	)

	moveKeyPlusUser := strings.Join([]string{routing.ArmyMovesPrefix, username}, ".")
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		moveKeyPlusUser,
		routing.ArmyMovesPrefix+".*",
		pubsub.Transient,
		handleMove(gs),
	)

	if err != nil {
		log.Fatalf("could not subscribe to pause: %v", err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "move":
			newArmyMoviment, err := gs.CommandMove(words)
			if err != nil {
				fmt.Println(err)
				continue
			}

			if err := pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilTopic,
				moveKeyPlusUser,
				newArmyMoviment,
			); err != nil {
				fmt.Printf("Failed to publish the move event: %v\n", err)
				return
			}
			fmt.Println("Moviment published successfully!")

		case "spawn":
			err = gs.CommandSpawn(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("unknown command")
		}
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.Acktype {
	return func(gamestate routing.PlayingState) pubsub.Acktype {
		defer fmt.Print("> ")
		gs.HandlePause(gamestate)
		return pubsub.Ack
	}
}

func handleMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) pubsub.Acktype {
	return func(armyState gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Print("> ")
		move := gs.HandleMove(armyState)
		if move == gamelogic.MoveOutComeSafe || move == gamelogic.MoveOutcomeMakeWar {
			return pubsub.Ack
		}
		return pubsub.NackDiscard
	}
}
