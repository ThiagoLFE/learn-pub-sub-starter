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

	// subscribing at the army moves
	moveKeyPlusUser := strings.Join([]string{routing.ArmyMovesPrefix, username}, ".")
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		moveKeyPlusUser,
		routing.ArmyMovesPrefix+".*",
		pubsub.Transient,
		handleMove(gs, publishCh),
	)
	if err != nil {
		log.Fatalf("could not subscribe to pause: %v", err)
	}

	// subscribing at the war handler
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		"war",
		routing.WarRecognitionsPrefix+".*",
		pubsub.Durable,
		handleConsumeAllWarMessages(gs, publishCh),
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

func handleMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.Acktype {
	return func(armyState gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Print("> ")
		move := gs.HandleMove(armyState)
		if move == gamelogic.MoveOutcomeMakeWar {
			if err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				routing.WarRecognitionsPrefix+"."+gs.GetUsername(),
				gamelogic.RecognitionOfWar{
					Attacker: armyState.Player,
					Defender: gs.GetPlayerSnap(),
				}); err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}
		return pubsub.Ack
	}
}

func handleConsumeAllWarMessages(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.Acktype {
	return func(warMsg gamelogic.RecognitionOfWar) pubsub.Acktype {
		defer fmt.Print("> ")
		outcome, winner, looser := gs.HandleWar(warMsg)
		playerName := gs.GetUsername()

		if outcome == gamelogic.WarOutcomeNotInvolved {
			return pubsub.NackRequeue
		}
		if outcome == gamelogic.WarOutcomeNoUnits {
			return pubsub.NackDiscard
		}
		if outcome == gamelogic.WarOutcomeOpponentWon {
			if err := logWar(ch, outcome, playerName, winner, looser); err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}
		if outcome == gamelogic.WarOutcomeYouWon {
			if err := logWar(ch, outcome, playerName, winner, looser); err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}
		if outcome == gamelogic.WarOutcomeDraw {
			if err := logWar(ch, outcome, playerName, winner, looser); err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}
		fmt.Printf("Error to handle the message: %v", warMsg)

		return pubsub.NackDiscard
	}
}

func logWar(ch *amqp.Channel, outcome gamelogic.WarOutcome, player, winner, looser string) error {

	switch outcome {
	case gamelogic.WarOutcomeNotInvolved, gamelogic.WarOutcomeNoUnits:
		return nil

	case gamelogic.WarOutcomeYouWon, gamelogic.WarOutcomeOpponentWon:
		msg := fmt.Sprintf("%s won a war against %s\n", winner, looser)
		return pubsub.PublishGameLog(ch, player, msg)

	case gamelogic.WarOutcomeDraw:
		msg := fmt.Sprintf("A war between %s and %s resulted in a draw\n", winner, looser)
		return pubsub.PublishGameLog(ch, player, msg)
	}
	return nil
}
