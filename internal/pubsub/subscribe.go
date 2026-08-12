package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Acktype string

const (
	Ack         Acktype = "Ack"
	NackRequeue Acktype = "NackRequeue"
	NackDiscard Acktype = "NackDiscard"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) Acktype,
) error {
	subscribeCh, _, err := DeclareAndBind(conn, exchange, key, queueName, queueType)
	if err != nil {
		return err
	}
	receiver, err := subscribeCh.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("Failed to consume queue: %w", err)
	}

	go func() {
		for event := range receiver {
			var data T
			if err := json.Unmarshal(event.Body, &data); err != nil {
				fmt.Printf("could not unmarshal message: %v\n", err)
				continue
			}

			ackType := handler(data)
			switch ackType {
			case Ack:
				if err := event.Ack(false); err != nil {
					fmt.Printf("Unexpected error to send Ack back: %v", err)
					return
				}
				fmt.Println("Ack sended back to the server")
			case NackRequeue:
				if err := event.Nack(false, true); err != nil {
					fmt.Printf("Unexpected error to send Ack Requeue back: %v", err)
					return
				}
				fmt.Println("Ack Requeue sended back to the server")
			case NackDiscard:
				if err := event.Nack(false, false); err != nil {
					fmt.Printf("Unexpected error to send Ack Discard back: %v", err)
					return
				}
				fmt.Println("Ack Discard sended back to the server")
			}

			if err := event.Ack(false); err != nil {
				fmt.Printf("failed to send knowledgment ACK: %v\n", err)
				return
			}
		}
	}()
	return nil
}
