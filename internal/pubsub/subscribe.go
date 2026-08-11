package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T),
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

			handler(data)

			if err := event.Ack(false); err != nil {
				fmt.Printf("failed to send knowledgment ACK: %v\n", err)
				return
			}
		}
	}()
	return nil
}
