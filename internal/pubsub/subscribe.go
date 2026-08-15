package pubsub

import (
	"bytes"
	"encoding/gob"
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
			case NackRequeue:
				if err := event.Nack(false, true); err != nil {
					return
				}
				fmt.Println("Ack Requeue sended back to the server")
			case NackDiscard:
				if err := event.Nack(false, false); err != nil {
					fmt.Printf("Unexpected error to send Ack Discard back: %v", err)
					return
				}
			}
		}
	}()
	return nil
}

func SubscribeGob[T any](
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
	if err := subscribeCh.Qos(10, 0, false); err != nil {
		return fmt.Errorf("Failed to config prefetch: %v", err)
	}
	receiver, err := subscribeCh.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("Failed to consume queue: %w", err)
	}

	go func() {
		for event := range receiver {
			body := bytes.NewBuffer(event.Body)

			var data T
			if err := gob.NewDecoder(body).Decode(&data); err != nil {
				fmt.Printf("Failed to decode message: %v", err)
				continue
			}

			ackType := handler(data)
			switch ackType {
			case Ack:
				if err := event.Ack(false); err != nil {
					fmt.Printf("Unexpected error to send Ack back: %v", err)
					return
				}
			case NackRequeue:
				if err := event.Nack(false, true); err != nil {
					return
				}
				fmt.Println("Ack Requeue sended back to the server")
			case NackDiscard:
				if err := event.Nack(false, false); err != nil {
					fmt.Printf("Unexpected error to send Ack Discard back: %v", err)
					return
				}
			}
		}
	}()
	return nil
}
