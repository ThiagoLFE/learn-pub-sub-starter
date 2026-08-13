package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"os"
	"time"

	log "log/slog"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

const (
	Durable   SimpleQueueType = "durable"
	Transient SimpleQueueType = "transient"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("Failed to marshal value %v to json. Error: %w", val, err)
	}

	payload := amqp.Publishing{
		ContentType: "application/json",
		Body:        data,
	}
	return ch.PublishWithContext(context.Background(), exchange, key, false, false, payload)
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buffer bytes.Buffer

	if err := gob.NewEncoder(&buffer).Encode(val); err != nil {
		return fmt.Errorf("Failed to encode val: %v", err)
	}

	payload := amqp.Publishing{
		ContentType: "application/gob",
		Body:        buffer.Bytes(),
	}
	return ch.PublishWithContext(context.Background(), exchange, key, false, false, payload)
}

func PublishGameLog(ch *amqp.Channel, username, message string) error {
	gamelog := routing.GameLog{
		CurrentTime: time.Now(),
		Message:     message,
		Username:    username,
	}
	return PublishGob(ch, routing.ExchangePerilTopic, routing.GameLogSlug+"."+username, gamelog)
}

// Rules of all our queues
//
// Durable: 	Always are be non-exclusive and non-auto-delete.
// Transient:	Always are exclusive and auto-delete.
//
// Exclusive: 	The queue can only be used by the connection that created it.
// Auto-Delete: The queue will be automatically deleted when its last connection is closed.
func DeclareAndBind(
	conn *amqp.Connection,
	exchange, key, queueName string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	channel, err := conn.Channel()
	if err != nil {
		log.Error("Failed to create a channel at rabbitMQ", "error", err)
		os.Exit(1)
	}

	isDurable := queueType == Durable
	isAutoDelete := queueType == Transient
	isExclusive := queueType == Transient
	noWait := false

	queue, err := channel.QueueDeclare(
		queueName,
		isDurable,
		isAutoDelete,
		isExclusive,
		noWait,
		amqp.Table{
			"x-dead-letter-exchange": "peril_dlx",
		},
	)
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, fmt.Errorf("Failed to create a queue: %w", err)
	}

	if err := channel.QueueBind(queueName, key, exchange, noWait, nil); err != nil {
		return &amqp.Channel{}, amqp.Queue{}, fmt.Errorf("Failed to link queue to exchange: %w", err)
	}

	return channel, queue, nil
}
