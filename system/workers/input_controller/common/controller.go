package common

import (
	"fmt"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("log")

type ControllerConfig struct {
	ServerPort string
}

type Controller struct {
	conn   *amqp.Connection
	config ControllerConfig
}

func NewController(config ControllerConfig) (*Controller, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@rabbitmq:%s/", config.ServerPort))
	if err != nil {
		return nil, err
	}
	controller := &Controller{
		config: config,
		conn:   conn,
	}

	return controller, nil
}

func (c *Controller) Start() {
	defer c.conn.Close()

	// Create a channel
	ch, err := c.conn.Channel()
	if err != nil {
		log.Fatalf("Failed to create channel: %s", err)
	}
	defer ch.Close()

	// Declare the queue
	q, err := ch.QueueDeclare(
		"games",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to declare queue: %s", err)
	}

	// Consume messages
	msgs, err := ch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to consume messages: %s", err)
	}

	go func() {
		for d := range msgs {
			log.Infof("Received message: %s\n", d.Body)
			d.Ack(false)
		}
	}()

	log.Infof("Waiting for messages...")
	select {}
}
