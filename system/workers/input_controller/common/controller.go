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

func (c *Controller) InitializeRabbit() (amqp.Queue, amqp.Queue, *amqp.Channel, error) {
	// Create a channel
	ch, err := c.conn.Channel()
	if err != nil {
		log.Fatalf("Failed to create channel: %s", err)
	}

	// Declare the games queue
	gamesQueue, err := ch.QueueDeclare(
		"games",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to declare games queue: %s", err)
		return gamesQueue, gamesQueue, nil, err
	}

	// Declare the reviews queue
	reviewsQueue, err := ch.QueueDeclare(
		"reviews",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to declare reviews queue: %s", err)
		return gamesQueue, reviewsQueue, nil, err
	}
	return gamesQueue, reviewsQueue, ch, nil
}

func (c *Controller) InitializeGracefulExit() {

}

func (c *Controller) Start() {
	// c.InitializeGracefulExit()
	defer c.conn.Close()

	gamesQueue, reviewsQueue, ch, err := c.InitializeRabbit()

	defer ch.Close()

	// Consume games messages
	gameMsgs, err := ch.Consume(
		gamesQueue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to consume game messages: %s", err)
		return
	}

	// Consume reviews messages
	reviewMsgs, err := ch.Consume(
		reviewsQueue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to consume review messages: %s", err)
		return
	}

	go func() {
		for d := range gameMsgs {
			log.Infof("Received game: %s\n", d.Body)
			d.Ack(false)
		}
	}()

	go func() {
		for d := range reviewMsgs {
			log.Infof("Received review: %s\n", d.Body)
			d.Ack(false)
		}
	}()

	log.Infof("Waiting for messages...")
	select {}
}
