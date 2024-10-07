package common

import (
	"encoding/csv"
	"fmt"
	"strings"

	"example.com/system/communication/protocol"
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

func (c *Controller) InitializeRabbit() (amqp.Queue, amqp.Queue, amqp.Queue, *amqp.Channel, error) {
	// Create a channel
	ch, err := c.conn.Channel()
	if err != nil {
		log.Fatalf("Failed to create channel: %s", err)
	}

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
		return gamesQueue, gamesQueue, gamesQueue, nil, err
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
		return gamesQueue, reviewsQueue, reviewsQueue, nil, err
	}

	gamesToCountQueue, err := ch.QueueDeclare(
		"games_to_count",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to declare games queue: %s", err)
		return gamesQueue, reviewsQueue, gamesToCountQueue, nil, err
	}

	return gamesQueue, reviewsQueue, gamesToCountQueue, ch, nil
}

func (c *Controller) InitializeGracefulExit() {

}

func (c *Controller) Start() {
	// c.InitializeGracefulExit()
	defer c.conn.Close()

	gamesQueue, reviewsQueue, gamesToCountQueue, ch, err := c.InitializeRabbit()

	defer ch.Close()

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
			str := string(d.Body)
			r := csv.NewReader(strings.NewReader(str))
			record, err := r.Read()
			if err != nil {
				fmt.Println("Error reading CSV:", err)
				continue
			}

			game, err := protocol.GameFromRecord(record)
			if err != nil {
				fmt.Println("Error parsing record:", err)
				continue
			}

			log.Info("Input controller. Sending game to game_to_count queue ", game.Name)

			body := []byte(protocol.SerializeGame(&game))
			err = ch.Publish(
				"",
				gamesToCountQueue.Name,
				false,
				false,
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        body,
				},
			)

			d.Ack(false)
		}
	}()

	go func() {
		for d := range reviewMsgs {
			str := string(d.Body)
			r := csv.NewReader(strings.NewReader(str))
			record, err := r.Read()
			if err != nil {
				fmt.Println("Error reading CSV:", err)
				continue
			}

			review, err := protocol.ReviewFromRecord(record)
			if err != nil {
				fmt.Println("Error parsing record:", err)
				continue
			}

			log.Info("Input controller parsed review: ", review.AppID, review.ReviewText, review.ReviewVotes)

			d.Ack(false)
		}
	}()

	log.Infof("Waiting for messages...")
	select {}
}
