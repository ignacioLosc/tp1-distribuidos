package common

import (
	"fmt"

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

func (c *Controller) InitializeRabbit() (amqp.Queue, *amqp.Channel, error) {
	ch, err := c.conn.Channel()
	if err != nil {
		log.Fatalf("Failed to create channel: %s", err)
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
		return gamesToCountQueue, nil, err
	}

	return gamesToCountQueue, ch, nil
}

func (c *Controller) InitializeGracefulExit() {

}

func (c *Controller) Start() {
	// c.InitializeGracefulExit()
	defer c.conn.Close()

	gamesToCountQueue, ch, err := c.InitializeRabbit()

	defer ch.Close()

	gameMsgs, err := ch.Consume(
		gamesToCountQueue.Name,
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

	go func() {
		for d := range gameMsgs {
			game, err := protocol.DeserializeGame(d.Body)
			if err != nil {
				log.Errorf("Failed to deserialize game: %s", err)
				d.Ack(false)
				continue
			}

			windowsCount := 0
			linuxCount := 0
			macCount := 0

			if game.WindowsCompatible {
				windowsCount++
			}
			if game.LinuxCompatible {
				linuxCount++
			}
			if game.MacCompatible {
				macCount++
			}

			log.Infof("Game %s has %d windows, %d linux, %d mac", game.AppID, windowsCount, linuxCount, macCount)

			d.Ack(false)
		}
	}()

	log.Infof("Waiting for messages...")
	select {}
}
