package common

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/op/go-logging"
	"github.com/pebbe/zmq4"
	zmq "github.com/pebbe/zmq4"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("log")

type ServerConfig struct {
	ServerPort string
}

type Server struct {
	responder zmq.Socket
	config    ServerConfig
}

func NewServer(config ServerConfig) (*Server, error) {
	responder, err := zmq.NewSocket(zmq.Type(zmq.REP))
	if err != nil {
		return nil, err
	}

	addr := "tcp://*:" + config.ServerPort
	err = responder.Bind(addr)
	if err != nil {
		return nil, err
	}

	server := &Server{
		config:    config,
		responder: *responder,
	}

	return server, nil
}

func (s *Server) CreateRabbitQueue() (*amqp.Connection, amqp.Queue, amqp.Queue, *amqp.Channel) {
	// Connect to RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %s", err)
	}

	// Create a channel
	ch, err := conn.Channel()
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
	}
	return conn, gamesQueue, reviewsQueue, ch
}

func (s *Server) PublishNewMessage(msg string, ch *amqp.Channel, q *amqp.Queue) {
	// Publish a message
	body := []byte(msg)
	err := ch.Publish(
		"",
		q.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		},
	)
	if err != nil {
		log.Errorf("Failed to publish message: %s", err)
	}
	log.Infof("Message sent successfully!")
}

func (s *Server) receiveMessage(channel chan string) error {
	for {
		msg, err := s.responder.Recv(0)
		if err != nil {
			log.Errorf("Recv error: %v", err)
			return err
		}

		fmt.Println("Received ", msg)
		if msg == "EOF" {
			s.responder.Send("ACK", 0)
			return nil
		}
		channel <- msg
		s.responder.Send("ACK", 0)
	}
}

func (s *Server) Start() {
	defer s.responder.Close()

	zctx, _ := zmq.NewContext()

	ctx, cancel := context.WithCancel(context.Background())
	s.responder.SetRcvtimeo(5000 * time.Millisecond)
	s.responder.SetSndtimeo(5000 * time.Millisecond)

	go func() {
		chSignal := make(chan os.Signal, 1)
		signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
		<-chSignal
		zmq4.SetRetryAfterEINTR(false)
		zctx.SetRetryAfterEINTR(false)
		cancel()
	}()

	chGames := make(chan string, 1)
	chReviews := make(chan string, 1)

	conn, gamesQueue, reviewsQueue, ch := s.CreateRabbitQueue()
	defer conn.Close()
	defer ch.Close()

	go func() {
		for game := range chGames {
			s.PublishNewMessage(game, ch, &gamesQueue)
		}
	}()

	go func() {
		for review := range chReviews {
			s.PublishNewMessage(review, ch, &reviewsQueue)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			log.Infof("action: received_sigterm | result: success ")
			s.responder.Close()
			return
		default:
			log.Infof("action: [BEGIN] receiving_games")
			err := s.receiveMessage(chGames)
			if err != nil {
				return
			}
			log.Infof("action: [BEGIN] receiving_reviews")
			err = s.receiveMessage(chReviews)
			if err != nil {
				return
			}
		}
	}
}
