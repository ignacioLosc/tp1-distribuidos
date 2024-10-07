package common

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	mw "example.com/system/communication/middleware"
	"github.com/op/go-logging"
	zmq "github.com/pebbe/zmq4"
)

var log = logging.MustGetLogger("log")

type ServerConfig struct {
	ServerPort string
}

type Server struct {
	responder   zmq.Socket
	config      ServerConfig
	middleware  *mw.Middleware
	gamesChan   chan string
	reviewsChan chan string
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
		config:      config,
		responder:   *responder,
		gamesChan:   make(chan string),
		reviewsChan: make(chan string),
	}

	server.middleware, err = middlewareGatewayInit()

	if err != nil {
		return nil, err
	}

	return server, nil
}

func (s *Server) Close() {
	err := s.responder.Close()
	if err != nil {
		log.Errorf("Error closing responder: %v", err)
	}

	s.middleware.Close()
	if err != nil {
		log.Errorf("Error closing middleware: %v", err)
	}
}

func middlewareGatewayInit() (*mw.Middleware, error) {
	middleware, err := mw.ConnectToMiddleware()
	if err != nil {
		return nil, err
	}
	middleware.DeclareDirectQueue("games")
	middleware.DeclareDirectQueue("reviews")

	return middleware, nil
}

func (s *Server) PublishNewMessage(msg string, queue string) error {
	// Publish a message
	body := []byte(msg)
	switch queue {
	case "games":
		err := s.middleware.PublishInQueue("games", body)
		if err != nil {
			return err
		}
	case "reviews":
		err := s.middleware.PublishInQueue("reviews", body)
		if err != nil {
			return err
		}
		log.Infof("Message sent successfully!")
	}
	return nil
}

func (s *Server) receiveMessage(channel chan string, ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			msg, err := s.responder.Recv(0)
			if err != nil {
				log.Errorf("Recv error: %v", err)
				return err
			}
			if msg == "EOF" {
				s.responder.Send("ACK", 0)
				return nil
			}
			channel <- msg
			s.responder.Send("ACK", 0)
		}
	}
}

func (s *Server) Start() {
	defer s.Close()

	zctx, _ := zmq.NewContext()

	ctx, cancel := context.WithCancel(context.Background())

	go s.signalListener(zctx, cancel)

	go s.listenOnChannels(ctx)

	for {
		s.recieveDatasets(ctx)
		// Esperar Respuestas de los workers
	}
}

func (s *Server) signalListener(zctx *zmq.Context, cancel context.CancelFunc) {
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
	<-chSignal
	zctx.SetRetryAfterEINTR(false)
	zmq.SetRetryAfterEINTR(false)
	cancel()
}

func (s *Server) recieveDatasets(ctx context.Context) {
	log.Infof("action: [BEGIN] receiving_games")
	err := s.receiveMessage(s.gamesChan, ctx)
	if err != nil {
		log.Criticalf("action: [RECIEVE] | result: fail | err: %s", err)
	}
	log.Infof("action: [BEGIN] receiving_reviews")
	err = s.receiveMessage(s.reviewsChan, ctx)
	if err != nil {
		log.Criticalf("action: [RECIEVE] | result: fail | err: %s", err)
	}
}

func (s *Server) listenOnChannels(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case game := <-s.gamesChan:
			s.PublishNewMessage(game, "games")
		case review := <-s.reviewsChan:
			s.PublishNewMessage(review, "reviews")
		}
	}
}
