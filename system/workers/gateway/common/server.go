package common

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"

	mw "example.com/system/communication/middleware"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ServerConfig struct {
	ServerPort string
}

type Server struct {
	listener  net.Listener
	config      ServerConfig
	middleware  *mw.Middleware
	gamesChan   chan string
	reviewsChan chan string
}

func NewServer(config ServerConfig) (*Server, error) {
	addr := ":" + config.ServerPort

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	server := &Server{
		listener: listener,
		config:      config,
		gamesChan:   make(chan string),
		reviewsChan: make(chan string),
	}

	server.middleware, err = middlewareGatewayInit()

	if err != nil {
		return nil, err
	}

	log.Infof("Server listening on %s", addr)

	return server, nil
}

func (s *Server) Close() {
	err := s.listener.Close()
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
	}
	return nil
}

func (s *Server) receiveMessage(conn net.Conn, channel chan string, ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			log.Infof("action: [RECEIVE] | result: fail | err: context cancelled")
			return nil
		default:
			buffer := make([]byte, 1024)
			// conn.SetReadDeadline(time.Now().Add(5 * time.Second)) // Timeout for receiving
			n, err := conn.Read(buffer)
			if err != nil {
				return err
			}
			message := string(buffer[:n])
			channel <- message
			return nil
		}
	}
}

func (s *Server) Start() {
	defer s.listener.Close()

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		chSignal := make(chan os.Signal, 1)
		signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
		<-chSignal
		cancel()
	}()

	go s.listenOnChannels(ctx)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Errorf("Error accepting connection: %v", err)
			continue
		}

		s.receiveDatasets(conn, ctx)
	}
}

func (s *Server) signalListener(cancel context.CancelFunc) {
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
	<-chSignal
	cancel()
}

func (s *Server) receiveDatasets(conn net.Conn, ctx context.Context) {
	log.Infof("action: [BEGIN] receiving_games")
	err := s.receiveMessage(conn, s.gamesChan, ctx)
	if err != nil {
		log.Criticalf("action: [RECEIVE] | result: fail | err: %s", err)
	}
	log.Infof("action: [BEGIN] receiving_reviews")
	err = s.receiveMessage(conn, s.reviewsChan, ctx)
	if err != nil {
		log.Criticalf("action: [RECEIVE] | result: fail | err: %s", err)
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
