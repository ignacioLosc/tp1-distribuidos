package common

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"example.com/system/communication/middleware"
	mw "example.com/system/communication/middleware"
	"example.com/system/communication/protocol"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ServerConfig struct {
	ServerPort string
}

type Server struct {
	listener    net.Listener
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
		listener:    listener,
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

	middleware.DeclareExchange("results", "direct")
	middleware.DeclareDirectQueue("query_results")

	middleware.BindQueueToExchange("results", "query_results", "query1")
	middleware.BindQueueToExchange("results", "query_results", "query2")
	middleware.BindQueueToExchange("results", "query_results", "query3")
	middleware.BindQueueToExchange("results", "query_results", "query4")
	middleware.BindQueueToExchange("results", "query_results", "query5")

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
			lenBuffer, err := middleware.RecvAll(conn, 8)
			if err != nil {
				log.Infof("action: [RECEIVE LEN BUFFER] | result: fail | err: %s", err)
				return err
			}
			lenData := binary.BigEndian.Uint64(lenBuffer)

			data, err := middleware.RecvAll(conn, int(lenData))
			if err != nil {
				log.Infof("action: [RECEIVE DATA] | result: fail | err: %s", err)
				return err
			}

			message := string(data)
			channel <- message
			if message == "EOF" {
				log.Infof("action: [RECEIVE] | result: success")
				return nil
			}
			continue
		}
	}
}

func (s *Server) waitForResults(conn net.Conn) {
	returnResultsCallback := func(msg []byte, routingKey string, x *bool) error {
		stringResult := ""
		switch routingKey {
		case "query1":
			counter, err := protocol.DeserializeCounter(msg)
			if err != nil {
				return fmt.Errorf("failed to deserialize counter: %w.", err)
			}
			log.Info("Received results for query 1", counter)
			stringResult = fmt.Sprintf("QUERY 1 RESULTS: Windows: %d, Linux: %d, Mac: %d", counter.Windows, counter.Linux, counter.Mac)
		case "query2":
			log.Info("Received results for query 2")
		case "query3":
			log.Info("Received results for query 3")
		case "query4":
			log.Info("Received results for query 4")
		case "query5":
			log.Info("Received results for query 5")
		default:
			log.Errorf("invalid routing key")
		}

		lenBuffer := make([]byte, 8)
		binary.BigEndian.PutUint64(lenBuffer, uint64(len(stringResult)))
		err := mw.SendAll(conn, lenBuffer)
		if err != nil {
			return fmt.Errorf("failed to write data: %w.", err)
		}

		err = mw.SendAll(conn, []byte(stringResult))
		if err != nil {
			return fmt.Errorf("failed to write data: %w.", err)
		}

		return nil
	}

	s.middleware.ConsumeExchange("query_results", returnResultsCallback)
}

func (s *Server) Start() {
	defer s.listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	stop := make(chan bool)

	go func() {
		chSignal := make(chan os.Signal, 1)
		signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
		<-chSignal
		stop <- true
		cancel()
	}()

	go s.listenOnChannels(ctx)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Errorf("Error accepting connection: %v", err)
			continue
		}

		go s.waitForResults(conn)
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
