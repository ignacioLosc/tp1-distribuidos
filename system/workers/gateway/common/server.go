package common

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"

	mw "example.com/system/communication/middleware"
	"example.com/system/communication/utils"
	"github.com/op/go-logging"
)

const (
	control       = "control"
	communication = "communication"

	games_to_count  = "games_to_count"
	games_to_filter = "games_to_filter"
)

var log = logging.MustGetLogger("log")

type ServerConfig struct {
	ServerPort  string
	NumCounters int
	NumMappers  int
}

type Server struct {
	listener    net.Listener
	config      ServerConfig
	middleware  *mw.Middleware
	gamesChan   chan []byte
	reviewsChan chan []byte
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
		gamesChan:   make(chan []byte),
		reviewsChan: make(chan []byte),
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
	ctx, cancel := context.WithCancel(context.Background())
	middleware, err := mw.CreateMiddleware(ctx, cancel)
	if err != nil {
		return nil, err
	}

	err = middleware.DeclareChannel(communication)
	if err != nil {
		return nil, err
	}

	err = middleware.DeclareChannel(control)
	if err != nil {
		return nil, err
	}

	middleware.DeclareDirectQueue(communication, games_to_count)
	middleware.DeclareDirectQueue(communication, games_to_filter)
	middleware.DeclareDirectQueue(communication, "reviews")

	middleware.DeclareExchange(control, "eof", "topic")

	middleware.DeclareDirectQueue(communication, "reviews")

	middleware.DeclareExchange(communication, "results", "direct")
	middleware.DeclareDirectQueue(communication, "query_results")

	middleware.BindQueueToExchange(communication, "results", "query_results", "query1")
	middleware.BindQueueToExchange(communication, "results", "query_results", "query2")
	middleware.BindQueueToExchange(communication, "results", "query_results", "query3")
	middleware.BindQueueToExchange(communication, "results", "query_results", "query4")
	middleware.BindQueueToExchange(communication, "results", "query_results", "query5")

	return middleware, nil
}


type NetConnResponse struct {
	Conn  net.Conn
	Error error
}

func (s *Server) Start() {
	defer s.listener.Close()

	go func() {
		chSignal := make(chan os.Signal, 1)
		signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
		<-chSignal
		log.Infof("received signal")
		s.middleware.CtxCancel()
	}()

	go s.listenOnChannels()

	connectionChan := make(chan NetConnResponse, 1)
	go func() {
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				log.Errorf("Error accepting connection: %v", err)
				break
			}
			connectionChan <- NetConnResponse{conn, err}
		}
	}()

	for {
		select {
		case <-s.middleware.Ctx.Done():
			return
		case connectionResult := <-connectionChan:
			go s.receiveDatasets(connectionResult.Conn)
			connectionResult.Error = s.waitForResults(connectionResult.Conn)
			if connectionResult.Error != nil {
				log.Errorf("Error while receiving results: %v", connectionResult.Error)
				continue
			}
		}
	}
}

func (s *Server) signalListener(cancel context.CancelFunc) {
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
	<-chSignal
	cancel()
}

func (s *Server) receiveDatasets(conn net.Conn) {
	err := s.receiveMessage(conn, s.gamesChan)
	if err != nil {
		log.Criticalf("action: receiving_games | result: fail | err: %s", err)
	}
	log.Infof("action: receiving_games | result: success")

	err = s.receiveMessage(conn, s.reviewsChan)
	if err != nil {
		log.Criticalf("action: receiving_reviews | result: fail | err: %s", err)
	}
	log.Infof("action: receiving_reviews | result: success")
}

func (s *Server) receiveMessage(conn net.Conn, channel chan []byte) error {
	resultChan := make(chan utils.StringResult)
	for {
		go utils.DeserealizeString(conn, resultChan)
		select {
		case <-s.middleware.Ctx.Done():
			log.Infof("action: [receive] | result: fail | err: context cancelled")
			return nil
		case result := <-resultChan:
			if result.Error != nil {
				return result.Error
			}

			channel <- result.Message

			if string(result.Message) == "EOF" {
				return nil
			}
		}
	}
}

func (s *Server) listenOnChannels() {
	for {
		select {
		case <-s.middleware.Ctx.Done():
			log.Infof("listenOnChannels returning")
			return
		case games := <-s.gamesChan:
			s.middleware.PublishInQueue(communication, "games_to_filter", games)
			if string(games) == "EOF" {
				// err := s.middleware.PublishInExchange(control, "eof", "games", []byte("EOF"))
				// if err != nil {
				// 	log.Errorf("games Error sending EOF: %v", err)
				// } else {
				// 	log.Infof("games EOF sent")
				// }

				for i := 0; i < s.config.NumCounters; i++ {
					s.middleware.PublishInQueue(communication, "games_to_count", []byte("EOF"))
				}
			} else {
				s.middleware.PublishInQueue(communication, "games_to_count", games)
			}
		case review := <-s.reviewsChan:
			if string(review) == "EOF" {
				for i := 0; i < s.config.NumMappers; i++ {
					s.middleware.PublishInQueue(communication, "reviews", []byte("EOF"))
				}
			} else {
				s.middleware.PublishInQueue(communication, "reviews", review)
			}
		}
	}
}
