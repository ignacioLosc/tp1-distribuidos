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

	log.Infof("action: server_listening | address: %s", addr)

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

	middleware.DeclareExchange(communication, "results", "topic")

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
			clientId, err := utils.GenerateRandomID(16)
			if err != nil {
				clientId = connectionResult.Conn.LocalAddr().String()
			}
			log.Infof("action: new_connection | id: %s", clientId)
			go s.receiveDatasets(connectionResult.Conn, clientId)
			go s.waitForResults(connectionResult.Conn, clientId)
		}
	}
}

func (s *Server) signalListener(cancel context.CancelFunc) {
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
	<-chSignal
	cancel()
}

func (s *Server) receiveDatasets(conn net.Conn, clientId string) {
	gamesChan := make(chan []byte)
	reviewsChan := make(chan []byte)
	go s.listenOnChannels(gamesChan, reviewsChan, clientId)

	err := s.receiveMessage(conn, gamesChan)
	if err != nil {
		log.Criticalf("action: receiving_games | result: fail | err: %s", err)
	}
	log.Infof("action: receiving_games | result: success")

	err = s.receiveMessage(conn, reviewsChan)
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

func (s *Server) listenOnChannels(gamesChan chan []byte, reviewsChan chan []byte, clientId string) {
	for {
		select {
		case <-s.middleware.Ctx.Done():
			log.Infof("listenOnChannels returning")
			return
		case games := <-gamesChan:
			s.middleware.PublishInQueue(communication, "games_to_filter", games, clientId)
			if string(games) == "EOF" {
				for i := 0; i < s.config.NumCounters; i++ {
					s.middleware.PublishInQueue(communication, "games_to_count", []byte("EOF"), clientId)
				}
			} else {
				s.middleware.PublishInQueue(communication, "games_to_count", games, clientId)
			}
		case review := <-reviewsChan:
			if string(review) == "EOF" {
				for i := 0; i < s.config.NumMappers; i++ {
					s.middleware.PublishInQueue(communication, "reviews", []byte("EOF"), clientId)
				}
			} else {
				s.middleware.PublishInQueue(communication, "reviews", review, clientId)
			}
		}
	}
}
