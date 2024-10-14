package common

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"example.com/system/communication/middleware"
	mw "example.com/system/communication/middleware"
	"example.com/system/communication/protocol"
	"example.com/system/communication/utils"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ServerConfig struct {
	ServerPort  string
	NumCounters int
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
	middleware, err := mw.ConnectToMiddleware()
	if err != nil {
		return nil, err
	}
	middleware.DeclareDirectQueue("games_to_count")
	middleware.DeclareDirectQueue("games_to_filter")
	middleware.DeclareDirectQueue("reviews")

	middleware.DeclareExchange("results", "direct")
	middleware.DeclareExchange("end_of_games", "fanout")
	middleware.DeclareDirectQueue("query_results")

	middleware.BindQueueToExchange("results", "query_results", "query1")
	middleware.BindQueueToExchange("results", "query_results", "query2")
	middleware.BindQueueToExchange("results", "query_results", "query3")
	middleware.BindQueueToExchange("results", "query_results", "query4")
	middleware.BindQueueToExchange("results", "query_results", "query5")

	return middleware, nil
}

func (s *Server) receiveMessage(conn net.Conn, channel chan []byte, ctx context.Context) error {
	resultChan := make(chan utils.StringResult)
	for {
		go utils.DeserealizeString(conn, resultChan)
		select {
		case <-ctx.Done():
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

func getGameNames(msg []byte) []string {
	lenGames := binary.BigEndian.Uint64(msg[:8])

	games := make([]string, 0)

	index := 8
	for i := 0; i < int(lenGames); i++ {
		game, err, j := protocol.DeserializeGame(msg[index:])

		if err != nil {
			log.Errorf("Failed to deserialize game", err)
			continue
		}

		games = append(games, game.Name)
		index += j
	}
	return games
}

func getGameReviewCountNames(msg []byte) []string {
	lenGames := binary.BigEndian.Uint64(msg[:8])

	games := make([]string, 0)

	index := 8
	for i := 0; i < int(lenGames); i++ {
		game, err, j := protocol.DeserializeGameReviewCount(msg[index:])

		if err != nil {
			log.Errorf("Failed to deserialize game", err)
			continue
		}

		games = append(games, game.AppName)
		index += j
	}
	return games
}

func formatGameNames(stringResult string, gameNames []string) string {
	for idx, gameName := range gameNames {
		if idx > 0 {
			stringResult += ", " + gameName
		} else {
			stringResult += gameName
		}
	}
	return stringResult
}

func (s *Server) waitForResults(conn net.Conn, ctx context.Context) error {
	msgChan := make(chan middleware.MsgResponse)
	go s.middleware.ConsumeExchange2("query_results", msgChan)
	for {
		select {
		case <-ctx.Done():
			log.Infof("waitForResults returning")
			return nil

		case result := <-msgChan:
			stringResult := ""
			msg := result.Msg.Body
			switch result.Msg.RoutingKey {
			case "query1":
				counter, err := protocol.DeserializeCounter(msg)
				if err != nil {
					return fmt.Errorf("failed to deserialize counter: %w.", err)
				}
				log.Info("Received results for query 1", counter)
				stringResult = fmt.Sprintf("QUERY 1 RESULTS: Windows: %d, Linux: %d, Mac: %d", counter.Windows, counter.Linux, counter.Mac)
				break
			case "query2":
				log.Info("Received results for query 2")
				gameNames := getGameNames(msg)
				stringResult = fmt.Sprintf("QUERY 2 RESULTS: ")
				stringResult = formatGameNames(stringResult, gameNames)
				break
			case "query3":
				log.Info("Received results for query 3")
				gameNames := getGameReviewCountNames(msg)
				stringResult = fmt.Sprintf("QUERY 3 RESULTS: ")
				stringResult = formatGameNames(stringResult, gameNames)
				break
			case "query4":
				log.Info("Received results for query 4")
				gameNames := getGameReviewCountNames(msg)
				stringResult = fmt.Sprintf("QUERY 4 RESULTS: ")
				stringResult = formatGameNames(stringResult, gameNames)
				break
			case "query5":
				log.Info("Received results for query 5")
				gameNames := getGameReviewCountNames(msg)
				stringResult = fmt.Sprintf("QUERY 5 RESULTS: ")
				stringResult = formatGameNames(stringResult, gameNames)
				break
			default:
				log.Errorf("invalid routing key")
			}

			data, err := utils.SerializeString(stringResult)
			if err != nil {
				return fmt.Errorf("failed to serialize data: %w.", err)
			}

			err = utils.SendAll(conn, data)
			if err != nil {
				return fmt.Errorf("failed to write data: %w.", err)
			}

			return nil
		}
	}
}

type NetConnResponse struct {
	Conn  net.Conn
	Error error
}

func (s *Server) Start() {
	defer s.listener.Close()

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		chSignal := make(chan os.Signal, 1)
		signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
		<-chSignal
		log.Infof("received signal")
		cancel()
	}()

	go s.listenOnChannels(ctx)

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
		case <-ctx.Done():
			return
		case connectionResult := <-connectionChan:
			go s.receiveDatasets(connectionResult.Conn, ctx)
			connectionResult.Error = s.waitForResults(connectionResult.Conn, ctx)
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

func (s *Server) receiveDatasets(conn net.Conn, ctx context.Context) {
	err := s.receiveMessage(conn, s.gamesChan, ctx)
	if err != nil {
		log.Criticalf("action: receiving_games | result: fail | err: %s", err)
	}
	log.Infof("action: receiving_games | result: success")

	err = s.receiveMessage(conn, s.reviewsChan, ctx)
	if err != nil {
		log.Criticalf("action: receiving_reviews | result: fail | err: %s", err)
	}
	log.Infof("action: receiving_reviews | result: success")
}

func (s *Server) listenOnChannels(ctx context.Context) {
	for {
		time.Sleep(time.Second)
		log.Infof("sleeping")
		select {
		case <-ctx.Done():
			log.Infof("listenOnChannels returning")
			return
		case games := <-s.gamesChan:
			s.middleware.PublishInQueue("games_to_filter", games)
			if string(games) == "EOF" {
				s.middleware.PublishInExchange("end_of_games", "", []byte("EOF"))
			} else {
				s.middleware.PublishInQueue("games_to_count", games)
			}
		case review := <-s.reviewsChan:
			s.middleware.PublishInQueue("reviews", review)
		}
	}
}
