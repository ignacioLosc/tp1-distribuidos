package common

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	mw "example.com/system/communication/middleware"
	"example.com/system/communication/protocol"
	"example.com/system/communication/utils"
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
	middleware.DeclareDirectQueue("query_results")

	middleware.BindQueueToExchange("results", "query_results", "query1")
	middleware.BindQueueToExchange("results", "query_results", "query2")
	middleware.BindQueueToExchange("results", "query_results", "query3")
	middleware.BindQueueToExchange("results", "query_results", "query4")
	middleware.BindQueueToExchange("results", "query_results", "query5")

	return middleware, nil
}

func (s *Server) receiveMessage(conn net.Conn, channel chan []byte, ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			log.Infof("action: [receive] | result: fail | err: context cancelled")
			return nil
		default:
			message, err := utils.DeserealizeString(conn)
			if err != nil {
				return err
			}

			channel <- message

			if string(message) == "EOF" {
				return nil
			}
			continue
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

		go s.receiveDatasets(conn, ctx)
		s.waitForResults(conn)
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
		select {
		case <-ctx.Done():
			return
		case games := <-s.gamesChan:
			s.middleware.PublishInQueue("games_to_count", games)
			s.middleware.PublishInQueue("games_to_filter", games)
		case review := <-s.reviewsChan:
			s.middleware.PublishInQueue("reviews", review)
		}
	}
}
