package common

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"example.com/client/utils"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ClientConfig struct {
	ServerAddress string
	DataPath      string
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	addr := config.ServerAddress
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %v", err)
	}

	client := &Client{
		conn:   conn,
		config: config,
	}

	log.Info("Client connected to server at ", addr)
	return client, nil
}

func (c *Client) Start() {
	defer c.conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go c.sendFiles()
	go handleSignals(ctx, cancel)

	go func() {
		<-ctx.Done()
		log.Infof("action: received_sigterm | result: success ")
		c.conn.Close()
	}()

	c.waitForResults()
}

func (c *Client) waitForResults() {
	results := 0

	for results < 5 {
		buf, err := utils.RecvAll(c.conn, 8)
		if err != nil {
			log.Error("Unable to read query response size: ", err)
			return
		}
		size := binary.BigEndian.Uint64(buf)

		buf, err = utils.RecvAll(c.conn, int(size))
		if err != nil {
			log.Error("Unable to read query response: ", err)
			return
		}

		log.Info("QUERY REPONSE: ", string(buf))
		results++
	}

	log.Info("action: received_all_results | result: success")
}

func (c *Client) sendFiles() {
	gamesFile, gamesReader, err := utils.OpenFile("games.csv")
	if err != nil {
		log.Error("Unable to open games file: ", err)
		return
	}

	reviewsFile, reviewsReader, err := utils.OpenFile("dataset.csv")
	if err != nil {
		log.Error("Unable to open reviews file: ", err)
		return
	}

	defer gamesFile.Close()
	defer reviewsFile.Close()

	log.Info("action: [BEGIN] sending_games")
	err = utils.SendCSV(c.conn, gamesReader)
	if err != nil {
		log.Error("action: sending_games | result: error ")
		return
	}
	log.Info("action: sent_games | result: success ")

	log.Info("action: [BEGIN] sending_reviews")
	err = utils.SendCSV(c.conn, reviewsReader)
	if err != nil {
		log.Errorf("action: sending_reviews | result: error ")
		return
	}
	log.Infof("action: sent_reviews | result: success ")
}

func handleSignals(_ context.Context, cancel context.CancelFunc) {
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
	<-chSignal
	cancel()
	signal.Stop(chSignal)
}
