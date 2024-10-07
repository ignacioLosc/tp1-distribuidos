package common

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/op/go-logging"
	"github.com/pebbe/zmq4"
	zmq "github.com/pebbe/zmq4"
)

var log = logging.MustGetLogger("log")

type ClientConfig struct {
	ServerAddress string
	DataPath      string
}

type Client struct {
	requester zmq.Socket
	config    ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	requester, err := zmq.NewSocket(zmq.Type(zmq.REQ))
	if err != nil {
		return nil, err
	}
	addr := "tcp://" + config.ServerAddress

	if err = requester.Connect(addr); err != nil {
		return nil, err
	}

	client := &Client{
		requester: *requester,
		config:    config,
	}

	return client, nil
}

func (c *Client) sendGames(csvReader *csv.Reader) error {
	for request := 0; request < 10; request++ {
		if request == 9 {
			c.requester.Send("EOF", 0)
			c.requester.Recv(0)
			return nil
		}
		msg, err := csvReader.Read()
		if msg == nil {
			log.Infof("action: EOF_games | result: success ")
			c.requester.Send("EOF", 0)
			return nil
		}
		if err != nil {
			log.Errorf("Unable to read line from file", err, msg)
			return err
		}
		_, err = c.requester.Send(strings.Join(msg[:], ","), 0)
		if err != nil {
			log.Errorf("Unable to send game", err)
			return err
		}
		log.Infof(fmt.Sprintf("action: sending_game | gameId: %s| result: success", msg[0]))
		c.requester.Recv(0)
	}
	return nil
}

func (c *Client) sendReviews(csvReader *csv.Reader) error {
	for request := 0; request < 10; request++ {
		if request == 9 {
			c.requester.Send("EOF", 0)
			c.requester.Recv(0)
			return nil
		}
		msg, err := csvReader.Read()
		if msg == nil {
			log.Infof("action: EOF_reviews | result: success ")
			c.requester.Send("EOF", 0)
			return nil
		}
		if err != nil {
			log.Errorf("Unable to read line from file", err, msg)
			return err
		}
		_, err = c.requester.Send(strings.Join(msg[:], ","), 0)
		if err != nil {
			log.Errorf("Unable to send review", err)
			return err
		}
		log.Infof(fmt.Sprintf("action: sending_review | gameId: %s| result: success", msg[0]))
		c.requester.Recv(0)
	}
	return nil
}

func (c *Client) OpenCsvFile(filePath string) (*os.File, *csv.Reader, error) {
	f, err := os.Open(filePath)
	if err != nil {
		log.Errorf("Unable to read input file ", err)
		return nil, nil, err
	}
	csvReader := csv.NewReader(f)
	csvReader.FieldsPerRecord = -1
	return f, csvReader, nil
}

func handleSignals(ctx context.Context, zctx *zmq.Context, cancel context.CancelFunc) {
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
	<-chSignal
	zmq4.SetRetryAfterEINTR(false)
	zctx.SetRetryAfterEINTR(false)
	cancel()
	signal.Stop(chSignal)
}

func (c *Client) Start() {
	defer c.requester.Close()

	zctx, _ := zmq.NewContext()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.requester.SetRcvtimeo(5000 * time.Millisecond)
	c.requester.SetSndtimeo(5000 * time.Millisecond)

	gamesFile, csvGamesReader, err := c.OpenCsvFile("games.csv")
	if err != nil {
		log.Errorf("Unable to open games file ", err)
		return
	}
	reviewsFile, csvReviewsReader, err := c.OpenCsvFile("dataset.csv")
	if err != nil {
		log.Errorf("Unable to open reviews file ", err)
		return
	}
	defer gamesFile.Close()
	defer reviewsFile.Close()

	// Handle signals
	go handleSignals(ctx, zctx, cancel)

	// Handle context cancelation
	go func() {
		<-ctx.Done()
		log.Infof("action: received_sigterm | result: success ")
		c.requester.Close()
	}()

	log.Infof("action: [BEGIN] sending_games")
	err = c.sendGames(csvGamesReader)
	if err != nil {
		log.Errorf("action: sending_games | result: error ")
		return
	}
	log.Infof("action: sent_games | result: success ")
	log.Infof("action: [BEGIN] sending_reviews")
	err = c.sendReviews(csvReviewsReader)
	if err != nil {
		log.Errorf("action: sending_reviews | result: error ")
		return
	}
	log.Infof("action: sent_reviews | result: success ")
}
