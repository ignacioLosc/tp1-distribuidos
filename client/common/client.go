package common

import (
	"bufio"
	"context"
	"fmt"
	"io"
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


func (c *Client) sendGames(fileReader *bufio.Reader) error {
	_, err := fileReader.ReadString('\n') // skipping first csv line
	if err != nil {
		log.Errorf("Unable to read line from file: %v", err)
		return err
	}

	for request := 0; request < 10; request++ {
		if request == 9 {
			c.requester.Send("EOF", 0)
			c.requester.Recv(0)
			return nil
		}
		line, err := fileReader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				log.Infof("action: EOF_games | result: success")
				c.requester.Send("EOF", 0)
				return nil
			}
			log.Errorf("Unable to read line from file: %v", err)
			return err
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		_, err = c.requester.Send(line, 0)
		if err != nil {
			log.Errorf("Unable to send game: %v", err)
			return err
		}
		log.Infof(fmt.Sprintf("action: sending_game | result: success"))
		c.requester.Recv(0)
	}
	return nil
}


func (c *Client) sendReviews(fileReader *bufio.Reader) error {
	_, err := fileReader.ReadString('\n') // skipping first csv line
	if err != nil {
		log.Errorf("Unable to read line from file: %v", err)
		return err
	}

	for request := 0; request < 10; request++ {
		if request == 9 {
			c.requester.Send("EOF", 0)
			c.requester.Recv(0)
			return nil
		}
		line, err := fileReader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				log.Infof("action: EOF_reviews | result: success")
				c.requester.Send("EOF", 0)
				return nil
			}
			log.Errorf("Unable to read line from file", err)
			return err
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		_, err = c.requester.Send(line, 0)
		if err != nil {
			log.Errorf("Unable to send review", err)
			return err
		}
		log.Infof(fmt.Sprintf("action: sending_review | result: success"))
		c.requester.Recv(0)
	}
	return nil
}

func (c *Client) OpenFile(filePath string) (*os.File, *bufio.Reader, error) {
	f, err := os.Open(filePath)
	if err != nil {
		log.Errorf("Unable to read input file: %v", err)
		return nil, nil, err
	}
	fileReader := bufio.NewReader(f)
	return f, fileReader, nil
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

	gamesFile, gamesReader, err := c.OpenFile("games.csv")
	if err != nil {
		log.Errorf("Unable to open games file ", err)
		return
	}
	reviewsFile, reviewsReader, err := c.OpenFile("dataset.csv")
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
	err = c.sendGames(gamesReader)
	if err != nil {
		log.Errorf("action: sending_games | result: error ")
		return
	}
	log.Infof("action: sent_games | result: success ")
	log.Infof("action: [BEGIN] sending_reviews")
	err = c.sendReviews(reviewsReader)
	if err != nil {
		log.Errorf("action: sending_reviews | result: error ")
		return
	}
	log.Infof("action: sent_reviews | result: success ")
}
