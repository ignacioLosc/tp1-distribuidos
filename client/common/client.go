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

func (c *Client) read_games() {

}

func (c *Client) read_reviews() {

}

func (c *Client) sendAndReceive(csvReader *csv.Reader, request int) {
	msg, err := csvReader.Read()
	if err != nil {
		log.Errorf("Unable to read line from file", err, msg)
		return
	}
	fmt.Printf("Sending game: ", strings.Join(msg[:], ","))
	c.requester.Send(strings.Join(msg[:], ","), 0)
	reply, _ := c.requester.Recv(0)
	fmt.Printf("Received reply %d [%s]\n", request, reply)
}

func (c *Client) Start() {
	defer c.requester.Close()

	zctx, _ := zmq.NewContext()

	ctx, cancel := context.WithCancel(context.Background())
	c.requester.SetRcvtimeo(5000 * time.Millisecond)
	c.requester.SetSndtimeo(5000 * time.Millisecond)

	f, err := os.Open("games.csv")
	if err != nil {
		log.Errorf("Unable to read input file ", err)
		return
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	csvReader.FieldsPerRecord = -1

	go func() {
		chSignal := make(chan os.Signal, 1)
		signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
		<-chSignal
		zmq4.SetRetryAfterEINTR(false)
		zctx.SetRetryAfterEINTR(false)
		cancel()
	}()

	for request := 0; request < 10; request++ {

		select {
		case <-ctx.Done():
			log.Infof("action: received_sigterm | result: success ")
			c.requester.Close()
			return
		default:
		}

		c.sendAndReceive(csvReader, request)

		time.Sleep(time.Second * 4)
	}
}
