package common

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"example.com/system/communication/utils"
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
	gamesFile, gamesReader, err := OpenFile("games.csv")
	if err != nil {
		log.Error("Unable to open games file: ", err)
		return
	}

	reviewsFile, reviewsReader, err := OpenFile("dataset.csv")
	if err != nil {
		log.Error("Unable to open reviews file: ", err)
		return
	}

	defer gamesFile.Close()
	defer reviewsFile.Close()

	log.Info("action: [BEGIN] sending_games")
	err = SendCSV(c.conn, gamesReader)
	if err != nil {
		log.Error("action: sending_games | result: error ")
		return
	}
	log.Info("action: sent_games | result: success ")

	log.Info("action: [BEGIN] sending_reviews")
	err = SendCSV(c.conn, reviewsReader)
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

func SendBatchTCP(conn net.Conn, data string) error {
	lenBuffer := make([]byte, 8)
	binary.BigEndian.PutUint64(lenBuffer, uint64(len(data)))
	err := utils.SendAll(conn, lenBuffer)
	if err != nil {
		return err
	}

	buffer := []byte(data)
	err = utils.SendAll(conn, buffer)
	if err != nil {
		return err
	}

	return nil
}

func OpenFile(filePath string) (*os.File, *bufio.Reader, error) {
	f, err := os.Open(filePath)
	if err != nil {
		log.Errorf("Unable to read input file: %v", err)
		return nil, nil, err
	}
	fileReader := bufio.NewReader(f)
	return f, fileReader, nil
}

func SendCSV(conn net.Conn, fileReader *bufio.Reader) error {
	_, err := fileReader.ReadString('\n') // Skipping first CSV line
	if err != nil {
		log.Errorf("Unable to read line from file: %v", err)
		return err
	}

	var batch []string
	batchSize := 10

	for {
		for len(batch) < batchSize {
			line, err := fileReader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					log.Info("action: EOF_games | result: success")
					SendBatchTCP(conn, "EOF")
					return nil
				}

				if len(batch) > 0 {
					err = SendBatchTCP(conn, strings.Join(batch, "\n"))
					if err != nil {
						return err
					}
				}

				log.Info("Unable to read line from file: %v", err)
				return err
			}

			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}

			batch = append(batch, line)

			// MaxBufferSize := 65536
			MaxBufferSize := 500
			if len(strings.Join(batch, "\n")) >= MaxBufferSize {
				break
			}
		}

		log.Infof("action: sending_batch | len: %d | result: success", len(batch))
		err := SendBatchTCP(conn, strings.Join(batch, "\n"))
		if err != nil {
			return err
		}
		batch = batch[:0] // Reset the batch
	}
}
