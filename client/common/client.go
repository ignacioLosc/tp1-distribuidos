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

func (c *Client) sendCSV(fileReader *bufio.Reader) error {
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
					sendBatchTCP(c.conn, "EOF")
					return nil
				}

				if len(batch) > 0 {
					err = sendBatchTCP(c.conn, strings.Join(batch, "\n"))
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
		err := sendBatchTCP(c.conn, strings.Join(batch, "\n"))
		if err != nil {
			return err
		}
		batch = batch[:0] // Reset the batch
	}
}

func send_all(sock net.Conn, buf []byte) error {
	totalWritten := 0
	for totalWritten < len(buf) {
		size, err := sock.Write(buf[totalWritten:])
		if err != nil {
			return fmt.Errorf("failed to write data: %w.", err)
		}
		totalWritten += size
	}
	return nil
}

func sendBatchTCP(conn net.Conn, data string) error {
	lenBuffer := make([]byte, 8)
	binary.BigEndian.PutUint64(lenBuffer, uint64(len(data)))
	err := send_all(conn, lenBuffer)
	if err != nil {
		return err
	}

	buffer := []byte(data)
	err = send_all(conn, buffer)
	if err != nil {
		return err
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

func handleSignals(ctx context.Context, cancel context.CancelFunc) {
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
	<-chSignal
	// zmq4.SetRetryAfterEINTR(false)
	// zctx.SetRetryAfterEINTR(false)
	cancel()
	signal.Stop(chSignal)
}

func (c *Client) Start() {
	defer c.conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	go handleSignals(ctx, cancel)

	go func() {
		<-ctx.Done()
		log.Infof("action: received_sigterm | result: success ")
		c.conn.Close()
	}()

	log.Infof("action: [BEGIN] sending_games")
	err = c.sendCSV(gamesReader)
	if err != nil {
		log.Errorf("action: sending_games | result: error ")
		return
	}
	log.Infof("action: sent_games | result: success ")
	log.Infof("action: [BEGIN] sending_reviews")
	err = c.sendCSV(reviewsReader)
	if err != nil {
		log.Errorf("action: sending_reviews | result: error ")
		return
	}
	log.Infof("action: sent_reviews | result: success ")
}
