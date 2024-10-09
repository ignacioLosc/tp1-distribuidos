package common

import (
	"context"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"

	"example.com/system/communication/protocol"
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

	log.Info("action: begin_sending_games")
	err = SendCSV(c.conn, gamesReader, gameParser)
	if err != nil {
		log.Errorf("action: sending_games | result: error | err: %v", err)
		return
	}
	log.Info("action: sent_games | result: success ")

	log.Info("action: begin_sending_reviews")
	err = SendCSV(c.conn, reviewsReader, reviewParser)
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

func OpenFile(filePath string) (*os.File, *csv.Reader, error) {
	f, err := os.Open(filePath)
	if err != nil {
		log.Errorf("Unable to read input file ", err)
		return nil, nil, err
	}
	csvReader := csv.NewReader(f)
	csvReader.FieldsPerRecord = -1
	csvReader.ReuseRecord = true
	return f, csvReader, nil
}

func gameParser(record []string) ([]byte, error) {
	game, err := protocol.GameFromRecord(record)
	if err != nil {
		return nil, fmt.Errorf("Error parsing game from csv record: %v", err)
	}
	return protocol.SerializeGame(&game), nil
}

func reviewParser(record []string) ([]byte, error) {
	review, err := protocol.ReviewFromRecord(record)
	if err != nil {
		return nil, fmt.Errorf("Error parsing review from csv record: %v", err)
	}
	return protocol.SerializeReview(&review), nil
}

func SendCSV(conn net.Conn, fileReader *csv.Reader, parser func([]string) ([]byte, error)) error {
	_, err := fileReader.Read()
	if err != nil {
		return fmt.Errorf("Unable to read header from file: %v", err)
	}

	batchSize := 20
	finished := false

	for !finished {
		parsed := 0
		buffer := make([]byte, 0)

		for parsed < batchSize {
			line, err := fileReader.Read()
			if err != nil {
				if err == io.EOF {
					finished = true
					break
				}

				log.Error("Unable to read line from file: ", err)
				return err
			}

			b, err := parser(line)
			if err != nil {
				log.Error("Error parsing record: ", err)
				continue
			}

			buffer = append(buffer, b...)
			parsed++
		}

		if parsed == 0 {
			break
		}

		LenBuffer := make([]byte, 8)
		binary.BigEndian.PutUint64(LenBuffer, uint64(8+len(buffer))) // sending LENGTH OF full buffer

		err := utils.SendAll(conn, LenBuffer)
		if err != nil {
			return fmt.Errorf("Unable to send buffer length: %v", err)
		}

		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(parsed))

		b = append(b, buffer...)
		err = utils.SendAll(conn, b)
		if err != nil {
			return fmt.Errorf("Unable to send batch: %v", err)
		}

		log.Debugf("action: batch_sent | len: %d | result: success", parsed)
	}

	log.Info("action: sending_eof")
	LenBuffer := make([]byte, 8)
	binary.BigEndian.PutUint64(LenBuffer, uint64(3)) // length of EOF
	err = utils.SendAll(conn, LenBuffer)
	if err != nil {
		return fmt.Errorf("Unable to send buffer length: %v", err)
	}

	err = utils.SendAll(conn, []byte("EOF"))
	if err != nil {
		return fmt.Errorf("Unable to send EOF: %v", err)
	}

	log.Info("action: EOF_games | result: success")

	return nil
}
