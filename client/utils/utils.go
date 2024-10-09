package utils

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func SendAll(sock net.Conn, buf []byte) error {
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

func RecvAll(sock net.Conn, sz int) ([]byte, error) {
	buffer := make([]byte, sz)
	totalRead := 0
	for totalRead < len(buffer) {
		size, err := sock.Read(buffer)
		if err != nil {
			return nil, fmt.Errorf("failed to read data: %w. Trying again.", err)
		}
		totalRead += size
	}
	return buffer, nil
}

func SendBatchTCP(conn net.Conn, data string) error {
	lenBuffer := make([]byte, 8)
	binary.BigEndian.PutUint64(lenBuffer, uint64(len(data)))
	err := SendAll(conn, lenBuffer)
	if err != nil {
		return err
	}

	buffer := []byte(data)
	err = SendAll(conn, buffer)
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
