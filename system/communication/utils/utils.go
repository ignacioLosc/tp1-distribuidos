package utils

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"

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
	buf := make([]byte, sz)
	totalRead := 0

	for totalRead < sz {
		n, err := sock.Read(buf[totalRead:])
		if err != nil {
			if err == io.EOF {
				if totalRead < sz {
					return nil, fmt.Errorf("failed to read data: %w.", err)
				} else {
					return buf[:totalRead], nil
				}
			}
			return nil, err
		}
		totalRead += n
	}
	return buf, nil
}

type StringResult struct {
	Message []byte
	Error   error
}

func DeserealizeString(conn net.Conn, resultChan chan StringResult) {
	lenBuffer, err := RecvAll(conn, 8)
	if err != nil {
		resultChan <- StringResult{nil, fmt.Errorf("failed to read len of data: %w.", err)}
		return
	}
	lenData := binary.BigEndian.Uint64(lenBuffer)

	data, err := RecvAll(conn, int(lenData))
	if err != nil {
		resultChan <- StringResult{nil, fmt.Errorf("failed to read data: %w.", err)}
		return
	}
	resultChan <- StringResult{data, nil}
	return
}

func SerializeString(data string) ([]byte, error) {
	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, uint64(len(data)))

	buffer = append(buffer, []byte(data)...)
	return buffer, nil
}

func GetRange(input string, r int) int {
	h := sha256.New()
	_, err := h.Write([]byte(input))
	if err != nil {
		return 0
	}
	hash := h.Sum(nil)
	v := int(binary.BigEndian.Uint64(hash[:8]))
	if v < 0 {
		v = -v
	}
	return v % r
}


func GenerateRandomID(length int) (string, error) {
	bytes := make([]byte, length)
	_, err := rand.Read(bytes)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
