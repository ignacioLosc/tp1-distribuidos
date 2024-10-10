package utils

import (
	"encoding/binary"
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
                return buf[:totalRead], nil 
            }
            return nil, err
        }
        totalRead += n
    }
    return buf, nil
}

func DeserealizeString(conn net.Conn) ([]byte, error) {
	lenBuffer, err := RecvAll(conn, 8)
	if err != nil {
		return nil, fmt.Errorf("failed to read len of data: %w.", err)
	}
	lenData := binary.BigEndian.Uint64(lenBuffer)

	data, err := RecvAll(conn, int(lenData))
	if err != nil {
		return nil, fmt.Errorf("failed to read data: %w.", err)
	}
	return data, nil
}

func SerializeString(data string) ([]byte, error) {
	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, uint64(len(data)))

	buffer = append(buffer, []byte(data)...)
	return buffer, nil
}
