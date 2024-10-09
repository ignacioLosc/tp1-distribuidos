package utils

import (
	"encoding/binary"
	"fmt"
	"net"
)

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

func DeserealizeString(conn net.Conn) (string, error) {
	lenBuffer, err := RecvAll(conn, 8)
	if err != nil {
		return "", fmt.Errorf("failed to read len of data: %w.", err)
	}
	lenData := binary.BigEndian.Uint64(lenBuffer)

	data, err := RecvAll(conn, int(lenData))
	if err != nil {
		return "", fmt.Errorf("failed to read data: %w.", err)
	}

	return string(data), nil
}

func SerializeString(data string) ([]byte, error) {
	buffer := make([]byte, 8)

	lenBuffer := make([]byte, 8)
	binary.BigEndian.PutUint64(lenBuffer, uint64(len(data)))
	buffer = append(buffer, lenBuffer...)

	buffer = append(buffer, []byte(data)...)
	return buffer, nil
}
