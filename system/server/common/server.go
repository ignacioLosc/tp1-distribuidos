package common

import (
	"bufio"
	"fmt"
	"net"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ServerConfig struct {
	ID      string
	Address string
}

type Server struct {
	config ServerConfig
	conns  []net.Conn
}

func NewServer(config ServerConfig) *Server {
	server := &Server{
		config: config,
	}
	return server
}

// StartServer starts the TCP server
func (s *Server) StartServer() {
	ln, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		log.Criticalf("action: listen | result: fail | server_id: %v | error: %v",
			s.config.ID,
			err,
		)
		return
	}
	defer ln.Close()

	log.Infof("action: server_started | server_id: %v | address: %v",
		s.config.ID,
		s.config.Address,
	)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Errorf("action: accept_connection | result: fail | server_id: %v | error: %v",
				s.config.ID,
				err,
			)
			continue
		}
		s.conns = append(s.conns, conn)

		// Handle connection in a new goroutine
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Read message from client
	msg, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		log.Errorf("action: receive_message | result: fail | error: %v",
			err,
		)
		return
	}

	log.Infof("action: receive_message | result: success | msg: %v",
		msg,
	)

	// Send response back to client
	resp := fmt.Sprintf("[SERVER %v]\n", s.config.ID)
	_, err = conn.Write([]byte(resp))
	if err != nil {
		log.Errorf("action: send_response | result: fail | error: %v",
			err,
		)
		return
	}

	log.Infof("action: send_response | result: success | msg: %v",
		resp,
	)
}
