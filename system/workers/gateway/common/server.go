package common

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/op/go-logging"
	"github.com/pebbe/zmq4"
	zmq "github.com/pebbe/zmq4"
)

var log = logging.MustGetLogger("log")

type ServerConfig struct {
	ServerPort string
}

type Server struct {
	responder zmq.Socket
	config    ServerConfig
}

func NewServer(config ServerConfig) (*Server, error) {
	responder, err := zmq.NewSocket(zmq.Type(zmq.REP))
	if err != nil {
		return nil, err
	}

	addr := "tcp://*:" + config.ServerPort
	err = responder.Bind(addr)
	if err != nil {
		return nil, err
	}

	server := &Server{
		config:    config,
		responder: *responder,
	}

	return server, nil
}

func (s *Server) Start() {
	defer s.responder.Close()

	zctx, _ := zmq.NewContext()

	ctx, cancel := context.WithCancel(context.Background())
	s.responder.SetRcvtimeo(5000 * time.Millisecond)
	s.responder.SetSndtimeo(5000 * time.Millisecond)

	go func() {
		chSignal := make(chan os.Signal, 1)
		signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
		<-chSignal
		zmq4.SetRetryAfterEINTR(false)
		zctx.SetRetryAfterEINTR(false)
		cancel()
	}()

	for {
		select {
		case <-ctx.Done():
			log.Infof("action: received_sigterm | result: success ")
			s.responder.Close()
			return
		default:
			msg, err := s.responder.Recv(0)
			if err != nil {
				log.Errorf("Recv error: %v", err)
				return
			}
			fmt.Println("Received ", msg)

			reply := "World"
			s.responder.Send(reply, 0)
			fmt.Println("Sent", reply)
		}
	}
}
