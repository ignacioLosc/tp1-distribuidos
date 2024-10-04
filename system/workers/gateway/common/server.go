package common

import (
    "fmt"
	zmq "github.com/pebbe/zmq4"
)

type ServerConfig struct {
    ServerPort string
}

type Server struct {
    responder zmq.Socket
    config ServerConfig
}


func NewServer(config ServerConfig) (*Server, error) {
    responder, err := zmq.NewSocket(zmq.REP)    
    if err != nil {
        return nil,err
    }
    
    addr := "tcp://*:" + config.ServerPort
    err = responder.Bind(addr)
    if err != nil {
        return nil,err
    }
    
    server := &Server {
        config: config,
        responder: *responder,
    }

    return server, nil
}


func (s *Server) Start() {
    defer s.responder.Close()
    for {
        msg , err := s.responder.Recv(0)
        if err != nil {
            fmt.Println("Server blew up")
            return
        }
        fmt.Println("Received ", msg)

        reply := "World"
        s.responder.Send(reply,0)
        fmt.Println("Sent", reply)
    }
}
