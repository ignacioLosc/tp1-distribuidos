package common

import (
	zmq "github.com/pebbe/zmq4"
    "fmt"
)


type ClientConfig struct {
    ServerAddress string
    DataPath string
}

type Client struct {
    requester zmq.Socket
    config ClientConfig
}


func NewClient ( config ClientConfig) (*Client , error) {
    requester , err := zmq.NewSocket(zmq.REQ)
    if err != nil {
        return nil , err
    }
    addr := "tcp://" + config.ServerAddress  
    
    if err = requester.Connect(addr); err != nil {
        return nil, err
    }

    client := &Client {
        requester: *requester,
        config: config,
    }

    return client, nil
}



func (c *Client) read_games() {

}


func (c *Client) read_reviews() {

}

func  (c *Client) Start() {
    defer c.requester.Close()
    for request := 0; request < 10; request++ {
        fmt.Printf("Sending request: Hello")
        c.requester.Send("Hello", 0)
        reply, _ := c.requester.Recv(0)
        fmt.Printf("Received reply %d [%s]\n", request, reply)
	}
}


