package main

import (
	"example.com/system/communication/middleware"
    zmq "github.com/pebbe/zmq4"
    "fmt"
)

func main() {
	middleware.ConnectToRabbitMQ()
    
    responder , _ := zmq.NewSocket(zmq.REP)
    defer responder.Close()
    responder.Bind("tcp://*:5555")

    fmt.Printf("Starting t0 receive Packages")
}
