package main

import (
	"example.com/system/communication/middleware"
    zmq "github.com/pebbe/zmq4"
    "fmt"
    "time"
)

func main() {
	middleware.ConnectToRabbitMQ()
    
    responder , _ := zmq.NewSocket(zmq.REP)
    defer responder.Close()

    err := responder.Bind("tcp://*:5555")
    if err != nil {
        fmt.Println("socket blew up or smth")
        return
    }

    fmt.Printf("Starting to receive Packages")
    for {
        // Wait for the request from the client
        msg, _ := responder.Recv(0)
        fmt.Println("Received ", msg)

        // Do some work
        time.Sleep(time.Second)

        //Send reply back
        reply := "World"
        responder.Send(reply, 0)
        fmt.Println("Sent ", reply)
    }
}
