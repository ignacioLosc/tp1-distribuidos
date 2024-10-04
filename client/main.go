package main

import (
    "fmt"
    zmq "github.com/pebbe/zmq4"
)

func main() {
    fmt.Println("Soy el cliente !")
    requester , _ := zmq.NewSocket(zmq.REQ)
    defer requester.Close()
    
    fmt.Println("Conectandome al servidor...")
    requester.Connect("tcp://gateway:5555")

    for request := 0 ; request < 10 ; request ++  {
        fmt.Printf("Sending request: Hello")
        requester.Send("Hello", 0)
        reply, _ := requester.Recv(0)
        fmt.Printf("Received Reply %d [%s]\n", request, reply)
    }
} 
