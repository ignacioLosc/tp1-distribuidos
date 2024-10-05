package middleware

import (
	"fmt"
    "log"


	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func ConnectToRabbitMQ() {
	fmt.Println("Connecting to Rabbitmq")
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
    defer conn.Close()
    fmt.Println("Connected to RabbitMQ")
}


