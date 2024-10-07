package middleware

import (
    "log"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Middleware struct {
	conn *amqp.Connection
	ch *amqp.Channel
	queues map[string]amqp.Queue
}

func (m *Middleware) Close() {
	m.ch.Close()
	m.conn.Close()
}

func ConnectToMiddleware() (*Middleware, error) {
	conn , err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		return nil,err
	}

	ch, err := conn.Channel()
	if err != nil { return nil,err }
	m := &Middleware{conn, ch, make(map[string]amqp.Queue)}
	return m, nil
}

func (m *Middleware) DeclareDirectQueue(name string) error{
	q, err := m.ch.QueueDeclare(
		name, // name
		true, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil, // arguments
	)

	if err != nil { return err }

	m.queues[name] = q
	return nil
}

func (m *Middleware) PublishInQueue(queueName string, message []byte) error {
	err := m.ch.Publish(
		"", // exchange
		queueName, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message,
		})

	if err != nil {
		log.Fatalf("Error publishing message: %s", err)
	}

	return nil
}


func (m *Middleware) ConsumeAndProcess(queueName string, process func([]byte) error, stop_chan chan bool) {
	msgs, err := m.ch.Consume(
		queueName, // queue
		"", // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil, // args
	)

	if err != nil {
		log.Fatalf("Error consuming message: %s", err)
	}

	for {
		select {
		case <-stop_chan:
			return
		default:
			d, ok := <-msgs
			if !ok {
				return
			}
			process(d.Body)
			d.Ack(false)
		}
	}
}
