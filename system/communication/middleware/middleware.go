package middleware

import (
	"fmt"
	"net"

	"github.com/op/go-logging"

	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("log")

type Middleware struct {
	conn   *amqp.Connection
	ch     *amqp.Channel
	queues map[string]amqp.Queue
}

func (m *Middleware) Close() {
	m.ch.Close()
	m.conn.Close()
}

func ConnectToMiddleware() (*Middleware, error) {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	m := &Middleware{conn, ch, make(map[string]amqp.Queue)}
	return m, nil
}

func (m *Middleware) DeclareDirectQueue(name string) error {
	q, err := m.ch.QueueDeclare(
		name,  // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)

	if err != nil {
		return err
	}

	m.queues[name] = q
	return nil
}

func (m *Middleware) DeclareExchange(name string) error {
	return m.ch.ExchangeDeclare(
		name,     // name
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
}

func (m *Middleware) BindQueueToExchange(exchangeName string, queueName string, routingKey string) error {
	return m.ch.QueueBind(
		queueName,    // queue name
		routingKey,   // routing key
		exchangeName, // exchange
		false,
		nil,
	)
}

func (m *Middleware) PublishInExchange(exchangeName string, routingKey string, message []byte) error {
	return m.ch.Publish(
		exchangeName, // exchange
		routingKey,   // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message,
		})
}

func (m *Middleware) PublishInQueue(queueName string, message []byte) error {
	err := m.ch.Publish(
		"",        // exchange
		queueName, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message,
		})

	if err != nil {
		log.Fatalf("Error publishing message: %s", err)
	}

	return nil
}

func (m *Middleware) ConsumeAndProcess(queueName string, processFunction func([]byte, *bool) error) {
	msgs, err := m.ch.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)

	if err != nil {
		log.Errorf("Error consuming message: %s", err)
		return
	}

	finished := false

	for !finished {
		d, ok := <-msgs
		if !ok {
			return
		}
		err := processFunction(d.Body, &finished)
		if err != nil {
			log.Errorf("Error processing message: %s", err)
			return
		}
		d.Ack(false)
		if finished {
			log.Info("Finished processing")
			return
		}
	}
}

func (m *Middleware) ConsumeExchange(queueName string, processFunction func([]byte, string, *bool) error) {
	msgs, err := m.ch.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)

	if err != nil {
		log.Errorf("Error consuming message: %s", err)
		return
	}

	finished := false

	for !finished {
		d, ok := <-msgs
		if !ok {
			return
		}
		err := processFunction(d.Body, d.RoutingKey, &finished)
		if err != nil {
			log.Errorf("Error processing message: %s", err)
			return
		}
		d.Ack(false)
		if finished {
			log.Info("Finished processing")
			return
		}
	}
}


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
