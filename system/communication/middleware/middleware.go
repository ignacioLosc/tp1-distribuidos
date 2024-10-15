package middleware

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/op/go-logging"

	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("log")

type Middleware struct {
	conn      *amqp.Connection
	ch        *amqp.Channel
	queues    map[string]amqp.Queue
	Ctx       context.Context
	CtxCancel context.CancelFunc
}

func (m *Middleware) Close() {
	m.ch.Close()
	m.conn.Close()
}

func connectRabbitMQ(retries int) (*amqp.Connection, error) {
	var conn *amqp.Connection
	var err error

	for i := 0; i < 5; i++ {
		conn, err = amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
		if err == nil {
			break
		}

		delay := (5 + time.Duration(math.Pow(2, float64(i)))) * time.Second
		log.Errorf("Retry %d/%d failed: %v. Retrying in %v...", i+1, retries, err, delay)
		time.Sleep(delay)
	}

	return conn, err
}

func ConnectToMiddleware(ctx context.Context, ctxCancel context.CancelFunc) (*Middleware, error) {
	conn, err := connectRabbitMQ(5)
	if err != nil {
		log.Fatalf("action: [connection to rabbitmq] | msg: Could not establish connection: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.Qos(1, 0, false)
	if err != nil {
		return nil, err
	}

	m := &Middleware{conn, ch, make(map[string]amqp.Queue), ctx, ctxCancel}
	return m, nil
}

func (m *Middleware) DeleteQueue(name string) error {
	_, ok := m.queues[name]
	if !ok {
		return nil
	}

	m.queues[name] = amqp.Queue{}
	_, err := m.ch.QueueDelete(name, false, false, false)

	return err
}

func (m *Middleware) DeclareDirectQueue(name string) (string, error) {
	q, err := m.ch.QueueDeclare(
		name,  // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)

	if err != nil {
		return "", err
	}

	m.queues[q.Name] = q
	return q.Name, nil
}

func (m *Middleware) DeclareTemporaryQueue() (string, error) {
	q, err := m.ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)

	if err != nil {
		return "", err
	}

	m.queues[q.Name] = q
	return q.Name, nil
}

func (m *Middleware) DeclareExchange(name string, exchangeType string) error {
	return m.ch.ExchangeDeclare(
		name,         // name
		exchangeType, // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
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

func (m *Middleware) ConsumeAndProcess(queueName string, msgChan chan MsgResponse) {
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
		msgChan <- MsgResponse{amqp.Delivery{}, nil}
		return
	}

	for {
		d, ok := <-msgs
		if !ok {
			d.Nack(false, false)
			msgChan <- MsgResponse{amqp.Delivery{}, errors.New(fmt.Sprintf("There was an error consuming a message from the %s queue", queueName))}
			return
		}
		msgChan <- MsgResponse{d, nil}
	}
}

func (m *Middleware) ConsumeMultipleAndProcess(queueName1 string, queueName2 string, processFunction1 func([]byte, *bool) error, processFunction2 func([]byte, *bool) error) {
	msgs1, err := m.ch.Consume(
		queueName1, // queue
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		log.Errorf("Error consuming message: %s", err)
		return
	}

	msgs2, err := m.ch.Consume(
		queueName2, // queue
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		log.Errorf("Error consuming message: %s", err)
		return
	}

	finished := false

	for !finished {
		select {
		case d, ok := <-msgs1:
			if !ok {
				log.Errorf("Error (!ok) consuming message: %s", err)
				return
			}

			err := processFunction1(d.Body, &finished)
			if err != nil {
				log.Errorf("Error processing message: %s", err)
				return
			}

			err = d.Ack(false)
			if err != nil {
				log.Errorf("Error acknowledging rabbitmq message: %s", err)
				return
			}

			if finished {
				log.Info("Finished processing")
				return
			}
		case d, ok := <-msgs2:
			if !ok {
				log.Errorf("Error (!ok) consuming message: %s", err)
				return
			}

			err := processFunction2(d.Body, &finished)
			if err != nil {
				log.Errorf("Error processing message: %s", err)
				return
			}

			err = d.Ack(false)
			if err != nil {
				log.Errorf("Error acknowledging rabbitmq message: %s", err)
				return
			}

			if finished {
				log.Info("Finished processing")
				return
			}
		}
	}
}

type MsgResponse struct {
	Msg      amqp.Delivery
	MsgError error
}

func (m *Middleware) ConsumeExchange(queueName string, msgChan chan MsgResponse) {
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
		msgChan <- MsgResponse{amqp.Delivery{}, nil}
		return
	}

	for {
		d, ok := <-msgs
		if !ok {
			d.Nack(false, false)
			msgChan <- MsgResponse{amqp.Delivery{}, errors.New(fmt.Sprintf("There was an error consuming a message from the %s queue", queueName))}
			return
		}
		msgChan <- MsgResponse{d, nil}
	}
}
