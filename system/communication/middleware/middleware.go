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
	Ctx       context.Context
	CtxCancel context.CancelFunc
	channels  map[string]*amqp.Channel
	queues    map[string]amqp.Queue
}

func (m *Middleware) Close() {
	for _, ch := range m.channels {
		ch.Close()
	}
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

func CreateMiddleware(ctx context.Context, ctxCancel context.CancelFunc) (*Middleware, error) {
	conn, err := connectRabbitMQ(5)
	if err != nil {
		log.Fatalf("action: [connection to rabbitmq] | msg: Could not establish connection: %v", err)
	}
	return &Middleware{conn, ctx, ctxCancel, make(map[string]*amqp.Channel), make(map[string]amqp.Queue)}, nil
}

func (m *Middleware) DeclareChannel(channelName string) error {
	ch, err := m.conn.Channel()
	if err != nil {
		return fmt.Errorf("Error declaring channel: %s", err)
	}

	err = ch.Qos(1, 0, false)
	if err != nil {
		log.Fatalf("Error setting QoS: %s", err)
		return err
	}

	m.channels[channelName] = ch
	return nil
}

func (m *Middleware) DeleteQueue(channelName string, name string) error {
	_, ok := m.queues[name]
	if !ok {
		return nil
	}

	m.queues[name] = amqp.Queue{}
	_, err := m.channels[channelName].QueueDelete(name, false, false, false)

	return err
}

func (m *Middleware) DeclareDirectQueue(channelName string, name string) (string, error) {
	ch, ok := m.channels[channelName]
	if !ok {
		return "", fmt.Errorf("Channel %s not found", channelName)
	}

	q, err := ch.QueueDeclare(
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

func (m *Middleware) DeclareTemporaryQueue(channelName string) (string, error) {
	ch, ok := m.channels[channelName]
	if !ok {
		return "", fmt.Errorf("Channel %s not found", channelName)
	}

	q, err := ch.QueueDeclare(
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

func (m *Middleware) DeclareExchange(channelName string, name string, exchangeType string) error {
	ch, ok := m.channels[channelName]
	if !ok {
		return fmt.Errorf("Channel %s not found", channelName)
	}

	return ch.ExchangeDeclare(
		name,         // name
		exchangeType, // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
}

func (m *Middleware) BindQueueToExchange(channelName string, exchangeName string, queueName string, routingKey string) error {
	ch, ok := m.channels[channelName]
	if !ok {
		return fmt.Errorf("Channel %s not found", channelName)
	}

	return ch.QueueBind(
		queueName,    // queue name
		routingKey,   // routing key
		exchangeName, // exchange
		false,
		nil,
	)
}

func (m *Middleware) PublishInExchange(channelName string, exchangeName string, routingKey string, message []byte) error {
	ch, ok := m.channels[channelName]
	if !ok {
		return fmt.Errorf("Channel %s not found", channelName)
	}

	return ch.Publish(
		exchangeName, // exchange
		routingKey,   // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message,
		})
}

func (m *Middleware) PublishInQueue(channelName string, queueName string, message []byte) error {
	ch, ok := m.channels[channelName]
	if !ok {
		return fmt.Errorf("Channel %s not found", channelName)
	}

	err := ch.Publish(
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

func (m *Middleware) ConsumeFromQueue(channelName string, queueName string, msgChan chan MsgResponse) {
	ch, ok := m.channels[channelName]
	if !ok {
		return
	}

	msgs, err := ch.Consume(
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
			log.Errorf("Error (!ok) consuming message: %s", err)
			d.Nack(false, false)
			msgChan <- MsgResponse{amqp.Delivery{}, errors.New(fmt.Sprintf("There was an error consuming a message from the %s queue", queueName))}
			return
		}
		msgChan <- MsgResponse{d, nil}
	}
}

func (m *Middleware) ConsumeMultipleAndProcess(channelName string, queueName1 string, queueName2 string, processFunction1 func([]byte, *bool) error, processFunction2 func([]byte, *bool) error) {
	ch, ok := m.channels[channelName]
	if !ok {
		return
	}

	msgs1, err := ch.Consume(
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

	msgs2, err := ch.Consume(
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

func (m *Middleware) ConsumeExchange(channelName string, queueName string, msgChan chan MsgResponse) {
	ch, ok := m.channels[channelName]
	if !ok {
		return
	}
	msgs, err := ch.Consume(
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
