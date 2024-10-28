package common

import (
	"context"
	"encoding/binary"
	"os"
	"os/signal"
	"syscall"

	"example.com/system/communication/middleware"
	mw "example.com/system/communication/middleware"
	"example.com/system/communication/protocol"
	prot "example.com/system/communication/protocol"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	games_to_count   = "games_to_count"
	count_acumulator = "count_accumulator"
	control          = "control"
	communication    = "communication"
)

type PlatformCounterConfig struct {
	ServerPort string
}

type PlatformCounter struct {
	config          PlatformCounterConfig
	middleware      *mw.Middleware
	countMap		map[string]prot.PlatformCount
	finishedMap		map[string]bool
	endOfGamesQueue string
}

func NewPlatformCounter(config PlatformCounterConfig) (*PlatformCounter, error) {
	ctx, cancel := context.WithCancel(context.Background())
	middleware, err := mw.CreateMiddleware(ctx, cancel)
	if err != nil {
		return nil, err
	}

	platformCounter := &PlatformCounter{
		config:     config,
		middleware: middleware,
		countMap: make(map[string]prot.PlatformCount),
		finishedMap: make(map[string]bool),
	}

	err = platformCounter.middlewareCounterInit()
	if err != nil {
		return nil, err
	}

	platformCounter.middleware = middleware
	return platformCounter, nil
}

func (p PlatformCounter) middlewareCounterInit() error {
	err := p.middleware.DeclareChannel(communication)
	if err != nil {
		return err
	}

	err = p.middleware.DeclareChannel(control)
	if err != nil {
		return err
	}

	_, err = p.middleware.DeclareDirectQueue(communication, games_to_count)
	if err != nil {
		return err
	}

	_, err = p.middleware.DeclareDirectQueue(communication, count_acumulator)
	if err != nil {
		return err
	}

	qName, err := p.middleware.DeclareTemporaryQueue(control)
	if err != nil {
		return err
	}
	p.endOfGamesQueue = qName

	err = p.middleware.DeclareExchange(control, "eof", "topic")
	if err != nil {
		return err
	}

	err = p.middleware.BindQueueToExchange(control, "eof", p.endOfGamesQueue, "games")
	if err != nil {
		return err
	}

	return nil
}

func (p *PlatformCounter) Close() {
	p.middleware.Close()
}

func (p *PlatformCounter) signalListener() {
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
	<-chSignal
	log.Infof("received signal")
	p.middleware.CtxCancel()
}

func (p *PlatformCounter) Start() {
	defer p.Close()

	go p.signalListener()

	msgChan := make(chan middleware.MsgResponse)
	go p.middleware.ConsumeFromQueue(communication, games_to_count, msgChan)

	for {
		select {
		case <-p.middleware.Ctx.Done():
			return
		case result := <-msgChan:
			clientId := result.Msg.Headers["clientId"].(string)
			msg := result.Msg.Body

			if p.finishedMap[clientId] {
				result.Msg.Nack(false, true)
				continue
			}

			err := p.countGames(msg, clientId)
			if err != nil {
				result.Msg.Nack(false, true)
			} else {
				result.Msg.Ack(false)
			}
		}
	}
}

func (p *PlatformCounter) countGames(msg []byte, clientId string) error {
	counter := p.countMap[clientId]

	if string(msg) == "EOF" {
		err := p.sendResults(clientId)
		if err != nil {
			log.Error("Error sending results: %v", err)
			return err
		}

		p.finishedMap[clientId] = true
		p.countMap[clientId] = prot.PlatformCount{}
		return nil
	}

	lenGames := binary.BigEndian.Uint64(msg[:8])

	index := 8
	for i := 0; i < int(lenGames); i++ {
		game, err, j := protocol.DeserializeGame(msg[index:])

		if err != nil {
			log.Errorf("Failed to deserialize game", err)
			continue
		}

		counter.Increment(game.WindowsCompatible, game.LinuxCompatible, game.MacCompatible)
		index += j
	}

	p.countMap[clientId] = counter

	return nil
}

func (p *PlatformCounter) sendResults(clientId string) error {
	counter := p.countMap[clientId]
	log.Debug("Results for client ", counter)

	err := p.middleware.PublishInQueue(communication, count_acumulator, counter.Serialize(), clientId)
	if err != nil {
		return err
	}
	err = p.middleware.PublishInQueue(communication, count_acumulator, []byte("EOF"), clientId)
	if err != nil {
		return err
	}
	return nil
}
