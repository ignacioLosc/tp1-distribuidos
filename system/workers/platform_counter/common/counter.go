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
	count           prot.PlatformCount
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
		count:      prot.PlatformCount{},
		middleware: middleware,
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

	eofChan := make(chan middleware.MsgResponse)
	go p.middleware.ConsumeFromQueue(control, p.endOfGamesQueue, eofChan)

	for {
		select {
		case <-p.middleware.Ctx.Done():
			return
		case result := <-msgChan:
			msg := result.Msg.Body
			err := p.countGames(msg)
			if err != nil {
				result.Msg.Nack(false, true)
			} else {
				result.Msg.Ack(false)
			}
		case result := <-eofChan:
			if result.MsgError != nil {
				log.Errorf("Error consuming message from eof chan: %v", result.MsgError)
				continue
			}

			err := p.sendResults()
			if err != nil {
				result.Msg.Nack(false, true)
				log.Error("Error sending results: %v", err)
				continue
			}
			result.Msg.Ack(false)
			p.count.Restart()
		}
	}
}

func (p *PlatformCounter) countGames(msg []byte) error {
	if string(msg) == "EOF" {
		err := p.sendResults()
		if err != nil {
			log.Error("Error sending results: %v", err)
			return err
		}
		p.count.Windows = 0
		p.count.Linux = 0
		p.count.Mac = 0
		return nil
	}

	lenGames := binary.BigEndian.Uint64(msg[:8])
	games := make([]protocol.Game, 0)

	index := 8
	for i := 0; i < int(lenGames); i++ {
		game, err, j := protocol.DeserializeGame(msg[index:])

		if err != nil {
			log.Errorf("Failed to deserialize game", err)
			continue
		}

		games = append(games, game)
		index += j
	}

	for _, game := range games {
		p.count.Increment(game.WindowsCompatible, game.LinuxCompatible, game.MacCompatible)
	}

	log.Debugf("Platform Counter: Windows: %d, Linux: %d, Mac: %d", p.count.Windows, p.count.Linux, p.count.Mac)

	return nil
}

func (p *PlatformCounter) sendResults() error {
	log.Debugf("Platform final: Windows: %d, Linux: %d, Mac: %d", p.count.Windows, p.count.Linux, p.count.Mac)
	err := p.middleware.PublishInQueue(communication, count_acumulator, p.count.Serialize())
	if err != nil {
		return err
	}
	err = p.middleware.PublishInQueue(communication, count_acumulator, []byte("EOF"))
	if err != nil {
		return err
	}
	return nil
}
