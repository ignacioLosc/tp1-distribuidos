package common

import (
	"context"
	"encoding/binary"
	"os"
	"os/signal"
	"syscall"

	mw "example.com/system/communication/middleware"
	"example.com/system/communication/protocol"
	prot "example.com/system/communication/protocol"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	games_to_count   = "games_to_count"
	count_acumulator = "count_accumulator"
)

type PlatformCounterConfig struct {
	ServerPort string
}

type PlatformCounter struct {
	config     PlatformCounterConfig
	middleware *mw.Middleware
	count      prot.PlatformCount
	endOfGamesQueue string
}

func NewPlatformCounter(config PlatformCounterConfig) (*PlatformCounter, error) {
	middleware, err := mw.ConnectToMiddleware()
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
	_, err := p.middleware.DeclareDirectQueue(games_to_count)
	if err != nil {
		return err
	}

	_, err = p.middleware.DeclareDirectQueue(count_acumulator)
	if err != nil {
		return err
	}

	qName, err := p.middleware.DeclareTemporaryQueue()
	if err != nil {
		return err
	}
	p.endOfGamesQueue = qName

	err = p.middleware.DeclareExchange("end_of_games", "fanout")
	if err != nil {
		return err
	}

	err = p.middleware.BindQueueToExchange("end_of_games", p.endOfGamesQueue, "")
	if err != nil {
		return err
	}

	return nil
}

func (p *PlatformCounter) Close() {
	p.middleware.Close()
}

func (p *PlatformCounter) signalListener(cancel context.CancelFunc) {
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
	<-chSignal
	cancel()
}

func (p *PlatformCounter) Start() {
	defer p.Close()

	ctx, cancel := context.WithCancel(context.Background())

	go p.signalListener(cancel)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			p.middleware.ConsumeMultipleAndProcess(games_to_count, p.endOfGamesQueue, p.countGames, p.endCounting)
			err := p.sendResults()
			if err != nil {
				log.Error("Error sending results: %v", err)
				return
			}
			p.count.Windows = 0
			p.count.Linux = 0
			p.count.Mac = 0
		}
	}
}

func (p *PlatformCounter) endCounting(msg []byte, finished *bool) error {
	log.Info("MESSAGE ON QUEUE 2", string(msg))
	if string(msg) == "EOF" {
		*finished = true
	}
	return nil
}

func (p *PlatformCounter) countGames(msg []byte, finished *bool) error {
	if string(msg) == "EOF" {
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
	err := p.middleware.PublishInQueue(count_acumulator, p.count.Serialize())
	if err != nil {
		return err
	}
	err = p.middleware.PublishInQueue(count_acumulator, []byte("EOF"))
	if err != nil {
		return err
	}
	return nil
}
