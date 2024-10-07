package common

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	mw "example.com/system/communication/middleware"
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
	config PlatformCounterConfig 
	middleware *mw.Middleware
	count prot.PlatformCount
	stop chan bool
}


func NewPlatformCounter(config PlatformCounterConfig) (*PlatformCounter, error){
	middleware, err := mw.ConnectToMiddleware()
	if err != nil {
		return nil, err
	}

	platformCounter := &PlatformCounter{
		config: config,
		stop: make(chan bool),
		count: prot.PlatformCount{},
	}	

	err = platformCounter.middlewareCounterInit()
	if err != nil {
		return nil, err
	}

	platformCounter.middleware = middleware
	return platformCounter, nil
}


func(p PlatformCounter) middlewareCounterInit() error {
	err := p.middleware.DeclareDirectQueue(games_to_count)
	if err != nil {
		return err
	}
	err = p.middleware.DeclareDirectQueue(count_acumulator)
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
		case <-ctx.Done() :
			p.stop <- true
			return
		default:
			p.middleware.ConsumeAndProcess(games_to_count, p.countgames, p.stop)
			err := p.sendResults()
			if err != nil {
				log.Error("Error sending results: %v", err)
				return
			}
			p.count = prot.PlatformCount{}
		}

	}
}


func (p *PlatformCounter) countgames(msg []byte) error {
		if string(msg) == "EOF" {
			p.stop <- true
			p.stop <- true
			return nil
		}

		game, err := prot.DeserializeGame(msg)
		if err != nil {
			return err
		}

		p.count.Increment(game.WindowsCompatible, game.LinuxCompatible, game.MacCompatible)

		log.Info("Counter: Windows: %d, Linux: %d, Mac: %d", p.count.Windows, p.count.Linux, p.count.Mac)

		return err
}



func (p *PlatformCounter) sendResults() error {
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