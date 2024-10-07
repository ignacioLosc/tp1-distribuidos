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
	count_acumulator = "count_accumulator"
	query_answer1 = "query_answer_1"
)

type PlatformAccumulatorConfig struct {
	ServerPort string
	Peers int
}


type PlatformAccumulator struct {
	config PlatformAccumulatorConfig
	middleware *mw.Middleware
	count prot.PlatformCount
	stop chan bool
	finished int
}


func NewPlatformAccumulator(config PlatformAccumulatorConfig) (*PlatformAccumulator, error){

	middleware, err := mw.ConnectToMiddleware()
	if err != nil {
		return nil, err
	}

	platformCounter := &PlatformAccumulator{
		config: config,
		stop: make(chan bool),
		count: prot.PlatformCount{},
	}	

	err = platformCounter.middlewareAccumulatorInit()
	if err != nil {
		return nil, err
	}

	platformCounter.middleware = middleware
	return platformCounter, nil
}

func(p PlatformAccumulator) middlewareAccumulatorInit() error {
	err := p.middleware.DeclareDirectQueue(count_acumulator)
	if err != nil {
		return err
	}

	err = p.middleware.DeclareDirectQueue(query_answer1)
	if err != nil {
		return err
	}
	return nil
}

func (p *PlatformAccumulator) Close() {
	p.middleware.Close()
}


func (p *PlatformAccumulator) signalListener(cancel context.CancelFunc) {
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
	<-chSignal
	cancel()
}

func (p *PlatformAccumulator) Start() {
	defer p.Close()

	ctx, cancel := context.WithCancel(context.Background())

	go p.signalListener(cancel)

	for { 
		select {
		case <-ctx.Done() :
			p.stop <- true
			return
		default:
			p.middleware.ConsumeAndProcess(count_acumulator, p.countgames, p.stop)
		}

	}
}

func (p *PlatformAccumulator) countgames(msg []byte) error {
	if string(msg) == "EOF" {
		p.finished += 1
		return nil
	}

	if p.finished == p.config.Peers {
		p.stop <- true
	}

	game, err := prot.DeserializeGame(msg)
	if err != nil {
		return err
	}

	p.count.Increment(game.WindowsCompatible, game.LinuxCompatible, game.MacCompatible)

	log.Info("Counter: Windows: %d, Linux: %d, Mac: %d", p.count.Windows, p.count.Linux, p.count.Mac)

	// Faltaria enviar a respuesta a query_answer 1 cada vez que se recibe una suma

	return nil
}

