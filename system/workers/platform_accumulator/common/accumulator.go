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
	query_answer1    = "query_answer_1"
)

type PlatformAccumulatorConfig struct {
	ServerPort string
	Peers      int
}

type PlatformAccumulator struct {
	config     PlatformAccumulatorConfig
	middleware *mw.Middleware
	count      prot.PlatformCount
	stop       chan bool
	finished   int
}

func NewPlatformAccumulator(config PlatformAccumulatorConfig) (*PlatformAccumulator, error) {

	middleware, err := mw.ConnectToMiddleware()
	if err != nil {
		return nil, err
	}

	platformCounter := &PlatformAccumulator{
		config:     config,
		stop:       make(chan bool),
		count:      prot.PlatformCount{},
		middleware: middleware,
		finished:   0,
	}

	err = platformCounter.middlewareAccumulatorInit()
	if err != nil {
		return nil, err
	}

	platformCounter.middleware = middleware
	return platformCounter, nil
}

func (p PlatformAccumulator) middlewareAccumulatorInit() error {
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
		case <-ctx.Done():
			return
		default:
			p.middleware.ConsumeAndProcess(count_acumulator, p.countGames)
		}

	}
}

func (p *PlatformAccumulator) countGames(msg []byte, _ *bool) error {
	if string(msg) == "EOF" {
		p.finished += 1
	}

	// TODO: SACAR CONSTANTE DE ACA Y PONERLA EN CONFIG
	if p.finished == 1 {
		log.Info("Resultado FINAL: Windows: %d, Linux: %d, Mac: %d", p.count.Windows, p.count.Linux, p.count.Mac)

		return nil
	}

	log.Info(msg)

	counter, err := prot.DeserializeCounter(msg)
	if err != nil {
		log.Error("Error deserializing counter: %v", err)
		return err
	}
	p.count.IncrementVals(counter.Windows, counter.Linux, counter.Mac)

	log.Info("Counter Resultado PARCIAL : Windows: %d, Linux: %d, Mac: %d", p.count.Windows, p.count.Linux, p.count.Mac)

	return nil
}
