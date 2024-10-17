package common

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"example.com/system/communication/middleware"
	mw "example.com/system/communication/middleware"
	prot "example.com/system/communication/protocol"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	count_acumulator = "count_accumulator"
	query_answer1    = "query_answer_1"
	results_exchange = "results"
	query_key        = "query1"
	control          = "control"
	communication    = "communication"
)

type PlatformAccumulatorConfig struct {
	ServerPort  string
	NumCounters int
}

type PlatformAccumulator struct {
	config     PlatformAccumulatorConfig
	middleware *mw.Middleware
	count      prot.PlatformCount
	stop       chan bool
	finished   int
}

func NewPlatformAccumulator(config PlatformAccumulatorConfig) (*PlatformAccumulator, error) {
	ctx, cancel := context.WithCancel(context.Background())
	middleware, err := mw.CreateMiddleware(ctx, cancel)
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
	err := p.middleware.DeclareChannel(communication)
	if err != nil {
		return err
	}

	err = p.middleware.DeclareChannel(control)
	if err != nil {
		return err
	}

	_, err = p.middleware.DeclareDirectQueue(communication, count_acumulator)
	if err != nil {
		return err
	}

	err = p.middleware.DeclareExchange(communication, results_exchange, "direct")
	if err != nil {
		return err
	}

	_, err = p.middleware.DeclareDirectQueue(communication, query_answer1)
	if err != nil {
		return err
	}

	return nil
}

func (p *PlatformAccumulator) Close() {
	p.middleware.Close()
}

func (p *PlatformAccumulator) signalListener() {
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
	<-chSignal
	log.Infof("received signal")
	p.middleware.CtxCancel()
}

func (p *PlatformAccumulator) Start() {
	defer p.Close()

	go p.signalListener()
	msgChan := make(chan middleware.MsgResponse)
	go p.middleware.ConsumeFromQueue(communication, count_acumulator, msgChan)

	for {
		select {
		case <-p.middleware.Ctx.Done():
			return
		case result := <-msgChan:
			msg := result.Msg.Body
			err := p.countGames(msg)
			if err != nil {
				result.Msg.Nack(false, false)
			} else {
				result.Msg.Ack(false)
			}
		}

	}
}

func (p *PlatformAccumulator) countGames(msg []byte) error {
	if string(msg) == "EOF" {
		p.finished += 1
		if p.finished == p.config.NumCounters {
			log.Infof("Resultado FINAL: Windows: %d, Linux: %d, Mac: %d", p.count.Windows, p.count.Linux, p.count.Mac)
			p.middleware.PublishInExchange(communication, results_exchange, query_key, p.count.Serialize())
			p.count.Linux = 0
			p.count.Mac = 0
			p.count.Windows = 0
		}
		return nil
	}

	counter, err := prot.DeserializeCounter(msg)
	if err != nil {
		log.Errorf("Error deserializing counter: %v :", err, msg)
		return err
	}
	p.count.IncrementVals(counter.Windows, counter.Linux, counter.Mac)

	log.Debugf("Counter Resultado PARCIAL : Windows: %d, Linux: %d, Mac: %d", p.count.Windows, p.count.Linux, p.count.Mac)

	return nil
}
