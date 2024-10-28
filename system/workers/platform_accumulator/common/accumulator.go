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
	countMap   map[string]prot.PlatformCount
	finishedMap map[string]int
	stop       chan bool
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
		countMap:   make(map[string]prot.PlatformCount),
		middleware: middleware,
		finishedMap:  make(map[string]int),
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

	err = p.middleware.DeclareExchange(communication, results_exchange, "topic")
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
			clientId := result.Msg.Headers["clientId"].(string)

			msg := result.Msg.Body
			err := p.countGames(msg, clientId)
			if err != nil {
				result.Msg.Nack(false, false)
			} else {
				result.Msg.Ack(false)
			}
		}

	}
}

func (p *PlatformAccumulator) countGames(msg []byte, clientId string) error {
	counterAccu := p.countMap[clientId]

	if string(msg) == "EOF" {
		log.Debugf("Received EOF")
		p.finishedMap[clientId] = p.finishedMap[clientId] + 1
		if p.finishedMap[clientId] == p.config.NumCounters {
			p.middleware.PublishInExchange(communication, results_exchange, clientId+"."+query_key, counterAccu.Serialize(), clientId)
			p.countMap[clientId] = prot.PlatformCount{}
		}
		return nil
	}

	counter, err := prot.DeserializeCounter(msg)
	if err != nil {
		log.Errorf("Error deserializing counter: %v :", err, msg)
		return err
	}
	counterAccu.IncrementVals(counter.Windows, counter.Linux, counter.Mac)
	p.countMap[clientId] = counterAccu

	return nil
}
