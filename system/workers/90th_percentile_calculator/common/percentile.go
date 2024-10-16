package common

import (
	"context"
	"encoding/binary"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"example.com/system/communication/middleware"
	"example.com/system/communication/protocol"
	prot "example.com/system/communication/protocol"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	shooter_positive_joined_queue = "joined_shooter_positive"
	query_key                     = "query5"
	results_exchange              = "results"

	control       = "control"
	communication = "communication"
)

type PercentileCalculatorConfig struct {
	ServerPort string
	Id         string
	NumJoiners int
}

type PercentileCalculator struct {
	middleware *middleware.Middleware
	config     PercentileCalculatorConfig
	stop       chan bool
	games      []protocol.GameReviewCount
	finished   int
}

func NewPercentileCalculator(config PercentileCalculatorConfig) (*PercentileCalculator, error) {
	ctx, cancel := context.WithCancel(context.Background())
	middleware, err := middleware.CreateMiddleware(ctx, cancel)
	if err != nil {
		log.Infof("Error connecting to middleware")
		return nil, err
	}
	controller := &PercentileCalculator{
		config:     config,
		middleware: middleware,
		games:      make([]prot.GameReviewCount, 0),
		finished:   0,
	}

	err = controller.middlewareInit()
	if err != nil {
		log.Errorf("Error initializing middleware")
		return nil, err
	}
	return controller, nil
}

func (c *PercentileCalculator) middlewareInit() error {
	err := c.middleware.DeclareChannel(communication)
	if err != nil {
		return err
	}

	err = c.middleware.DeclareChannel(control)
	if err != nil {
		return err
	}

	err = c.middleware.DeclareExchange(communication, results_exchange, "direct")
	if err != nil {
		log.Errorf("Error declaring results exchange")
		return err
	}

	_, err = c.middleware.DeclareDirectQueue(communication, shooter_positive_joined_queue)
	if err != nil {
		log.Errorf("Error declaring action_negative_reviews queue")
		return err
	}

	return nil
}

func (c *PercentileCalculator) Close() {
	c.middleware.Close()
}

func (c *PercentileCalculator) signalListener() {
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
	<-chSignal
	log.Infof("received signal")
	c.middleware.CtxCancel()
}

func (p *PercentileCalculator) Start() {
	log.Info("Starting game 5k reviews")
	defer p.Close()

	go p.signalListener()

	msgChan := make(chan middleware.MsgResponse)
	go p.middleware.ConsumeFromQueue(communication, shooter_positive_joined_queue, msgChan)
	for {
		select {
		case <-p.middleware.Ctx.Done():
			log.Info("Received sigterm")
			return
		case result := <-msgChan:
			msg := result.Msg.Body
			err := p.accumulateGames(msg)
			if err != nil {
				result.Msg.Nack(false, false)
			} else {
				result.Msg.Ack(false)
			}
		}

	}
}

func (p *PercentileCalculator) calculatePercentile() {
	sort.Slice(p.games, func(i, j int) bool {
		return p.games[i].NegativeReviewCount < p.games[j].NegativeReviewCount
	})
	percentileIndex := len(p.games) * 9 / 10
	threshold := p.games[percentileIndex].PositiveReviewCount

	topGames := make([]protocol.GameReviewCount, 0)

	for _, game := range p.games {
		if game.PositiveReviewCount >= threshold {
			topGames = append(topGames, game)
		}
	}
	p.sendGames(topGames)
}

func (p *PercentileCalculator) sendGames(games []protocol.GameReviewCount) {
	gamesBuffer := make([]byte, 8)
	l := len(games)
	binary.BigEndian.PutUint64(gamesBuffer, uint64(l))

	for _, game := range games {
		gameBuffer := protocol.SerializeGameReviewCount(&game)
		gamesBuffer = append(gamesBuffer, gameBuffer...)
	}
	p.middleware.PublishInExchange(communication, results_exchange, query_key, gamesBuffer)
}

func (p *PercentileCalculator) accumulateGames(msg []byte) error {
	if string(msg) == "EOF" {
		log.Info("Received EOF")
		p.finished++
		if p.finished == p.config.NumJoiners {
			p.calculatePercentile()
			p.finished = 0
		}
		return nil
	}

	game, err, _ := protocol.DeserializeGameReviewCount(msg)
	if err != nil {
		log.Errorf("Failed to deserialize games", err)
		return err
	}

	p.games = append(p.games, game)

	return nil
}
