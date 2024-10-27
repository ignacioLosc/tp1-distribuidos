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
	middleware    *middleware.Middleware
	config        PercentileCalculatorConfig
	stop          chan bool
	gamesSavedMap map[string][]protocol.GameReviewCount
	finishedMap   map[string]int
}

func NewPercentileCalculator(config PercentileCalculatorConfig) (*PercentileCalculator, error) {
	ctx, cancel := context.WithCancel(context.Background())
	middleware, err := middleware.CreateMiddleware(ctx, cancel)
	if err != nil {
		log.Infof("Error connecting to middleware")
		return nil, err
	}
	controller := &PercentileCalculator{
		config:        config,
		middleware:    middleware,
		gamesSavedMap: make(map[string][]protocol.GameReviewCount),
		finishedMap:   make(map[string]int),
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
			if result.MsgError != nil {
				log.Errorf("Error consuming message", result.MsgError)
				continue
			}

			msg := result.Msg.Body
			clientId := result.Msg.Headers["clientId"].(string)

			err := p.accumulateGames(msg, clientId)
			if err != nil {
				result.Msg.Nack(false, false)
			} else {
				result.Msg.Ack(false)
			}
		}

	}
}

func (p *PercentileCalculator) calculatePercentile(clientId string) {
	games := p.gamesSavedMap[clientId]

	sort.Slice(games, func(i, j int) bool {
		return games[i].NegativeReviewCount < games[j].NegativeReviewCount
	})
	percentileIndex := len(games) * 9 / 10
	threshold := games[percentileIndex].PositiveReviewCount

	topGames := make([]protocol.GameReviewCount, 0)

	for _, game := range games {
		if game.PositiveReviewCount >= threshold {
			topGames = append(topGames, game)
		}
	}

	p.sendGames(topGames, clientId)
}

func (p *PercentileCalculator) sendGames(games []protocol.GameReviewCount, clientId string) {
	gamesBuffer := make([]byte, 8)
	l := len(games)
	binary.BigEndian.PutUint64(gamesBuffer, uint64(l))

	for _, game := range games {
		gameBuffer := protocol.SerializeGameReviewCount(&game)
		gamesBuffer = append(gamesBuffer, gameBuffer...)
	}

	p.middleware.PublishInExchange(communication, results_exchange, query_key, gamesBuffer)
}

func (p *PercentileCalculator) accumulateGames(msg []byte, clientId string) error {
	if string(msg) == "EOF" {
		log.Debug("Received EOF")
		p.finishedMap[clientId] = p.finishedMap[clientId] + 1
		if p.finishedMap[clientId] == p.config.NumJoiners {
			p.calculatePercentile(clientId)
			p.finishedMap[clientId] = 0
			delete(p.gamesSavedMap, clientId)
		}
		return nil
	}

	games := p.gamesSavedMap[clientId]

	game, err, _ := protocol.DeserializeGameReviewCount(msg)
	if err != nil {
		log.Errorf("Failed to deserialize games", err)
		return err
	}

	p.gamesSavedMap[clientId] = append(games, game)


	return nil
}
