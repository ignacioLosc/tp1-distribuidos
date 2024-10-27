package common

import (
	"context"
	"encoding/binary"
	"os"
	"os/signal"
	"syscall"

	"example.com/system/communication/middleware"
	"example.com/system/communication/protocol"
	prot "example.com/system/communication/protocol"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	action_negative_reviews = "joined_shooter_negative"
	query_key               = "query4"
	results_exchange        = "results"
	control                 = "control"
	communication           = "communication"
)

type AggregatorConfig struct {
	ServerPort string
	Id         string
	NumJoiners int
}

type Aggregator struct {
	middleware    *middleware.Middleware
	config        AggregatorConfig
	stop          chan bool
	finishedMap   map[string]int
	gamesSavedMap map[string][]protocol.GameReviewCount
}

func NewAggregator(config AggregatorConfig) (*Aggregator, error) {
	ctx, cancel := context.WithCancel(context.Background())
	middleware, err := middleware.CreateMiddleware(ctx, cancel)
	if err != nil {
		log.Infof("Error connecting to middleware")
		return nil, err
	}
	controller := &Aggregator{
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

func (c *Aggregator) middlewareInit() error {
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

	_, err = c.middleware.DeclareDirectQueue(communication, action_negative_reviews)
	if err != nil {
		log.Errorf("Error declaring action_negative_reviews queue")
		return err
	}

	return nil
}

func (c *Aggregator) Close() {
	c.middleware.Close()
}

func (c *Aggregator) signalListener() {
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
	<-chSignal
	log.Infof("received signal")
	c.middleware.CtxCancel()
}

func (p *Aggregator) Start() {
	defer p.Close()

	go p.signalListener()

	msgChan := make(chan middleware.MsgResponse)
	go p.middleware.ConsumeFromQueue(communication, action_negative_reviews, msgChan)
	for {
		select {
		case <-p.middleware.Ctx.Done():
			log.Info("Received sigterm")
			return
		case result := <-msgChan:
			msg := result.Msg.Body
			clientId := result.Msg.Headers["clientId"].(string)

			err := p.filterGames(msg, clientId)
			if err != nil {
				result.Msg.Nack(false, false)
			} else {
				result.Msg.Ack(false)
			}
		}

	}
}

type GameSummary struct {
	gameId                 string
	AveragePlaytimeForever int
}

func (p *Aggregator) sendGames(clientId string) {
	games := p.gamesSavedMap[clientId]

	gamesBuffer := make([]byte, 8)
	l := len(games)
	binary.BigEndian.PutUint64(gamesBuffer, uint64(l))

	for _, game := range games {
		gameBuffer := protocol.SerializeGameReviewCount(&game)
		gamesBuffer = append(gamesBuffer, gameBuffer...)
	}
	p.middleware.PublishInExchange(communication, results_exchange, query_key, gamesBuffer)
}

func (p *Aggregator) shouldKeep(game prot.GameReviewCount) (bool, error) {
	if game.PositiveEnglishReviewCount > 5000 {
		return true, nil
	} else {
		return false, nil
	}
}

func (p *Aggregator) filterGames(msg []byte, clientId string) error {
	if string(msg) == "EOF" {
		log.Debug("Received EOF")
		p.finishedMap[clientId] = p.finishedMap[clientId] + 1
		if p.finishedMap[clientId] == p.config.NumJoiners {
			p.sendGames(clientId)
			p.finishedMap[clientId] = 0
			delete(p.gamesSavedMap, clientId)
		}
		return nil
	}

	game, err, _ := protocol.DeserializeGameReviewCount(msg)
	if err != nil {
		log.Errorf("Failed to deserialize games", err)
		return err
	}

	shouldKeep, err := p.shouldKeep(game)
	if err != nil {
		log.Errorf("Error keeping games: ", err)
		return err
	}
	if shouldKeep {
		games := p.gamesSavedMap[clientId]
		p.gamesSavedMap[clientId] = append(games, game)
	}

	return nil
}
