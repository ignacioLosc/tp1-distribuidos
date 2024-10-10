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
)

type AggregatorConfig struct {
	ServerPort string
	Id         string
	Top        string
}

type Aggregator struct {
	middleware *middleware.Middleware
	config     AggregatorConfig
	stop       chan bool
	finished   int
	games      []protocol.GameReviewCount
}

func NewAggregator(config AggregatorConfig) (*Aggregator, error) {
	middleware, err := middleware.ConnectToMiddleware()
	if err != nil {
		log.Infof("Error connecting to middleware")
		return nil, err
	}
	controller := &Aggregator{
		config:     config,
		middleware: middleware,
		games:      make([]prot.GameReviewCount, 0),
		finished: 	0,
	}

	err = controller.middlewareInit()
	if err != nil {
		log.Errorf("Error initializing middleware")
		return nil, err
	}
	return controller, nil
}

func (c *Aggregator) middlewareInit() error {
	err := c.middleware.DeclareExchange(results_exchange, "direct")
	if err != nil {
		log.Errorf("Error declaring results exchange")
		return err
	}

	_, err = c.middleware.DeclareDirectQueue(action_negative_reviews)
	if err != nil {
		log.Errorf("Error declaring action_negative_reviews queue")
		return err
	}

	return nil
}

func (c *Aggregator) Close() {
	c.middleware.Close()
}

func (c *Aggregator) signalListener(cancel context.CancelFunc) {
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
	<-chSignal
	cancel()
}

func (p *Aggregator) Start() {
	log.Info("Starting game 5k reviews")
	defer p.Close()

	ctx, cancel := context.WithCancel(context.Background())

	go p.signalListener(cancel)

	for {
		select {
		case <-ctx.Done():
			log.Info("Received sigterm")
			return
		default:
			p.middleware.ConsumeAndProcess(action_negative_reviews, p.filterGames)
			p.sendGames()
			p.finished = 0
		}

	}
}

type GameSummary struct {
	gameId                 string
	AveragePlaytimeForever int
}

func (p *Aggregator) sendGames() {
	gamesBuffer := make([]byte, 8)
	l := len(p.games)
	binary.BigEndian.PutUint64(gamesBuffer, uint64(l))

	for _, game := range p.games {
		gameBuffer := protocol.SerializeGameReviewCount(&game)
		gamesBuffer = append(gamesBuffer, gameBuffer...)
	}
	p.middleware.PublishInExchange(results_exchange, query_key, gamesBuffer)
}

func (p *Aggregator) shouldKeep(game prot.GameReviewCount) (bool, error) {
	if game.PositiveEnglishReviewCount > 5000 {
		return true, nil
	} else {
		return false, nil
	}
}

func (p *Aggregator) filterGames(msg []byte, finished *bool) error {
	if string(msg) == "EOF" {
		p.finished++
		if p.finished == 5 {
			*finished = true
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
		log.Info("Keeping game:", game.AppName, game.PositiveReviewCount, game.NegativeReviewCount, game.PositiveEnglishReviewCount)
		p.games = append(p.games, game)
	}

	return nil
}
