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
)

type PercentileCalculatorConfig struct {
	ServerPort string
	Id         string
	Top        string
}

type PercentileCalculator struct {
	middleware *middleware.Middleware
	config     PercentileCalculatorConfig
	stop       chan bool
	games      []protocol.GameReviewCount
}

func NewPercentileCalculator(config PercentileCalculatorConfig) (*PercentileCalculator, error) {
	middleware, err := middleware.ConnectToMiddleware()
	if err != nil {
		log.Infof("Error connecting to middleware")
		return nil, err
	}
	controller := &PercentileCalculator{
		config:     config,
		middleware: middleware,
		games:      make([]prot.GameReviewCount, 0),
	}

	err = controller.middlewareInit()
	if err != nil {
		log.Errorf("Error initializing middleware")
		return nil, err
	}
	return controller, nil
}

func (c *PercentileCalculator) middlewareInit() error {
	err := c.middleware.DeclareExchange(results_exchange, "direct")
	if err != nil {
		log.Errorf("Error declaring results exchange")
		return err
	}

	_, err = c.middleware.DeclareDirectQueue(shooter_positive_joined_queue)
	if err != nil {
		log.Errorf("Error declaring action_negative_reviews queue")
		return err
	}

	return nil
}

func (c *PercentileCalculator) Close() {
	c.middleware.Close()
}

func (c *PercentileCalculator) signalListener(cancel context.CancelFunc) {
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
	<-chSignal
	cancel()
}

func (p *PercentileCalculator) Start() {
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
			p.middleware.ConsumeAndProcess(shooter_positive_joined_queue, p.accumulateGames)
			p.calculatePercentile()
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
	p.middleware.PublishInExchange(results_exchange, query_key, gamesBuffer)
}

func (p *PercentileCalculator) accumulateGames(msg []byte, finished *bool) error {
	if string(msg) == "EOF" {
		log.Info("Received EOF %s")
		*finished = true
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
