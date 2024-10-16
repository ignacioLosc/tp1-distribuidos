package common

import (
	"context"
	"encoding/binary"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"syscall"

	"example.com/system/communication/middleware"
	"example.com/system/communication/protocol"
	prot "example.com/system/communication/protocol"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	top_5_partial_results = "top_5_partial_results"
	query_key             = "query3"
	results_exchange      = "results"
	control               = "control"
	communication         = "communication"
)

type AggregatorConfig struct {
	ServerPort string
	Id         string
	Top        string
}

type Aggregator struct {
	middleware    *middleware.Middleware
	config        AggregatorConfig
	stop          chan bool
	games         []protocol.GameReviewCount
	finishedCount int
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
		games:         make([]prot.GameReviewCount, 0),
		finishedCount: 0,
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

	_, err = c.middleware.DeclareDirectQueue(communication, top_5_partial_results)
	if err != nil {
		log.Errorf("Error declaring top_5_partial_results queue")
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
	log.Info("Starting game positiveReviewCount aggregator top 5")
	defer p.Close()

	go p.signalListener()

	msgChan := make(chan middleware.MsgResponse)
	go p.middleware.ConsumeFromQueue(communication, top_5_partial_results, msgChan)
	for {
		select {
		case <-p.middleware.Ctx.Done():
			log.Info("Received sigterm")
			return
		case result := <-msgChan:
			msg := result.Msg.Body
			err := p.aggregateGames(msg)
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

func (p *Aggregator) sendResults() {
	log.Infof("Resultado FINAL top 5:")
	for _, game := range p.games {
		log.Infof("Name: %s, positiveReviewCount: %d", game.AppName, game.PositiveReviewCount)
	}

	gamesBuffer := make([]byte, 8)
	l := len(p.games)
	binary.BigEndian.PutUint64(gamesBuffer, uint64(l))

	for _, game := range p.games {
		gameBuffer := protocol.SerializeGameReviewCount(&game)
		gamesBuffer = append(gamesBuffer, gameBuffer...)
	}
	p.middleware.PublishInExchange(communication, results_exchange, query_key, gamesBuffer)
}

func (p *Aggregator) shouldKeep(game prot.GameReviewCount, top int) (bool, error) {
	if len(p.games) < top {
		return true, nil
	} else if p.games[0].PositiveReviewCount < game.PositiveReviewCount {
		return true, nil
	} else {
		return false, nil
	}
}

func (p *Aggregator) saveGame(game prot.GameReviewCount, top int) error {
	if len(p.games) < top {
		p.games = append(p.games, game)
	} else {
		p.games = p.games[1:]
		p.games = append(p.games, game)
	}
	sort.Slice(p.games, func(i, j int) bool {
		return p.games[i].PositiveReviewCount < p.games[j].PositiveReviewCount
	})
	return nil
}

func (p *Aggregator) aggregateGames(msg []byte) error {
	if string(msg) == "EOF" {
		log.Info("Received EOF")
		p.finishedCount++
		top, err := strconv.Atoi(p.config.Top)
		if err != nil {
			log.Errorf("Failed to parse top number", err)
			return err
		}

		if p.finishedCount == top {
			p.sendResults()
		}

		return nil
	}
	lenGames := binary.BigEndian.Uint64(msg[:8])

	index := 8
	for i := 0; i < int(lenGames); i++ {
		game, err, j := protocol.DeserializeGameReviewCount(msg[index:])
		if err != nil {
			log.Errorf("Failed to deserialize games", err)
			return err
		}

		top, err := strconv.Atoi(p.config.Top)
		if err != nil {
			log.Errorf("Failed to parse top number", err)
			return err
		}

		shouldKeep, err := p.shouldKeep(game, top)
		if err != nil {
			log.Errorf("Error keeping games: ", err)
			return err
		}
		if shouldKeep {
			log.Info("Keeping game:", game.AppName, game.PositiveReviewCount, game.NegativeReviewCount, game.PositiveEnglishReviewCount)
			p.saveGame(game, top)
		}
		index += j

	}

	return nil
}
