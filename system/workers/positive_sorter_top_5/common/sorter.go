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
	game_positive_reviews = "joined_reviews_indies"
	top_5_partial_results = "top_5_partial_results"
)

type SorterConfig struct {
	ServerPort string
	Id         string
	Top        string
}

type Sorter struct {
	middleware *middleware.Middleware
	config     SorterConfig
	stop       chan bool
	games      []protocol.GameReviewCount
}

func NewSorter(config SorterConfig) (*Sorter, error) {
	middleware, err := middleware.ConnectToMiddleware()
	if err != nil {
		log.Infof("Error connecting to middleware")
		return nil, err
	}
	controller := &Sorter{
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

func (c *Sorter) middlewareInit() error {
	_, err := c.middleware.DeclareDirectQueue(game_positive_reviews)
	if err != nil {
		log.Errorf("Error declaring game_positive_reviews queue")
		return err
	}

	_, err = c.middleware.DeclareDirectQueue(top_5_partial_results)
	if err != nil {
		log.Errorf("Error declaring top_5_partial_results queue")
		return err
	}

	return nil
}

func (c *Sorter) Close() {
	c.middleware.Close()
}

func (c *Sorter) signalListener(cancel context.CancelFunc) {
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
	<-chSignal
	cancel()
}

func (p *Sorter) Start() {
	log.Info("Starting game positiveReviewCount sorter and top 5")
	defer p.Close()

	ctx, cancel := context.WithCancel(context.Background())

	go p.signalListener(cancel)

	for {
		select {
		case <-ctx.Done():
			log.Info("Received sigterm")
			return
		default:
			p.middleware.ConsumeAndProcess(game_positive_reviews, p.sortGames)
			p.sendResults()
		}

	}
}

type GameSummary struct {
	gameId                 string
	AveragePlaytimeForever int
}

func (p *Sorter) sendResults() {
	log.Infof("Resultado FINAL sort y top 5:")
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
	p.middleware.PublishInQueue(top_5_partial_results, gamesBuffer)
}

func (p *Sorter) shouldKeep(game prot.GameReviewCount, sortBy string, top int) (bool, error) {
	if sortBy == "positiveReviewCount" {
		if len(p.games) < top {
			return true, nil
		} else if p.games[0].PositiveReviewCount < game.PositiveReviewCount {
			return true, nil
		} else {
			return false, nil
		}
	}
	return true, nil
}

func (p *Sorter) saveGame(game prot.GameReviewCount, top int) error {
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

func (p *Sorter) sortGames(msg []byte, finished *bool) error {
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

	// Can be set by config
	sortBy := "positiveReviewCount"

	top, err := strconv.Atoi(p.config.Top)
	if err != nil {
		log.Errorf("Failed to parse top number", err)
		return err
	}

	shouldKeep, err := p.shouldKeep(game, sortBy, top)
	if err != nil {
		log.Errorf("Error keeping games: ", err)
		return err
	}
	if shouldKeep {
		log.Info("Keeping game:", game.AppName, game.PositiveReviewCount, game.NegativeReviewCount, game.PositiveEnglishReviewCount)
		p.saveGame(game, top)
	}

	return nil
}
