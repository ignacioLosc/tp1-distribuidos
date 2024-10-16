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
	joined_reviews_indies = "joined_reviews_indies"
	top_5_partial_results = "top_5_partial_results"
	control               = "control"
	communication         = "communication"
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
	ctx, cancel := context.WithCancel(context.Background())
	middleware, err := middleware.CreateMiddleware(ctx, cancel)
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
	err := c.middleware.DeclareChannel(communication)
	if err != nil {
		return err
	}

	err = c.middleware.DeclareChannel(control)
	if err != nil {
		return err
	}

	_, err = c.middleware.DeclareDirectQueue(communication, joined_reviews_indies)
	if err != nil {
		log.Errorf("Error declaring joined_reviews_indies queue")
		return err
	}

	_, err = c.middleware.DeclareDirectQueue(communication, top_5_partial_results)
	if err != nil {
		log.Errorf("Error declaring top_5_partial_results queue")
		return err
	}

	return nil
}

func (c *Sorter) Close() {
	c.middleware.Close()
}

func (c *Sorter) signalListener() {
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
	<-chSignal
	log.Infof("received signal")
	c.middleware.CtxCancel()
}

func (p *Sorter) Start() {
	log.Info("Starting game positiveReviewCount sorter and top 5")
	defer p.Close()

	go p.signalListener()

	msgChan := make(chan middleware.MsgResponse)
	go p.middleware.ConsumeFromQueue(communication, joined_reviews_indies, msgChan)
	for {
		select {
		case <-p.middleware.Ctx.Done():
			log.Info("Received sigterm")
			return
		case result := <-msgChan:
			msg := result.Msg.Body
			err := p.sortGames(msg)
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

func (p *Sorter) sendResults() {
	log.Infof("Resultado PARCIAL sort y top %s:", p.config.Top)
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
	p.middleware.PublishInQueue(communication, top_5_partial_results, gamesBuffer)
	p.middleware.PublishInQueue(communication, top_5_partial_results, []byte("EOF"))
}

func (p *Sorter) shouldKeep(game prot.GameReviewCount, sortBy string, top int) (bool, error) {
	if sortBy == "positiveReviewCount" {
		if len(p.games) < top {
			return true, nil
		} else if p.games[0].PositiveReviewCount < game.PositiveReviewCount {
			return true, nil
		} else {
			log.Info("Discarding game %d, current lowest %d", game.PositiveReviewCount, p.games[0].PositiveReviewCount)
			return false, nil
		}
	}
	return false, nil
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

func (p *Sorter) sortGames(msg []byte) error {
	if string(msg) == "EOF" {
		log.Info("Received EOF")
		p.sendResults()
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
