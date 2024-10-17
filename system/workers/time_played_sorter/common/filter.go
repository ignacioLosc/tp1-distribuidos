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
	results_exchange    = "results"
	query_key           = "query2"
	results_from_filter = "results_from_filter"
	filtered_games      = "filtered_games"

	control       = "control"
	communication = "communication"
)

type SorterConfig struct {
	ServerPort string
}

type Sorter struct {
	middleware *middleware.Middleware
	config     SorterConfig
	stop       chan bool
	games      []protocol.Game
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
		games:      make([]prot.Game, 0),
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

	err = c.middleware.DeclareExchange(communication, filtered_games, "topic")
	if err != nil {
		log.Errorf("Error declaring filtered_games exchange")
		return err
	}

	_, err = c.middleware.DeclareDirectQueue(communication, results_from_filter)
	if err != nil {
		log.Errorf("Error declaring results_from_filter queue")
		return err
	}

	err = c.middleware.BindQueueToExchange(communication, filtered_games, results_from_filter, "indie.2010.*")
	if err != nil {
		log.Errorf("Error binding queue to filtered_games exchange")
		return err
	}

	err = c.middleware.DeclareExchange(communication, results_exchange, "direct")
	if err != nil {
		log.Errorf("Error declaring results exchange")
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
	log.Info("Starting game sorter and top")
	defer p.Close()

	go p.signalListener()

	msgChan := make(chan middleware.MsgResponse)
	go p.middleware.ConsumeFromQueue(communication, results_from_filter, msgChan)
	for {
		select {
		case <-p.middleware.Ctx.Done():
			log.Info("Received sigterm")
			return
		case result := <-msgChan:
			msg := result.Msg.Body
			result.Msg.Ack(false)
			p.sortGames(msg)
		}

	}
}

type GameSummary struct {
	gameId                 string
	AveragePlaytimeForever int
}

func (p *Sorter) sendResults() {
	log.Infof("Resultado FINAL sort y top:")
	for _, game := range p.games {
		log.Infof("Name: %s, AvgPlaytime: %d", game.AppID, game.AveragePlaytimeForever)
	}

	gamesBuffer := make([]byte, 8)
	l := len(p.games)
	binary.BigEndian.PutUint64(gamesBuffer, uint64(l))

	for _, game := range p.games {
		gameBuffer := protocol.SerializeGame(&game)
		gamesBuffer = append(gamesBuffer, gameBuffer...)
	}
	p.middleware.PublishInExchange(communication, results_exchange, query_key, gamesBuffer)
}

func (p *Sorter) shouldKeep(game prot.Game, sortBy string, top int) (bool, error) {
	if sortBy == "timePlayed" {
		if len(p.games) < top {
			return true, nil
		} else if p.games[0].AveragePlaytimeForever < game.AveragePlaytimeForever {
			return true, nil
		} else {
			return false, nil
		}
	}
	return true, nil
}

func (p *Sorter) saveGame(game prot.Game, top int) error {
	if len(p.games) < top {
		p.games = append(p.games, game)
	} else {
		p.games = p.games[1:]
		p.games = append(p.games, game)
	}
	sort.Slice(p.games, func(i, j int) bool {
		return p.games[i].AveragePlaytimeForever < p.games[j].AveragePlaytimeForever
	})
	return nil
}

func (p *Sorter) sortGames(msg []byte) error {
	if string(msg) == "EOF" {
		log.Info("Received EOF %s")
		p.sendResults()
		return nil
	}

	game, err, _ := protocol.DeserializeGame(msg)

	if err != nil {
		log.Errorf("Failed to deserialize games", err)
		return err
	}

	// Can be set by config
	sortBy := "timePlayed"

	// Can be set by config
	top := 10

	shouldKeep, err := p.shouldKeep(game, sortBy, top)
	if err != nil {
		log.Errorf("Error keeping games: ", err)
		return err
	}
	if shouldKeep {
		log.Debug("Keeping game:", game.AppID, game.ReleaseDate, game.AveragePlaytimeForever, shouldKeep)
		p.saveGame(game, top)
	}

	return nil
}
