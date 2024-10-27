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
	middleware    *middleware.Middleware
	config        SorterConfig
	stop          chan bool
	gamesSavedMap map[string][]prot.Game
}

func NewSorter(config SorterConfig) (*Sorter, error) {
	ctx, cancel := context.WithCancel(context.Background())
	middleware, err := middleware.CreateMiddleware(ctx, cancel)
	if err != nil {
		log.Infof("Error connecting to middleware")
		return nil, err
	}
	controller := &Sorter{
		config:        config,
		middleware:    middleware,
		gamesSavedMap:	make(map[string][]prot.Game),
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
			clientId := result.Msg.Headers["clientId"].(string)

			err := p.sortGames(msg, clientId)
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

func (p *Sorter) sendResults(clientId string) {
	games := p.gamesSavedMap[clientId]

	gamesBuffer := make([]byte, 8)
	l := len(p.gamesSavedMap)
	binary.BigEndian.PutUint64(gamesBuffer, uint64(l))

	for _, game := range games {
		gameBuffer := protocol.SerializeGame(&game)
		gamesBuffer = append(gamesBuffer, gameBuffer...)
	}
	p.middleware.PublishInExchange(communication, results_exchange, query_key, gamesBuffer)
}

func (p *Sorter) shouldKeep(game prot.Game, sortBy string, top int, clientId string) (bool, error) {
	games := p.gamesSavedMap[clientId]

	if sortBy == "timePlayed" {
		if len(games) < top {
			return true, nil
		} else if games[0].AveragePlaytimeForever < game.AveragePlaytimeForever {
			return true, nil
		} else {
			return false, nil
		}
	}

	return true, nil
}

func (p *Sorter) saveGame(game prot.Game, top int, clientId string) error {
	games := p.gamesSavedMap[clientId]

	if len(p.gamesSavedMap) < top {
		games = append(games, game)
	} else {
		games = games[1:]
		games = append(games, game)
	}

	sort.Slice(games, func(i, j int) bool {
		return games[i].AveragePlaytimeForever < games[j].AveragePlaytimeForever
	})

	p.gamesSavedMap[clientId] = games

	return nil
}

func (p *Sorter) sortGames(msg []byte, clientId string) error {
	if string(msg) == "EOF" {
		log.Debug("Received EOF")
		p.sendResults(clientId)
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

	shouldKeep, err := p.shouldKeep(game, sortBy, top, clientId)
	if err != nil {
		log.Errorf("Error keeping games: ", err)
		return err
	}
	if shouldKeep {
		p.saveGame(game, top, clientId)
	}

	return nil
}
