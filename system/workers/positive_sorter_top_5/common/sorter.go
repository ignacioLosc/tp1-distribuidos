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
	joined_reviews_indies = "joined_reviews_indies"
	top_5_partial_results = "top_5_partial_results"
	control               = "control"
	communication         = "communication"
)

type SorterConfig struct {
	ServerPort string
	Id         string
	Top        int
	NumJoiners int
}

type Sorter struct {
	middleware    *middleware.Middleware
	config        SorterConfig
	stop          chan bool
	gamesSavedMap map[string][]prot.GameReviewCount
	eofJoinersMap   map[string]int
	finishedClients   map[string]bool
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
		gamesSavedMap: make(map[string][]prot.GameReviewCount),
		eofJoinersMap:   make(map[string]int),
		finishedClients:   make(map[string]bool),
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
			clientId := result.Msg.Headers["clientId"].(string)

			if p.finishedClients[clientId] {
				result.Msg.Nack(false, true)
				continue
			}

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
	games, ok := p.gamesSavedMap[clientId]
	if !ok {
		p.gamesSavedMap[clientId] = make([]prot.GameReviewCount, 0)
	}

	for _, game := range games {
		log.Debugf("Name: %s, positiveReviewCount: %d", game.AppName, game.PositiveReviewCount)
	}

	gamesBuffer := make([]byte, 8)
	l := len(games)
	binary.BigEndian.PutUint64(gamesBuffer, uint64(l))

	for _, game := range games {
		gameBuffer := protocol.SerializeGameReviewCount(&game)
		gamesBuffer = append(gamesBuffer, gameBuffer...)
	}
	p.middleware.PublishInQueue(communication, top_5_partial_results, gamesBuffer)
	p.middleware.PublishInQueue(communication, top_5_partial_results, []byte("EOF"))
}

func (p *Sorter) shouldKeep(game prot.GameReviewCount, sortBy string, top int, clientId string) (bool, error) {
	games, ok := p.gamesSavedMap[clientId]
	if !ok {
		p.gamesSavedMap[clientId] = make([]prot.GameReviewCount, 0)
	}

	if sortBy == "positiveReviewCount" {
		if len(games) < top {
			return true, nil
		} else if games[0].PositiveReviewCount < game.PositiveReviewCount {
			return true, nil
		} else {
			return false, nil
		}
	}
	return false, nil
}

func (p *Sorter) saveGame(game prot.GameReviewCount, top int, clientId string) error {
	games, ok := p.gamesSavedMap[clientId]
	if !ok {
		p.gamesSavedMap[clientId] = make([]prot.GameReviewCount, 0)
	}

	if len(games) < top {
		games = append(games, game)
	} else {
		games = games[1:]
		games = append(games, game)
	}
	sort.Slice(games, func(i, j int) bool {
		return games[i].PositiveReviewCount < games[j].PositiveReviewCount
	})

	p.gamesSavedMap[clientId] = games
	return nil
}

func (p *Sorter) sortGames(msg []byte, clientId string) error {
	if string(msg) == "EOF" {
		log.Debug("Received EOF")
		p.eofJoinersMap[clientId] = p.eofJoinersMap[clientId] + 1
		if p.eofJoinersMap[clientId] == p.config.NumJoiners {
			p.sendResults(clientId)
			p.eofJoinersMap[clientId] = 0
			delete(p.gamesSavedMap, clientId)
			p.finishedClients[clientId] = true
		}
		return nil
	}

	game, err, _ := protocol.DeserializeGameReviewCount(msg)

	if err != nil {
		log.Errorf("Failed to deserialize games", err)
		return err
	}

	// Can be set by config
	sortBy := "positiveReviewCount"

	shouldKeep, err := p.shouldKeep(game, sortBy, p.config.Top, clientId)
	if err != nil {
		log.Errorf("Error keeping games: ", err)
		return err
	}

	if shouldKeep {
		p.saveGame(game, p.config.Top, clientId)
	}

	return nil
}
