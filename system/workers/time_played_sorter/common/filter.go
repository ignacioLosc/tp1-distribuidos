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
	middleware, err := middleware.ConnectToMiddleware()
	if err != nil {
		return nil, err
	}
	controller := &Sorter{
		config:     config,
		middleware: middleware,
		games:      make([]prot.Game, 0),
	}

	err = controller.middlewareInit()
	if err != nil {
		return nil, err
	}
	return controller, nil
}

func (c *Sorter) middlewareInit() error {
	err := c.middleware.DeclareExchange(filtered_games, "topic")
	if err != nil {
		return err
	}
	err = c.middleware.DeclareDirectQueue(results_from_filter)
	if err != nil {
		return err
	}
	err = c.middleware.BindQueueToExchange(filtered_games, results_from_filter, "indie.10.*")
	if err != nil {
		return err
	}

	err = c.middleware.DeclareExchange(results_exchange, "direct")
	if err != nil {
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
	log.Info("Starting game sorter and top")
	defer p.Close()

	ctx, cancel := context.WithCancel(context.Background())

	go p.signalListener(cancel)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			p.middleware.ConsumeAndProcess(results_from_filter, p.sortGames)
			p.sendResults()
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
		log.Infof("%d %d", game.AppID, game.AveragePlaytimeForever)
	}

	gamesBuffer := make([]byte, 8)
	l := len(p.games)
	binary.BigEndian.PutUint64(gamesBuffer, uint64(l))

	for _, game := range p.games {
		gameBuffer := protocol.SerializeGame(&game)
		gamesBuffer = append(gamesBuffer, gameBuffer...)
	}
	p.middleware.PublishInExchange(results_exchange, query_key, gamesBuffer)
}

func (p *Sorter) shouldKeep(game prot.Game, sortBy string) (bool, error) {
	if sortBy == "timePlayed" {
		if game.AveragePlaytimeForever < p.games[0].AveragePlaytimeForever {
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

func (p *Sorter) sortGames(msg []byte, _ *bool) error {
	if string(msg) == "EOF" {
		return nil
	}

	lenGames := binary.BigEndian.Uint64(msg[:8])

	index := 8
	for i := 0; i < int(lenGames); i++ {
		game, err, j := protocol.DeserializeGame(msg[index:])

		if err != nil {
			log.Errorf("Failed to deserialize game: %s", err)
			continue
		}

		// Can be set by config
		sortBy := "timePlayed"

		// Can be set by config
		top := 10

		shouldKeep, err := p.shouldKeep(game, sortBy)
		if err != nil {
			return err
		}
		if shouldKeep {
			log.Info("Keeping game:", game.AppID, game.ReleaseDate, shouldKeep)
			p.saveGame(game, top)
		}
		index += j
	}

	return nil
}
