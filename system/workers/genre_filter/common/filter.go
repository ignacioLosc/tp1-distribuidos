package common

import (
	"context"
	"encoding/binary"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"example.com/system/communication/middleware"
	"example.com/system/communication/protocol"
	prot "example.com/system/communication/protocol"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	games_to_filter = "games_to_filter"
)

type GenreFilterConfig struct {
	ServerPort string
}

type GenreFilter struct {
	middleware *middleware.Middleware
	config     GenreFilterConfig
	stop       chan bool
}

func NewGenreFilter(config GenreFilterConfig) (*GenreFilter, error) {
	middleware, err := middleware.ConnectToMiddleware()
	if err != nil {
		return nil, err
	}
	controller := &GenreFilter{
		config:     config,
		middleware: middleware,
	}

	err = controller.middlewareInit()
	if err != nil {
		return nil, err
	}
	return controller, nil
}

func (c *GenreFilter) middlewareInit() error {
	err := c.middleware.DeclareDirectQueue("games")
	if err != nil {
		return err
	}
	err = c.middleware.DeclareDirectQueue(games_to_filter)
	if err != nil {
		return err
	}
	return nil
}

func (c *GenreFilter) Close() {
	c.middleware.Close()
}

func (c *GenreFilter) signalListener(cancel context.CancelFunc) {
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
	<-chSignal
	cancel()
}

func (p *GenreFilter) Start() {
	log.Info("Starting genre filter")
	defer p.Close()

	ctx, cancel := context.WithCancel(context.Background())

	go p.signalListener(cancel)

	for {
		select {
		case <-ctx.Done():
			p.stop <- true
			return
		default:
			p.middleware.ConsumeAndProcess(games_to_filter, p.filterGames, p.stop)
		}

	}
}

func parseDate(releaseDate string) (int, error) {
	return strconv.Atoi(releaseDate[len(releaseDate)-4:])
}

func shouldFilter(game prot.Game, filterBy string) (bool, error) {
	if filterBy == "decade" {
		decade, err := parseDate(game.ReleaseDate)
		if err != nil {
			log.Errorf("Failed to parse decade: %s", game.ReleaseDate)
			return false, err
		}
		return decade >= 2020 || decade < 2009, nil
	} else if filterBy == "genre.indie" {
		return strings.Contains(game.Genres, "Indie"), nil
	} else if filterBy == "genre.shooter" {
		return strings.Contains(game.Genres, "Shooter"), nil
	}
	return true, nil
}

func (p *GenreFilter) filterGames(msg []byte) error {
	if string(msg) == "EOF" {
		p.stop <- true
		p.stop <- true
		return nil
	}

	lenGames := binary.BigEndian.Uint64(msg[:8])
	games := make([]protocol.Game, 0)

	index := 8
	for i := 0; i < int(lenGames); i++ {
		game, err, j := protocol.DeserializeGame(msg[index:])

		if err != nil {
			log.Errorf("Failed to deserialize game: %s", err)
			continue
		}

		// Can be set by config
		filterBy := "decade"

		shouldFilter, err := shouldFilter(game, filterBy)
		if err != nil {
			return err
		}
		if !shouldFilter {
			log.Info("Sending game:", game.AppID, game.ReleaseDate, shouldFilter)
			games = append(games, game)
		}

		index += j
	}

	gamesBuffer := make([]byte, 8)
	l := len(games)
	binary.BigEndian.PutUint64(gamesBuffer, uint64(l))

	for _, game := range games {
		gameBuffer := protocol.SerializeGame(&game)
		gamesBuffer = append(gamesBuffer, gameBuffer...)
	}

	// Can receive queue name by config
	err := p.middleware.PublishInQueue("indie_games", gamesBuffer)
	if err != nil {
		return err
	}

	return nil
}
