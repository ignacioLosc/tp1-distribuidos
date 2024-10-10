package common

import (
	"context"
	"encoding/binary"
	"fmt"
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
	filtered_games  = "filtered_games"
	LEN_JOINERS     = 5
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
	_, err := c.middleware.DeclareDirectQueue(games_to_filter)
	if err != nil {
		return err
	}

	err = c.middleware.DeclareExchange(filtered_games, "topic")
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
			log.Info("Received sigterm")
			return
		default:
			p.middleware.ConsumeAndProcess(games_to_filter, p.filterGames)
		}
	}
}

func parseDate(releaseDate string) (int, error) {
	return strconv.Atoi(releaseDate[len(releaseDate)-4:])
}

func (p *GenreFilter) filterGame(game prot.Game) error {
	year, err := parseDate(game.ReleaseDate)
	if err != nil {
		log.Errorf("Failed to parse decade: %s", game.ReleaseDate)
		return err
	}

	decade := strconv.Itoa(year - (year % 10))

	appId, err := strconv.Atoi(game.AppID)
	if err != nil {
		return err
	}

	appIdRange := strconv.Itoa(appId % LEN_JOINERS)

	if strings.Contains(game.Genres, "Indie") {
		t := fmt.Sprintf("indie.%s.%s", decade, appIdRange)
		err = p.middleware.PublishInExchange(filtered_games, t, prot.SerializeGame(&game))
		if err != nil {
			return err
		}
	} else if strings.Contains(game.Genres, "Shooter") {
		t := fmt.Sprintf("shooter.%s.%s", decade, appIdRange)
		err = p.middleware.PublishInExchange(filtered_games, t, prot.SerializeGame(&game))
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *GenreFilter) filterGames(msg []byte, finished *bool) error {
	if string(msg) == "EOF" {
		*finished = true
		log.Infof("Sending games EOF")
		err := p.middleware.PublishInExchange(filtered_games, "shooter.*.0", []byte("EOF"))
		fmt.Println(err)
		err = p.middleware.PublishInExchange(filtered_games, "shooter.*.1", []byte("EOF"))
		fmt.Println(err)
		err = p.middleware.PublishInExchange(filtered_games, "shooter.*.2", []byte("EOF"))
		fmt.Println(err)
		err = p.middleware.PublishInExchange(filtered_games, "shooter.*.3", []byte("EOF"))
		fmt.Println(err)
		err = p.middleware.PublishInExchange(filtered_games, "shooter.*.4", []byte("EOF"))
		fmt.Println(err)
		err = p.middleware.PublishInExchange(filtered_games, "indie.*.0", []byte("EOF"))
		fmt.Println(err)
		err = p.middleware.PublishInExchange(filtered_games, "indie.*.1", []byte("EOF"))
		fmt.Println(err)
		err = p.middleware.PublishInExchange(filtered_games, "indie.*.2", []byte("EOF"))
		fmt.Println(err)
		err = p.middleware.PublishInExchange(filtered_games, "indie.*.3", []byte("EOF"))
		fmt.Println(err)
		err = p.middleware.PublishInExchange(filtered_games, "indie.*.4", []byte("EOF"))
		fmt.Println(err)
		err = p.middleware.PublishInExchange(filtered_games, "indie.2010.*", []byte("EOF"))
		fmt.Println(err)
		return err
	}

	lenGames := binary.BigEndian.Uint64(msg[:8])

	index := 8
	for i := 0; i < int(lenGames); i++ {
		game, err, j := protocol.DeserializeGame(msg[index:])

		if err != nil {
			log.Errorf("Failed to deserialize game: %s", err)
			continue
		}

		err = p.filterGame(game)
		if err != nil {
			return err
		}

		index += j
	}

	return nil
}
