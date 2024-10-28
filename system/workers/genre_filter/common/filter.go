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
	"example.com/system/communication/utils"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	games_to_filter = "games_to_filter"
	filtered_games  = "filtered_games"

	control       = "control"
	communication = "communication"
)

type GenreFilterConfig struct {
	ServerPort string
	NumJoiners int
}

type GenreFilter struct {
	middleware *middleware.Middleware
	config     GenreFilterConfig
	stop       chan bool
}

func NewGenreFilter(config GenreFilterConfig) (*GenreFilter, error) {
	ctx, cancel := context.WithCancel(context.Background())
	middleware, err := middleware.CreateMiddleware(ctx, cancel)
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
	err := c.middleware.DeclareChannel(communication)
	if err != nil {
		return err
	}

	err = c.middleware.DeclareChannel(control)
	if err != nil {
		return err
	}

	_, err = c.middleware.DeclareDirectQueue(communication, games_to_filter)
	if err != nil {
		return err
	}

	err = c.middleware.DeclareExchange(communication, filtered_games, "topic")
	if err != nil {
		return err
	}
	return nil
}

func (c *GenreFilter) Close() {
	c.middleware.Close()
}

func (c *GenreFilter) signalListener() {
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
	<-chSignal
	log.Infof("received signal")
	c.middleware.CtxCancel()
}

func (p *GenreFilter) Start() {
	defer p.Close()

	go p.signalListener()

	msgChan := make(chan middleware.MsgResponse)
	go p.middleware.ConsumeFromQueue(communication, games_to_filter, msgChan)

	for {
		select {
		case <-p.middleware.Ctx.Done():
			log.Info("Received sigterm")
			return
		case result := <-msgChan:
			msg := result.Msg.Body
			clientId := result.Msg.Headers["clientId"].(string)

			err := p.filterGames(msg, clientId)
			if err != nil {
				result.Msg.Nack(false, false)
			} else {
				result.Msg.Ack(false)
			}
		}
	}
}

func parseDate(releaseDate string) (int, error) {
	return strconv.Atoi(releaseDate[len(releaseDate)-4:])
}

func (p *GenreFilter) filterGame(game prot.Game, clientId string) error {
	year, err := parseDate(game.ReleaseDate)
	if err != nil {
		log.Errorf("Failed to parse decade: %s", game.ReleaseDate)
		return err
	}

	decade := strconv.Itoa(year - (year % 10))
	appIdRange := strconv.Itoa(utils.GetRange(game.AppID, p.config.NumJoiners))

	if strings.Contains(game.Genres, "Indie") {
		t := fmt.Sprintf("indie.%s.%s", decade, appIdRange)
		err = p.middleware.PublishInExchange(communication, filtered_games, t, prot.SerializeGame(&game), clientId)
		if err != nil {
			return err
		}
	} else if strings.Contains(game.Genres, "Shooter") {
		t := fmt.Sprintf("shooter.%s.%s", decade, appIdRange)
		err = p.middleware.PublishInExchange(communication, filtered_games, t, prot.SerializeGame(&game), clientId)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *GenreFilter) filterGames(msg []byte, clientId string) error {
	if string(msg) == "EOF" {
		log.Debug("Received EOF")
		err := p.middleware.PublishInExchange(communication, filtered_games, "shooter.*.0", []byte("EOF"), clientId)
		if err != nil {
			log.Errorf("failed to publish EOF: %s", err)
		}
		err = p.middleware.PublishInExchange(communication, filtered_games, "shooter.*.1", []byte("EOF"), clientId)
		if err != nil {
			log.Errorf("failed to publish EOF: %s", err)
		}
		err = p.middleware.PublishInExchange(communication, filtered_games, "shooter.*.2", []byte("EOF"), clientId)
		if err != nil {
			log.Errorf("failed to publish EOF: %s", err)
		}
		err = p.middleware.PublishInExchange(communication, filtered_games, "shooter.*.3", []byte("EOF"), clientId)
		if err != nil {
			log.Errorf("failed to publish EOF: %s", err)
		}
		err = p.middleware.PublishInExchange(communication, filtered_games, "shooter.*.4", []byte("EOF"), clientId)
		if err != nil {
			log.Errorf("failed to publish EOF: %s", err)
		}
		err = p.middleware.PublishInExchange(communication, filtered_games, "indie.*.0", []byte("EOF"), clientId)
		if err != nil {
			log.Errorf("failed to publish EOF: %s", err)
		}
		err = p.middleware.PublishInExchange(communication, filtered_games, "indie.*.1", []byte("EOF"), clientId)
		if err != nil {
			log.Errorf("failed to publish EOF: %s", err)
		}
		err = p.middleware.PublishInExchange(communication, filtered_games, "indie.*.2", []byte("EOF"), clientId)
		if err != nil {
			log.Errorf("failed to publish EOF: %s", err)
		}
		err = p.middleware.PublishInExchange(communication, filtered_games, "indie.*.3", []byte("EOF"), clientId)
		if err != nil {
			log.Errorf("failed to publish EOF: %s", err)
		}
		err = p.middleware.PublishInExchange(communication, filtered_games, "indie.*.4", []byte("EOF"), clientId)
		if err != nil {
			log.Errorf("failed to publish EOF: %s", err)
		}
		err = p.middleware.PublishInExchange(communication, filtered_games, "indie.2010.*", []byte("EOF"), clientId)
		if err != nil {
			log.Errorf("failed to publish EOF: %s", err)
		}
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

		err = p.filterGame(game, clientId)
		if err != nil {
			log.Errorf("Failed to filter game: %s", err)
			return nil
		}

		index += j
	}

	return nil
}
