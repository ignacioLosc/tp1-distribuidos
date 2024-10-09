package common

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"example.com/system/communication/middleware"
	"example.com/system/communication/protocol"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	games_queue      = "games_to_join"
	reviews_queue    = "reviews_to_join"
	filtered_games   = "filtered_games"
	filtered_reviews = "filtered_reviews"
)

type JoinerConfig struct {
	ServerPort string
	Id         string // 0..9
	Genre      string // Shooter, Indie
}

type Joiner struct {
	middleware *middleware.Middleware
	config     JoinerConfig
	savedGames []protocol.Game
}

func NewJoiner(config JoinerConfig) (*Joiner, error) {
	middleware, err := middleware.ConnectToMiddleware()
	if err != nil {
		log.Errorf("Error connecting to middleware: %s", err)
		return nil, err
	}

	joiner := &Joiner{
		config:     config,
		middleware: middleware,
	}

	err = joiner.middlewareInit()
	if err != nil {
		return nil, err
	}

	return joiner, nil
}

func (c *Joiner) middlewareInit() error {
	err := c.middleware.DeclareDirectQueue(games_queue)
	if err != nil {
		return err
	}

	err = c.middleware.DeclareDirectQueue(reviews_queue)
	if err != nil {
		return err
	}

	err = c.middleware.DeclareExchange(filtered_games, "topic")
	if err != nil {
		return err
	}

	err = c.middleware.DeclareExchange(filtered_reviews, "direct")
	if err != nil {
		return err
	}

	topic := strings.ToLower(fmt.Sprintf("%s.*.%s", c.config.Genre, c.config.Id))
	err = c.middleware.BindQueueToExchange(filtered_games, games_queue, topic)
	if err != nil {
		log.Errorf("Error binding games queue to exchange: %s", err)
		return err
	}

	err = c.middleware.BindQueueToExchange(filtered_reviews, reviews_queue, c.config.Id)
	if err != nil {
		log.Errorf("Error binding reviews queue to exchange: %s", err)
		return err
	}

	return nil
}

func (c *Joiner) Close() {
	c.middleware.Close()
}

func (c *Joiner) signalListener(cancel context.CancelFunc) {
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
	<-chSignal
	cancel()
}

func (p *Joiner) Start() {
	log.Info("Starting joiner")
	defer p.Close()

	ctx, cancel := context.WithCancel(context.Background())

	go p.signalListener(cancel)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			p.middleware.ConsumeAndProcess(games_queue, p.saveGames)

			p.middleware.ConsumeAndProcess(reviews_queue, p.joinReviewsAndGames)

			time.Sleep(5 * time.Second)
		}
	}
}

func (p *Joiner) saveGames(msg []byte, finished *bool) error {
	log.Info("Calling saveGames")

	if string(msg) == "EOF" {
		*finished = true
		return nil
	}

	game, err, _ := protocol.DeserializeGame(msg)
	log.Info("Received game: %s", game)
	if err != nil {
		return fmt.Errorf("Error deserializing game: %s", err)
	}

	p.savedGames = append(p.savedGames, game)

	return nil
}

func (p *Joiner) joinReviewsAndGames(msg []byte, finished *bool) error {
	if string(msg) == "EOF" {
		*finished = true
		return nil
	}

	return nil
}
