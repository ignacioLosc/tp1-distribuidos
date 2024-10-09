package common

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"example.com/system/communication/middleware"
	"example.com/system/communication/protocol"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	games_queue = "games_to_join"
	reviews_queue = "reviews_to_join"
	filtered_games  = "filtered_games"
	filtered_reviews  = "filtered_reviews"
)

type JoinerConfig struct {
	ServerPort string
	Id         string // 0..9
	Genre string // Shooter, Indie
}

type Joiner struct {
	middleware *middleware.Middleware
	config     JoinerConfig
	savedGames []protocol.Game
}

func NewJoiner(config JoinerConfig) (*Joiner, error) {
	middleware, err := middleware.ConnectToMiddleware()
	if err != nil {
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

	topic := fmt.Sprintf("%s.*.%s", c.config.Genre, c.config.Id)
	c.middleware.BindQueueToExchange(games_queue, filtered_games, topic)

	err = c.middleware.BindQueueToExchange(reviews_queue, filtered_reviews, c.config.Id)

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
	log.Info("Starting genre filter")
	defer p.Close()

	_, cancel := context.WithCancel(context.Background())

	go p.signalListener(cancel)

	p.middleware.ConsumeAndProcess(games_queue, p.saveGames)

	p.middleware.ConsumeAndProcess(reviews_queue, p.joinReviewsAndGames)
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
