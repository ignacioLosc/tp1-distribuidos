package common

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"example.com/system/communication/middleware"
	"example.com/system/communication/protocol"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	joined_reviews_queue = "joined_reviews"
	filtered_games       = "filtered_games"
	filtered_reviews     = "filtered_reviews"
)

type JoinerConfig struct {
	ServerPort string
	Id         string // 0..9
	Genre      string // Shooter, Indie
}

type Joiner struct {
	middleware            *middleware.Middleware
	config                JoinerConfig
	gamesQueue            string
	reviewsQueue          string
	savedGameReviewCounts map[string]protocol.GameReviewCount
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
		savedGameReviewCounts: make(map[string]protocol.GameReviewCount),
	}

	err = joiner.middlewareInit()
	if err != nil {
		return nil, err
	}

	return joiner, nil
}

func (c *Joiner) middlewareInit() error {
	_, err := c.middleware.DeclareDirectQueue(joined_reviews_queue)
	if err != nil {
		return err
	}

	name, err := c.middleware.DeclareDirectQueue("")
	if err != nil {
		return err
	}
	c.gamesQueue = name

	name, err = c.middleware.DeclareDirectQueue("")
	if err != nil {
		return err
	}
	c.reviewsQueue = name

	err = c.middleware.DeclareExchange(filtered_games, "topic")
	if err != nil {
		return err
	}

	err = c.middleware.DeclareExchange(filtered_reviews, "direct")
	if err != nil {
		return err
	}

	topic := strings.ToLower(fmt.Sprintf("%s.*.%s", c.config.Genre, c.config.Id))
	err = c.middleware.BindQueueToExchange(filtered_games, c.gamesQueue, topic)
	if err != nil {
		log.Errorf("Error binding games queue to exchange: %s", err)
		return err
	}

	err = c.middleware.BindQueueToExchange(filtered_reviews, c.reviewsQueue, c.config.Id)
	if err != nil {
		log.Errorf("Error binding reviews queue to exchange: %s", err)
		return err
	}

	return nil
}

func (j *Joiner) Close() {
	j.middleware.DeleteQueue(j.gamesQueue)
	j.middleware.DeleteQueue(j.reviewsQueue)

	j.middleware.Close()
}

func (j *Joiner) signalListener(cancel context.CancelFunc) {
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
	<-chSignal
	cancel()
}

func (j *Joiner) Start() {
	defer j.Close()

	ctx, cancel := context.WithCancel(context.Background())

	go j.signalListener(cancel)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			log.Info("Joiner reading games")
			j.middleware.ConsumeAndProcess(j.gamesQueue, j.saveGames)

			log.Info("Joiner reading reviews")
			j.middleware.ConsumeAndProcess(j.reviewsQueue, j.joinReviewsAndGames)

			log.Info("Joiner finished reading games and reviews")
			j.sendJoinedResults()
		}
	}
}

func (j *Joiner) sendJoinedResults() {
	for appId, gameReviewCount := range j.savedGameReviewCounts {
		log.Infof("PRINTING joined results for appId: %s -> ", appId, gameReviewCount)

		// err := j.middleware.PublishToDirect(joined_reviews_queue, protocol.SerializeGameReviewCount(&gameReviewCount))
		// if err != nil {
		// 	log.Errorf("Error publishing game review count: %s", err)
		// }
		//
		delete(j.savedGameReviewCounts, appId)
	}
}

func (p *Joiner) saveGames(msg []byte, finished *bool) error {
	if string(msg) == "EOF" {
		*finished = true
		return nil
	}

	game, err, _ := protocol.DeserializeGame(msg)
	if err != nil {
		return fmt.Errorf("Error deserializing game: %s", err)
	}

	p.savedGameReviewCounts[game.AppID]  = protocol.GameReviewCount{
		AppName:                    game.Name,
		PositiveReviewCount:        0,
		NegativeReviewCount:        0,
		PositiveEnglishReviewCount: 0,
	}

	return nil
}

func (p *Joiner) joinReviewsAndGames(msg []byte, finished *bool) error {
	if string(msg) == "EOF" {
		*finished = true
		return nil
	}

	mappedReview, err, _ := protocol.DeserializeMappedReview(msg)
	if err != nil {
		return fmt.Errorf("Error deserializing review: %s", err)
	}

	gameReviewCount, ok := p.savedGameReviewCounts[mappedReview.AppID]
	if !ok {
		return fmt.Errorf("No saved game review count found for appId: %s", mappedReview.AppID)
	}

	if mappedReview.IsPositive {
		gameReviewCount.PositiveReviewCount++
	}
	if mappedReview.IsNegative {
		gameReviewCount.NegativeReviewCount++
	}
	if mappedReview.IsPositiveEnglish {
		gameReviewCount.PositiveEnglishReviewCount++
	}

	return nil
}
