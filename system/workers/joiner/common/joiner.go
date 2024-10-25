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
	shooter_negative_joined_queue = "joined_shooter_negative"
	shooter_positive_joined_queue = "joined_shooter_positive"
	indies_joined_queue           = "joined_reviews_indies"

	filtered_games   = "filtered_games"
	filtered_reviews = "filtered_reviews"

	control       = "control"
	communication = "communication"
)

type JoinerConfig struct {
	ServerPort string
	Id         string // 0..9
	Genre      string // Shooter, Indie
}

type Joiner struct {
	middleware               *middleware.Middleware
	config                   JoinerConfig
	gamesQueue               string
	reviewsQueue             string
	savedGameReviewCountsMap map[string]map[string]protocol.GameReviewCount
	finishedMappers          map[string]int
	readyForReviews		     map[string]bool
}

func NewJoiner(config JoinerConfig) (*Joiner, error) {
	ctx, cancel := context.WithCancel(context.Background())
	middleware, err := middleware.CreateMiddleware(ctx, cancel)
	if err != nil {
		log.Errorf("Error connecting to middleware: %s", err)
		return nil, err
	}

	joiner := &Joiner{
		config:                   config,
		middleware:               middleware,
		savedGameReviewCountsMap: make(map[string]map[string]protocol.GameReviewCount),
		finishedMappers:          make(map[string]int),
		readyForReviews:          make(map[string]bool),
	}

	err = joiner.middlewareInit()
	if err != nil {
		return nil, err
	}

	return joiner, nil
}

func (c *Joiner) middlewareInit() error {
	err := c.middleware.DeclareChannel(communication)
	if err != nil {
		return err
	}

	err = c.middleware.DeclareChannel(control)
	if err != nil {
		return err
	}

	_, err = c.middleware.DeclareDirectQueue(communication, shooter_negative_joined_queue)
	if err != nil {
		return err
	}

	_, err = c.middleware.DeclareDirectQueue(communication, shooter_positive_joined_queue)
	if err != nil {
		return err
	}

	_, err = c.middleware.DeclareDirectQueue(communication, indies_joined_queue)
	if err != nil {
		return err
	}

	name, err := c.middleware.DeclareTemporaryQueue(communication)
	if err != nil {
		return err
	}
	c.gamesQueue = name

	nameReviews, err := c.middleware.DeclareTemporaryQueue(communication)
	if err != nil {
		return err
	}
	c.reviewsQueue = nameReviews

	err = c.middleware.DeclareExchange(communication, filtered_games, "topic")
	if err != nil {
		return err
	}

	err = c.middleware.DeclareExchange(communication, filtered_reviews, "direct")
	if err != nil {
		return err
	}

	topic := strings.ToLower(fmt.Sprintf("%s.*.%s", c.config.Genre, c.config.Id))
	err = c.middleware.BindQueueToExchange(communication, filtered_games, c.gamesQueue, topic)
	if err != nil {
		log.Errorf("Error binding games queue to exchange: %s", err)
		return err
	}

	err = c.middleware.BindQueueToExchange(communication, filtered_reviews, c.reviewsQueue, c.config.Id)
	if err != nil {
		log.Errorf("Error binding reviews queue to exchange: %s", err)
		return err
	}

	return nil
}

func (j *Joiner) Close() {
	j.middleware.DeleteQueue(communication, j.gamesQueue)
	j.middleware.DeleteQueue(communication, j.reviewsQueue)

	j.middleware.Close()
}

func (j *Joiner) signalListener() {
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
	<-chSignal
	log.Infof("received signal")
	j.middleware.CtxCancel()
}

func (j *Joiner) Start() {
	defer j.Close()

	go j.signalListener()

	gamesMsgChan := make(chan middleware.MsgResponse)
	go j.middleware.ConsumeFromQueue(communication, j.gamesQueue, gamesMsgChan)

	reviewsMsgChan := make(chan middleware.MsgResponse)
	go j.middleware.ConsumeFromQueue(communication, j.reviewsQueue, reviewsMsgChan)
	
	log.Info("Joiner started listening")
	for {
		select {
		case <-j.middleware.Ctx.Done():
			return

		case gameResult := <-gamesMsgChan:
			msg := gameResult.Msg.Body
			clientId := gameResult.Msg.Headers["clientId"].(string)
			err := j.saveGames(msg, clientId)
			if err != nil {
				gameResult.Msg.Nack(false, false)
			} else {
				gameResult.Msg.Ack(false)
			}

		case reviewResult := <-reviewsMsgChan:
			msg := reviewResult.Msg.Body
			clientId := reviewResult.Msg.Headers["clientId"].(string)

			if !j.readyForReviews[clientId] {
				log.Info("client not ready bro")
				reviewResult.Msg.Nack(false, true)
				continue
			}

			err := j.joinReviewsAndGames(msg, clientId)
			if err != nil {
				reviewResult.Msg.Nack(false, false)
			} else {
				reviewResult.Msg.Ack(false)
			}
		}
	}
}

func (j *Joiner) sendJoinedResults(clientId string) {
	for appId, gameReviewCount := range j.savedGameReviewCountsMap[clientId] {
		if j.config.Genre == "Indie" {
			err := j.middleware.PublishInQueue(communication, indies_joined_queue, protocol.SerializeGameReviewCount(&gameReviewCount))
			if err != nil {
				log.Errorf("Error publishing game review count to indies joined queue: %s", err)
				continue
			}
		} else if j.config.Genre == "Shooter" {
			err := j.middleware.PublishInQueue(communication, shooter_negative_joined_queue, protocol.SerializeGameReviewCount(&gameReviewCount))
			if err != nil {
				log.Errorf("Error publishing game review count to shooter negative joined queue: %s", err)
				continue
			}

			err = j.middleware.PublishInQueue(communication, shooter_positive_joined_queue, protocol.SerializeGameReviewCount(&gameReviewCount))
			if err != nil {
				log.Errorf("Error publishing game review count to shooter positive joined queue: %s", err)
				continue
			}
		}

		delete(j.savedGameReviewCountsMap[clientId], appId)
	}

	err := j.middleware.PublishInQueue(communication, shooter_negative_joined_queue, []byte("EOF"))
	if err != nil {
		log.Errorf("Error publishing game review count to shooter negative joined queue: %s", err)
	}

	err = j.middleware.PublishInQueue(communication, shooter_positive_joined_queue, []byte("EOF"))
	if err != nil {
		log.Errorf("Error publishing game review count to shooter positive joined queue: %s", err)
	}

	err = j.middleware.PublishInQueue(communication, indies_joined_queue, []byte("EOF"))
	if err != nil {
		log.Errorf("Error publishing game review count to indies joined queue: %s", err)
	}
}

func (p *Joiner) saveGames(msg []byte, clientId string) error {
	if string(msg) == "EOF" {
		log.Info("Joiner finished reading games")
		p.readyForReviews[clientId] = true
		return nil
	}

	game, err, _ := protocol.DeserializeGame(msg)
	if err != nil {
		return fmt.Errorf("Error deserializing game: %s", err)
	}

	_, ok := p.savedGameReviewCountsMap[clientId]
	if !ok {
		p.savedGameReviewCountsMap[clientId] = make(map[string]protocol.GameReviewCount)
	}

	p.savedGameReviewCountsMap[clientId][game.AppID] = protocol.GameReviewCount{
		AppName:                    game.Name,
		PositiveReviewCount:        0,
		NegativeReviewCount:        0,
		PositiveEnglishReviewCount: 0,
	}

	return nil
}

func (p *Joiner) joinReviewsAndGames(msg []byte, clientId string) error {
	if string(msg) == "EOF" {
		p.finishedMappers[clientId] = p.finishedMappers[clientId] + 1
		if p.finishedMappers[clientId] == 10 {
			p.sendJoinedResults(clientId)
			log.Info("Joiner finished reading games and reviews")
		}

		return nil
	}

	mappedReview, err, _ := protocol.DeserializeMappedReview(msg)
	if err != nil {
		return fmt.Errorf("Error deserializing review: %s", err)
	}

	gameReviewCount, ok := p.savedGameReviewCountsMap[clientId][mappedReview.AppID]
	if !ok {
		return nil
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

	p.savedGameReviewCountsMap[clientId][mappedReview.AppID] = gameReviewCount
	return nil
}
