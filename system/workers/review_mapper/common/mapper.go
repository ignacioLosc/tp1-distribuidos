package common

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	mw "example.com/system/communication/middleware"
	"example.com/system/communication/protocol"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	reviews          = "reviews"
	filtered_reviews = "filtered_reviews"
)

type ReviewMapperConfig struct {
	ServerPort string
}

type ReviewMapper struct {
	config     ReviewMapperConfig
	middleware *mw.Middleware
	stop       chan bool
}

func NewReviewMapper(config ReviewMapperConfig) (*ReviewMapper, error) {
	middleware, err := mw.ConnectToMiddleware()
	if err != nil {
		return nil, err
	}

	platformCounter := &ReviewMapper{
		config:     config,
		stop:       make(chan bool),
		middleware: middleware,
	}

	err = platformCounter.middlewareCounterInit()
	if err != nil {
		return nil, err
	}

	platformCounter.middleware = middleware
	return platformCounter, nil
}

func (p ReviewMapper) middlewareCounterInit() error {
	err := p.middleware.DeclareDirectQueue(reviews)
	if err != nil {
		return err
	}

	err = p.middleware.DeclareExchange(filtered_reviews, "direct")
	if err != nil {
		return err
	}
	return nil
}

func (p *ReviewMapper) Close() {
	p.middleware.Close()
}

func (p *ReviewMapper) signalListener(cancel context.CancelFunc) {
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
	<-chSignal
	cancel()
}

func (p *ReviewMapper) Start() {
	defer p.Close()

	ctx, cancel := context.WithCancel(context.Background())

	go p.signalListener(cancel)

	for {
		select {
		case <-ctx.Done():
			p.stop <- true
			return
		default:
			p.middleware.ConsumeAndProcess(reviews, p.mapReviews)
		}
	}
}

func (p *ReviewMapper) sendEOF() error {
	err := p.middleware.PublishInExchange(filtered_reviews, "*", []byte("EOF"))
	if err != nil {
		log.Info("Error: Couldn't send EOF")
		return err
	}
	return nil
}

func mapReview(review protocol.Review) protocol.MappedReview {
	isPositive := review.ReviewScore == 1
	language := "english"
	return protocol.MappedReview{AppID: review.AppID, IsPositive: isPositive, IsNegative: !isPositive, IsPositiveEnglish: isPositive && language == "english"}
}

func (p *ReviewMapper) mapReviews(msg []byte, finished *bool) error {
	if string(msg) == "EOF" {
		log.Info("Received EOF. Stopping")
		*finished = true
		p.sendEOF()
		return nil
	}

	lenReviews := binary.BigEndian.Uint64(msg[:8])

	log.Infof("Received %d reviews. WITH BUFFER LENGTH: %d", lenReviews, len(msg))

	index := 8
	for i := 0; i < int(lenReviews); i++ {
		review, err, j := protocol.DeserializeReview(msg[index:])

		if err != nil {
			log.Errorf("Failed to deserialize reviews", err)
			break
		}

		mappedReview := mapReview(review)
		reviewBuffer := protocol.SerializeMappedReview(&mappedReview)

		appId, err := strconv.Atoi(review.AppID)
		if err != nil {
			return err
		}
		gameRange := appId % 10

		err = p.sendReview(reviewBuffer, gameRange)
		if err != nil {
			log.Error("Error sending mapped review: %v", err)
			return err
		}

		index += j
	}

	return nil
}

func (p *ReviewMapper) sendReview(review []byte, gameRange int) error {
	err := p.middleware.PublishInExchange(filtered_reviews, fmt.Sprintf("%s", gameRange), review)
	if err != nil {
		return err
	}
	return nil
}
