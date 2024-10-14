package common

import (
	"context"
	"encoding/binary"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	mw "example.com/system/communication/middleware"
	"example.com/system/communication/protocol"
	"example.com/system/communication/utils"
	"github.com/op/go-logging"
	"github.com/pemistahl/lingua-go"
)

var log = logging.MustGetLogger("log")

const (
	reviews          = "reviews"
	filtered_reviews = "filtered_reviews"
)

type ReviewMapperConfig struct {
	ServerPort string
	NumJoiners int
}

type ReviewMapper struct {
	config           ReviewMapperConfig
	middleware       *mw.Middleware
	stop             chan bool
	languageDetector lingua.LanguageDetector
}

func NewReviewMapper(config ReviewMapperConfig) (*ReviewMapper, error) {
	middleware, err := mw.ConnectToMiddleware()
	if err != nil {
		return nil, err
	}

	languages := []lingua.Language{
		lingua.English,
		lingua.Spanish,
	}

	detector := lingua.NewLanguageDetectorBuilder().
		FromLanguages(languages...).
		Build()

	reviewMapper := &ReviewMapper{
		config:           config,
		stop:             make(chan bool),
		middleware:       middleware,
		languageDetector: detector,
	}

	err = reviewMapper.middlewareCounterInit()
	if err != nil {
		return nil, err
	}

	reviewMapper.middleware = middleware
	return reviewMapper, nil
}

func (p ReviewMapper) middlewareCounterInit() error {
	_, err := p.middleware.DeclareDirectQueue(reviews)
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
	log.Info("Sending EOF IN REVIEW MAPPER")
	err := p.middleware.PublishInExchange(filtered_reviews, "0", []byte("EOF"))
	err = p.middleware.PublishInExchange(filtered_reviews, "1", []byte("EOF"))
	err = p.middleware.PublishInExchange(filtered_reviews, "2", []byte("EOF"))
	err = p.middleware.PublishInExchange(filtered_reviews, "3", []byte("EOF"))
	err = p.middleware.PublishInExchange(filtered_reviews, "4", []byte("EOF"))
	if err != nil {
		log.Info("Error: Couldn't send EOF")
		return err
	}
	return nil
}

func (p *ReviewMapper) mapReview(review protocol.Review) protocol.MappedReview {
	isPositive := review.ReviewScore == 1
	if language, exists := p.languageDetector.DetectLanguageOf(review.ReviewText); exists {
		return protocol.MappedReview{AppID: review.AppID, IsPositive: isPositive, IsNegative: !isPositive, IsPositiveEnglish: isPositive && language == lingua.English}
	} else {
		return protocol.MappedReview{AppID: review.AppID, IsPositive: isPositive, IsNegative: !isPositive, IsPositiveEnglish: false}
	}
	// return protocol.MappedReview{AppID: review.AppID, IsPositive: isPositive, IsNegative: !isPositive, IsPositiveEnglish: true}
}

func (p *ReviewMapper) mapReviews(msg []byte, finished *bool) error {
	if string(msg) == "EOF" {
		log.Info("Received EOF. Stopping")
		*finished = true
		p.sendEOF()
		return nil
	}

	lenReviews := binary.BigEndian.Uint64(msg[:8])

	index := 8
	for i := 0; i < int(lenReviews); i++ {
		review, err, j := protocol.DeserializeReview(msg[index:])

		if err != nil {
			log.Errorf("Failed to deserialize reviews", err)
			break
		}

		mappedReview := p.mapReview(review)
		reviewBuffer := protocol.SerializeMappedReview(&mappedReview)

		gameRange := utils.GetRange(review.AppID, p.config.NumJoiners)
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
	err := p.middleware.PublishInExchange(filtered_reviews, strconv.Itoa(gameRange), review)
	if err != nil {
		log.Error("Error sending mapped review: %v", err)
		return err
	}
	return nil
}
