package common

import (
	"context"
	"encoding/binary"
	"encoding/csv"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"example.com/system/communication/middleware"
	"example.com/system/communication/protocol"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ControllerConfig struct {
	ServerPort string
}

type Controller struct {
	middleware *middleware.Middleware
	config     ControllerConfig
}

func NewController(config ControllerConfig) (*Controller, error) {
	middleware, err := middleware.ConnectToMiddleware()
	if err != nil {
		return nil, err
	}
	controller := &Controller{
		config:     config,
		middleware: middleware,
	}

	err = controller.middlewareInit()
	if err != nil {
		return nil, err
	}
	return controller, nil
}

func (c *Controller) middlewareInit() error {
	err := c.middleware.DeclareDirectQueue("games")
	if err != nil {
		return err
	}
	err = c.middleware.DeclareDirectQueue("reviews")
	if err != nil {
		return err
	}
	err = c.middleware.DeclareDirectQueue("games_to_count")
	if err != nil {
		return err
	}
	return nil
}

func (c *Controller) Close() {
	c.middleware.Close()
}

func (c *Controller) signalListener(cancel context.CancelFunc) {
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
	<-chSignal
	cancel()
}

func (c *Controller) Start() {
	log.Info("Starting input controller")
	ctx, cancel := context.WithCancel(context.Background())
	stop := make(chan bool)

	go c.signalListener(cancel)
	// c.InitializeGracefulExit()

	log.Infof("Reading games and reviews")
	go c.middleware.ConsumeAndProcess("games", c.processGame, stop)
	go c.middleware.ConsumeAndProcess("reviews", c.processReview, stop)

	<-ctx.Done()
}

func (c *Controller) processGame(msg []byte) error {
	log.Info("Processing batch games")
	str := string(msg)
	reader := csv.NewReader(strings.NewReader(str))
	records, err := reader.ReadAll()
	if err != nil {
		log.Error("Error reading CSV:", err)
		return err
	}

	log.Info("Input controller. Parsed records from CSV: ", len(records))

	gamesBuffer := make([]byte, 8)
	l := len(records)
	binary.BigEndian.PutUint64(gamesBuffer, uint64(l))

	for _, record := range records {
		game, err := protocol.GameFromRecord(record)
		if err != nil {
			log.Error("Error parsing record:", err)
			continue
		}
		gameBuffer := protocol.SerializeGame(&game)
		gamesBuffer = append(gamesBuffer, gameBuffer...)
	}

	c.middleware.PublishInQueue("games_to_count", gamesBuffer)
	return nil
}

func (c *Controller) processReview(msg []byte) error {
	// record, err := c.readRecord(msg)
	// if err != nil { return err}

	// review, err := protocol.ReviewFromRecord(record)
	// if err != nil {
	// 	return err
	// }
	//
	// log.Info("Input controller. Sending review to reviews queue ", review.AppID)	
	// Should send to the next queue

	return nil
}


func (c *Controller) readRecord(msg[]byte) ([]string,error) {
	r := csv.NewReader(strings.NewReader(string(msg)))
	record, err := r.Read()
	if err != nil {
		return nil,err
	}
	return record, nil
}
