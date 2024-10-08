package common

import (
	"context"
	"encoding/binary"
	"os"
	"os/signal"
	"syscall"

	"example.com/system/communication/middleware"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type AccumulatorConfig struct {
	ServerPort       string
	AccumulatorField string
	AccumulatorQueue string
}

type Accumulator struct {
	middleware      *middleware.Middleware
	config          AccumulatorConfig
	accumulatedData map[string]uint32
	stop            chan bool
}

func NewAccumulator(config AccumulatorConfig) (*Accumulator, error) {
	middleware, err := middleware.ConnectToMiddleware()
	if err != nil {
		return nil, err
	}
	accumulator := &Accumulator{
		config:     config,
		middleware: middleware,
		accumulatedData: map[string]uint32{
			"windows": 0,
			"linux":   0,
			"mac":     0,
		},
		stop: make(chan bool),
	}

	err = accumulator.middlewareInit()
	if err != nil {
		return nil, err
	}
	return accumulator, nil
}

func (c *Accumulator) middlewareInit() error {
	err := c.middleware.DeclareDirectQueue(c.config.AccumulatorQueue)
	if err != nil {
		return err
	}
	return nil
}

func (c *Accumulator) Close() {
	c.middleware.Close()
}

func (c *Accumulator) signalListener(cancel context.CancelFunc) {
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, syscall.SIGTERM)
	<-chSignal
	cancel()
}

func (c *Accumulator) Start() {
	log.Info("Starting accumulator")
	ctx, cancel := context.WithCancel(context.Background())

	go c.signalListener(cancel)
	// c.InitializeGracefulExit()

	log.Infof("Accumulating games by %s", c.config.AccumulatorField)

	for {
		select {
		case <-ctx.Done():
			c.stop <- true
			return
		default:
			c.middleware.ConsumeAndProcess("games", c.accumulateData, c.stop)
			err := c.sendResults()
			if err != nil {
				log.Error("Error sending results: %v", err)
				return
			}
		}

	}
}

func (c *Accumulator) deserializeCounter(bytes []byte) (uint32, uint32, uint32, error) {
	windows := binary.BigEndian.Uint32(bytes[:4])
	linux := binary.BigEndian.Uint32(bytes[4:8])
	mac := binary.BigEndian.Uint32(bytes[8:12])
	return windows, linux, mac, nil
}

func (c *Accumulator) SerializeData() []byte {
	bytes := make([]byte, 0)

	windows := make([]byte, 4)
	binary.BigEndian.PutUint32(windows, c.accumulatedData["windows"])
	bytes = append(bytes, windows...)

	linux := make([]byte, 4)
	binary.BigEndian.PutUint32(linux, c.accumulatedData["linux"])
	bytes = append(bytes, linux...)

	mac := make([]byte, 4)
	binary.BigEndian.PutUint32(mac, c.accumulatedData["mac"])
	bytes = append(bytes, mac...)

	return bytes
}
func (c *Accumulator) accumulateData(msg []byte) error {
	log.Info("Processing batch games")
	if string(msg) == "EOF" {
		c.stop <- true
		return nil
	}
	windows, linux, mac, err := c.deserializeCounter(msg)
	if err != nil {
		log.Errorf("Failed to deserialize game: %s", err)
		return nil
	}
	c.accumulatedData["windows"] += uint32(windows)
	c.accumulatedData["linux"] += uint32(linux)
	c.accumulatedData["mac"] += uint32(mac)
	log.Info("Subtotal: Windows: %d, Linux: %d, Mac: %d", c.accumulatedData["windows"], c.accumulatedData["linux"], c.accumulatedData["mac"])
	return nil
}

func (c *Accumulator) sendResults() error {
	err := c.middleware.PublishInQueue("accumulator_result", c.SerializeData())
	if err != nil {
		return err
	}
	err = c.middleware.PublishInQueue("accumulator_result", []byte("EOF"))
	if err != nil {
		return err
	}
	return nil
}
