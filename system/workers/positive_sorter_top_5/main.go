package main

import (
	"os"
	"strings"

	"example.com/system/workers/positive_sorter_top_5/common"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

var log = logging.MustGetLogger("log")

func InitConfig() (*viper.Viper, error) {
	v := viper.New()

	v.AutomaticEnv()
	v.SetEnvPrefix("cli")

	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	v.BindEnv("server", "port")
	v.BindEnv("log", "level")
	v.BindEnv("joiner", "id")
	v.BindEnv("top")

	return v, nil
}
func InitLogger(logLevel string) error {
	baseBackend := logging.NewLogBackend(os.Stdout, "", 0)
	format := logging.MustStringFormatter(
		`%{time:2006-01-02 15:04:05} %{level:.5s}     %{message}`,
	)
	backendFormatter := logging.NewBackendFormatter(baseBackend, format)

	backendLeveled := logging.AddModuleLevel(backendFormatter)
	logLevelCode, err := logging.LogLevel(logLevel)
	if err != nil {
		return err
	}
	backendLeveled.SetLevel(logLevelCode, "")

	// Set the backends to be used.
	logging.SetBackend(backendLeveled)
	return nil
}

func PrintConfig(v *viper.Viper) {
	log.Infof("action: config | result: sucess | server_port: %s | log_level: %s | sorter_id: %s | top: %s",
		v.GetString("server.port"),
		v.GetString("log.level"),
		v.GetString("sorter.id"),
		v.GetString("top"),
	)
}

func main() {
	v, err := InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
	}

	if err := InitLogger(v.GetString("log.level")); err != nil {
		log.Criticalf("%s", err)
	}

	PrintConfig(v)

	config := common.SorterConfig{
		ServerPort: v.GetString("server.port"),
		Id:         v.GetString("sorter.id"),
		Top:        v.GetString("top"),
	}
	sorter, err := common.NewSorter(config)

	if err != nil {
		log.Critical("action: bind server | result: fail | err: %s", err)
	}

	sorter.Start()

}
