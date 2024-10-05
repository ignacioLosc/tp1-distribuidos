package main

import (
	"fmt"
	"os"
	"strings"

	"example.com/system/workers/input_controller/common"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

var log = logging.MustGetLogger("log")

func InitConfig() (*viper.Viper, error) {
	v := viper.New()

	//Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	v.SetEnvPrefix("cli")

	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Add env variables supported
	v.BindEnv("server", "port")
	v.BindEnv("log", "level")

	// Tries to read config or check envs
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config.file. using env variables instead")
	}

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
	log.Infof("action: config | result: sucess | server_port: %s | log_level: %s",
		v.GetString("server.port"),
		v.GetString("log.level"),
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

	config := common.ControllerConfig{
		ServerPort: v.GetString("server.port"),
	}
	controller, err := common.NewController(config)

	if err != nil {
		log.Critical("action: bind server | result: fail | err: %s", err)
	}

	controller.Start()

}
