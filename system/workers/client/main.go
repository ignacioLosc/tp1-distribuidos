package main

import (
	"os"
	"strings"

	"example.com/system/workers/client/common"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

var log = logging.MustGetLogger("log")

func InitConfig() (*viper.Viper, error) {
	v := viper.New()

	v.AutomaticEnv()
	v.SetEnvPrefix("cli")

	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	v.BindEnv("server", "address")
	v.BindEnv("log", "level")
	v.BindEnv("data_path", "path")

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
	log.Infof("action: config | result: sucess | server_address: %s | log_level: %s | data_path: %s",
		v.GetString("server.address"),
		v.GetString("log.level"),
		v.GetString("data_path.path"),
	)
}

func main() {
	v, err := InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
		return
	}

	PrintConfig(v)

	if err := InitLogger(v.GetString("log.level")); err != nil {
		log.Criticalf("%s", err)
		return
	}

	clientConfig := common.ClientConfig{
		ServerAddress: v.GetString("server.address"),
		DataPath:      v.GetString("data_path.path"),
	}

	client, err := common.NewClient(clientConfig)
	if err != nil {
		log.Criticalf("%s", err)
	}

	client.Start()
}