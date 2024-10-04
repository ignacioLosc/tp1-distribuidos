package main

import (
	"fmt"
	"os"
	"strings"


	"github.com/op/go-logging"
	"github.com/spf13/viper"
    "example.com/client/common"
)

var log = logging.MustGetLogger("log")

func InitConfig() (*viper.Viper, error) {
    v := viper.New()

    //Configure viper to read env variables with the CLI_ prefix
    v.AutomaticEnv()
    v.SetEnvPrefix("cli")

    v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

    // Add env variables supported
    v.BindEnv("server", "address")
    v.BindEnv("log", "level")
    v.BindEnv("data_path", "path")

    // Tries to read config or check envs

    v.SetConfigFile("./config.yaml")
    if err := v.ReadInConfig(); err != nil {
        fmt.Printf("Configuration could not be read from config.file. using env variables instead")
    }

    return v,nil
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
    v , err := InitConfig()
    if err != nil {
        log.Criticalf("%s", err)
        return
    }

    PrintConfig(v)

    if err := InitLogger(v.GetString("log.level")); err != nil {
        log.Criticalf("%s", err)
        return
    }



    clientConfig := common.ClientConfig {
        ServerAddress: v.GetString("server.address"),
        DataPath: v.GetString("data_path.path"),
    }

    client, err := common.NewClient(clientConfig)
    if err != nil {
        log.Criticalf("%s", err)
    }
    
    fmt.Println("Starting Client")
    client.Start()

    

    
}

