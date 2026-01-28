package cmd

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/spf13/viper"

	"github.com/opensvc/oc3/oc2websocket"
)

var (
	configCandidateDirs = []string{"/etc/oc3/", "$HOME/.config/oc3", "./"}
)

func logConfigDir() {
	slog.Info(fmt.Sprintf("candidate config directories: %s", configCandidateDirs))
}

func logConfigFileUsed() {
	slog.Info(fmt.Sprintf("used config file: %s", viper.ConfigFileUsed()))
}

func initConfig() error {
	// env
	viper.SetEnvPrefix("OC3")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// defaults
	viper.SetDefault("feeder.addr", "127.0.0.1:8080")
	viper.SetDefault("feeder.pprof.enable", false)
	viper.SetDefault("feeder.metrics.enable", false)
	viper.SetDefault("feeder.ui.enable", false)
	viper.SetDefault("feeder.sync.timeout", "2s")
	viper.SetDefault("server.addr", "127.0.0.1:8081")
	viper.SetDefault("server.pprof.enable", false)
	viper.SetDefault("server.metrics.enable", false)
	viper.SetDefault("server.ui.enable", false)
	viper.SetDefault("server.sync.timeout", "2s")
	viper.SetDefault("db.username", "opensvc")
	viper.SetDefault("db.host", "127.0.0.1")
	viper.SetDefault("db.port", "3306")
	viper.SetDefault("db.log.level", "warn")
	viper.SetDefault("db.log.slow_query_threshold", "1s")
	viper.SetDefault("redis.db", 0)
	viper.SetDefault("redis.address", "localhost:6379")
	viper.SetDefault("redis.password", "")
	viper.SetDefault("feeder.tx", true)
	viper.SetDefault("messenger.key", "magix123")
	viper.SetDefault("messenger.url", "http://127.0.0.1:8889")
	viper.SetDefault("messenger.require_token", false)
	viper.SetDefault("messenger.key_file", "")
	viper.SetDefault("messenger.cert_file", "")
	viper.SetDefault("worker.runners", 1)
	viper.SetDefault("worker.pprof.uxsocket", "/var/run/oc3_worker_pprof.sock")
	//viper.SetDefault("worker.pprof.addr", "127.0.0.1:9999")
	viper.SetDefault("worker.pprof.enable", false)
	viper.SetDefault("worker.metrics.enable", false)
	viper.SetDefault("worker.metrics.addr", "127.0.0.1:2112")
	viper.SetDefault("scheduler.pprof.uxsocket", "/var/run/oc3_scheduler_pprof.sock")
	//viper.SetDefault("scheduler.pprof.addr", "127.0.0.1:9998")
	viper.SetDefault("scheduler.pprof.enable", false)
	viper.SetDefault("scheduler.metrics.enable", false)
	viper.SetDefault("scheduler.metrics.addr", "127.0.0.1:2111")
	viper.SetDefault("scheduler.task.trim.retention", 365)
	viper.SetDefault("scheduler.task.trim.batch_size", 1000)
	viper.SetDefault("w2p_hmac", "sha512:7755f108-1b83-45dc-8302-54be8f3616a1")

	// config file
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	for _, d := range configCandidateDirs {
		viper.AddConfigPath(d)
	}
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return err
		} else {
			slog.Info(err.Error())
		}
	}
	return nil
}

func newEv() *oc2websocket.T {
	return &oc2websocket.T{
		Url: viper.GetString("messenger.url"),
		Key: []byte(viper.GetString("messenger.key")),
	}
}
