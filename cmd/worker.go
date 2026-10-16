package cmd

import (
	"database/sql"
	"fmt"
	"log/slog"
	_ "net/http/pprof"

	"github.com/go-redis/redis/v8"
	"github.com/labstack/echo/v4"
	"github.com/shaj13/go-guardian/v2/auth/strategies/union"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/util/logkey"
	"github.com/opensvc/oc3/worker"
	"github.com/opensvc/oc3/xauth"
)

type (
	workerT struct {
		db        *sql.DB
		redis     *redis.Client
		section   string
		runners   int
		queues    []string
		uploadDir string
	}
)

func newWorker(section string, runners int, queues []string) (*workerT, error) {
	db, err := newDatabase()
	if err != nil {
		return nil, err
	}
	t := &workerT{db: db, section: section, runners: runners, redis: newRedis()}
	if runners < viper.GetInt(t.section+".runners") {
		t.runners = viper.GetInt(t.section + ".runners")
	}
	if len(queues) == 0 {
		queues = viper.GetStringSlice(t.section + ".queues")
	}
	for _, q := range queues {
		t.queues = append(t.queues, cachekeys.QueuePrefix+q)
	}
	t.uploadDir = viper.GetString(t.section + ".directories.uploads")
	return t, nil
}

func (t *workerT) Section() string { return t.section }

func (t *workerT) authMiddleware(publicPath, publicPrefix []string) echo.MiddlewareFunc {
	return AuthMiddleware(union.New(
		xauth.NewPublicStrategy(publicPath, publicPrefix),
	))
}

func (t *workerT) run() error {
	if len(t.queues) == 0 {
		return fmt.Errorf("no queues specified")
	}

	if ok, errC := start(t); ok {
		slog.Info(t.Section() + " started")
		go func() {
			if err := <-errC; err != nil {
				slog.Error(t.Section()+" stopped", logkey.Error, err)
			} else {
				slog.Debug(t.Section()+" stopped", logkey.Error, err)
			}
		}()
	}

	ev := newEv()

	w := &worker.Worker{
		DB:        t.db,
		Redis:     t.redis,
		Queues:    t.queues,
		Ev:        ev,
		Runners:   t.runners,
		SubSystem: t.Section(),
		UploadDir: t.uploadDir,
	}
	return w.Run()
}
