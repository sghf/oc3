package cmd

import (
	"context"
	"log/slog"

	"github.com/labstack/echo-contrib/echoprometheus"
	"github.com/labstack/echo-contrib/pprof"
	"github.com/labstack/echo/v4"
	"github.com/shaj13/go-guardian/v2/auth/strategies/union"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/server"
	serverhandlers "github.com/opensvc/oc3/server/handlers"
	"github.com/opensvc/oc3/xauth"
)

func startServer() error {
	addr := viper.GetString("server.addr")
	return listenAndServeServer(addr)
}

func listenAndServeServer(addr string) error {
	enableUI := viper.GetBool("server.ui.enable")

	db, err := newDatabase()
	if err != nil {
		return err
	}

	redisClient := newRedis()

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	if viper.GetBool("server.pprof.enable") {
		slog.Info("add handler /oc3/api/public/pprof")
		pprof.Register(e, "/oc3/api/public/pprof")
	}

	strategy := union.New(
		xauth.NewPublicStrategy("/oc3/api/public/", "/oc3/api/docs", "/oc3/api/version", "/oc3/api/openapi"),
		xauth.NewBasicWeb2py(db, viper.GetString("w2p_hmac")),
	)
	if viper.GetBool("server.metrics.enable") {
		slog.Info("add handler /oc3/api/public/metrics")
		e.Use(echoprometheus.NewMiddleware("oc3_api"))
		e.GET("/oc3/api/public/metrics", echoprometheus.NewHandler())
	}
	e.Use(serverhandlers.AuthMiddleware(strategy))
	slog.Info("register openapi handlers with base url: /oc3/api")
	server.RegisterHandlersWithBaseURL(e, &serverhandlers.Api{
		DB:          db,
		Redis:       redisClient,
		UI:          enableUI,
		SyncTimeout: viper.GetDuration("server.sync.timeout"),
	}, "/oc3/api")
	if enableUI {
		registerServerUI(e)
	}
	slog.Info("listen on " + addr)
	return e.Start(addr)
}

func registerServerUI(e *echo.Echo) {
	slog.Info("add handler /oc3/api/docs/")
	g := e.Group("/oc3/api/docs")
	g.Use(serverhandlers.UIMiddleware(context.Background()))
}
