package main

import (
	"context"
	"log/slog"

	"github.com/labstack/echo-contrib/echoprometheus"
	"github.com/labstack/echo-contrib/pprof"
	"github.com/labstack/echo/v4"
	"github.com/shaj13/go-guardian/v2/auth/strategies/union"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/api"
	"github.com/opensvc/oc3/apihandlers"
	"github.com/opensvc/oc3/xauth"
)

var (
	mwProm = echoprometheus.NewMiddleware("oc3_api")
)

func startApi() error {
	addr := viper.GetString("listener.addr")
	return listenAndServe(addr)
}

func listenAndServe(addr string) error {
	enableUI := viper.GetBool("listener.ui.enable")

	db, err := newDatabase()
	if err != nil {
		return err
	}

	redisClient := newRedis()

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	if viper.GetBool("listener.pprof.enable") {
		slog.Info("add handler /oc3/public/pprof")
		// TODO: move to authenticated path
		pprof.Register(e, "/oc3/public/pprof")
	}

	strategy := union.New(
		xauth.NewPublicStrategy("/oc3/public/", "/oc3/docs"),
		xauth.NewBasicNode(db),
	)
	if viper.GetBool("listener.metrics.enable") {
		slog.Info("add handler /oc3/public/metrics")
		e.Use(mwProm)
		e.GET("/oc3/public/metrics", echoprometheus.NewHandler())
	}
	e.Use(apihandlers.AuthMiddleware(strategy))
	slog.Info("register openapi handlers with base url: /oc3")
	api.RegisterHandlersWithBaseURL(e, &apihandlers.Api{
		DB:          db,
		Redis:       redisClient,
		UI:          enableUI,
		SyncTimeout: viper.GetDuration("listener.sync.timeout"),
		Ev:          newEv(),
	}, "/oc3")
	if enableUI {
		registerAPIUI(e)
	}
	slog.Info("listen on " + addr)
	return e.Start(addr)
}

func registerAPIUI(e *echo.Echo) {
	slog.Info("add handler /oc3/docs/")
	g := e.Group("/oc3/docs")
	g.Use(apihandlers.UIMiddleware(context.Background()))
}
