package serverhandlers

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// PostService handles POST /services/{svc_id} update a service's properties.
func (a *Api) PostService(c echo.Context, svcId string) error {
	log := echolog.GetLogHandler(c, "PostService")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "authentication required")
	}

	var body server.PostServiceJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	svc, err := odb.ServiceBySvcIDOrName(ctx, svcId)
	if err != nil {
		log.Error("cannot lookup service", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup service")
	}
	if svc == nil {
		return JSONProblemf(c, http.StatusNotFound, "service %s not found", svcId)
	}

	return postServiceUpdate(c, log, odb, ctx, svc.SvcID, serviceBodyFieldsFromPostService(body).toFields())
}

func postServiceUpdate(c echo.Context, log *slog.Logger, odb *cdb.DB, ctx context.Context, svcID string, fields map[string]any) error {
	if !IsManager(c) {
		responsible, err := odb.ServiceResponsible(ctx, svcID, UserGroupsFromContext(c), false)
		if err != nil {
			log.Error("cannot check service responsibility", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot check service responsibility")
		}
		if !responsible {
			return JSONProblemf(c, http.StatusForbidden, "user is not responsible for service %s", svcID)
		}
	}

	if app, ok := fields["svc_app"].(string); ok {
		fields["svc_app"] = resolveApp(ctx, log, odb, c, app)
	}

	if err := odb.UpdateServiceFields(ctx, svcID, fields); err != nil {
		log.Error("cannot update service", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot update service")
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "service.change",
		User:   userEmail,
		Fmt:    "change properties %(data)s",
		Dict:   map[string]any{"data": fields},
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	info := fmt.Sprintf("Service %s change: %v", svcID, fields)
	return fetchAndReturnService(c, odb, ctx, svcID, info, "cannot fetch updated service")
}

func fetchAndReturnService(c echo.Context, odb *cdb.DB, ctx context.Context, svcID, info, errMsg string) error {
	mapping := propsMapping["service"]
	props := defaultProps(mapping)
	selectExprs, err := buildSelectClause(props, mapping)
	if err != nil {
		return JSONProblemf(c, http.StatusInternalServerError, errMsg)
	}
	rows, err := odb.GetService(ctx, svcID, cdb.ListParams{
		IsManager:   true,
		Props:       props,
		SelectExprs: selectExprs,
	})
	if err != nil || len(rows) == 0 {
		return JSONProblemf(c, http.StatusInternalServerError, errMsg)
	}
	return c.JSON(http.StatusOK, map[string]any{
		"info": info,
		"data": []any{rows[0]},
	})
}
