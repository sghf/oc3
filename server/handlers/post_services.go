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

// PostServices handles POST /services create or update a service.
func (a *Api) PostServices(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostServices")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "authentication required")
	}

	var body server.PostServicesJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	if body.SvcId == nil && body.Svcname == nil {
		return JSONProblemf(c, http.StatusBadRequest, "svc_id or svcname is required")
	}

	// Resolve lookup key: svc_id, fall back to svcname.
	lookupKey := ""
	if body.SvcId != nil {
		lookupKey = *body.SvcId
	} else {
		lookupKey = *body.Svcname
	}

	existing, err := odb.ServiceBySvcIDOrName(ctx, lookupKey)
	if err != nil {
		log.Error("cannot lookup service", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup service")
	}

	// Update path: service already exists.
	if existing != nil {
		fields := serviceBodyFieldsFromPostServices(body).toFields()
		return postServiceUpdate(c, log, odb, ctx, existing.SvcID, fields)
	}

	// Create path: requires svcname and cluster_id.
	if body.Svcname == nil || *body.Svcname == "" {
		return JSONProblemf(c, http.StatusBadRequest, "svcname is required to create a service")
	}
	if body.ClusterId == nil || *body.ClusterId == "" {
		return JSONProblemf(c, http.StatusBadRequest, "cluster_id is required to create a service")
	}

	_, svcID, err := odb.ObjectIDFindOrCreate(ctx, *body.Svcname, *body.ClusterId)
	if err != nil {
		log.Error("cannot find or create service id", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot find or create service id")
	}

	svcApp := ""
	if body.SvcApp != nil {
		svcApp = *body.SvcApp
	}
	svcApp = resolveApp(ctx, log, odb, c, svcApp)

	if err := odb.InsertService(ctx, svcID, *body.Svcname, *body.ClusterId, svcApp); err != nil {
		log.Error("cannot insert service", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot create service")
	}

	// Apply remaining optional fields.
	fields := serviceBodyFieldsFromPostServices(body).toFields()
	delete(fields, "svc_app") // already set at insert
	if len(fields) > 0 {
		if err := odb.UpdateServiceFields(ctx, svcID, fields); err != nil {
			log.Error("cannot update service fields after insert", logkey.Error, err)
		}
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "service.add",
		User:   userEmail,
		Fmt:    "create properties %(data)s",
		Dict:   map[string]any{"data": body},
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		slog.Debug("PostServices: cannot notify changes", logkey.Error, err)
	}

	info := fmt.Sprintf("Service svcname: %s, svc_id: %s, cluster_id: %s, svc_app: %s added", *body.Svcname, svcID, *body.ClusterId, svcApp)
	return fetchAndReturnService(c, odb, ctx, svcID, info, "cannot fetch created service")
}
