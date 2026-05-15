package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// PostNode handles POST /nodes/{node_id}
func (a *Api) PostNode(c echo.Context, nodeId string) error {
	log := echolog.GetLogHandler(c, "PostNode")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) && !IsAuthByNode(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "authentication required")
	}

	var body server.PostNodeJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	existing, err := odb.NodeByNodeIDOrNodename(ctx, nodeId)
	if err != nil {
		log.Error("cannot lookup node", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup node")
	}
	if existing == nil {
		return JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}

	return postNodeUpdate(c, log, odb, ctx, existing.NodeID, nodeBodyFieldsFromPostNode(body).toFields())
}
