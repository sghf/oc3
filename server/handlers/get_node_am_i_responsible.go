package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetNodeAmIResponsible handles GET /nodes/{node_id}/am_i_responsible
func (a *Api) GetNodeAmIResponsible(c echo.Context, nodeId string) error {
	log := echolog.GetLogHandler(c, "GetNodeAmIResponsible")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)

	log.Info("called", "node_id", nodeId, "is_manager", isManager)

	node, err := odb.NodeByNodeIDOrNodename(ctx, nodeId)
	if err != nil {
		log.Error("cannot resolve node", "node_id", nodeId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve node %s", nodeId)
	}
	if node == nil {
		return JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}

	responsible, err := odb.NodeResponsible(ctx, node.NodeID, groups, isManager)
	if err != nil {
		log.Error("cannot check node responsibility", "node_id", nodeId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check responsibility for node %s", nodeId)
	}
	if !responsible {
		return JSONProblemf(c, http.StatusForbidden, "you are not responsible for node %s", nodeId)
	}

	return c.JSON(http.StatusOK, map[string]any{"data": true})
}
