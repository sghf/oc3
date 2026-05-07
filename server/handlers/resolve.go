package serverhandlers

import (
	"log/slog"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/logkey"
)

// resolveNode looks up a node by ID or name
func (a *Api) resolveNode(c echo.Context, log *slog.Logger, nodeId string) (*cdb.DBNode, error) {
	ctx := c.Request().Context()
	node, err := a.getODB().NodeByNodeIDOrNodename(ctx, nodeId)
	if err != nil {
		log.Error("cannot resolve node", logkey.NodeID, nodeId, logkey.Error, err)
		return nil, JSONProblemf(c, http.StatusInternalServerError, "cannot resolve node")
	}
	if node == nil {
		return nil, JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}
	return node, nil
}

// resolveService verifies that a service exists and is accessible
func (a *Api) resolveService(c echo.Context, log *slog.Logger, svcId string) error {
	ctx := c.Request().Context()
	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)
	svcs, err := a.getODB().GetService(ctx, svcId, cdb.ListParams{Limit: 1, Groups: groups, IsManager: isManager})
	if err != nil {
		log.Error("cannot resolve service", "svc_id", svcId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve service")
	}
	if len(svcs) == 0 {
		return JSONProblemf(c, http.StatusNotFound, "service %s not found", svcId)
	}
	return nil
}
