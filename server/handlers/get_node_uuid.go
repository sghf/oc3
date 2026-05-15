package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetNodeUUID handles GET /nodes/{node_id}/uuid
func (a *Api) GetNodeUUID(c echo.Context, nodeId string) error {
	log := echolog.GetLogHandler(c, "GetNodeUUID")
	odb := a.ODB
	ctx := c.Request().Context()
	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)

	log.Info("called", "node_id", nodeId)

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}

	node, err := odb.NodeByNodeIDOrNodename(ctx, nodeId)
	if err != nil {
		log.Error("cannot resolve node", "node_id", nodeId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve node")
	}
	if node == nil {
		return JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}

	responsible, err := odb.NodeResponsible(ctx, node.NodeID, groups, isManager)
	if err != nil {
		log.Error("cannot check node responsibility", "node_id", nodeId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check node responsibility")
	}
	if !responsible {
		return JSONProblemf(c, http.StatusForbidden, "you are not responsible for this node")
	}

	authNodes, err := odb.AuthNodesByNodeID(ctx, node.NodeID)
	if err != nil {
		log.Error("cannot get auth node", "node_id", node.NodeID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get node uuid")
	}
	if len(authNodes) == 0 {
		return JSONProblemf(c, http.StatusNotFound, "no uuid found for node %s", nodeId)
	}

	an := authNodes[0]
	return c.JSON(http.StatusOK, map[string]any{
		"data": []map[string]any{
			{
				"id":       an.ID,
				"nodename": an.Nodename,
				"uuid":     an.UUID,
				"node_id":  an.NodeID,
				"updated":  an.Updated,
			},
		},
	})
}
