package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetNodeHardware handles GET /nodes/{node_id}/hardware
func (a *Api) GetNodeHardware(c echo.Context, nodeId string, params server.GetNodeHardwareParams) error {
	log := echolog.GetLogHandler(c, "GetNodeHardware")
	odb := a.getODB()
	ctx := c.Request().Context()

	node, err := odb.NodeByNodeIDOrNodename(ctx, nodeId)
	if err != nil {
		log.Error("cannot resolve node", "node_id", nodeId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve node")
	}
	if node == nil {
		return JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}

	return a.handleList(c, "GetNodeHardware", "node_hw", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return odb.GetNodeHardware(ctx, node.NodeID, p)
	})
}
