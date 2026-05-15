package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
)

// GetNodeComplianceStatus handles GET /nodes/{node_id}/compliance/status
func (a *Api) GetNodeComplianceStatus(c echo.Context, nodeId string, params server.GetNodeComplianceStatusParams) error {
	log := echolog.GetLogHandler(c, "GetNodeComplianceStatus")
	node, err := a.resolveNode(c, log, nodeId)
	if err != nil {
		return err
	}
	return a.handleList(c, "GetNodeComplianceStatus", "comp_status", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetNodeComplianceStatus(ctx, node.NodeID, p)
	})
}
