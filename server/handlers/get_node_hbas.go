package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
)

// GetNodeHbas handles GET /nodes/{node_id}/hbas
func (a *Api) GetNodeHbas(c echo.Context, nodeId string, params server.GetNodeHbasParams) error {
	log := echolog.GetLogHandler(c, "GetNodeHbas")
	node, err := a.resolveNode(c, log, nodeId)
	if err != nil {
		return err
	}
	return a.handleList(c, "GetNodeHbas", "hba", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetNodeHbas(ctx, node.NodeID, p)
	})
}
