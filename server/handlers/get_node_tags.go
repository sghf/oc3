package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
)

// GetNodeTags handles GET /nodes/{node_id}/tags
func (a *Api) GetNodeTags(c echo.Context, nodeId string, params server.GetNodeTagsParams) error {
	log := echolog.GetLogHandler(c, "GetNodeTags")
	node, err := a.resolveNode(c, log, nodeId)
	if err != nil {
		return err
	}
	return a.handleList(c, "GetNodeTags", "tag", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.getODB().GetNodeTags(ctx, node.NodeID, p)
	})
}
