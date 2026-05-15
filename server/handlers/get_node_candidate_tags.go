package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
)

// GetNodeCandidateTags handles GET /nodes/{node_id}/candidate_tags
func (a *Api) GetNodeCandidateTags(c echo.Context, nodeId string, params server.GetNodeCandidateTagsParams) error {
	log := echolog.GetLogHandler(c, "GetNodeCandidateTags")
	node, err := a.resolveNode(c, log, nodeId)
	if err != nil {
		return err
	}
	return a.handleList(c, "GetNodeCandidateTags", "tag", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetNodeCandidateTags(ctx, node.NodeID, p)
	})
}
