package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetUserNodes handles GET /users/{user_id}/nodes: the nodes a user the caller
// can see is responsible for.
func (a *Api) GetUserNodes(c echo.Context, userId string, params server.GetUserNodesParams) error {
	return a.handleList(c, "GetUserNodes", "node", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter, withUserID: true,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetUserNodes(ctx, userId, p)
	})
}

// GetUserServices handles GET /users/{user_id}/services: the services a user
// the caller can see is responsible for.
func (a *Api) GetUserServices(c echo.Context, userId string, params server.GetUserServicesParams) error {
	return a.handleList(c, "GetUserServices", "service", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter, withUserID: true,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetUserServices(ctx, userId, p)
	})
}

// GetUserPrimaryGroup handles GET /users/{user_id}/primary_group: the primary
// group of a user the caller can see, as a list of none or one group.
func (a *Api) GetUserPrimaryGroup(c echo.Context, userId string, params server.GetUserPrimaryGroupParams) error {
	return a.handleList(c, "GetUserPrimaryGroup", "auth_group", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter, withUserID: true,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetUserPrimaryGroup(ctx, userId, p)
	})
}
