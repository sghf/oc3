package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetArrays handles GET /arrays
func (a *Api) GetArrays(c echo.Context, params server.GetArraysParams) error {
	odb := a.ODB
	return a.handleList(c, "GetArrays", "array", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return odb.GetArrays(ctx, p)
	})
}
