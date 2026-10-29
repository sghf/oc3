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

// GetSaves handles GET /saves: the backups the backup servers index, as the
// historical Saves view lists them.
func (a *Api) GetSaves(c echo.Context, params server.GetSavesParams) error {
	return a.handleList(c, "GetSaves", "save", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetSaves(ctx, p)
	})
}

// GetSave handles GET /saves/{save_id}.
func (a *Api) GetSave(c echo.Context, saveId int64, params server.GetSaveParams) error {
	return a.handleList(c, "GetSave", "save", listEndpointParams{props: params.Props},
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetSave(ctx, saveId, p)
		})
}

// GetSavesSummary handles GET /saves/summary: the size and the number of the
// backups by app, server, group or object, under the filters of the list and
// the session filterset, for the charts of the Saves view.
func (a *Api) GetSavesSummary(c echo.Context, params server.GetSavesSummaryParams) error {
	log := echolog.GetLogHandler(c, "GetSavesSummary")
	by := params.By
	if !cdb.IsSaveShareKey(by) {
		return JSONProblemf(c, http.StatusBadRequest, "by: %q is none of app, server, group, object", by)
	}
	mapping := propsMapping["save"]
	filters, err := buildFilters(params.Filter, mapping)
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	session, err := a.sessionFilters(c, mapping)
	if err != nil {
		log.Error("cannot apply the session filterset", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot apply the session filterset")
	}
	filters = append(filters, session...)
	shares, err := a.ODB.SavesSummary(c.Request().Context(), UserGroupsFromContext(c), IsManager(c), filters, by)
	if err != nil {
		log.Error("cannot sum the backups", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot sum the backups")
	}
	resp := server.SavesSummaryResponse{Data: make([]struct {
		Count int64  `json:"count"`
		Key   string `json:"key"`
		Label string `json:"label"`
		Size  int64  `json:"size"`
	}, 0, len(shares))}
	for _, s := range shares {
		resp.TotalSize += s.Size
		resp.TotalCount += s.Count
		resp.Data = append(resp.Data, struct {
			Count int64  `json:"count"`
			Key   string `json:"key"`
			Label string `json:"label"`
			Size  int64  `json:"size"`
		}{Count: s.Count, Key: s.Key, Label: s.Label, Size: s.Size})
	}
	return c.JSON(http.StatusOK, resp)
}
