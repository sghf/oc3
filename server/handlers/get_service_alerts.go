package serverhandlers

import (
	"errors"
	"net/http"
	"slices"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetServiceAlerts handles GET /services/{svc_id}/alerts.
func (a *Api) GetServiceAlerts(c echo.Context, svcId string, params server.GetServiceAlertsParams) error {
	mapping := propsMapping["alert"]

	query, err := buildListQueryParameters(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, mapping)
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	log := echolog.GetLogHandler(c, "GetServiceAlerts")

	svc, err := a.ODB.ServiceBySvcIDOrName(c.Request().Context(), svcId, UserGroupsFromContext(c), IsManager(c))
	if errors.Is(err, cdb.ErrServiceAmbiguous) {
		return JSONProblemf(c, http.StatusConflict, "%s", err)
	}
	if err != nil {
		log.Error("cannot resolve service", "svc_id", svcId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve service %s", svcId)
	}
	if svc == nil {
		return JSONProblemf(c, http.StatusNotFound, "service %s not found", svcId)
	}

	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)

	log.Info("called",
		"svc_id", svc.SvcID,
		"limit", query.Page.Limit, "offset", query.Page.Offset,
		"props", query.Props,
		"is_manager", isManager,
	)

	hasAlert := slices.Contains(query.Props, "alert")
	fetchProps := query.Props
	if hasAlert {
		fetchProps = stripProp(fetchProps, "alert")
		fetchProps = ensureProps(fetchProps, "dash_fmt", "dash_dict")
	}

	selectExprs, err := buildSelectClause(fetchProps, mapping)
	if err != nil {
		log.Error("cannot build select clause", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot build select clause")
	}

	items, err := a.ODB.GetServiceAlerts(c.Request().Context(), svc.SvcID, cdb.ListParams{
		Groups:      groups,
		IsManager:   isManager,
		Limit:       query.Page.Limit,
		Offset:      query.Page.Offset,
		Props:       fetchProps,
		SelectExprs: selectExprs,
		TypeHints:   buildTypeHints(fetchProps, mapping),
		OrderBy:     query.OrderBy,
		GroupBy:     query.GroupBy,
	})
	if err != nil {
		log.Error("cannot fetch alerts", "svc_id", svc.SvcID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get alerts for service %s", svc.SvcID)
	}

	if hasAlert {
		mangleAlerts(items, query.Props)
	}

	return c.JSON(http.StatusOK, newListResponse(items, mapping, query))
}
