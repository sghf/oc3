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

// listFetcher is the DB call signature shared by all list endpoints.
type listFetcher func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error)

// listEndpointParams bundles the standard query parameters shared by every list endpoint.
type listEndpointParams struct {
	props   *server.InQueryProps
	limit   *server.InQueryLimit
	offset  *server.InQueryOffset
	meta    *server.InQueryMeta
	stats   *server.InQueryStats
	orderby *server.InQueryOrderby
	groupby *server.InQueryGroupby
}

// handleList implements the common pipeline for all list endpoints:
//  1. Parse and validate query parameters (props, pagination, meta, stats)
//  2. Build SQL SELECT expressions from the resolved props
//  3. Build SQL JOIN fragments required by cross-table props
//  4. Call fetch to retrieve data from the database
//  5. Return a formatted JSON response with optional metadata
func (a *Api) handleList(
	c echo.Context,
	handlerName string,
	mappingKey string,
	p listEndpointParams,
	fetch listFetcher,
) error {
	mapping := propsMapping[mappingKey]

	query, err := buildListQueryParameters(p.props, p.limit, p.offset, p.meta, p.stats, p.orderby, p.groupby, mapping)
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	log := echolog.GetLogHandler(c, handlerName)
	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)

	log.Info("called",
		"limit", query.Page.Limit,
		"offset", query.Page.Offset,
		"props", query.Props,
		"meta", query.WithMeta,
		"stats", query.WithStats,
		"orderby", query.OrderBy,
		"groupby", query.GroupBy,
		"is_manager", isManager,
	)

	selectExprs, err := buildSelectClause(query.Props, mapping)
	if err != nil {
		log.Error("cannot build select clause", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot build select clause")
	}

	dbParams := cdb.ListParams{
		Groups:      groups,
		IsManager:   isManager,
		Limit:       query.Page.Limit,
		Offset:      query.Page.Offset,
		Props:       query.Props,
		SelectExprs: selectExprs,
		TypeHints:   buildTypeHints(query.Props, mapping),
		OrderBy:     query.OrderBy,
		GroupBy:     query.GroupBy,
	}

	items, err := fetch(c.Request().Context(), dbParams)
	if err != nil {
		log.Error("cannot fetch items", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get %s", mappingKey)
	}

	return c.JSON(http.StatusOK, newListResponse(items, mapping, query))
}

// handleItem is like handleList but expects exactly one result and returns 404
// when none is found. idKey and idVal are added to the "called" log entry.
func (a *Api) handleItem(
	c echo.Context,
	handlerName string,
	mappingKey string,
	idKey string,
	idVal string,
	p listEndpointParams,
	fetch listFetcher,
) error {
	mapping := propsMapping[mappingKey]

	query, err := buildListQueryParameters(p.props, p.limit, p.offset, p.meta, p.stats, p.orderby, p.groupby, mapping)
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	log := echolog.GetLogHandler(c, handlerName)
	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)

	log.Info("called", idKey, idVal, "props", query.Props, "is_manager", isManager)

	selectExprs, err := buildSelectClause(query.Props, mapping)
	if err != nil {
		log.Error("cannot build select clause", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot build select clause")
	}

	dbParams := cdb.ListParams{
		Groups:      groups,
		IsManager:   isManager,
		Limit:       query.Page.Limit,
		Offset:      query.Page.Offset,
		Props:       query.Props,
		SelectExprs: selectExprs,
	}

	items, err := fetch(c.Request().Context(), dbParams)
	if err != nil {
		log.Error("cannot fetch item", idKey, idVal, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get %s", mappingKey)
	}
	if len(items) == 0 {
		return JSONProblemf(c, http.StatusNotFound, "%s %s not found", mappingKey, idVal)
	}

	return c.JSON(http.StatusOK, newListResponse(items, mapping, query))
}
