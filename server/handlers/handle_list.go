package serverhandlers

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"slices"

	"github.com/labstack/echo/v4"
	"github.com/spf13/viper"

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
	filter  *server.InQueryFilter

	// withUserID asks for the authenticated user's id to be forwarded in
	// cdb.ListParams. Set it only on the endpoints whose access control
	// references the caller's identity and not just its groups.
	withUserID bool

	// statsInGo counts the stats values over the fetched rows rather than in the
	// database, for the fetchers with a grouping of their own, which the grouping
	// of the counts would replace.
	statsInGo bool

	// virtual are the props computed from other props rather than read from a
	// column, as the historical collector's vprops, e.g. form_definition parsed
	// from form_yaml. They are listed in the mapping's Available props, without a
	// SQL expression.
	virtual map[string]virtualProp
}

// virtualProp is a prop computed after the fetch, from the props it requires.
// The required props are fetched even when not asked for, then dropped.
type virtualProp struct {
	requires []string
	// compute returns the value, or false to leave the prop out of the row.
	compute func(item map[string]any) (any, bool)
}

// fetchPropsFor replaces the virtual props of a selection by the props they
// require, keeping the order and without duplicates.
func fetchPropsFor(props []string, virtual map[string]virtualProp) []string {
	if len(virtual) == 0 {
		return props
	}
	out := make([]string, 0, len(props))
	for _, prop := range props {
		if v, ok := virtual[prop]; ok {
			out = ensureProps(out, v.requires...)
			continue
		}
		out = ensureProps(out, prop)
	}
	return out
}

// computeVirtualProps fills the virtual props asked for, then removes the props
// fetched only to compute them.
func computeVirtualProps(items []map[string]any, props []string, virtual map[string]virtualProp) {
	if len(virtual) == 0 {
		return
	}
	var wanted []string
	for _, prop := range props {
		if _, ok := virtual[prop]; ok {
			wanted = append(wanted, prop)
		}
	}
	if len(wanted) == 0 {
		return
	}
	for _, item := range items {
		for _, prop := range wanted {
			if value, ok := virtual[prop].compute(item); ok {
				item[prop] = value
			}
		}
		for _, prop := range wanted {
			for _, required := range virtual[prop].requires {
				if !slices.Contains(props, required) {
					delete(item, required)
				}
			}
		}
	}
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

	filters, err := buildFilters(p.filter, mapping)
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	// The session filterset of the caller narrows every list naming a node or a
	// service, on top of the filters of the request.
	session, err := a.sessionFilters(c, mapping)
	if err != nil {
		echolog.GetLogHandler(c, handlerName).Error("cannot apply the session filterset", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot apply the session filterset")
	}
	filters = append(filters, session...)

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
		"filters", len(filters),
		"is_manager", isManager,
	)

	fetchProps := fetchPropsFor(query.Props, p.virtual)
	selectExprs, err := buildSelectClause(fetchProps, mapping)
	if err != nil {
		log.Error("cannot build select clause", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot build select clause")
	}

	dbParams := cdb.ListParams{
		Groups:      groups,
		IsManager:   isManager,
		Limit:       query.Page.Limit,
		Offset:      query.Page.Offset,
		Props:       fetchProps,
		SelectExprs: selectExprs,
		TypeHints:   buildTypeHints(fetchProps, mapping),
		OrderBy:     query.OrderBy,
		GroupBy:     query.GroupBy,
		Filters:     filters,
	}
	if p.withUserID {
		dbParams.UserID = authUserID(c)
	}

	if query.WithStats {
		stats, err := listStats(c.Request().Context(), log, fetch, dbParams, query.Props, mapping, p.virtual, statsLimit(c, query), p.statsInGo)
		if errors.Is(err, errStatsTimeout) {
			return JSONProblem(c, http.StatusBadRequest, err.Error())
		}
		if err != nil {
			log.Error("cannot count the values", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot get %s stats", mappingKey)
		}
		return c.JSON(http.StatusOK, listResponse{
			Data: stats.values,
			Meta: &listMeta{Distinct: stats.distinct, Other: stats.other, Total: intPtr(stats.total)},
		})
	}

	items, err := fetch(c.Request().Context(), dbParams)
	if err != nil {
		log.Error("cannot fetch items", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get %s", mappingKey)
	}
	computeVirtualProps(items, query.Props, p.virtual)

	response := newListResponse(items, mapping, query)
	if query.WithMeta {
		response = response.withTotal(listTotal(c.Request().Context(), log, fetch, dbParams, len(items)))
	}
	return c.JSON(http.StatusOK, response)
}

// totalFromPage returns the list total when the page alone tells it: every row
// was asked for, the page is not full, or the first page is empty. Only a full
// page, or an empty page past the first, needs a count.
func totalFromPage(limit, offset, pageLen int) (int, bool) {
	switch {
	case limit <= 0:
		// No limit: the offset is not applied either, the page is the whole list.
		return pageLen, true
	case pageLen > 0 && pageLen < limit:
		return offset + pageLen, true
	case pageLen == 0 && offset == 0:
		return 0, true
	}
	return 0, false
}

// listTotal counts the rows of a list without pagination, for meta.total. The
// fetcher runs again with the same access control, filters and grouping, but
// without sort, and selects one column of up to server.list.total_max + 1 rows,
// counted here: a list longer than that has no total. With total_max 0, it
// selects a window count, for one row: COUNT(*) OVER () counts the rows of the
// final result. Either way a grouped list counts its groups, as the historical
// collector did. A failed count is logged and the total left out, rather than
// failing a list whose rows were read.
func listTotal(ctx context.Context, log *slog.Logger, fetch listFetcher, p cdb.ListParams, pageLen int) *int {
	if total, known := totalFromPage(p.Limit, p.Offset, pageLen); known {
		return &total
	}
	count := p
	count.CountOnly = true
	if max := viper.GetInt("server.list.total_max"); max > 0 {
		// Up to max+1 rows of one column, unsorted, counted here: a list of more
		// than max rows has no total, the client pages without it.
		count.SelectExprs = []string{"1"}
		count.Props = []string{"one"}
		count.TypeHints = map[string]string{"one": "int64"}
		count.Limit = max + 1
		count.Offset = 0
		rows, err := fetch(ctx, count)
		if err != nil {
			log.Error("cannot count the list rows", logkey.Error, err)
			return nil
		}
		if len(rows) > max {
			return nil
		}
		return intPtr(len(rows))
	}
	count.SelectExprs = []string{"COUNT(*) OVER ()"}
	count.Props = []string{"total"}
	count.TypeHints = map[string]string{"total": "int64"}
	count.Limit = 1
	count.Offset = 0
	rows, err := fetch(ctx, count)
	if err != nil {
		log.Error("cannot count the list rows", logkey.Error, err)
		return nil
	}
	if len(rows) == 0 {
		return intPtr(0)
	}
	switch n := rows[0]["total"].(type) {
	case int64:
		return intPtr(int(n))
	case int:
		return intPtr(n)
	}
	log.Error("unexpected list count", "value", rows[0]["total"])
	return nil
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
		// Typed as the lists type them: a decimal or an integer column reads as a
		// number, not as the text the driver gives.
		TypeHints: buildTypeHints(query.Props, mapping),
	}
	if p.withUserID {
		dbParams.UserID = authUserID(c)
	}

	items, err := fetch(c.Request().Context(), dbParams)
	if errors.Is(err, cdb.ErrServiceAmbiguous) {
		// A svcname several services bear: the caller names one by its svc_id.
		return JSONProblemf(c, http.StatusConflict, "%s", err)
	}
	if err != nil {
		log.Error("cannot fetch item", idKey, idVal, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get %s", mappingKey)
	}
	if len(items) == 0 {
		return JSONProblemf(c, http.StatusNotFound, "%s %s not found", mappingKey, idVal)
	}

	return c.JSON(http.StatusOK, newListResponse(items, mapping, query))
}
