package serverhandlers

import (
	"fmt"
	"slices"
	"strings"

	"github.com/opensvc/oc3/schema"
	"github.com/opensvc/oc3/server"
)

type ListQueryParameters struct {
	Page      PageParams
	Props     []string
	WithMeta  bool
	WithStats bool
	OrderBy   []string
	GroupBy   []string
}

func buildListQueryParameters(
	props *server.InQueryProps,
	limit *server.InQueryLimit,
	offset *server.InQueryOffset,
	meta *server.InQueryMeta,
	stats *server.InQueryStats,
	orderby *server.InQueryOrderby,
	groupby *server.InQueryGroupby,
	mapping propMapping,
) (ListQueryParameters, error) {
	selectedProps, err := buildProps(props, mapping)
	if err != nil {
		return ListQueryParameters{}, err
	}

	groupExprs, err := buildGroupBy(groupby, mapping)
	if err != nil {
		return ListQueryParameters{}, err
	}

	orderExprs, err := buildOrderBy(orderby, mapping, len(groupExprs) > 0)
	if err != nil {
		return ListQueryParameters{}, err
	}

	return ListQueryParameters{
		Page:      buildPageParams(limit, offset),
		Props:     selectedProps,
		WithMeta:  queryWithMeta(meta),
		WithStats: queryWithStats(stats),
		OrderBy:   orderExprs,
		GroupBy:   groupExprs,
	}, nil
}

func buildGroupBy(groupby *server.InQueryGroupby, mapping propMapping) ([]string, error) {
	if groupby == nil || *groupby == "" {
		return nil, nil
	}
	tokens := strings.Split(*groupby, ",")
	exprs := make([]string, 0, len(tokens))
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		// As a sort or a filter: a prop of the list, or a column of a joined table
		// as table.column, stor_array.id say.
		col, err := resolvePropCol(token, mapping, "groupby")
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, col.Qualified())
	}
	return exprs, nil
}

// buildOrderBy returns the ORDER BY expressions of the requested sort, ended by
// the primary key of the main table (propMapping.primaryKey) for the rows the
// requested columns leave equal: without it the database may return them in any
// order, from one page to the next. Not for grouped rows, which the key does not
// name. No requested sort, no expression: the query keeps its default order.
func buildOrderBy(orderby *server.InQueryOrderby, mapping propMapping, grouped bool) ([]string, error) {
	if orderby == nil || *orderby == "" {
		return nil, nil
	}
	tokens := strings.Split(*orderby, ",")
	exprs := make([]string, 0, len(tokens))
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		desc := false
		if strings.HasPrefix(token, "-") {
			desc = true
			token = token[1:]
		}
		col, err := resolvePropCol(token, mapping, "orderby")
		if err != nil {
			return nil, err
		}
		expr := col.Qualified()
		if desc {
			expr += " DESC"
		}
		exprs = append(exprs, expr)
	}
	if len(exprs) == 0 || grouped {
		return exprs, nil
	}
	// The key in the direction of the last requested column: a sort all in one
	// direction can be read from an index, backwards when descending, and a
	// page stop after its rows; mixed, the database sorts every row first.
	suffix := ""
	if strings.HasSuffix(exprs[len(exprs)-1], " DESC") {
		suffix = " DESC"
	}
	for _, col := range mapping.primaryKey() {
		key := col.Qualified()
		if !slices.ContainsFunc(exprs, func(e string) bool {
			return strings.EqualFold(strings.TrimSuffix(e, " DESC"), key)
		}) {
			exprs = append(exprs, key+suffix)
		}
	}
	return exprs, nil
}

// resolvePropCol resolves an orderby or filter prop to its column. A
// "table.column" prop is resolved through the mapping's Joins, the same way props
// selection does, so that a list can be sorted or filtered by a joined name (e.g.
// "services.svcname"). usage names the parameter in the error messages.
func resolvePropCol(token string, mapping propMapping, usage string) (*schema.Col, error) {
	if table, column, ok := strings.Cut(token, "."); ok {
		jd, joinKnown := mapping.Joins[table]
		if !joinKnown {
			return nil, fmt.Errorf("unknown %s prop %q", usage, token)
		}
		refMapping, refFound := propsMapping[jd.MappingKey]
		if !refFound {
			return nil, fmt.Errorf("unknown %s prop %q", usage, token)
		}
		def, ok := refMapping.Props[column]
		if !ok || !slices.Contains(refMapping.Available, column) {
			return nil, fmt.Errorf("unknown %s prop %q", usage, token)
		}
		// The join key names the table the query joins: refuse a column that lives
		// elsewhere, which would produce SQL referencing an absent table.
		if def.Col == nil || def.Col.T.Name != table {
			return nil, fmt.Errorf("prop %q cannot be used in %s (no column reference)", token, usage)
		}
		return def.Col, nil
	}
	def, ok := mapping.Props[token]
	if !ok {
		return nil, fmt.Errorf("unknown %s prop %q", usage, token)
	}
	if def.Col == nil {
		return nil, fmt.Errorf("prop %q cannot be used in %s (no column reference)", token, usage)
	}
	return def.Col, nil
}
