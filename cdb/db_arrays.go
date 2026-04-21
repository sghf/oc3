package cdb

import (
	"context"
	"fmt"

	"github.com/opensvc/oc3/schema"
)

func buildArraysQuery(selectExprs []string) (string, []any, error) {
	q := From(schema.TStorArray).
		RawSelect(selectExprs...).
		Where(schema.StorArrayID, ">", 0)

	query, args, err := q.Build()
	if err != nil {
		return "", nil, fmt.Errorf("buildArraysQuery: %w", err)
	}
	return query, args, nil
}

func (oDb *DB) GetArrays(ctx context.Context, p ListParams) ([]map[string]any, error) {
	query, args, err := buildArraysQuery(p.SelectExprs)
	if err != nil {
		return nil, err
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("stor_array.array_name, stor_array.id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getArrays: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}
