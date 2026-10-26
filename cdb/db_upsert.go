package cdb

import (
	"context"
	"fmt"
	"strings"
)

// rowsUpsert describes rows to write in a table with an auto-increment id,
// each kept in place when already there.
//
// An INSERT ... ON DUPLICATE KEY UPDATE consumes an auto-increment value for
// each row it is given, a row it only updates included: the feeders write the
// same rows again and again, and the int ids of resmon, svcmon and dashboard
// ran out of values that way. upsertRows updates the rows already there, which
// consumes no value, then inserts only those missing.
type rowsUpsert struct {
	table string

	// source is a SELECT giving the rows to write, its columns named after
	// those of the table.
	source string
	args   []any

	// columns are the columns of source, written in a row missing.
	columns []string

	// match is the condition a row t of the table and the row s of source
	// meet when they have the same unique key.
	match string

	// update are the assignments to a row already there, as
	// "column = expression", the expression reading the row of source as s.
	update []string
}

// upsertRows writes the rows u describes and returns the count of rows
// changed, and of rows inserted.
func (oDb *DB) upsertRows(ctx context.Context, u rowsUpsert) (changed, inserted int64, err error) {
	if len(u.update) > 0 {
		assignments := make([]string, len(u.update))
		for i, a := range u.update {
			assignments[i] = "t." + strings.TrimSpace(a)
		}
		query := fmt.Sprintf("UPDATE %s t JOIN (%s) s ON %s SET %s", u.table, u.source, u.match, strings.Join(assignments, ", "))
		if changed, err = oDb.execCountContext(ctx, query, u.args...); err != nil {
			return 0, 0, fmt.Errorf("upsert %s: update: %w", u.table, err)
		}
	}

	selected := make([]string, len(u.columns))
	for i, c := range u.columns {
		selected[i] = "s." + c
	}
	// A row inserted meanwhile by another writer is left as it is: it holds
	// what that writer saw, as recent as what this one has.
	query := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM (%s) s WHERE NOT EXISTS (SELECT 1 FROM %s t WHERE %s) ON DUPLICATE KEY UPDATE %s.id = %s.id",
		u.table, strings.Join(u.columns, ", "), strings.Join(selected, ", "), u.source, u.table, u.match, u.table, u.table)
	if inserted, err = oDb.execCountContext(ctx, query, u.args...); err != nil {
		return 0, 0, fmt.Errorf("upsert %s: insert: %w", u.table, err)
	}
	return changed, inserted, nil
}

// givenRows returns a SELECT giving rows, each holding the values of columns,
// and its arguments: a source for rowsUpsert.
func givenRows(columns []string, rows [][]any) (string, []any) {
	parts := make([]string, len(rows))
	args := make([]any, 0, len(columns)*len(rows))
	for i, row := range rows {
		values := make([]string, len(columns))
		for j, c := range columns {
			if i == 0 {
				values[j] = "? AS " + c
			} else {
				values[j] = "?"
			}
		}
		parts[i] = "SELECT " + strings.Join(values, ", ")
		args = append(args, row...)
	}
	return strings.Join(parts, " UNION ALL "), args
}
