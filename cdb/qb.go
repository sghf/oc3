// Package cdb - query builder
//
// Lightweight SQL query builder with automatic JOIN resolution based on the FK
// relationships declared in schema/relations.go.
//
// # Basic usage
//
//	sql, args, err := From(schema.TApps).
//	    Select(schema.AppsID, schema.AppsApp).
//	    Where(schema.AppsID, ">", 0).
//	    Build()
//
// # Automatic JOIN resolution
//
// When a selected column belongs to a table other than the FROM table, the
// builder resolves the required JOINs automatically by walking the Ref chain
// declared in schema/relations.go:
//
//	sql, args, err := From(schema.TAuthGroup).
//	    Select(schema.AuthGroupID, schema.AuthGroupRole).
//	    Via(schema.TAppsResponsibles).        // disambiguates: apps_responsibles vs apps_publications
//	    Where(schema.AppsResponsiblesAppID, "=", appID).
//	    Build()
//	// → SELECT auth_group.id, auth_group.role
//	//   FROM auth_group
//	//   JOIN apps_responsibles ON apps_responsibles.group_id = auth_group.id
//	//   WHERE apps_responsibles.app_id = ?
//
// # Ambiguity and Via()
//
// When multiple FK paths connect the FROM table to a needed table, Build()
// returns an error. Use Via() to specify the intermediate table(s) that
// disambiguate the path:
//
//	// Error: both apps_responsibles and apps_publications connect apps to auth_group
//	From(schema.TApps).Select(schema.AuthGroupRole).Build()
//
//	// OK: path is explicit
//	From(schema.TApps).Select(schema.AuthGroupRole).Via(schema.TAppsResponsibles).Build()
package cdb

import (
	"fmt"
	"strings"

	"github.com/opensvc/oc3/schema"
)

// joinStep describes one JOIN clause to add to the query.
type joinStep struct {
	newTable *schema.Table // table being joined
	left     *schema.Col   // left side of the ON condition
	right    *schema.Col   // right side of the ON condition
	leftJoin bool          // true → LEFT OUTER JOIN, false → INNER JOIN
	straight bool          // an inner join as STRAIGHT_JOIN, see Query.StraightJoin
}

func (j joinStep) sql() string {
	keyword := "JOIN"
	if j.leftJoin {
		keyword = "LEFT JOIN"
	} else if j.straight {
		keyword = "STRAIGHT_JOIN"
	}
	return fmt.Sprintf("%s %s ON %s = %s", keyword, j.newTable.Name, j.left.Qualified(), j.right.Qualified())
}

// rawJoin is a LEFT JOIN whose ON condition is written out, for a relation that
// the Ref chain cannot express, such as one on two columns.
type rawJoin struct {
	table *schema.Table
	on    string
}

// condition holds a WHERE clause fragment and its bound arguments.
type condition struct {
	col  *schema.Col // used for JOIN resolution; nil for raw conditions
	expr string
	args []any
}

// Query builds a SQL SELECT statement.
type Query struct {
	from       *schema.Table
	selects    []*schema.Col
	rawSelects []string        // raw SQL expressions (e.g. "COALESCE(apps.updated, '')")
	via        []*schema.Table // explicit intermediate tables for join disambiguation
	leftJoins  []*schema.Table // tables that must be joined with LEFT JOIN
	rawJoins   []rawJoin       // LEFT JOINs on a condition of their own, see LeftJoinOn
	wheres     []condition
	distinct   bool
	straight   bool
}

// From starts a new query against the given table.
func From(t *schema.Table) *Query {
	return &Query{from: t}
}

// Distinct adds the DISTINCT keyword to the SELECT clause.
func (q *Query) Distinct() *Query {
	q.distinct = true
	return q
}

// StraightJoin writes the inner joins as STRAIGHT_JOIN: the database then reads
// the FROM table first. For a list sorted on a column of the FROM table and
// read a page at a time, where the optimizer, blind to the LIMIT, would rather
// start from a smaller joined table and sort the whole join.
func (q *Query) StraightJoin() *Query {
	q.straight = true
	return q
}

// Select adds columns to the SELECT clause.
// Columns from tables other than the FROM table trigger automatic JOIN resolution.
func (q *Query) Select(cols ...*schema.Col) *Query {
	q.selects = append(q.selects, cols...)
	return q
}

// RawSelect adds raw SQL expressions to the SELECT clause (e.g. "COALESCE(apps.updated, ")").
// These are emitted verbatim and do not participate in JOIN resolution.
// Use Select() for typed column references whenever possible.
func (q *Query) RawSelect(exprs ...string) *Query {
	q.rawSelects = append(q.rawSelects, exprs...)
	return q
}

// Via declares intermediate tables required to resolve ambiguous join paths.
// Use it when Build() returns an ambiguity error.
func (q *Query) Via(tables ...*schema.Table) *Query {
	q.via = append(q.via, tables...)
	return q
}

// LeftJoin declares tables that must be joined with LEFT JOIN instead of INNER JOIN.
// Use it when rows from the FROM table should be preserved even if the joined
// table has no matching row (optional relationship).
// These tables are also added to the set of needed tables, so Via() is not required
// unless the path is ambiguous.
func (q *Query) LeftJoin(tables ...*schema.Table) *Query {
	q.leftJoins = append(q.leftJoins, tables...)
	q.via = append(q.via, tables...)
	return q
}

// LeftJoinOn LEFT JOINs a table on the given ON condition, written out, for a
// relation the Ref chain of schema/relations.go cannot express, such as one on two
// columns. The table is then joined: its columns may be selected and filtered on
// without JOIN resolution looking for a path to it. The condition may only name
// the FROM table and the tables reached by declared relations.
func (q *Query) LeftJoinOn(t *schema.Table, on string) *Query {
	q.rawJoins = append(q.rawJoins, rawJoin{table: t, on: on})
	return q
}

// Where adds a "col op ?" condition to the WHERE clause.
// If the column belongs to a table not yet reachable, add it via Via().
func (q *Query) Where(col *schema.Col, op string, val any) *Query {
	q.wheres = append(q.wheres, condition{
		col:  col,
		expr: fmt.Sprintf("%s %s ?", col.Qualified(), op),
		args: []any{val},
	})
	return q
}

// WhereRaw adds a raw SQL condition to the WHERE clause.
// The expression is emitted verbatim; args are bound in order.
// Use for subqueries or expressions that cannot be expressed with Where/WhereIn.
func (q *Query) WhereRaw(expr string, args ...any) *Query {
	q.wheres = append(q.wheres, condition{expr: expr, args: args})
	return q
}

// WhereFilters adds the column filters of a list request. Each carries its column,
// so a filter on a joined table not otherwise selected still gets its join.
func (q *Query) WhereFilters(filters []ColumnFilter) *Query {
	for _, f := range filters {
		q.wheres = append(q.wheres, condition{col: f.Col, expr: f.Expr, args: f.Args})
	}
	return q
}

// WhereIn adds a "col IN (?,...)" condition. Produces "1=0" for an empty slice.
func (q *Query) WhereIn(col *schema.Col, vals []string) *Query {
	if len(vals) == 0 {
		q.wheres = append(q.wheres, condition{expr: "1=0"})
		return q
	}
	ph := strings.Repeat("?,", len(vals))
	args := make([]any, len(vals))
	for i, v := range vals {
		args[i] = v
	}
	q.wheres = append(q.wheres, condition{
		col:  col,
		expr: fmt.Sprintf("%s IN (%s)", col.Qualified(), ph[:len(ph)-1]),
		args: args,
	})
	return q
}

// Build assembles the SQL query and its bound arguments.
// Returns an error if a required JOIN path is ambiguous or unreachable.
func (q *Query) Build() (string, []any, error) {
	if len(q.selects) == 0 && len(q.rawSelects) == 0 {
		return "", nil, fmt.Errorf("qb: no columns selected")
	}

	// Collect tables that need to be joined (SELECT + WHERE columns + Via hints).
	needed := map[*schema.Table]bool{}
	for _, col := range q.selects {
		if col.T != q.from {
			needed[col.T] = true
		}
	}
	for _, w := range q.wheres {
		if w.col != nil && w.col.T != q.from {
			needed[w.col.T] = true
		}
	}
	for _, t := range q.via {
		if t != q.from {
			needed[t] = true
		}
	}

	// The tables joined on a condition of their own need no path.
	for _, j := range q.rawJoins {
		delete(needed, j.table)
	}

	// Build the LEFT JOIN set for resolveJoins.
	leftJoinSet := make(map[*schema.Table]bool, len(q.leftJoins))
	for _, t := range q.leftJoins {
		leftJoinSet[t] = true
	}

	joins, err := resolveJoins(q.from, needed, leftJoinSet)
	if err != nil {
		return "", nil, err
	}

	// SELECT
	fields := make([]string, 0, len(q.selects)+len(q.rawSelects))
	for _, c := range q.selects {
		fields = append(fields, c.Qualified())
	}
	fields = append(fields, q.rawSelects...)

	keyword := "SELECT"
	if q.distinct {
		keyword = "SELECT DISTINCT"
	}
	sb := &strings.Builder{}
	fmt.Fprintf(sb, "%s %s\nFROM %s", keyword, strings.Join(fields, ", "), q.from.Name)

	for _, j := range joins {
		j.straight = q.straight
		fmt.Fprintf(sb, "\n%s", j.sql())
	}
	for _, j := range q.rawJoins {
		fmt.Fprintf(sb, "\nLEFT JOIN %s ON %s", j.table.Name, j.on)
	}

	var args []any
	if len(q.wheres) > 0 {
		parts := make([]string, len(q.wheres))
		for i, w := range q.wheres {
			parts[i] = w.expr
			args = append(args, w.args...)
		}
		fmt.Fprintf(sb, "\nWHERE %s", strings.Join(parts, "\n  AND "))
	}

	return sb.String(), args, nil
}

// resolveJoins computes the ordered list of JOIN steps needed to reach all
// tables in `needed` starting from `from`.
//
// It supports FK traversal in both directions:
//   - parent → child : col.Ref.T is joined, col.T is needed
//     → JOIN col.T ON col.Qualified() = col.Ref.Qualified()
//   - child → parent : col.T is joined, col.Ref.T is needed
//     → JOIN col.Ref.T ON col.Ref.Qualified() = col.Qualified()
//
// If multiple FK paths lead to the same needed table in the same iteration,
// an ambiguity error is returned. Use Via() to add intermediate tables to
// `needed` and force a specific path.
func resolveJoins(from *schema.Table, needed map[*schema.Table]bool, leftJoinSet map[*schema.Table]bool) ([]joinStep, error) {
	if len(needed) == 0 {
		return nil, nil
	}

	joined := map[*schema.Table]bool{from: true}
	remaining := make(map[*schema.Table]bool, len(needed))
	for t := range needed {
		remaining[t] = true
	}

	var steps []joinStep

	for len(remaining) > 0 {
		// Collect all candidates found in this pass (detect ambiguity).
		type candidate struct {
			step joinStep
		}
		found := map[*schema.Table][]joinStep{}

		for _, col := range schema.AllCols {
			if col.Ref == nil {
				continue
			}
			// Direction 1: parent already joined → join child
			if joined[col.Ref.T] && !joined[col.T] && remaining[col.T] {
				found[col.T] = append(found[col.T], joinStep{
					newTable: col.T,
					left:     col,
					right:    col.Ref,
					leftJoin: leftJoinSet[col.T],
				})
			}
			// Direction 2: child already joined → join parent
			if joined[col.T] && !joined[col.Ref.T] && remaining[col.Ref.T] {
				found[col.Ref.T] = append(found[col.Ref.T], joinStep{
					newTable: col.Ref.T,
					left:     col.Ref,
					right:    col,
					leftJoin: leftJoinSet[col.Ref.T],
				})
			}
		}

		if len(found) == 0 {
			names := make([]string, 0, len(remaining))
			for t := range remaining {
				names = append(names, fmt.Sprintf("%q", t.Name))
			}
			return nil, fmt.Errorf("qb: no FK path from %q to [%s]; add the intermediate table(s) with Via()",
				from.Name, strings.Join(names, ", "))
		}

		for table, candidates := range found {
			if len(candidates) > 1 {
				paths := make([]string, len(candidates))
				for i, c := range candidates {
					paths[i] = fmt.Sprintf("%s.%s", c.left.T.Name, c.left.Name)
				}
				return nil, fmt.Errorf("qb: ambiguous join path to %q from %q (candidates: %s); use Via() to pick one",
					table.Name, from.Name, strings.Join(paths, ", "))
			}
			step := candidates[0]
			joined[table] = true
			delete(remaining, table)
			steps = append(steps, step)
		}
	}

	return steps, nil
}
