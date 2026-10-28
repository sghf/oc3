package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

type (
	VFilterset struct {
		FsetName    string
		FsetID      int
		JoinID      int
		FOrder      int
		FID         *int
		EncapFsetID *int
		FLogOp      string
		FTable      *string
		FField      *string
		FValue      *string
		FOp         *string
		FLabel      *string
	}

	Filterset struct {
		ID      int
		Name    string
		Author  string
		Updated time.Time
		Stats   bool
	}

	FiltersetIDsMap map[int]FiltersetIDs

	FiltersetIDs struct {
		NodeIDs string
		SvcIDs  string
	}
)

// FiltersetRulesetRef names a compliance ruleset restricted by a filterset.
type FiltersetRulesetRef struct {
	ID   int    `json:"id"`
	Name string `json:"ruleset_name"`
}

type FiltersetThreshold struct {
	ChkType     string
	ChkInstance string
	ChkLow      string
	ChkHigh     string
}

// FiltersetByIDOrName returns the (id, fset_name) of a filterset
func (oDb *DB) FiltersetByIDOrName(ctx context.Context, idOrName string) (int, string, error) {
	var (
		query string
		args  []any
	)
	if _, err := strconv.Atoi(idOrName); err == nil {
		query = "SELECT id, fset_name FROM gen_filtersets WHERE id = ?"
		args = []any{idOrName}
	} else {
		query = "SELECT id, fset_name FROM gen_filtersets WHERE fset_name = ?"
		args = []any{idOrName}
	}
	row := oDb.DB.QueryRowContext(ctx, query, args...)
	var (
		id   int
		name sql.NullString
	)
	if err := row.Scan(&id, &name); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, "", nil
		}
		return 0, "", fmt.Errorf("FiltersetByIDOrName: %w", err)
	}
	return id, name.String, nil
}

// FiltersetUsageRulesets lists the comp_rulesets attached to the given filterset.
func (oDb *DB) FiltersetUsageRulesets(ctx context.Context, fsetID int) ([]FiltersetRulesetRef, error) {
	const query = `SELECT comp_rulesets.ruleset_name, comp_rulesets.id
		FROM comp_rulesets_filtersets
		JOIN comp_rulesets ON comp_rulesets.id = comp_rulesets_filtersets.ruleset_id
		WHERE comp_rulesets_filtersets.fset_id = ?
		ORDER BY comp_rulesets.ruleset_name`
	rows, err := oDb.DB.QueryContext(ctx, query, fsetID)
	if err != nil {
		return nil, fmt.Errorf("FiltersetUsageRulesets: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := make([]FiltersetRulesetRef, 0)
	for rows.Next() {
		var (
			name sql.NullString
			id   int
		)
		if err := rows.Scan(&name, &id); err != nil {
			return nil, fmt.Errorf("FiltersetUsageRulesets scan: %w", err)
		}
		out = append(out, FiltersetRulesetRef{ID: id, Name: name.String})
	}
	return out, nil
}

// FiltersetUsageThresholds returns the gen_filterset_check_threshold rows for the given filterset.
func (oDb *DB) FiltersetUsageThresholds(ctx context.Context, fsetID int) ([]FiltersetThreshold, error) {
	const query = `SELECT chk_type, chk_instance, chk_low, chk_high
		FROM gen_filterset_check_threshold
		WHERE fset_id = ?`
	rows, err := oDb.DB.QueryContext(ctx, query, fsetID)
	if err != nil {
		return nil, fmt.Errorf("FiltersetUsageThresholds: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := make([]FiltersetThreshold, 0)
	for rows.Next() {
		var t FiltersetThreshold
		if err := rows.Scan(&t.ChkType, &t.ChkInstance, &t.ChkLow, &t.ChkHigh); err != nil {
			return nil, fmt.Errorf("FiltersetUsageThresholds scan: %w", err)
		}
		out = append(out, t)
	}
	return out, nil
}

type FiltersetExportFilter struct {
	ID     int    `json:"id"`
	FTable string `json:"f_table"`
	FField string `json:"f_field"`
	FOp    string `json:"f_op"`
	FValue string `json:"f_value"`
}

type FiltersetExportFilterEntry struct {
	FLogOp    string                 `json:"f_log_op"`
	FOrder    int                    `json:"f_order"`
	Filter    *FiltersetExportFilter `json:"filter"`
	Filterset *string                `json:"filterset"`
}

type FiltersetExport struct {
	ID        int                          `json:"id"`
	FsetName  string                       `json:"fset_name"`
	FsetStats *string                      `json:"fset_stats"`
	Filters   []FiltersetExportFilterEntry `json:"filters"`
}

type FiltersetExportData struct {
	Filtersets []FiltersetExport `json:"filtersets"`
}

func (oDb *DB) ExportFiltersets(ctx context.Context, rootFsetIDs []int) (FiltersetExportData, error) {
	out := FiltersetExportData{Filtersets: []FiltersetExport{}}
	if len(rootFsetIDs) == 0 {
		return out, nil
	}

	relations := make(map[int][]int)
	relRows, err := oDb.DB.QueryContext(ctx,
		"SELECT fset_id, encap_fset_id FROM gen_filtersets_filters WHERE encap_fset_id > 0")
	if err != nil {
		return out, fmt.Errorf("ExportFiltersets relations: %w", err)
	}
	for relRows.Next() {
		var parent, child int
		if err := relRows.Scan(&parent, &child); err != nil {
			_ = relRows.Close()
			return out, fmt.Errorf("ExportFiltersets relations scan: %w", err)
		}
		relations[parent] = append(relations[parent], child)
	}
	_ = relRows.Close()

	all := make(map[int]bool)
	for _, id := range rootFsetIDs {
		all[id] = true
	}
	queue := append([]int{}, rootFsetIDs...)
	for len(queue) > 0 {
		head := queue[0]
		queue = queue[1:]
		for _, child := range relations[head] {
			if !all[child] {
				all[child] = true
				queue = append(queue, child)
			}
		}
	}

	allIDs := make([]int, 0, len(all))
	for id := range all {
		allIDs = append(allIDs, id)
	}

	fsetByID := make(map[int]*FiltersetExport, len(allIDs))
	nameByID := make(map[int]string, len(allIDs))
	{
		placeholders := Placeholders(len(allIDs))
		args := make([]any, len(allIDs))
		for i, id := range allIDs {
			args[i] = id
		}
		query := "SELECT id, fset_name, fset_stats FROM gen_filtersets WHERE id IN (" + placeholders + ")"
		rows, err := oDb.DB.QueryContext(ctx, query, args...)
		if err != nil {
			return out, fmt.Errorf("ExportFiltersets gen_filtersets: %w", err)
		}
		for rows.Next() {
			var (
				id    int
				name  sql.NullString
				stats sql.NullString
			)
			if err := rows.Scan(&id, &name, &stats); err != nil {
				_ = rows.Close()
				return out, fmt.Errorf("ExportFiltersets gen_filtersets scan: %w", err)
			}
			fs := &FiltersetExport{
				ID:       id,
				FsetName: name.String,
				Filters:  []FiltersetExportFilterEntry{},
			}
			if stats.Valid {
				s := stats.String
				fs.FsetStats = &s
			}
			fsetByID[id] = fs
			nameByID[id] = name.String
		}
		_ = rows.Close()
	}

	type fsetFilterRow struct {
		fsetID      int
		fID         sql.NullInt64
		fLogOp      string
		fOrder      sql.NullInt64
		encapFsetID sql.NullInt64
	}
	var filterRows []fsetFilterRow
	fIDs := make(map[int64]bool)
	{
		placeholders := Placeholders(len(allIDs))
		args := make([]any, len(allIDs))
		for i, id := range allIDs {
			args[i] = id
		}
		query := "SELECT fset_id, f_id, f_log_op, f_order, encap_fset_id" +
			" FROM gen_filtersets_filters WHERE fset_id IN (" + placeholders + ")" +
			" ORDER BY f_order, id"
		rows, err := oDb.DB.QueryContext(ctx, query, args...)
		if err != nil {
			return out, fmt.Errorf("ExportFiltersets gen_filtersets_filters: %w", err)
		}
		for rows.Next() {
			var r fsetFilterRow
			if err := rows.Scan(&r.fsetID, &r.fID, &r.fLogOp, &r.fOrder, &r.encapFsetID); err != nil {
				_ = rows.Close()
				return out, fmt.Errorf("ExportFiltersets gen_filtersets_filters scan: %w", err)
			}
			filterRows = append(filterRows, r)
			if r.fID.Valid && r.fID.Int64 > 0 {
				fIDs[r.fID.Int64] = true
			}
		}
		_ = rows.Close()
	}

	filters := make(map[int64]FiltersetExportFilter, len(fIDs))
	if len(fIDs) > 0 {
		ids := make([]any, 0, len(fIDs))
		for id := range fIDs {
			ids = append(ids, id)
		}
		placeholders := Placeholders(len(ids))
		query := "SELECT id, f_table, f_field, f_op, f_value FROM gen_filters WHERE id IN (" + placeholders + ")"
		rows, err := oDb.DB.QueryContext(ctx, query, ids...)
		if err != nil {
			return out, fmt.Errorf("ExportFiltersets gen_filters: %w", err)
		}
		for rows.Next() {
			var (
				id     int64
				fTable sql.NullString
				fField sql.NullString
				fOp    sql.NullString
				fValue sql.NullString
			)
			if err := rows.Scan(&id, &fTable, &fField, &fOp, &fValue); err != nil {
				_ = rows.Close()
				return out, fmt.Errorf("ExportFiltersets gen_filters scan: %w", err)
			}
			filters[id] = FiltersetExportFilter{
				ID:     int(id),
				FTable: fTable.String,
				FField: fField.String,
				FOp:    fOp.String,
				FValue: fValue.String,
			}
		}
		_ = rows.Close()
	}

	for _, r := range filterRows {
		fs, ok := fsetByID[r.fsetID]
		if !ok {
			continue
		}
		entry := FiltersetExportFilterEntry{FLogOp: r.fLogOp}
		if r.fOrder.Valid {
			entry.FOrder = int(r.fOrder.Int64)
		}
		if r.fID.Valid && r.fID.Int64 > 0 {
			if f, ok := filters[r.fID.Int64]; ok {
				fcopy := f
				entry.Filter = &fcopy
			}
		}
		if r.encapFsetID.Valid {
			if name, ok := nameByID[int(r.encapFsetID.Int64)]; ok {
				n := name
				entry.Filterset = &n
			}
		}
		fs.Filters = append(fs.Filters, entry)
	}

	for _, id := range allIDs {
		if fs, ok := fsetByID[id]; ok {
			out.Filtersets = append(out.Filtersets, *fs)
		}
	}
	return out, nil
}

// GetFiltersetEncapFiltersets returns the gen_filtersets rows encapsulated by fsetID.
func (oDb *DB) GetFiltersetEncapFiltersets(ctx context.Context, fsetID int, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getFiltersetEncapFiltersets: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM gen_filtersets" +
		" JOIN gen_filtersets_filters ON gen_filtersets.id = gen_filtersets_filters.encap_fset_id" +
		" WHERE gen_filtersets_filters.fset_id = ?"
	args := []any{fsetID}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("gen_filtersets.fset_name, gen_filtersets.id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getFiltersetEncapFiltersets: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetFilterset returns a single gen_filtersets row by id or fset_name.
func (oDb *DB) GetFilterset(ctx context.Context, idOrName string, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getFilterset: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") + " FROM gen_filtersets WHERE "
	args := []any{}
	if _, err := strconv.Atoi(idOrName); err == nil {
		query += "gen_filtersets.id = ?"
		args = append(args, idOrName)
	} else {
		query += "gen_filtersets.fset_name = ?"
		args = append(args, idOrName)
	}
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getFilterset: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func (oDb *DB) FiltersetID(ctx context.Context, idOrName string) (int, bool, error) {
	if id, err := strconv.Atoi(idOrName); err == nil {
		return id, true, nil
	}
	var id int
	err := oDb.DB.QueryRowContext(ctx,
		"SELECT id FROM gen_filtersets WHERE fset_name = ? LIMIT 1", idOrName).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("FiltersetID: %w", err)
	}
	return id, true, nil
}

type FiltersetRow struct {
	ID        int
	FsetName  string
	FsetStats string
}

// GetFiltersetRow returns the gen_filtersets row for the given id, or nil when absent.
func (oDb *DB) GetFiltersetRow(ctx context.Context, id int) (*FiltersetRow, error) {
	const query = "SELECT id, fset_name, fset_stats FROM gen_filtersets WHERE id = ? LIMIT 1"
	var (
		row                 FiltersetRow
		fsetName, fsetStats sql.NullString
	)
	err := oDb.DB.QueryRowContext(ctx, query, id).Scan(&row.ID, &fsetName, &fsetStats)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("GetFiltersetRow: %w", err)
	}
	row.FsetName = fsetName.String
	row.FsetStats = fsetStats.String
	return &row, nil
}

type UpdateFiltersetFields struct {
	FsetName  *string
	FsetStats *string
}

func (oDb *DB) UpdateFilterset(ctx context.Context, id int, fields UpdateFiltersetFields) error {
	setClauses := []string{}
	args := []any{}
	if fields.FsetName != nil {
		setClauses = append(setClauses, "fset_name = ?")
		args = append(args, sql.NullString{String: *fields.FsetName, Valid: true})
	}
	if fields.FsetStats != nil {
		setClauses = append(setClauses, "fset_stats = ?")
		args = append(args, sql.NullString{String: *fields.FsetStats, Valid: true})
	}
	if len(setClauses) == 0 {
		return nil
	}
	query := "UPDATE gen_filtersets SET " + strings.Join(setClauses, ", ") + " WHERE id = ?"
	args = append(args, id)
	if _, err := oDb.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("UpdateFilterset: %w", err)
	}
	oDb.SetChange("gen_filtersets")
	return nil
}

func (oDb *DB) DeleteFiltersetCascade(ctx context.Context, id int) error {
	stmts := []struct {
		table string
		query string
	}{
		{"gen_filtersets_filters", "DELETE FROM gen_filtersets_filters WHERE fset_id = ?"},
		{"gen_filtersets_filters", "DELETE FROM gen_filtersets_filters WHERE encap_fset_id = ?"},
		{"comp_rulesets_filtersets", "DELETE FROM comp_rulesets_filtersets WHERE fset_id = ?"},
		{"gen_filterset_team_responsible", "DELETE FROM gen_filterset_team_responsible WHERE fset_id = ?"},
		{"gen_filterset_check_threshold", "DELETE FROM gen_filterset_check_threshold WHERE fset_id = ?"},
		{"gen_filterset_user", "DELETE FROM gen_filterset_user WHERE fset_id = ?"},
		{"stats_compare_fset", "DELETE FROM stats_compare_fset WHERE fset_id = ?"},
		// Not removed by the historical collector, which left the grant pointing to
		// a filterset that no longer exists.
		{"sysrep_allow", "DELETE FROM sysrep_allow WHERE fset_id = ?"},
		{"gen_filtersets", "DELETE FROM gen_filtersets WHERE id = ?"},
	}
	for _, s := range stmts {
		if _, err := oDb.ExecContext(ctx, s.query, id); err != nil {
			return fmt.Errorf("DeleteFiltersetCascade %s: %w", s.table, err)
		}
		oDb.SetChange(s.table)
	}
	return nil
}

func (oDb *DB) GetFiltersetEncapAttachment(ctx context.Context, parentID, childID int) (*FiltersetFilterAttachment, error) {
	const query = "SELECT f_order, f_log_op FROM gen_filtersets_filters WHERE fset_id = ? AND encap_fset_id = ? LIMIT 1"
	var (
		fOrder sql.NullInt64
		fLogOp sql.NullString
	)
	err := oDb.DB.QueryRowContext(ctx, query, parentID, childID).Scan(&fOrder, &fLogOp)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("GetFiltersetEncapAttachment: %w", err)
	}
	return &FiltersetFilterAttachment{FOrder: int(fOrder.Int64), FLogOp: fLogOp.String}, nil
}

func (oDb *DB) InsertFiltersetEncap(ctx context.Context, parentID, childID, fOrder int, fLogOp string) error {
	if _, err := oDb.ExecContext(ctx,
		"INSERT INTO gen_filtersets_filters (f_id, fset_id, encap_fset_id, f_order, f_log_op) VALUES (0, ?, ?, ?, ?)",
		parentID, childID, fOrder, fLogOp); err != nil {
		return fmt.Errorf("InsertFiltersetEncap: %w", err)
	}
	oDb.SetChange("gen_filtersets_filters")
	return nil
}

func (oDb *DB) UpdateFiltersetEncap(ctx context.Context, parentID, childID int, fOrder *int, fLogOp *string) error {
	setClauses := []string{}
	args := []any{}
	if fOrder != nil {
		setClauses = append(setClauses, "f_order = ?")
		args = append(args, *fOrder)
	}
	if fLogOp != nil {
		setClauses = append(setClauses, "f_log_op = ?")
		args = append(args, *fLogOp)
	}
	if len(setClauses) == 0 {
		return nil
	}
	query := "UPDATE gen_filtersets_filters SET " + strings.Join(setClauses, ", ") + " WHERE fset_id = ? AND encap_fset_id = ?"
	args = append(args, parentID, childID)
	if _, err := oDb.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("UpdateFiltersetEncap: %w", err)
	}
	oDb.SetChange("gen_filtersets_filters")
	return nil
}

func (oDb *DB) DetachFiltersetFromFilterset(ctx context.Context, parentID, childID int) (int64, error) {
	res, err := oDb.ExecContext(ctx,
		"DELETE FROM gen_filtersets_filters WHERE fset_id = ? AND encap_fset_id = ?", parentID, childID)
	if err != nil {
		return 0, fmt.Errorf("DetachFiltersetFromFilterset: %w", err)
	}
	oDb.SetChange("gen_filtersets_filters")
	n, _ := res.RowsAffected()
	return n, nil
}

func (oDb *DB) FiltersetEncapWouldLoop(ctx context.Context, childID, parentID int) (bool, error) {
	if childID == parentID {
		return true, nil
	}
	rows, err := oDb.DB.QueryContext(ctx,
		"SELECT encap_fset_id, fset_id FROM gen_filtersets_filters WHERE f_id = 0")
	if err != nil {
		return false, fmt.Errorf("FiltersetEncapWouldLoop: %w", err)
	}
	defer func() { _ = rows.Close() }()
	ancestors := make(map[int][]int)
	for rows.Next() {
		var encap, fset sql.NullInt64
		if err := rows.Scan(&encap, &fset); err != nil {
			return false, fmt.Errorf("FiltersetEncapWouldLoop scan: %w", err)
		}
		if !encap.Valid {
			continue
		}
		ancestors[int(encap.Int64)] = append(ancestors[int(encap.Int64)], int(fset.Int64))
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("FiltersetEncapWouldLoop rows: %w", err)
	}

	visited := make(map[int]bool)
	var recurse func(node int) bool
	recurse = func(node int) bool {
		if visited[node] {
			return false
		}
		visited[node] = true
		for _, parent := range ancestors[node] {
			if parent == childID {
				return true
			}
			if recurse(parent) {
				return true
			}
		}
		return false
	}
	return recurse(parentID), nil
}

// GetFiltersets returns rows from the gen_filtersets table.
func (oDb *DB) GetFiltersets(ctx context.Context, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getFiltersets: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM gen_filtersets WHERE gen_filtersets.id > 0"
	filterConds, args := p.FilterConditions()
	for _, cond := range filterConds {
		query += " AND " + cond
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("gen_filtersets.fset_name, gen_filtersets.id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getFiltersets: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func (oDb *DB) GetVFiltersetsWithEncap(ctx context.Context, fsetID int) (l []VFilterset, err error) {
	l, err = oDb.GetVFiltersets(ctx, fsetID)
	if err != nil {
		return
	}
	var more []VFilterset
	for _, e := range l {
		if e.EncapFsetID == nil {
			continue
		}
		if l, err := oDb.GetVFiltersets(ctx, *e.EncapFsetID); err != nil {
			return nil, err
		} else {
			more = append(more, l...)
		}
	}
	l = append(l, more...)
	return
}

func (oDb *DB) GetVFiltersets(ctx context.Context, fsetID int) (l []VFilterset, err error) {
	const query = `
			SELECT
				fset_name,
				fset_id,
				join_id,
				f_order,
				f_id,
				encap_fset_id,
				f_log_op,
				f_table,
				f_field,
				f_value,
				f_op,
				f_label
			FROM
				v_gen_filtersets
			WHERE
				fset_id=?
				-- A filterset without entries has one row of NULLs in the
				-- view, a left join: it is not an entry.
				AND join_id IS NOT NULL
			ORDER BY
				f_order, f_id
		`

	rows, err := oDb.DB.QueryContext(ctx, query, fsetID)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var e VFilterset
		if err = rows.Scan(&e.FsetName, &e.FsetID, &e.JoinID, &e.FOrder, &e.FID, &e.EncapFsetID, &e.FLogOp, &e.FTable, &e.FField, &e.FValue, &e.FOp, &e.FLabel); err != nil {
			return
		}
		l = append(l, e)
	}
	if err = rows.Err(); err != nil {
		return
	}
	return
}

// ResolveFilterset returns the node ids (refField "node_id") or the service
// ids ("svc_id") a filterset selects, as filterset_encap_query_id
// (init/models/views.py:144) computes them: its entries in f_order, the first
// taken as is, each next one intersected (AND) or united (OR) with the result
// so far. An encapsulated filterset is resolved as a whole, then its entry's
// NOT takes the complement of what it selects among all the nodes or services.
// A filterset without entries selects nothing.
func (oDb *DB) ResolveFilterset(ctx context.Context, fsetID int, refField string) (l []string, err error) {
	query, args, err := filtersetQuery(ctx, oDb.GetVFiltersets, fsetID, refField, map[int]bool{})
	if err != nil {
		return nil, err
	}
	slog.Debug(fmt.Sprintf("%d: %s, %v", fsetID, query, args))
	if query == "" {
		return
	}
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return
		}
		l = append(l, id)
	}
	slog.Debug(fmt.Sprintf("%d: %v", fsetID, l))
	if err = rows.Err(); err != nil {
		return
	}
	return
}

// filtersetQuery builds the query of the ids a filterset selects, "" when it
// has no usable entry, the entries of each filterset read by entriesOf. path
// holds the filtersets being resolved, encapsulating this one: a filterset
// found again in it encapsulates itself, an error.
func filtersetQuery(ctx context.Context, entriesOf func(context.Context, int) ([]VFilterset, error), fsetID int, refField string, path map[int]bool) (string, []any, error) {
	if path[fsetID] {
		return "", nil, fmt.Errorf("filterset %d encapsulates itself", fsetID)
	}
	path[fsetID] = true
	defer delete(path, fsetID)
	entries, err := entriesOf(ctx, fsetID)
	if err != nil {
		return "", nil, err
	}
	var (
		query string
		args  []any
	)
	encapDone := map[int]bool{}
	for _, e := range entries {
		var (
			part     string
			partArgs []any
		)
		if e.EncapFsetID != nil {
			// Once each, as the historical collector does.
			if encapDone[*e.EncapFsetID] {
				continue
			}
			encapDone[*e.EncapFsetID] = true
			if part, partArgs, err = filtersetQuery(ctx, entriesOf, *e.EncapFsetID, refField, path); err != nil {
				return "", nil, err
			}
			if part == "" {
				part = noIDQuery(refField)
			}
			if negatedLogOp(e.FLogOp) {
				part = complementQuery(refField, part)
			}
		} else {
			if part, partArgs, err = filterQuery(e, refField); err != nil {
				return "", nil, err
			}
			if part == "" {
				continue
			}
		}
		if query == "" {
			query, args = part, partArgs
			continue
		}
		switch e.FLogOp {
		case "OR NOT", "OR":
			query = fmt.Sprintf("(%s) UNION (%s)", query, part)
		case "AND NOT", "AND":
			query = fmt.Sprintf("(%s) INTERSECT (%s)", query, part)
		default:
			return "", nil, fmt.Errorf("unexpected f_log_op: %s", e.FLogOp)
		}
		args = append(args, partArgs...)
	}
	return query, args, nil
}

// filterQuery builds the query of the ids a filter entry of a filterset
// selects, "" when the entry names no filter. Its NOT keeps the ids the filter
// does not select.
func filterQuery(e VFilterset, refField string) (string, []any, error) {
	if e.FOp == nil || e.FValue == nil || e.FTable == nil || e.FField == nil {
		return "", nil, nil
	}
	var not, pre, sub string
	if negatedLogOp(e.FLogOp) {
		not = "NOT"
	}
	switch refField {
	case "svc_id":
		pre = fmt.Sprintf(`SELECT svc_id FROM services WHERE svc_id != "" AND svc_id %s IN`, not)
		switch *e.FTable {
		case "nodes", "node_ip", "node_hba", "packages", "patches", "diskinfo":
			sub = fmt.Sprintf("SELECT svc_id FROM services JOIN svcmon USING (svc_id) JOIN %s USING (node_id)", *e.FTable)
		case "apps":
			sub = "SELECT svc_id FROM services JOIN apps ON apps.app = services.svc_app"
		case "services":
			sub = "SELECT svc_id FROM services"
		default:
			sub = fmt.Sprintf("SELECT svc_id FROM services JOIN %s USING (svc_id)", *e.FTable)
		}
	case "node_id":
		pre = fmt.Sprintf(`SELECT node_id FROM nodes WHERE node_id != "" AND node_id %s IN`, not)
		switch *e.FTable {
		case "services":
			sub = fmt.Sprintf("SELECT node_id FROM nodes JOIN svcmon USING (node_id) JOIN %s USING (svc_id)", *e.FTable)
		case "apps":
			sub = "SELECT node_id FROM nodes JOIN apps ON apps.app = nodes.app"
		case "nodes":
			sub = "SELECT node_id FROM nodes"
		default:
			sub = fmt.Sprintf("SELECT node_id FROM nodes JOIN %s USING (node_id)", *e.FTable)
		}
	}
	switch *e.FOp {
	case "=", "<=", ">=", "<", ">", "!=", "LIKE", "NOT LIKE":
		return fmt.Sprintf(`%s (%s WHERE %s.%s %s ?)`, pre, sub, *e.FTable, *e.FField, *e.FOp), []any{*e.FValue}, nil
	case "IN", "NOT IN":
		values := strings.Split(*e.FValue, ",")
		args := make([]any, len(values))
		for i, v := range values {
			args[i] = v
		}
		return fmt.Sprintf(`%s (%s WHERE %s.%s %s (%s))`, pre, sub, *e.FTable, *e.FField, *e.FOp, Placeholders(len(values))), args, nil
	default:
		return "", nil, fmt.Errorf("unexpected f_op: %s", *e.FOp)
	}
}

// negatedLogOp tells the entries whose selection is negated, "AND NOT" and
// "OR NOT".
func negatedLogOp(logOp string) bool {
	return logOp == "AND NOT" || logOp == "OR NOT"
}

// noIDQuery is a query of the ids of refField selecting none.
func noIDQuery(refField string) string {
	if refField == "svc_id" {
		return "SELECT svc_id FROM services WHERE 1=0"
	}
	return "SELECT node_id FROM nodes WHERE 1=0"
}

// complementQuery is the query of the ids of refField query does not select.
func complementQuery(refField, query string) string {
	if refField == "svc_id" {
		return fmt.Sprintf(`SELECT svc_id FROM services WHERE svc_id != "" AND svc_id NOT IN (%s)`, query)
	}
	return fmt.Sprintf(`SELECT node_id FROM nodes WHERE node_id != "" AND node_id NOT IN (%s)`, query)
}

func (oDb *DB) ResolveFiltersets(ctx context.Context) (FiltersetIDsMap, error) {
	m := make(FiltersetIDsMap)
	filtersets, err := oDb.GetStatsFiltersets(ctx)
	if err != nil {
		return nil, err
	}
	toString := func(l []string) string {
		for i, s := range l {
			l[i] = fmt.Sprintf(`"%s"`, s)
		}
		return strings.Join(l, ",")
	}
	for _, filterset := range filtersets {
		var e FiltersetIDs
		if l, err := oDb.ResolveFilterset(ctx, filterset.ID, "node_id"); err != nil {
			return nil, err
		} else {
			e.NodeIDs = toString(l)
		}
		if l, err := oDb.ResolveFilterset(ctx, filterset.ID, "svc_id"); err != nil {
			return nil, err
		} else {
			e.SvcIDs = toString(l)
		}
		m[filterset.ID] = e
		slog.Debug(fmt.Sprintf("%d: %#v", filterset.ID, e))
	}
	return m, nil
}

/*
	// ResolveFiltersetsFromCache is not needed as its only user is the metrics task
	// that refreshed the cache ... just use the ResolveFiltersets() directly from this
	// task.
	func (oDb *DB) ResolveFiltersetsFromCache(ctx context.Context) (FiltersetIDsMap, error) {
		const query = `
			SELECT
			    fset_id,
			    GROUP_CONCAT(IF(obj_type = 'svc_id', CONCAT('"', obj_id, '"'), NULL)) AS svc_ids,
			    GROUP_CONCAT(IF(obj_type = 'node_id', CONCAT('"', obj_id, '"'), NULL)) AS node_ids
			FROM
			    fset_cache
			GROUP BY
			    fset_id;
			`
		m := make(FiltersetIDsMap)
		rows, err := oDb.DB.QueryContext(ctx, query)
		if err != nil {
			return m, err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var (
				id              int
				svcIDs, nodeIDs sql.NullString
			)
			if err = rows.Scan(&id, &svcIDs, &nodeIDs); err != nil {
				return m, err
			}
			e := FiltersetIDs{}
			if nodeIDs.Valid {
				e.NodeIDs = nodeIDs.String
			} else {
				e.NodeIDs = "NULL"
			}
			if svcIDs.Valid {
				e.SvcIDs = svcIDs.String
			} else {
				e.SvcIDs = "NULL"
			}
			m[id] = e
		}
		if err = rows.Err(); err != nil {
			return m, err
		}
		return m, nil
	}
*/

func (oDb *DB) GetStatsFiltersets(ctx context.Context) (fsets []Filterset, err error) {
	const query = `
                SELECT id, fset_name, fset_author, fset_updated, fset_stats
                FROM gen_filtersets
                WHERE fset_stats="T"`
	rows, err := oDb.DB.QueryContext(ctx, query)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var fset Filterset
		if err = rows.Scan(&fset.ID, &fset.Name, &fset.Author, &fset.Updated, &fset.Stats); err != nil {
			return
		}
		fsets = append(fsets, fset)
	}
	if err = rows.Err(); err != nil {
		return
	}
	return
}

// FiltersetByName returns the id of the filterset having the given name.
func (oDb *DB) FiltersetByName(ctx context.Context, name string) (int, bool, error) {
	var id int
	err := oDb.DB.QueryRowContext(ctx,
		"SELECT id FROM gen_filtersets WHERE fset_name = ? LIMIT 1", name).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("FiltersetByName: %w", err)
	}
	return id, true, nil
}

// InsertFilterset creates a filterset and returns its id.
func (oDb *DB) InsertFilterset(ctx context.Context, fsetName, fsetStats, author string) (int, error) {
	const query = `INSERT INTO gen_filtersets (fset_name, fset_stats, fset_author, fset_updated)
		VALUES (?, ?, ?, NOW())`
	res, err := oDb.ExecContext(ctx, query, fsetName,
		sql.NullString{String: fsetStats, Valid: true}, author)
	if err != nil {
		return 0, fmt.Errorf("InsertFilterset: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("InsertFilterset lastInsertId: %w", err)
	}
	oDb.SetChange("gen_filtersets")
	return int(id), nil
}
