package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/opensvc/oc3/schema"
)

func buildServicesQuery(groups []string, isManager bool, selectExprs []string, filters []ColumnFilter) (string, []any, error) {
	q := From(schema.TServices).
		RawSelect(selectExprs...)

	if !isManager {
		cleanGroups := cleanGroups(groups)
		if len(cleanGroups) == 0 {
			q = q.WhereRaw("1=0")
		} else {
			args := make([]any, len(cleanGroups))
			for i, g := range cleanGroups {
				args[i] = g
			}
			q = q.WhereRaw(
				"services.svc_app IN ("+
					"SELECT a.app FROM apps a"+
					" JOIN apps_responsibles ar ON ar.app_id = a.id"+
					" JOIN auth_group ag ON ag.id = ar.group_id"+
					" WHERE ag.role IN ("+Placeholders(len(cleanGroups))+")"+
					")",
				args...,
			)
		}
	} else {
		q = q.Where(schema.ServicesID, ">", 0)
	}

	// Column filters of the request, ANDed with the access control above.
	q = q.WhereFilters(filters)

	query, args, err := q.Build()
	if err != nil {
		return "", nil, fmt.Errorf("buildServicesQuery: %w", err)
	}
	return query, args, nil
}

func (oDb *DB) GetServices(ctx context.Context, p ListParams) ([]map[string]any, error) {
	query, args, err := buildServicesQuery(p.Groups, p.IsManager, p.SelectExprs, p.Filters)
	if err != nil {
		return nil, err
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("services.svcname")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getServices: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetServicesByIDs fetches services whose svc_id is in the given list.
func (oDb *DB) GetServicesByIDs(ctx context.Context, ids []string, p ListParams) ([]map[string]any, error) {
	query, args, err := buildServicesQuery(p.Groups, p.IsManager, p.SelectExprs, p.Filters)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		query += " AND 1=0"
	} else {
		query += " AND services.svc_id IN (" + Placeholders(len(ids)) + ")"
		for _, id := range ids {
			args = append(args, id)
		}
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("services.svcname")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getServicesByIDs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetService fetches a single service by svc_id (UUID) or svcname.
func (oDb *DB) GetService(ctx context.Context, svcID string, p ListParams) ([]map[string]any, error) {
	query, args, err := buildServicesQuery(p.Groups, p.IsManager, p.SelectExprs, p.Filters)
	if err != nil {
		return nil, err
	}
	query += " AND (services.svc_id = ? OR services.svcname = ?)"
	args = append(args, svcID, svcID)
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getService: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// Minimal service data needed
type DBService struct {
	SvcID   string
	Svcname string
	SvcApp  string
}

// ServiceBySvcIDOrName looks up a service by UUID or svcname
func (oDb *DB) ServiceBySvcIDOrName(ctx context.Context, id string) (*DBService, error) {
	const query = `SELECT svc_id, svcname, COALESCE(svc_app, '') FROM services WHERE svc_id = ? OR svcname = ? LIMIT 1`
	var svcID, svcname, svcApp sql.NullString
	err := oDb.DB.QueryRowContext(ctx, query, id, id).Scan(&svcID, &svcname, &svcApp)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("serviceBySvcIDOrName: %w", err)
	}
	return &DBService{SvcID: svcID.String, Svcname: svcname.String, SvcApp: svcApp.String}, nil
}

// Access is granted when the service's svc_app is in the list of apps the groups are responsible for.
func (oDb *DB) ServiceResponsible(ctx context.Context, svcID string, groups []string, isManager bool) (bool, error) {
	if isManager {
		return true, nil
	}
	const query = `SELECT svc_app FROM services WHERE svc_id = ? LIMIT 1`
	var svcApp sql.NullString
	if err := oDb.DB.QueryRowContext(ctx, query, svcID).Scan(&svcApp); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf("serviceResponsible: service %s does not exist", svcID)
		}
		return false, fmt.Errorf("serviceResponsible: %w", err)
	}
	allowedApps, err := oDb.AppsForGroups(ctx, groups)
	if err != nil {
		return false, fmt.Errorf("serviceResponsible: %w", err)
	}
	for _, a := range allowedApps {
		if strings.EqualFold(a, svcApp.String) {
			return true, nil
		}
	}
	return false, nil
}

func (oDb *DB) UpdateServiceFields(ctx context.Context, svcID string, fields map[string]any) error {
	defer logDuration("UpdateServiceFields", time.Now())
	allowed := map[string]bool{
		"svcname": true, "svc_app": true, "svc_env": true, "svc_comment": true,
		"svc_nodes": true, "svc_drpnode": true, "svc_drpnodes": true,
		"svc_autostart": true, "svc_drptype": true, "svc_drnoaction": true,
		"svc_metrocluster": true, "svc_wave": true, "svc_topology": true,
		"svc_flex_min_nodes": true, "svc_flex_max_nodes": true,
		"svc_flex_cpu_low_threshold": true, "svc_flex_cpu_high_threshold": true,
		"svc_ha": true, "svc_frozen": true, "svc_provisioned": true,
		"svc_placement": true, "svc_notifications": true, "svc_snooze_till": true,
		"svc_sla": true,
	}
	setClauses := []string{"updated = NOW()"}
	args := []any{}
	for col, val := range fields {
		if !allowed[col] {
			continue
		}
		// A datetime column rejects the empty string, which would fail the whole
		// UPDATE: an empty snooze date clears it.
		if s, ok := val.(string); ok && s == "" && col == "svc_snooze_till" {
			setClauses = append(setClauses, col+" = NULL")
			continue
		}
		setClauses = append(setClauses, col+" = ?")
		args = append(args, val)
	}
	if len(setClauses) == 1 {
		return nil
	}
	args = append(args, svcID)
	query := "UPDATE services SET " + strings.Join(setClauses, ", ") + " WHERE svc_id = ?"
	if _, err := oDb.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("updateServiceFields: %w", err)
	}
	oDb.SetChange("services")
	return nil
}

func (oDb *DB) InsertService(ctx context.Context, svcID, svcname, clusterID, svcApp string) error {
	defer logDuration("InsertService", time.Now())
	const query = `INSERT INTO services (svc_id, svcname, cluster_id, svc_app, updated) VALUES (?, ?, ?, ?, NOW())`
	if _, err := oDb.ExecContext(ctx, query, svcID, svcname, clusterID, svcApp); err != nil {
		return fmt.Errorf("insertService: %w", err)
	}
	oDb.SetChange("services")
	return nil
}

// DeleteServiceCascade deletes a service and all its related data
// serviceCascadeTables are the tables holding rows of a service, which its
// deletion empties of them.
var serviceCascadeTables = []string{
	"services",
	"drpservices",
	"checks_settings",
	"comp_rulesets_services",
	"comp_modulesets_services",
	"action_queue",
	"svc_tags",
	"svcmon",
	"dashboard",
	"svcdisks",
	"resmon",
	"checks_live",
	"comp_status",
	"resinfo",
	"saves",
	"svcactions",
	"svcmon_log",
	"resmon_log",
	"svcmon_log_ack",
	"comp_log",
	"comp_log_daily",
	"form_output_results",
	"svcmon_log_last",
	"resmon_log_last",
	"dashboard_events",
}

func (oDb *DB) DeleteServiceCascade(ctx context.Context, svcID string) error {
	defer logDuration("DeleteServiceCascade", time.Now())
	if svcID == "" {
		return fmt.Errorf("DeleteServiceCascade: empty svc_id")
	}
	tables := serviceCascadeTables
	for _, t := range tables {
		query := "DELETE FROM " + t + " WHERE svc_id = ?"
		if _, err := oDb.ExecContext(ctx, query, svcID); err != nil {
			return fmt.Errorf("DeleteServiceCascade %s: %w", t, err)
		}
		oDb.SetChange(t)
	}
	return nil
}

// DeleteServicesCascade deletes several services as DeleteServiceCascade does
// one, a statement per table for all of them: a batch costs the 25 statements
// once, not once per service.
func (oDb *DB) DeleteServicesCascade(ctx context.Context, svcIDs []string) error {
	defer logDuration("DeleteServicesCascade", time.Now())
	if len(svcIDs) == 0 {
		return nil
	}
	args := make([]any, len(svcIDs))
	for i, id := range svcIDs {
		if id == "" {
			return fmt.Errorf("DeleteServicesCascade: empty svc_id")
		}
		args[i] = id
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(svcIDs)), ",")
	for _, t := range serviceCascadeTables {
		query := "DELETE FROM " + t + " WHERE svc_id IN (" + placeholders + ")"
		if _, err := oDb.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("DeleteServicesCascade %s: %w", t, err)
		}
		oDb.SetChange(t)
	}
	return nil
}
