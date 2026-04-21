package cdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/opensvc/oc3/schema"
)

type Tag struct {
	ID         int    `json:"id"`
	TagName    string `json:"tag_name"`
	TagCreated string `json:"tag_created"`
	TagExclude string `json:"tag_exclude"`
	TagData    any    `json:"tag_data"`
	TagID      string `json:"tag_id"`
}

// GetTags returns all tags with id > 0, or a specific tag if tagID is provided
func (oDb *DB) GetTags(ctx context.Context, tagID *int, limit, offset int) ([]Tag, error) {
	query := `
		SELECT id, tag_name, tag_created, tag_exclude, tag_data, tag_id
		FROM tags
		WHERE id > 0
	`
	var args []interface{}

	if tagID != nil {
		query += " AND id = ?"
		args = append(args, *tagID)
	}

	query += " ORDER BY tag_name, id"
	if tagID == nil {
		query, args = appendLimitOffset(query, args, limit, offset)
	}

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getTags: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var tags []Tag
	for rows.Next() {
		var tag Tag
		var tagCreated, tagExclude, tagData, tagID sql.NullString
		if err := rows.Scan(&tag.ID, &tag.TagName, &tagCreated, &tagExclude, &tagData, &tagID); err != nil {
			return nil, fmt.Errorf("getTags scan: %w", err)
		}
		if tagCreated.Valid {
			tag.TagCreated = tagCreated.String
		}
		if tagExclude.Valid {
			tag.TagExclude = tagExclude.String
		}
		if tagData.Valid && tagData.String != "" {
			// Try to parse as JSON
			var parsed any
			if err := json.Unmarshal([]byte(tagData.String), &parsed); err == nil {
				tag.TagData = parsed
			} else {
				// If not valid JSON, keep as string
				tag.TagData = tagData.String
			}
		}
		if tagID.Valid {
			tag.TagID = tagID.String
		}
		tags = append(tags, tag)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("getTags rows: %w", err)
	}

	return tags, nil
}

// GetTagNodes returns nodes where a tag (by integer id) is attached, with app-based auth.
func (oDb *DB) GetTagNodes(ctx context.Context, tagID int, p ListParams) ([]map[string]any, error) {
	query, args, err := buildNodesQuery(p.Groups, p.IsManager, p.SelectExprs)
	if err != nil {
		return nil, err
	}
	query += " AND nodes.node_id IN (SELECT node_id FROM node_tags WHERE node_tags.tag_id = (SELECT tag_id FROM tags WHERE id = ?))"
	args = append(args, tagID)
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("nodes.nodename")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("GetTagNodes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetNodeTags returns tags attached to a node (identified by node_id UUID).
func (oDb *DB) GetNodeTags(ctx context.Context, nodeID string, p ListParams) ([]map[string]any, error) {
	q := From(schema.TTags).
		RawSelect(p.SelectExprs...).
		WhereRaw("tags.tag_id IN (SELECT tag_id FROM node_tags WHERE node_id = ?)", nodeID)
	q = applyNodeAppAuth(q, nodeID, p.Groups, p.IsManager)
	query, args, err := q.Build()
	if err != nil {
		return nil, fmt.Errorf("GetNodeTags build: %w", err)
	}
	query += " " + p.OrderByClause("tags.tag_name")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("GetNodeTags: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetServiceTags returns tags attached to a service (identified by svc_id UUID or svcname).
func (oDb *DB) GetServiceTags(ctx context.Context, svcID string, p ListParams) ([]map[string]any, error) {
	q := From(schema.TTags).
		RawSelect(p.SelectExprs...).
		WhereRaw("tags.tag_id IN (SELECT tag_id FROM svc_tags WHERE svc_id = (SELECT svc_id FROM services WHERE svc_id = ? OR svcname = ? LIMIT 1))", svcID, svcID)
	q = applySvcAppAuth(q, svcID, p.Groups, p.IsManager)
	query, args, err := q.Build()
	if err != nil {
		return nil, fmt.Errorf("GetServiceTags build: %w", err)
	}
	query += " " + p.OrderByClause("tags.tag_name")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("GetServiceTags: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetNodeCandidateTags returns tags not yet attached to the node and not excluded by existing tag rules.
func (oDb *DB) GetNodeCandidateTags(ctx context.Context, nodeID string, p ListParams) ([]map[string]any, error) {
	type attachedTag struct {
		TagID      string
		TagExclude sql.NullString
	}
	rowsAttached, err := oDb.DB.QueryContext(ctx,
		"SELECT tags.tag_id, tags.tag_exclude FROM tags JOIN node_tags ON node_tags.tag_id = tags.tag_id WHERE node_tags.node_id = ?",
		nodeID,
	)
	if err != nil {
		return nil, fmt.Errorf("GetNodeCandidateTags attached: %w", err)
	}
	defer func() { _ = rowsAttached.Close() }()

	var tagIDs []string
	var excludePatterns []string
	for rowsAttached.Next() {
		var t attachedTag
		if err := rowsAttached.Scan(&t.TagID, &t.TagExclude); err != nil {
			return nil, fmt.Errorf("GetNodeCandidateTags scan: %w", err)
		}
		tagIDs = append(tagIDs, t.TagID)
		if t.TagExclude.Valid && t.TagExclude.String != "" {
			excludePatterns = append(excludePatterns, t.TagExclude.String)
		}
	}
	if err := rowsAttached.Err(); err != nil {
		return nil, fmt.Errorf("GetNodeCandidateTags rows: %w", err)
	}

	q := From(schema.TTags).RawSelect(p.SelectExprs...).Where(schema.TagsID, ">", 0)
	q = applyNodeAppAuth(q, nodeID, p.Groups, p.IsManager)
	if len(tagIDs) > 0 {
		q = q.WhereRaw("tags.tag_id NOT IN ("+Placeholders(len(tagIDs))+")", stringsToAny(tagIDs)...)
	}
	if len(excludePatterns) > 0 {
		q = q.WhereRaw("tags.tag_name NOT REGEXP ?", strings.Join(excludePatterns, "|"))
	}
	query, args, err := q.Build()
	if err != nil {
		return nil, fmt.Errorf("GetNodeCandidateTags build: %w", err)
	}
	query += " " + p.OrderByClause("tags.tag_name")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("GetNodeCandidateTags: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetServiceCandidateTags returns tags not yet attached to the service and not excluded by existing tag rules.
func (oDb *DB) GetServiceCandidateTags(ctx context.Context, svcID string, p ListParams) ([]map[string]any, error) {
	type attachedTag struct {
		TagID      string
		TagExclude sql.NullString
	}
	rowsAttached, err := oDb.DB.QueryContext(ctx,
		"SELECT tags.tag_id, tags.tag_exclude FROM tags JOIN svc_tags ON svc_tags.tag_id = tags.tag_id"+
			" JOIN services ON services.svc_id = svc_tags.svc_id WHERE services.svc_id = ? OR services.svcname = ?",
		svcID, svcID,
	)
	if err != nil {
		return nil, fmt.Errorf("GetServiceCandidateTags attached: %w", err)
	}
	defer func() { _ = rowsAttached.Close() }()

	var tagIDs []string
	var excludePatterns []string
	for rowsAttached.Next() {
		var t attachedTag
		if err := rowsAttached.Scan(&t.TagID, &t.TagExclude); err != nil {
			return nil, fmt.Errorf("GetServiceCandidateTags scan: %w", err)
		}
		tagIDs = append(tagIDs, t.TagID)
		if t.TagExclude.Valid && t.TagExclude.String != "" {
			excludePatterns = append(excludePatterns, t.TagExclude.String)
		}
	}
	if err := rowsAttached.Err(); err != nil {
		return nil, fmt.Errorf("GetServiceCandidateTags rows: %w", err)
	}

	q := From(schema.TTags).RawSelect(p.SelectExprs...).Where(schema.TagsID, ">", 0)
	q = applySvcAppAuth(q, svcID, p.Groups, p.IsManager)
	if len(tagIDs) > 0 {
		q = q.WhereRaw("tags.tag_id NOT IN ("+Placeholders(len(tagIDs))+")", stringsToAny(tagIDs)...)
	}
	if len(excludePatterns) > 0 {
		q = q.WhereRaw("tags.tag_name NOT REGEXP ?", strings.Join(excludePatterns, "|"))
	}
	query, args, err := q.Build()
	if err != nil {
		return nil, fmt.Errorf("GetServiceCandidateTags build: %w", err)
	}
	query += " " + p.OrderByClause("tags.tag_name")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("GetServiceCandidateTags: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetTagServices returns services where a tag (by integer id) is attached, with app-based auth.
func (oDb *DB) GetTagServices(ctx context.Context, tagID int, p ListParams) ([]map[string]any, error) {
	query, args, err := buildServicesQuery(p.Groups, p.IsManager, p.SelectExprs)
	if err != nil {
		return nil, err
	}
	query += " AND services.svc_id IN (SELECT svc_id FROM svc_tags WHERE svc_tags.tag_id = (SELECT tag_id FROM tags WHERE id = ?))"
	args = append(args, tagID)
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("services.svcname")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("GetTagServices: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetTagsNodes returns all node_tag attachment records with node app-based auth.
func (oDb *DB) GetTagsNodes(ctx context.Context, p ListParams) ([]map[string]any, error) {
	q := From(schema.TNodeTags).RawSelect(p.SelectExprs...)
	if !p.IsManager {
		cleanG := cleanGroups(p.Groups)
		if len(cleanG) == 0 {
			q = q.WhereRaw("1=0")
		} else {
			gArgs := stringsToAny(cleanG)
			q = q.WhereRaw(
				"node_tags.node_id IN (SELECT n.node_id FROM nodes n WHERE n.app IN ("+
					"SELECT a.app FROM apps a"+
					" JOIN apps_responsibles ar ON ar.app_id = a.id"+
					" JOIN auth_group ag ON ag.id = ar.group_id"+
					" WHERE ag.role IN ("+Placeholders(len(cleanG))+")))",
				gArgs...,
			)
		}
	} else {
		q = q.Where(schema.NodeTagsID, ">", 0)
	}
	query, args, err := q.Build()
	if err != nil {
		return nil, fmt.Errorf("GetTagsNodes build: %w", err)
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("node_tags.id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("GetTagsNodes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetTagsServices returns all svc_tag attachment records with service app-based auth.
func (oDb *DB) GetTagsServices(ctx context.Context, p ListParams) ([]map[string]any, error) {
	q := From(schema.TSvcTags).RawSelect(p.SelectExprs...)
	if !p.IsManager {
		cleanG := cleanGroups(p.Groups)
		if len(cleanG) == 0 {
			q = q.WhereRaw("1=0")
		} else {
			gArgs := stringsToAny(cleanG)
			q = q.WhereRaw(
				"svc_tags.svc_id IN (SELECT s.svc_id FROM services s WHERE s.svc_app IN ("+
					"SELECT a.app FROM apps a"+
					" JOIN apps_responsibles ar ON ar.app_id = a.id"+
					" JOIN auth_group ag ON ag.id = ar.group_id"+
					" WHERE ag.role IN ("+Placeholders(len(cleanG))+")))",
				gArgs...,
			)
		}
	} else {
		q = q.Where(schema.SvcTagsID, ">", 0)
	}
	query, args, err := q.Build()
	if err != nil {
		return nil, fmt.Errorf("GetTagsServices build: %w", err)
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("svc_tags.id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("GetTagsServices: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// applyNodeAppAuth adds a WHERE condition ensuring the node's app is accessible to the user's groups.
func applyNodeAppAuth(q *Query, nodeID string, groups []string, isManager bool) *Query {
	if isManager {
		return q
	}
	cleanG := cleanGroups(groups)
	if len(cleanG) == 0 {
		return q.WhereRaw("1=0")
	}
	gArgs := stringsToAny(cleanG)
	return q.WhereRaw(
		"EXISTS (SELECT 1 FROM nodes n WHERE n.node_id = ? AND n.app IN ("+
			"SELECT a.app FROM apps a"+
			" JOIN apps_responsibles ar ON ar.app_id = a.id"+
			" JOIN auth_group ag ON ag.id = ar.group_id"+
			" WHERE ag.role IN ("+Placeholders(len(cleanG))+")))",
		append([]any{nodeID}, gArgs...)...,
	)
}

// applySvcAppAuth adds a WHERE condition ensuring the service's app is accessible to the user's groups.
func applySvcAppAuth(q *Query, svcID string, groups []string, isManager bool) *Query {
	if isManager {
		return q
	}
	cleanG := cleanGroups(groups)
	if len(cleanG) == 0 {
		return q.WhereRaw("1=0")
	}
	gArgs := stringsToAny(cleanG)
	return q.WhereRaw(
		"EXISTS (SELECT 1 FROM services s WHERE (s.svc_id = ? OR s.svcname = ?) AND s.svc_app IN ("+
			"SELECT a.app FROM apps a"+
			" JOIN apps_responsibles ar ON ar.app_id = a.id"+
			" JOIN auth_group ag ON ag.id = ar.group_id"+
			" WHERE ag.role IN ("+Placeholders(len(cleanG))+")))",
		append([]any{svcID, svcID}, gArgs...)...,
	)
}

func stringsToAny(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}
