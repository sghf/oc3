package cdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"
)

type (
	Metric struct {
		ID               int64     `json:"id"`
		Name             *string   `json:"name,omitempty"`
		SQL              *string   `json:"sql,omitempty"`
		Author           *string   `json:"author,omitempty"`
		Created          time.Time `json:"created"`
		ColValueIndex    int       `json:"col_value_index"`
		ColInstanceIndex *int      `json:"col_instance_index,omitempty"`
		ColInstanceLabel *string   `json:"col_instance_label,omitempty"`
		Historize        bool      `json:"historize"`
	}
)

func (t Metric) String() string {
	b, _ := json.Marshal(t)
	return string(b)
}

func (t Metric) HasFilterset() bool {
	if t.SQL == nil {
		return false
	}
	if strings.Contains(*t.SQL, "%%fset_svc_ids%%") {
		return true
	}
	if strings.Contains(*t.SQL, "%%fset_node_ids%%") {
		return true
	}
	return false
}

func (t Metric) HasIndex() bool {
	if t.ColValueIndex >= 0 {
		return true
	}
	if t.ColInstanceIndex != nil && *t.ColInstanceIndex >= 0 {
		return true
	}
	return false
}

func (oDb *DB) GetMetricsWithHistorize(ctx context.Context) (lines []Metric, err error) {
	query := `SELECT
		    id,
		    metric_name,
		    metric_sql,
		    metric_author,
		    metric_created,
		    metric_col_value_index,
		    metric_col_instance_index,
		    metric_col_instance_label,
		    metric_historize
		FROM metrics
		WHERE
		    metric_historize="T"
	    `
	var rows *sql.Rows
	rows, err = oDb.DB.QueryContext(ctx, query)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var line Metric
		if err = rows.Scan(
			&line.ID,
			&line.Name,
			&line.SQL,
			&line.Author,
			&line.Created,
			&line.ColValueIndex,
			&line.ColInstanceIndex,
			&line.ColInstanceLabel,
			&line.Historize,
		); err != nil {
			return
		}
		lines = append(lines, line)
	}
	err = rows.Err()
	return

}
