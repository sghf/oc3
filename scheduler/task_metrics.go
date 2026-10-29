package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-graphite/go-whisper"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/timeseries"
)

var TaskMetrics = Task{
	name:    "metrics",
	period:  24 * time.Hour,
	fn:      taskMetrics,
	timeout: 15 * time.Minute,
}

func MakeWSPFilename(format string, args ...any) (string, error) {
	directory := viper.GetString("scheduler.directories.uploads")
	if directory == "" {
		return "", fmt.Errorf("define scheduler.directories.uploads")
	}
	return filepath.Join(directory, "stats", fmt.Sprintf(format+".wsp", args...)), nil
}

func taskMetrics(ctx context.Context, task *Task) error {
	odb := task.DB()
	metrics, err := odb.GetMetricsWithHistorize(ctx)
	if err != nil {
		return err
	}
	rodb, err := task.DBXRO(ctx)
	if err != nil {
		return err
	}
	defer rodb.Rollback()
	cache, err := odb.ResolveFiltersets(ctx)
	if err != nil {
		return err
	}
	doMetricFilterset := func(metric cdb.Metric, query string, fsetID int) error {
		rows, err := rodb.DB.QueryContext(ctx, query)
		if err != nil {
			return fmt.Errorf("%s: %w", query, err)
		}
		defer func() { _ = rows.Close() }()
		names, err := rows.Columns()
		if err != nil {
			return err
		}
		now := int(time.Now().Unix())
		colCount := len(names)
		if metric.HasIndex() {
			for rows.Next() {
				var (
					value       sql.NullFloat64
					instance    sql.NullString
					wspFilename string
				)
				values := make([]any, colCount)
				if metric.ColInstanceIndex != nil && *metric.ColInstanceIndex >= 0 {
					if *metric.ColInstanceIndex >= colCount {
						return fmt.Errorf("the metric instance index exceeds the resulsets number of columns")
					}
					values[*metric.ColInstanceIndex] = &instance
				}
				if metric.ColValueIndex >= colCount {
					return fmt.Errorf("the metric value index exceeds the resulsets number of columns")
				}
				values[metric.ColValueIndex] = &value
				if err := rows.Scan(values...); err != nil {
					return err
				}
				if !instance.Valid || instance.String == "" {
					instance.String = "None"
				}
				wspFilename, err = MakeWSPFilename("/metrics/%d/fsets/%d/%s", metric.ID, fsetID, instance.String)
				if err != nil {
					return err
				}
				// no query result means zero value => no need to test value.Valid as invalid have 0.0 in Float64
				task.Infof("%d/%d: insert %s %f", metric.ID, fsetID, wspFilename, value.Float64)
				if err := timeseries.Update(wspFilename, value.Float64, now, timeseries.DailyRetentions, whisper.Last, 0.0); err != nil {
					return err
				}
			}
		} else {
			values := make([]any, colCount)
			for i, _ := range values {
				var f float64
				values[i] = &f
			}
			if v := rows.Next(); !v {
				return fmt.Errorf("empty resultset")
			}
			if err := rows.Scan(values...); err != nil {
				return err
			}
			for i, instance := range names {
				value, ok := values[i].(*float64)
				if !ok {
					return fmt.Errorf("value %v is not a float64", values[i])
				}
				wspFilename, err := MakeWSPFilename("/metrics/%d/fsets/%d/%s", metric.ID, fsetID, instance)
				if err != nil {
					return err
				}
				task.Debugf("%d/%d: insert %s %f", metric.ID, fsetID, wspFilename, *value)
				if err := timeseries.Update(wspFilename, *value, now, timeseries.DailyRetentions, whisper.Last, 0.0); err != nil {
					return err
				}
			}
			if v := rows.Next(); v {
				return fmt.Errorf("more than 1 line in the resultset")
			}
		}

		return nil
	}
	doMetricByFilterset := func(metric cdb.Metric) error {
		for fsetID, fsetCache := range cache {
			query := metricFiltersetSQL(*metric.SQL, fsetCache)
			task.Debugf("%d/%d: %s", metric.ID, fsetID, query)
			if err := doMetricFilterset(metric, query, fsetID); err != nil {
				return err
			}
		}
		return nil
	}
	doMetric := func(metric cdb.Metric) error {
		if metric.SQL == nil {
			return fmt.Errorf("no sql query")
		}
		if metric.HasFilterset() {
			if err := doMetricByFilterset(metric); err != nil {
				return err
			}
		} else {
			if err := doMetricFilterset(metric, *metric.SQL, 0); err != nil {
				return err
			}
		}
		return nil
	}
	var errs error
	for _, metric := range metrics {
		if err := doMetric(metric); err != nil {
			task.Errorf("%d: %s", metric.ID, err)
			errs = errors.Join(errs, err)
			continue
		}
	}
	return errs
}

// metricFiltersetSQL is the query of a metric for a filterset: the
// %%fset_svc_ids%% and %%fset_node_ids%% placeholders of the metric's template
// replaced by the ids the filterset resolves to, or by an id matching nothing
// when it resolves to none. The template itself is left as is, for the next
// filterset.
func metricFiltersetSQL(template string, ids cdb.FiltersetIDs) string {
	const none = "\"magic1234567890\""
	svcIDs, nodeIDs := ids.SvcIDs, ids.NodeIDs
	if svcIDs == "" {
		svcIDs = none
	}
	if nodeIDs == "" {
		nodeIDs = none
	}
	return strings.NewReplacer("%%fset_svc_ids%%", svcIDs, "%%fset_node_ids%%", nodeIDs).Replace(template)
}
