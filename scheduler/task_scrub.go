package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/viper"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/cdb"
)

// TaskScrubObjects marks services status "undef" if all instances have outdated data.
//
// For testing, force a scrubable dataset with (15 MINUTE being the default
// scheduler.task.scrub_object.max_age):
//
//	UPDATE services SET svc_status="up" WHERE svc_id IN (
//	  SELECT svc_id FROM svcmon GROUP BY svc_id
//	  HAVING SUM(mon_updated >= DATE_SUB(NOW(), INTERVAL 15 MINUTE)) = 0
//	);
var TaskScrubObjects = Task{
	name:    "scrub_object",
	desc:    "marks services status=undef if all instances have outdated (aged scheduler.task.scrub_object.max_age, default 15m) data",
	fn:      taskScrubObjects,
	timeout: time.Minute,
}

var TaskScrubUnfinishedActions = Task{
	name:    "scrub_unfinished_actions",
	desc:    "set a end date and status=err on actions not finished after scheduler.task.scrub_unfinished_actions.max_age (default 2h) running",
	fn:      taskScrubUnfinishedActions,
	timeout: time.Minute,
}

var TaskScrubResources = Task{
	name:    "scrub_resources",
	desc:    "marks status=undef outdated (aged scheduler.task.scrub_resources.max_age, default 15m) resources",
	fn:      taskScrubResources,
	timeout: time.Minute,
}

var TaskScrubInstances = Task{
	name:    "scrub_instances",
	desc:    "purges outdated (aged scheduler.task.scrub_instances.max_age, default 21m) instances",
	fn:      taskScrubInstances,
	timeout: time.Minute,
}

var TaskScrubChecksLive = Task{
	name:    "scrub_checks_live",
	fn:      taskScrubChecksLive,
	timeout: time.Minute,
}

var TaskScrubNodeHBA = Task{
	name:    "scrub_node_hba",
	fn:      taskScrubNodeHBA,
	timeout: time.Minute,
}

var TaskScrubPackages = Task{
	name:    "scrub_packages",
	fn:      taskScrubPackages,
	timeout: time.Minute,
}

var TaskScrubPatches = Task{
	name:    "scrub_patches",
	fn:      taskScrubPatches,
	timeout: time.Minute,
}

var TaskScrubResmon = Task{
	name:    "scrub_resmon",
	fn:      taskScrubResmon,
	timeout: time.Minute,
}

var TaskScrubStorArray = Task{
	name:    "scrub_stor_array",
	fn:      taskScrubStorArray,
	timeout: time.Minute,
}

var TaskScrubDiskinfo = Task{
	name:    "scrub_diskinfo",
	fn:      taskScrubDiskinfo,
	timeout: time.Minute,
}

var TaskScrubSvcdisks = Task{
	name:    "scrub_svcdisks",
	fn:      taskScrubSvcdisks,
	timeout: time.Minute,
}

var TaskScrubStatic = Task{
	name:    "scrub_static",
	fn:      taskScrubStatic,
	timeout: time.Minute,
}

var TaskScrubTempviz = Task{
	name:    "scrub_tempviz",
	fn:      taskScrubTempviz,
	timeout: time.Minute,
}

var TaskScrubPdf = Task{
	name:    "scrub_pdf",
	fn:      taskScrubPdf,
	timeout: time.Minute,
}

var TaskScrubCompModulesetsNodes = Task{
	name:    "scrub_comp_modulesets_nodes",
	fn:      taskScrubCompModulesetsNodes,
	timeout: time.Minute,
}

var TaskScrubCompModulesetsServices = Task{
	name:    "scrub_comp_modulesets_services",
	fn:      taskScrubCompModulesetsServices,
	timeout: time.Minute,
}

var TaskScrubCompRulesetsNodes = Task{
	name:    "scrub_comp_rulesets_nodes",
	fn:      taskScrubCompRulesetsNodes,
	timeout: time.Minute,
}

var TaskScrubCompRulesetsServices = Task{
	name:    "scrub_comp_rulesets_services",
	fn:      taskScrubCompRulesetsServices,
	timeout: time.Minute,
}

var TaskScrubCompStatus = Task{
	name:    "scrub_comp_status",
	fn:      taskScrubCompStatus,
	timeout: time.Minute,
}

var TaskUpdateStorArrayDGQuota = Task{
	name:    "scrub_update_stor_array_dg_quota",
	fn:      taskUpdateStorArrayDGQuota,
	timeout: time.Minute,
}

var TaskScrub1D = Task{
	name:   "scrub_1d",
	period: 24 * time.Hour,
	children: TaskList{
		TaskScrubChecksLive,
		TaskScrubCompModulesetsNodes,
		TaskScrubCompModulesetsServices,
		TaskScrubCompRulesetsNodes,
		TaskScrubCompRulesetsServices,
		TaskScrubCompStatus,
		TaskScrubDiskinfo,
		TaskScrubNodeHBA,
		TaskScrubPackages,
		TaskScrubPatches,
		TaskScrubPdf,
		TaskScrubResmon,
		TaskScrubStatic,
		TaskScrubStorArray,
		TaskScrubSvcdisks,
		TaskUpdateStorArrayDGQuota,
	},
	timeout: 5 * time.Minute,
}

var TaskScrub1H = Task{
	name:   "scrub_1h",
	period: time.Minute,
	children: TaskList{
		TaskScrubTempviz,
	},
	timeout: time.Minute,
}

var TaskScrub10M = Task{
	name:   "scrub_10m",
	period: 10 * time.Minute,
	children: TaskList{
		TaskScrubUnfinishedActions,
	},
	timeout: time.Minute,
}

var TaskScrub1M = Task{
	name:   "scrub_1m",
	period: time.Minute,
	children: TaskList{
		TaskScrubObjects,
		TaskScrubResources,
		TaskScrubInstances,
	},
	timeout: time.Minute,
}

func taskScrubInstances(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()
	age, err := maxAge("scrub_instances")
	if err != nil {
		return err
	}
	instanceIDs, err := odb.InstancesOutdated(ctx, age)
	if err != nil {
		return err
	}
	if instanceIDs != nil {
		slog.Info(fmt.Sprintf("purge outdated %s", instanceIDs))
	}
	for _, instanceID := range instanceIDs {
		odb.PurgeInstance(ctx, instanceID)
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskScrubResources(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	// Fetch the outdated resources still not in "undef" availstatus
	age, err := maxAge("scrub_resources")
	if err != nil {
		return err
	}
	resources, err := odb.ResourceOutdatedLists(ctx, age)
	if err != nil {
		return err
	}

	n := len(resources)
	if n == 0 {
		return nil
	}

	names := make([]string, n)

	// Historize `resmon` lines we will touch
	for i, resource := range resources {
		names[i] = resource.String()
		if err := odb.ResourceUpdateLog(ctx, resource, "undef"); err != nil {
			return err
		}
	}

	// Update the `resmon` table
	if modified, err := odb.ResourceUpdateStatus(ctx, resources, "undef"); err != nil {
		return err
	} else if int(modified) != n {
		task.Infof("set %d/%d resmon status to undef (no live instance) amongst %s", modified, n, names)
	} else {
		task.Infof("set %d resmon status to undef (no live instance) for %s", n, names)
	}

	// Create log table entries
	logEntries := make([]cdb.LogEntry, n)
	for i, resource := range resources {
		d := make(map[string]any)
		d["name"] = resource.String()
		logEntries[i] = cdb.LogEntry{
			Action: "resource.status",
			Fmt:    "resource '%(name)s' status flagged 'undef'",
			Dict:   d,
			User:   "scheduler",
			Level:  "error",
			SvcID:  &resource.OID,
			NodeID: &resource.NID,
		}
	}
	if err := odb.Log(ctx, logEntries...); err != nil {
		return err
	}
	odb.Session.SetChanges("log")

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}

	return odb.Commit()
}

func taskScrubObjects(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	// Fetch the outdated services still not in "undef" availstatus
	age, err := maxAge("scrub_object")
	if err != nil {
		return err
	}
	objects, err := odb.ObjectsOutdated(ctx, age)
	if err != nil {
		return err
	}

	n := len(objects)
	if n == 0 {
		return nil
	}

	// Historize `services` lines we will touch
	for _, o := range objects {
		if err := odb.ObjectUpdateLog(ctx, o.OID.String(), "undef"); err != nil {
			return err
		}
	}

	// Update the `services` table
	if modified, err := odb.ObjectUpdateStatusSimple(ctx, objects, "undef", "undef"); err != nil {
		return err
	} else if int(modified) != n {
		task.Infof("set %d/%d services status to undef (no live instance) amongst %s", modified, n, objects)
	} else {
		task.Infof("set %d services status to undef (no live instance) for %s", n, objects)
	}

	// Create log table entries
	logEntries := make([]cdb.LogEntry, n)
	for i, o := range objects {
		d := make(map[string]any)
		d["svc"] = o.String()
		logEntries[i] = cdb.LogEntry{
			Action: "service.status",
			Fmt:    "service '%(svc)s' has zero live instance. Status flagged 'undef'",
			Dict:   d,
			User:   "scheduler",
			Level:  "error",
			SvcID:  &o.OID,
		}
	}
	if err := odb.Log(ctx, logEntries...); err != nil {
		return err
	}
	odb.Session.SetChanges("log")

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}

	return odb.Commit()
}

func taskScrubChecksLive(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	age, err := maxAge("scrub_checks_live")
	if err != nil {
		return err
	}
	if err := odb.PurgeChecksOutdated(ctx, age); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskScrubNodeHBA(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	age, err := maxAge("scrub_node_hba")
	if err != nil {
		return err
	}
	if err := odb.PurgeNodeHBAsOutdated(ctx, age); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskScrubPackages(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	age, err := maxAge("scrub_packages")
	if err != nil {
		return err
	}
	if err := odb.PurgePackagesOutdated(ctx, age); err != nil {
		return err
	}
	// Every packages diff may have lost packages: a new generation for all.
	if task.Redis != nil {
		if err := task.Redis.Incr(ctx, cachekeys.PkgDiffGen).Err(); err != nil {
			task.Errorf("cannot renew the packages diff generation: %s", err)
		}
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskScrubPatches(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	age, err := maxAge("scrub_patches")
	if err != nil {
		return err
	}
	if err := odb.PurgePatchesOutdated(ctx, age); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskScrubResmon(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	age, err := maxAge("scrub_resmon")
	if err != nil {
		return err
	}
	if err := odb.PurgeResmonOutdated(ctx, age); err != nil {
		return err
	}
	// The status logs of the resources gone for as long, after the resources.
	if count, err := odb.PurgeResmonLogLastOrphans(ctx, age); err != nil {
		return err
	} else if count > 0 {
		slog.Info(fmt.Sprintf("scrub_resmon: purged %d resmon_log_last rows of resources gone", count))
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskScrubDiskinfo(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	age, err := maxAge("scrub_diskinfo")
	if err != nil {
		return err
	}
	if err := odb.PurgeDiskinfoOutdated(ctx, age); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskScrubSvcdisks(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	age, err := maxAge("scrub_svcdisks")
	if err != nil {
		return err
	}
	if err := odb.PurgeSvcdisksOutdated(ctx, age); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskScrubStorArray(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	age, err := maxAge("scrub_stor_array")
	if err != nil {
		return err
	}
	if err := odb.PurgeStorArrayOutdated(ctx, age); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskScrubCompModulesetsNodes(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	if err := odb.PurgeCompModulesetsNodes(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskScrubCompRulesetsNodes(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	if err := odb.PurgeCompRulesetsNodes(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskScrubCompModulesetsServices(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	if err := odb.PurgeCompModulesetsServices(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskScrubCompRulesetsServices(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	if err := odb.PurgeCompRulesetsServices(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskScrubCompStatus(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	age, err := maxAge("scrub_comp_status")
	if err != nil {
		return err
	}
	unattachedAge, err := maxAge("scrub_comp_status_unattached")
	if err != nil {
		return err
	}
	if err := odb.PurgeCompStatusOutdated(ctx, age); err != nil {
		return err
	}
	if err := odb.PurgeCompStatusSvcOrphans(ctx); err != nil {
		return err
	}
	if err := odb.PurgeCompStatusNodeOrphans(ctx); err != nil {
		return err
	}
	if err := odb.PurgeCompStatusModulesetOrphans(ctx, unattachedAge); err != nil {
		return err
	}
	if err := odb.PurgeCompStatusNodeUnattached(ctx, unattachedAge); err != nil {
		return err
	}
	if err := odb.PurgeCompStatusSvcUnattached(ctx, unattachedAge); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskUpdateStorArrayDGQuota(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	if err := odb.UpdateStorArrayDGQuota(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func scrubFiles(pattern string, threshold time.Time) error {
	var matches []string
	if m, err := filepath.Glob(pattern); err != nil {
		return fmt.Errorf("failed to glob files: %w", err)
	} else {
		matches = append(matches, m...)
	}
	for _, fpath := range matches {
		fileInfo, err := os.Stat(fpath)
		if err != nil {
			return err
		}
		mtime := fileInfo.ModTime()
		if mtime.Before(threshold) {
			slog.Info(fmt.Sprintf("rm %s mtime %s", fpath, mtime))
			if err := os.Remove(fpath); err != nil {
				return fmt.Errorf("failed to rm %s: %w", fpath, err)
			}
		}
	}
	return nil
}

func taskScrubStatic(ctx context.Context, task *Task) error {
	age, err := maxAge("scrub_static")
	if err != nil {
		return err
	}
	threshold := time.Now().Add(-age)
	directory := viper.GetString("scheduler.directories.static")
	if directory == "" {
		slog.Warn("skip: define scheduler.directories.static")
		return nil
	}
	if err := scrubFiles(filepath.Join(directory, "tempviz*.png"), threshold); err != nil {
		return err
	}
	if err := scrubFiles(filepath.Join(directory, "tempviz*.dot"), threshold); err != nil {
		return err
	}
	if err := scrubFiles(filepath.Join(directory, "stats_*_[0-9]*.png"), threshold); err != nil {
		return err
	}
	if err := scrubFiles(filepath.Join(directory, "stat_*_[0-9]*.png"), threshold); err != nil {
		return err
	}
	if err := scrubFiles(filepath.Join(directory, "stats_*_[0-9]*.svg"), threshold); err != nil {
		return err
	}
	if err := scrubFiles(filepath.Join(directory, "*-*-*-*.pdf"), threshold); err != nil {
		return err
	}
	return nil

}

func taskScrubTempviz(ctx context.Context, task *Task) error {
	age, err := maxAge("scrub_tempviz")
	if err != nil {
		return err
	}
	threshold := time.Now().Add(-age)
	directory := viper.GetString("scheduler.directories.static")
	if directory == "" {
		slog.Warn("skip: define scheduler.directories.static")
		return nil
	}
	return scrubFiles(filepath.Join(directory, "tempviz*"), threshold)
}

func taskScrubPdf(ctx context.Context, task *Task) error {
	age, err := maxAge("scrub_pdf")
	if err != nil {
		return err
	}
	threshold := time.Now().Add(-age)
	directory := viper.GetString("scheduler.directories.static")
	if directory == "" {
		slog.Warn("skip: define scheduler.directories.static")
		return nil
	}
	return scrubFiles(filepath.Join(directory, "*-*-*-*-*.pdf"), threshold)
}

func taskScrubUnfinishedActions(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	age, err := maxAge("scrub_unfinished_actions")
	if err != nil {
		return err
	}
	lines, err := odb.GetUnfinishedActions(ctx, age)
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	if len(lines) == 0 {
		return nil
	}
	var entries []cdb.LogEntry
	for _, line := range lines {
		entries = append(entries, cdb.LogEntry{
			Action: "action.timeout",
			User:   "collector",
			Fmt:    "action ids %(ids)s closed on timeout",
			Level:  "warning",
			SvcID:  &line.SvcID,
			NodeID: &line.NodeID,
			Dict: map[string]any{
				"ids": line.ID,
			},
		})

	}
	if err := odb.Log(ctx, entries...); err != nil {
		return fmt.Errorf("log: %w", err)
	}
	if err := odb.UpdateUnfinishedActions(ctx, age); err != nil {
		return fmt.Errorf("update: %w", err)
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return fmt.Errorf("notify: %w", err)
	}
	return odb.Commit()
}
