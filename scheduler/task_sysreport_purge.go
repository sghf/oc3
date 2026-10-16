package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/git"
)

// sysreportMu serializes the commits of the sysreports and their purges: a
// purge rewrites the history a commit would extend.
var sysreportMu sync.Mutex

var TaskSysreportPurge = Task{
	name:   "sysreport_purge",
	fn:     taskSysreportPurge,
	period: 15 * time.Second,
	// A purge rewrites every revision of the nodes holding the files.
	timeout: time.Hour,
}

func taskSysreportPurge(ctx context.Context, task *Task) error {
	if task.Redis == nil {
		return fmt.Errorf("redis client is nil")
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		raw, err := task.Redis.LPop(ctx, cachekeys.SysreportPurgeQ).Result()
		if err == redis.Nil {
			return nil
		}
		if err != nil {
			return fmt.Errorf("lpop: %w", err)
		}
		var req git.PurgeRequest
		if err := json.Unmarshal([]byte(raw), &req); err != nil {
			task.Errorf("sysreport purge: unmarshal %q: %s", raw, err)
			continue
		}
		runSysreportPurge(ctx, task, req)
	}
}

func runSysreportPurge(ctx context.Context, task *Task, req git.PurgeRequest) {
	repos := git.Sysreport{Dir: filepath.Join(viper.GetString("scheduler.directories.uploads"), "sysreport")}
	status := git.PurgeStatus{PurgeRequest: req, State: "running", Started: time.Now().Format(time.RFC3339), Nodes: []git.PurgeNodeStatus{}}
	save := func() {
		b, _ := json.Marshal(status)
		if err := task.Redis.Set(ctx, cachekeys.SysreportPurgePrefix+req.ID, b, git.PurgeStatusTTL).Err(); err != nil {
			task.Errorf("sysreport purge %s: cannot save the status: %s", req.ID, err)
		}
	}
	save()

	nodes := req.NodeIDs
	if len(nodes) == 0 {
		all, err := repos.Nodes()
		if err != nil {
			status.State, status.Error = "failed", "cannot list the sysreports: "+err.Error()
			status.Finished = time.Now().Format(time.RFC3339)
			save()
			return
		}
		nodes = all
	}

	odb := task.DB()
	files := make([]string, len(req.Files))
	for i, f := range req.Files {
		files[i] = f.Path
	}
	failed := 0
	for _, nodeID := range nodes {
		if ctx.Err() != nil {
			status.Error = "interrupted"
			break
		}
		sysreportMu.Lock()
		result, err := repos.Purge(nodeID, req.Files)
		sysreportMu.Unlock()
		node := git.PurgeNodeStatus{NodeID: nodeID, PurgeResult: result}
		if err != nil && err != git.ErrNoSysreport {
			node.Error = err.Error()
			failed++
		}
		// The nodes the files were not found on are not listed.
		if node.Error == "" && len(result.Paths) == 0 {
			continue
		}
		status.Nodes = append(status.Nodes, node)
		save()

		entry := cdb.LogEntry{
			Action: "sysreport.purge",
			User:   req.Author,
			Level:  "info",
			Fmt:    "purged %(files)s from the sysreport history: %(rewritten)s revisions rewritten, %(dropped)s dropped, %(removed)s contents destroyed",
			Dict: map[string]any{
				"files": strings.Join(files, ", "), "rewritten": result.Rewritten,
				"dropped": result.Dropped, "removed": result.Removed, "purge_id": req.ID,
			},
		}
		if node.Error != "" {
			entry.Level = "error"
			entry.Fmt = "could not purge %(files)s from the sysreport history: %(error)s"
			entry.Dict = map[string]any{"files": strings.Join(files, ", "), "error": node.Error, "purge_id": req.ID}
		}
		if id, err := uuid.Parse(nodeID); err == nil {
			entry.NodeID = &id
		}
		if err := odb.Log(ctx, entry); err != nil {
			task.Errorf("sysreport purge %s: cannot write audit log: %s", req.ID, err)
		}
	}
	status.State = "done"
	if failed > 0 || status.Error != "" {
		status.State = "failed"
	}
	status.Finished = time.Now().Format(time.RFC3339)
	save()
	task.Infof("sysreport purge %s: %s on %d nodes", req.ID, status.State, len(status.Nodes))
}
