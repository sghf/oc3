package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/util/git"
)

var TaskSysreport = Task{
	name:    "sysreport",
	fn:      taskSysreport,
	period:  time.Minute,
	timeout: 10 * time.Minute,
}

type sysreportData struct {
	NeedCommit bool     `json:"need_commit"`
	Deleted    []string `json:"deleted"`
	NodeID     string   `json:"node_id"`
}

func taskSysreport(ctx context.Context, task *Task) error {
	if task.Redis == nil {
		return fmt.Errorf("redis client is nil")
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		result, err := task.Redis.LPop(ctx, cachekeys.FeedSysreportQ).Result()
		if err == redis.Nil {
			break
		}
		if err != nil {
			return fmt.Errorf("lpop: %w", err)
		}

		task.Infof("sysreport: %s", result)

		var data sysreportData
		if err := json.Unmarshal([]byte(result), &data); err != nil {
			task.Errorf("unmarshal '%s': %s", result, err)
			continue
		}

		task.Infof("processing sysreport for node %s", data.NodeID)

		if data.NeedCommit {
			odb, err := task.DBX(ctx)
			if err != nil {
				task.Errorf("dbx: %s", err)
				continue
			}

			sysreportDir := viper.GetString("scheduler.directories.uploads")
			nodeDir := filepath.Join(sysreportDir, "sysreport", data.NodeID)
			// Not while a purge rewrites the history of the node.
			sysreportMu.Lock()
			err = git.Commit(nodeDir)
			sysreportMu.Unlock()
			if err != nil {
				return fmt.Errorf("git commit: %w", err)
			}

			if err := odb.Commit(); err != nil {
				task.Errorf("commit: %s", err)
				odb.Rollback()
			} else {
				task.Infof("commit done for %s", data.NodeID)
			}
		}
	}
	return nil
}
