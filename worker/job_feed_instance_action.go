package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/feeder"
	"github.com/opensvc/oc3/util/logkey"
)

type jobFeedInstanceAction struct {
	JobBase
	JobRedis
	JobDB

	nodeID    string
	clusterID string
	node      *cdb.DBNode

	// idX is the id of the posted action begin with the pattern: <objectName>@<nodeID>@<clusterID>:<uuid>
	idX string

	objectName string
	objectID   string

	// data is the posted action begin payload
	data *feeder.PostInstanceActionJSONRequestBody

	rawData []byte
}

func newAction(objectName, nodeID, clusterID, uuid string) *jobFeedInstanceAction {
	idX := fmt.Sprintf("%s@%s@%s:%s", objectName, nodeID, clusterID, uuid)

	return &jobFeedInstanceAction{
		JobBase: JobBase{
			name:   jtInstanceAction,
			detail: "ID: " + idX,
			logger: slog.With(logkey.NodeID, nodeID, logkey.ClusterID, clusterID, logkey.Object, objectName, logkey.JobName, jtInstanceAction),
		},
		JobRedis: JobRedis{
			cachePendingH:   cachekeys.FeedInstanceActionPendingH,
			cachePendingIDX: idX,
		},
		idX:        idX,
		nodeID:     nodeID,
		clusterID:  clusterID,
		objectName: objectName,
	}
}

func (d *jobFeedInstanceAction) Operations() []operation {
	return []operation{
		{name: "dropPending", do: d.dropPending},
		{name: "getData", do: d.getData},
		{name: "findNodeFromDb", do: d.findNodeFromDb},
		{name: "findObjectFromDb", do: d.findObjectFromDb},
		{name: "processAction", do: d.updateDB},
		{name: "pushFromTableChanges", do: d.pushFromTableChanges},
	}
}

func (d *jobFeedInstanceAction) getData(ctx context.Context) error {
	var (
		data feeder.PostInstanceActionJSONRequestBody
	)
	if b, err := d.redis.HGet(ctx, cachekeys.FeedInstanceActionH, d.idX).Bytes(); err != nil {
		return fmt.Errorf("getData: HGET %s %s: %w", cachekeys.FeedInstanceActionH, d.idX, err)
	} else if err = json.Unmarshal(b, &data); err != nil {
		return fmt.Errorf("getData: unexpected data from %s %s: %w", cachekeys.FeedInstanceActionH, d.idX, err)
	} else {
		d.rawData = b
		d.data = &data
	}

	slog.Debug(fmt.Sprintf("got action begin data for node %s:%#v", d.nodeID, d.data))
	return nil
}

func (d *jobFeedInstanceAction) findNodeFromDb(ctx context.Context) error {
	if n, err := d.oDb.NodeByNodeID(ctx, d.nodeID); err != nil {
		return fmt.Errorf("findNodeFromDb: node %s: %w", d.nodeID, err)
	} else {
		d.node = n
	}
	slog.Debug(fmt.Sprintf("jobFeedInstanceAction found node %s for id %s", d.node.Nodename, d.nodeID))
	return nil
}

func (d *jobFeedInstanceAction) findObjectFromDb(ctx context.Context) error {
	isNew, objId, err := d.oDb.ObjectIDFindOrCreate(ctx, d.objectName, d.clusterID)
	if err != nil {
		return fmt.Errorf("find or create object ID failed for %s: %w", d.objectName, err)
	}
	if isNew {
		// TODO: add metrics
		slog.Debug(fmt.Sprintf("jobFeedInstanceAction has created new object id %s@%s %s", d.objectName, d.clusterID, objId))
	} else {
		slog.Debug(fmt.Sprintf("jobFeedInstanceAction found object id %s@%s %s", d.objectName, d.clusterID, objId))
	}
	d.objectID = objId
	return nil
}

func (d *jobFeedInstanceAction) updateDB(ctx context.Context) error {
	if d.data == nil || d.data.Path == "" {
		return fmt.Errorf("invalid action data: missing path")
	}

	objectUUID, err := uuid.Parse(d.objectID)
	if err != nil {
		return fmt.Errorf("invalid object ID UUID: %w", err)
	}
	nodeUUID, err := uuid.Parse(d.nodeID)
	if err != nil {
		return fmt.Errorf("invalid node ID UUID: %w", err)
	}
	beginTime, err := time.Parse(time.RFC3339Nano, d.data.Begin)
	if err != nil {
		return fmt.Errorf("invalid begin time format: %w", err)
	}

	var statusLog string
	if len(d.data.StatusLog) > 0 {
		statusLog = d.data.StatusLog
	} else if len(d.data.Argv) > 0 {
		statusLog = strings.Join(d.data.Argv, " ")
	}

	if d.data.End != "" {
		// field End is present, process as an end action
		endTime, err := time.Parse(time.RFC3339Nano, d.data.End)
		if err != nil {
			return fmt.Errorf("invalid end time format: %w", err)
		}

		actionID, found, err := d.oDb.FindInstanceActionIDFromPidAndSID(ctx, d.nodeID, d.objectID, d.data.Pid, d.data.SessionUuid)
		if err != nil {
			return fmt.Errorf("find action ID failed: %w", err)
		}

		action := cdb.SvcAction{
			SvcID:     objectUUID,
			NodeID:    nodeUUID,
			RID:       d.data.Rids,
			Action:    d.data.Action,
			Command:   strings.Join(d.data.Argv, " "),
			Origin:    d.data.Origin,
			Session:   d.data.SessionUuid,
			Pid:       d.data.Pid,
			Cron:      d.data.Cron,
			BeginAt:   beginTime,
			EndAt:     endTime,
			Status:    d.data.Status,
			StatusLog: statusLog,
		}
		lines := make([]cdb.SvcActionLine, len(d.data.Lines))
		for i, v := range d.data.Lines {
			lines[i] = cdb.SvcActionLine{
				Begin:     v.Begin,
				RID:       v.Rid,
				Subset:    v.Subset,
				Pid:       v.Pid,
				Status:    v.Status,
				StatusLog: v.StatusLog,
			}
		}
		if !found {
			// begin not processed yet, insert full record
			if _, err := d.oDb.InsertSvcAction(ctx, action, lines...); err != nil {
				return fmt.Errorf("insert svc action failed: %w", err)
			}
		} else {
			// begin already processed, update record with end info
			action.ID = actionID
			if err := d.oDb.UpdateSvcAction(ctx, action, lines...); err != nil {
				return fmt.Errorf("end svc action failed: %w", err)
			}
		}

		if (d.data.Action == "start" || d.data.Action == "startcontainer") && d.data.Status == "ok" {
			err := d.oDb.UpdateVirtualAsset(ctx, d.objectID, d.nodeID)
			if err != nil {
				return fmt.Errorf("update virtual asset failed: %w", err)
			}
		}

		if d.data.Status == "err" {
			if err := d.oDb.UpdateActionErrors(ctx, d.objectID, d.nodeID); err != nil {
				return fmt.Errorf("update action errors failed: %w", err)
			}
			if err := d.oDb.UpdateDashActionErrors(ctx, d.objectID, d.nodeID); err != nil {
				return fmt.Errorf("update dash action errors failed: %w", err)
			}
		}

	} else {
		// field End is not present, process as action begin
		action := cdb.SvcAction{
			SvcID:     objectUUID,
			NodeID:    nodeUUID,
			RID:       d.data.Rids,
			Action:    d.data.Action,
			Command:   strings.Join(d.data.Argv, " "),
			Origin:    d.data.Origin,
			Session:   d.data.SessionUuid,
			Pid:       d.data.Pid,
			Cron:      d.data.Cron,
			BeginAt:   beginTime,
			StatusLog: statusLog,
		}
		if _, err := d.oDb.InsertSvcAction(ctx, action); err != nil {
			return fmt.Errorf("insert new action failed: %w", err)
		}
	}

	return nil
}

// pushFromTableChanges announces the changes as concerning the instance of the action: its object and the node.
func (d *jobFeedInstanceAction) pushFromTableChanges(ctx context.Context) error {
	return d.pushScopedTableChanges(ctx, []string{d.objectID}, []string{d.nodeID})
}
