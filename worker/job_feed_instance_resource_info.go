package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"strconv"

	"github.com/go-graphite/go-whisper"
	"github.com/go-redis/redis/v8"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/feeder"
	"github.com/opensvc/oc3/timeseries"
	"github.com/opensvc/oc3/util/logkey"
)

type (
	jobFeedInstanceResourceInfo struct {
		JobBase
		JobRedis
		JobDB
		JobUpload

		// idX is the id of the posted instance config with the expected pattern: <objectName>@<nodeID>@<clusterID>
		idX string

		objectName string

		// objectID is db ID of the object found or created in database
		objectID string

		// nodeID is db ID of the node that have posted object config
		nodeID string

		// clusterID is the db cluster ID of the node that have posted object config
		clusterID string

		// data is the posted instance resource info
		data feeder.InstanceResourceInfo
	}
)

var (
	ErrResInfoValue = fmt.Errorf("invalid resource info value")
)

func newjobFeedInstanceResourceInfo(objectName, nodeID, clusterID string) *jobFeedInstanceResourceInfo {
	idX := fmt.Sprintf("%s@%s@%s", objectName, nodeID, clusterID)
	return &jobFeedInstanceResourceInfo{
		JobBase: JobBase{
			name:   jtInstanceResourceInfo,
			detail: "ID: " + idX,
			logger: slog.With(logkey.NodeID, nodeID, logkey.ClusterID, clusterID, logkey.Object, objectName, logkey.JobName, jtInstanceResourceInfo),
		},
		JobRedis: JobRedis{
			cachePendingH:   cachekeys.FeedInstanceResourceInfoPendingH,
			cachePendingIDX: idX,
		},
		idX:        idX,
		nodeID:     nodeID,
		clusterID:  clusterID,
		objectName: objectName,
	}
}

func (j *jobFeedInstanceResourceInfo) Operations() []operation {
	return []operation{
		{name: "dropPending", do: j.dropPending, blocking: true},
		{name: "getData", do: j.getData, blocking: true},
		{name: "dbNow", do: j.dbNow, blocking: true},
		{name: "updateDB", do: j.updateDB, blocking: true},
		{name: "purgeDB", do: j.purgeDB, blocking: true},
		{name: "updateWSP", do: j.updateWSP, blocking: false},
		{name: "pushFromTableChanges", do: j.pushFromTableChanges},
	}
}

func (j *jobFeedInstanceResourceInfo) getData(ctx context.Context) error {
	cmd := j.redis.HGet(ctx, cachekeys.FeedInstanceResourceInfoH, j.idX)
	result, err := cmd.Result()
	switch err {
	case nil:
	case redis.Nil:
		return fmt.Errorf("HGET: no results")
	default:
		return fmt.Errorf("HGET: %w", err)
	}
	if err := json.Unmarshal([]byte(result), &j.data); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	return nil
}

func (j *jobFeedInstanceResourceInfo) updateDB(ctx context.Context) (err error) {
	created, objectID, err := j.oDb.ObjectIDFindOrCreate(ctx, j.objectName, j.clusterID)
	if err != nil {
		return fmt.Errorf("ObjectIDFindOrCreate: %w", err)
	}
	if created {
		// TODO: add metrics
		slog.Debug(fmt.Sprintf("jobFeedInstanceResourceInfo has created new object id %s@%s %s", j.objectName, j.clusterID, objectID))
	}
	j.objectID = objectID
	err = j.oDb.InstanceResourceInfoUpdate(ctx, objectID, j.nodeID, j.data)
	if err != nil {
		return fmt.Errorf("InstanceResourceInfoUpdate: %w", err)
	}

	return nil
}

func (j *jobFeedInstanceResourceInfo) purgeDB(ctx context.Context) (err error) {
	if j.objectID == "" {
		return fmt.Errorf("purgeDB: objectID is empty")
	}
	err = j.oDb.InstanceResourceInfoDelete(ctx, j.objectID, j.nodeID, j.now)
	if err != nil {
		return fmt.Errorf("InstanceResourceInfoDelete: %w", err)
	}

	return nil
}

// updateWSP updates Whisper files with new data points for instance resource information.
// Returns an error on failure.
// filename: <UploadDir>/stats/nodes/<nodeID>/services/<objectID>/resources/<rid>/info/<key>.wsp
func (j *jobFeedInstanceResourceInfo) updateWSP(ctx context.Context) (err error) {
	if j.objectID == "" {
		return fmt.Errorf("updateWSP: objectID is empty")
	}
	timestamp := int(j.now.Unix())
	baseDir := filepath.Join(j.UploadDir, "stats", "nodes", j.nodeID, "services", j.objectID)

	var okKeys []string
	var badKeys []string
	for _, info := range j.data.Info {
		rid := info.Rid
		for _, v := range info.Keys {
			value, err := j.valueToFloat64(v.Value)
			if err != nil {
				continue
			}
			fName := filepath.Join(baseDir, "resources", rid, "info", v.Key+".wsp")

			if err := timeseries.Update(fName, value, timestamp, timeseries.DefaultRetentions, whisper.Average, 0.0); err != nil {
				badKeys = append(badKeys, v.Key)
			} else {
				okKeys = append(okKeys, v.Key)
			}
		}
	}
	if len(okKeys) > 0 {
		j.logger.Debug(fmt.Sprintf("updateWSP done for keys %v", okKeys))
	}
	if len(badKeys) > 0 {
		return fmt.Errorf("jobFeedInstanceResourceInfo: updateWSP failed for keys %v", badKeys)
	}
	return nil
}

// valueToFloat64 converts an arbitrary value to a float64, returning an error if the conversion is not possible.
func (j *jobFeedInstanceResourceInfo) valueToFloat64(i any) (float64, error) {
	switch n := i.(type) {
	case string:
		// most common values are strings, so start with that
		return strconv.ParseFloat(n, 64)
	case int:
		return float64(n), nil
	case float64:
		return n, nil
	case float32:
		return float64(n), nil
	case int8:
		return float64(n), nil
	case int16:
		return float64(n), nil
	case int32:
		return float64(n), nil
	case int64:
		return float64(n), nil
	case uint:
		return float64(n), nil
	case uint8:
		return float64(n), nil
	case uint16:
		return float64(n), nil
	case uint32:
		return float64(n), nil
	case uint64:
		return float64(n), nil
	default:
		return 0, ErrResInfoValue
	}
}

// pushFromTableChanges announces the changes as concerning the instance whose resources the node posted.
func (j *jobFeedInstanceResourceInfo) pushFromTableChanges(ctx context.Context) error {
	return j.pushScopedTableChanges(ctx, []string{j.objectID}, []string{j.nodeID})
}
