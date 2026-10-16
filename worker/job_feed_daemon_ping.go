package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/feeder"
	"github.com/opensvc/oc3/util/logkey"
)

type (
	jobFeedDaemonPing struct {
		JobBase
		JobRedis
		JobDB

		nodeID     string
		clusterID  string
		callerNode *cdb.DBNode

		byObjectID map[string]*cdb.DBObject
		byNodeID   map[string]*cdb.DBNode

		// clusterNode is a map of cluster nodes (from POST feed daemon ping)
		clusterNode map[string]struct{}

		// clusterObject is a map of object names (from POST feed daemon ping)
		clusterObject map[string]struct{}
	}
)

func newDaemonPing(nodeID string) *jobFeedDaemonPing {
	return &jobFeedDaemonPing{
		JobBase: JobBase{
			name:   jtDaemonPing,
			detail: "nodeID: " + nodeID,
			logger: slog.With(logkey.NodeID, nodeID, logkey.JobName, jtDaemonPing),
		},
		JobRedis: JobRedis{
			cachePendingH:   cachekeys.FeedDaemonPingPendingH,
			cachePendingIDX: nodeID,
		},
		nodeID: nodeID,

		byNodeID:   make(map[string]*cdb.DBNode),
		byObjectID: make(map[string]*cdb.DBObject),

		clusterNode:   make(map[string]struct{}),
		clusterObject: make(map[string]struct{}),
	}
}

func (d *jobFeedDaemonPing) Operations() []operation {
	return []operation{
		{name: "dropPending", do: d.dropPending},
		{name: "getData", do: d.getData},
		{name: "dbFetchNodes", do: d.dbFetchNodes},
		{name: "dbFindObjects", do: d.dbFindObjects},
		{name: "dbPingInstances", do: d.dbPingInstances},
		{name: "dbPingObjects", do: d.dbPingObjects},
		{name: "cacheObjectsWithoutConfig", do: d.cacheObjectsWithoutConfig},
		{name: "pushFromTableChanges", do: d.pushFromTableChanges},
	}
}

func (d *jobFeedDaemonPing) getData(ctx context.Context) error {
	var data feeder.PostDaemonPing
	if b, err := d.redis.HGet(ctx, cachekeys.FeedDaemonPingH, d.nodeID).Bytes(); err != nil {
		return fmt.Errorf("getData: HGET %s %s: %w", cachekeys.FeedDaemonPingH, d.nodeID, err)
	} else if err = json.Unmarshal(b, &data); err != nil {
		return fmt.Errorf("getData: unexpected data from %s %s: %w", cachekeys.FeedDaemonPingH, d.nodeID, err)
	} else {
		for _, nodename := range data.Nodes {
			d.clusterNode[nodename] = struct{}{}
		}

		for _, objectName := range data.Objects {
			d.clusterObject[objectName] = struct{}{}
		}
	}
	return nil
}

// dbFetchNodes fetch the data nodes (that are associated with caller node ID) from the database
// and sets d.byNodeID and d.clusterID.
// It also updates node last_comm field.
func (d *jobFeedDaemonPing) dbFetchNodes(ctx context.Context) (err error) {
	var (
		dbNodes []*cdb.DBNode
	)
	if dbNodes, err = d.oDb.ClusterNodesFromNodeID(ctx, d.nodeID); err != nil {
		return fmt.Errorf("dbFetchNodes %s: %w", d.nodeID, err)
	}
	for _, n := range dbNodes {
		if _, ok := d.clusterNode[n.Nodename]; !ok {
			// skipped: not member of posted cluster nodenames
			continue
		}
		d.byNodeID[n.NodeID] = n
	}
	callerNode, ok := d.byNodeID[d.nodeID]
	if !ok {
		return fmt.Errorf("dbFetchNodes source node has been removed")
	}
	d.callerNode = callerNode
	d.clusterID = callerNode.ClusterID

	nodeIDs := make([]string, 0, len(d.byNodeID))
	for nodeID := range d.byNodeID {
		nodeIDs = append(nodeIDs, nodeID)
	}
	if err := d.oDb.NodeUpdateLastComm(ctx, nodeIDs...); err != nil {
		return fmt.Errorf("dbFetchNodes %s [%s] can't update node last comm for node ids %s: %w", d.callerNode, d.nodeID, nodeIDs, err)
	}
	return nil
}

func (d *jobFeedDaemonPing) dbFindObjects(ctx context.Context) (err error) {
	var (
		objects []*cdb.DBObject
	)
	if objects, err = d.oDb.ObjectsFromClusterID(ctx, d.clusterID); err != nil {
		return fmt.Errorf("dbFindObjects query node %s (%s) clusterID: %s: %w",
			d.callerNode.Nodename, d.nodeID, d.clusterID, err)
	}
	for _, o := range objects {
		if _, ok := d.clusterObject[o.Svcname]; !ok {
			// skipped: not member of posted object names
			continue
		}
		d.byObjectID[o.SvcID] = o
		slog.Debug("dbFindObjects", logkey.ObjectID, o.SvcID)
	}
	return nil
}

// dbPingInstances call oDb.InstancePingFromNodeID for all db fetched nodes
func (d *jobFeedDaemonPing) dbPingInstances(ctx context.Context) error {
	for nodeID := range d.byNodeID {
		if ok, err := d.oDb.InstancePingFromNodeID(ctx, nodeID); err != nil {
			return fmt.Errorf("dbPingInstances: %w", err)
		} else if ok {
			continue
		}
	}
	return nil
}

// dbPingObjects call oDb.objectPing for all db fetched objects
func (d *jobFeedDaemonPing) dbPingObjects(ctx context.Context) (err error) {
	objectIDPingM := make(map[string]struct{})
	for objectID, obj := range d.byObjectID {
		if obj.AvailStatus != "undef" {
			objectIDPingM[objectID] = struct{}{}
		}
	}
	if len(objectIDPingM) > 0 {
		objectIDL := make([]string, 0, len(objectIDPingM))
		for objectID := range objectIDPingM {
			objectIDL = append(objectIDL, objectID)
		}
		slog.Debug(fmt.Sprintf("ping object ids %v", objectIDL))
		if _, err := d.oDb.ObjectsPing(ctx, objectIDL); err != nil {
			return fmt.Errorf("dbUpdateServices can't ping objects [%v]: %w", objectIDL, err)
		}
	}
	return nil
}

// cacheObjectsWithoutConfig populate FeedObjectConfigForClusterIDH with names of objects without config
func (d *jobFeedDaemonPing) cacheObjectsWithoutConfig(ctx context.Context) error {
	objects, err := d.populateFeedObjectConfigForClusterIDH(ctx, d.clusterID, d.byObjectID)
	if len(objects) > 0 {
		// TODO: add metrics
		slog.Debug("daemonPing need object config", logkey.Objects, objects)
	}
	return err
}

// pushFromTableChanges announces the changes as concerning the nodes and the objects of the cluster the ping described.
func (d *jobFeedDaemonPing) pushFromTableChanges(ctx context.Context) error {
	svcIDs := make([]string, 0, len(d.byObjectID))
	for id := range d.byObjectID {
		svcIDs = append(svcIDs, id)
	}
	nodeIDs := []string{d.nodeID}
	for id := range d.byNodeID {
		nodeIDs = append(nodeIDs, id)
	}
	return d.pushScopedTableChanges(ctx, svcIDs, nodeIDs)
}
