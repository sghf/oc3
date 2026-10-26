package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/logkey"
	"github.com/opensvc/oc3/util/pendingchanges"
)

type (
	dataLister interface {
		objectNames() ([]string, error)
		nodeNames() ([]string, error)
	}

	objectInfoer interface {
		appFromObjectName(svcname string, nodes ...string) string
		objectStatus(objectName string) *cdb.DBObjStatus
	}

	instancer interface {
		InstanceStatus(objectName string, nodename string) *instanceData
	}

	nodeInfoer interface {
		nodeFrozen(string) (string, error)
		nodeHeartbeat(string) ([]heartbeatData, error)
	}

	clusterer interface {
		clusterID() (s string, err error)
		clusterName() (s string, err error)
	}

	dataProvider interface {
		dataLister
		clusterer
		nodeInfoer
		objectInfoer
		instancer
	}

	jobFeedDaemonStatus struct {
		JobBase
		JobRedis
		JobDB

		nodeID      string
		clusterID   string
		clusterName string
		nodeApp     string
		nodeEnv     string
		callerNode  *cdb.DBNode

		changes      map[string]struct{}
		takenChanges []string
		rawData      []byte

		data dataProvider

		byNodename map[string]*cdb.DBNode
		byNodeID   map[string]*cdb.DBNode
		nodes      []string

		byObjectName map[string]*cdb.DBObject
		byObjectID   map[string]*cdb.DBObject

		tableChange map[string]struct{}

		byInstanceName map[string]*cdb.DBInstance
		byInstanceID   map[string]*cdb.DBInstance

		objmonLogByObjectID   map[string]*cdb.DBObjectStatusLog
		svcmonLogByInstanceID map[string]*cdb.DBInstanceStatusLog

		// withResources are the instances, as "svc_id@node_id", having
		// resources in resmon.
		withResources    map[string]struct{}
		resmonLogByResID map[string]*cdb.DBResmonLog

		heartbeats []heartbeatData

		// purgedSvcIDs and purgedNodeIDs are the instances and the services the
		// job purged, absent from the report: changed too.
		purgedSvcIDs  []string
		purgedNodeIDs []string

		// updatedSvcIDs are the objects whose status the job updated, those the
		// report says changed and those just created: the others are only pinged.
		updatedSvcIDs []string
	}
)

func newDaemonStatus(nodeID string) *jobFeedDaemonStatus {
	return &jobFeedDaemonStatus{
		JobBase: JobBase{
			name:   jtDaemonStatus,
			detail: "nodeID: " + nodeID,
			logger: slog.With(logkey.NodeID, nodeID, logkey.JobName, jtDaemonStatus),
		},
		JobRedis: JobRedis{
			cachePendingH:   cachekeys.FeedDaemonStatusPendingH,
			cachePendingIDX: nodeID,
		},
		nodeID: nodeID,

		changes: make(map[string]struct{}),

		byNodename: make(map[string]*cdb.DBNode),
		byNodeID:   make(map[string]*cdb.DBNode),
		nodes:      make([]string, 0),

		byObjectID:   make(map[string]*cdb.DBObject),
		byObjectName: make(map[string]*cdb.DBObject),

		byInstanceID:   make(map[string]*cdb.DBInstance),
		byInstanceName: make(map[string]*cdb.DBInstance),

		objmonLogByObjectID:   make(map[string]*cdb.DBObjectStatusLog),
		svcmonLogByInstanceID: make(map[string]*cdb.DBInstanceStatusLog),
		withResources:         make(map[string]struct{}),
		resmonLogByResID:      make(map[string]*cdb.DBResmonLog),
	}
}

func (d *jobFeedDaemonStatus) Operations() []operation {
	return []operation{
		{name: "dropPending", do: d.dropPending},
		{name: "dbNow", do: d.dbNow, blocking: true},
		{name: "getChanges", do: d.getChanges},
		{name: "getData", do: d.getData, blocking: true},
		{name: "dbCheckClusterIDForNodeID", do: d.dbCheckClusterIDForNodeID, blocking: true},
		{name: "dbCheckClusters", do: d.dbCheckClusters, blocking: true},
		{name: "dbFetchNodes", do: d.dbFetchNodes, blocking: true},
		{name: "dataToNodeFrozen", do: d.dataToNodeFrozen, blocking: true},
		{name: "dataToNodeHeartbeat", do: d.dataToNodeHeartbeat, blocking: true},
		{name: "heartbeatToDB", do: d.heartbeatToDB, blocking: true},
		{name: "dbFindServices", do: d.dbFindServices, blocking: true},
		{name: "dbCreateServices", do: d.dbCreateServices, blocking: true},
		{name: "dbFindServicesLog", do: d.dbFindServicesLog, blocking: true},
		{name: "dbFindInstances", do: d.dbFindInstances, blocking: true},
		{name: "dbUpdateServices", do: d.dbUpdateServices, blocking: true},
		{name: "dbUpdateInstances", do: d.dbUpdateInstances, blocking: true},
		{name: "dbPurgeInstances", do: d.dbPurgeInstances, blocking: true},
		{name: "dbPurgeServices", do: d.dbPurgeServices, blocking: true},
		{name: "cacheObjectsWithoutConfig", do: d.cacheObjectsWithoutConfig},
		{name: "pushFromTableChanges", do: d.pushFromTableChanges},
	}
}

// pushFromTableChanges announces the changes as concerning the nodes of the
// cluster, the objects whose status the job updated, and those it purged: the
// other objects of the report were only pinged, their status unchanged.
func (d *jobFeedDaemonStatus) pushFromTableChanges(ctx context.Context) error {
	svcIDs := append(append([]string{}, d.updatedSvcIDs...), d.purgedSvcIDs...)
	nodeIDs := append([]string{d.nodeID}, d.purgedNodeIDs...)
	for id := range d.byNodeID {
		nodeIDs = append(nodeIDs, id)
	}
	return d.pushScopedTableChanges(ctx, svcIDs, nodeIDs)
}

func (d *jobFeedDaemonStatus) LogResult() {
	slog.Debug(fmt.Sprintf("handleDaemonStatus done for %s", d.byNodeID[d.nodeID]))
}

// getChanges takes the changes the node reported since the last run: the
// objects and instances listed are written in full, the others only have their
// timestamps renewed.
func (d *jobFeedDaemonStatus) getChanges(ctx context.Context) error {
	l, err := pendingchanges.Take(ctx, d.redis, cachekeys.FeedDaemonStatusChangesH, d.nodeID)
	if err != nil {
		return fmt.Errorf("getChanges: %w", err)
	}
	d.takenChanges = l
	for _, change := range l {
		d.changes[change] = struct{}{}
	}
	return nil
}

// onFailure gives the changes taken back to the next run: dropped, the objects
// and instances they list would keep a stale status until their next change.
// The job context may be the cause of the failure, cancelled at shutdown.
func (d *jobFeedDaemonStatus) onFailure(ctx context.Context) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := pendingchanges.Merge(ctx, d.redis, cachekeys.FeedDaemonStatusChangesH, d.nodeID, d.takenChanges); err != nil {
		d.Logger().Error("cannot give the changes back", logkey.Changes, d.takenChanges, logkey.Error, err)
	}
}

func (d *jobFeedDaemonStatus) getData(ctx context.Context) error {
	var (
		err  error
		data map[string]any
	)
	if b, err := d.redis.HGet(ctx, cachekeys.FeedDaemonStatusH, d.nodeID).Bytes(); err != nil {
		return fmt.Errorf("getData: HGET %s %s: %w", cachekeys.FeedDaemonStatusH, d.nodeID, err)
	} else if err = json.Unmarshal(b, &data); err != nil {
		return fmt.Errorf("getData: unexpected data from %s %s: %w", cachekeys.FeedDaemonStatusH, d.nodeID, err)
	} else {
		d.rawData = b
		var nilMap map[string]any
		clientVersion := mapToS(data, "", "version")
		switch {
		case strings.HasPrefix(clientVersion, "3."):
			d.data = &daemonDataV3{data: data, cluster: mapToMap(data, nilMap, "data", "cluster")}
		case strings.HasPrefix(clientVersion, "2."):
			d.data = &daemonDataV2{data: mapToMap(data, nilMap, "data")}
		default:
			return fmt.Errorf("no mapper for version %s", clientVersion)
		}
	}
	if d.clusterID, err = d.data.clusterID(); err != nil {
		return fmt.Errorf("getData %s: %w", d.nodeID, err)
	}
	if d.clusterName, err = d.data.clusterName(); err != nil {
		return fmt.Errorf("getData %s: %w", d.nodeID, err)
	}
	return nil
}

func (d *jobFeedDaemonStatus) dbCheckClusterIDForNodeID(ctx context.Context) error {
	if ok, err := d.oDb.NodeUpdateClusterIDForNodeID(ctx, d.nodeID, d.clusterID); err != nil {
		return fmt.Errorf("dbCheckClusterIDForNodeID for %s (%s): %w", d.callerNode.Nodename, d.nodeID, err)
	} else if ok {
		slog.Debug("dbCheckClusterIDForNodeID change cluster id value")
	}
	return nil
}

func (d *jobFeedDaemonStatus) dbCheckClusters(ctx context.Context) error {
	if err := d.oDb.UpdateClustersData(ctx, d.clusterName, d.clusterID, string(d.rawData)); err != nil {
		return fmt.Errorf("dbCheckClusters %s (%s): %w", d.nodeID, d.clusterID, err)
	}
	return nil
}

// dbFetchNodes fetch the data nodes (that are associated with caller node ID) from the database
// and sets d.byNodeID and d.clusterID.
// It also updates node last_comm field.
func (d *jobFeedDaemonStatus) dbFetchNodes(ctx context.Context) (err error) {
	var (
		nodes   []string
		dbNodes []*cdb.DBNode
	)

	// search the caller node from its node_id: we can't trust yet search from
	// d.data.nodeNames() because initial push daemon status may omit caller node.
	if callerNode, err := d.oDb.NodeByNodeID(ctx, d.nodeID); err != nil {
		return fmt.Errorf("dbFetchNodes nodeByNodeID %s: %w", d.nodeID, err)
	} else if callerNode == nil {
		return fmt.Errorf("dbFetchNodes can't find caller node %s", d.nodeID)
	} else {
		d.callerNode = callerNode
		d.nodeApp = callerNode.App
		d.nodeEnv = callerNode.NodeEnv
		d.byNodeID[d.nodeID] = callerNode
		d.byNodename[callerNode.Nodename] = callerNode
	}

	// search all cluster nodes
	nodes, err = d.data.nodeNames()
	if err != nil {
		return fmt.Errorf("getData %s: %w", d.nodeID, err)
	}
	if len(nodes) == 0 {
		return fmt.Errorf("dbFetchNodes: empty nodes for %s", d.nodeID)
	}
	if dbNodes, err = d.oDb.NodesFromClusterIDWithNodenames(ctx, d.clusterID, nodes); err != nil {
		return fmt.Errorf("dbFetchNodes %s [%s]: %w", nodes, d.nodeID, err)
	}
	for _, n := range dbNodes {
		if n.NodeID == d.nodeID {
			// already processed
			continue
		}
		if found, isDuplicate := d.byNodename[n.Nodename]; isDuplicate {
			return fmt.Errorf("dbFetchNodes %s [%s] duplicate nodename %s entry with node id: %s and %s", nodes, d.nodeID, n.Nodename, found.NodeID, n.NodeID)
		}
		d.byNodeID[n.NodeID] = n
		d.byNodename[n.Nodename] = n
	}
	d.nodes = make([]string, len(d.byNodename))
	var i = 0
	for nodename := range d.byNodename {
		d.nodes[i] = nodename
		i++
	}

	nodeIDs := make([]string, 0, len(d.byNodeID))
	for nodeID := range d.byNodeID {
		nodeIDs = append(nodeIDs, nodeID)
	}
	if err := d.oDb.NodeUpdateLastComm(ctx, nodeIDs...); err != nil {
		return fmt.Errorf("dbFetchNodes %s [%s] can't update node last comm for node ids %s: %w", d.callerNode, d.nodeID, nodeIDs, err)
	}

	slog.Debug(fmt.Sprintf("handleDaemonStatus run details: %s changes: [%s]", d.callerNode, strings.Join(d.takenChanges, " ")))
	return nil
}

func (d *jobFeedDaemonStatus) dataToNodeFrozen(ctx context.Context) error {
	for nodeID, dbNode := range d.byNodeID {
		nodename := dbNode.Nodename
		frozen, err := d.data.nodeFrozen(nodename)
		if err != nil {
			if nodeID == d.nodeID {
				// accept missing caller node in initial data
				continue
			} else {
				return fmt.Errorf("dataToNodeFrozen %s (%s): %w", nodename, nodeID, err)
			}
		}
		if frozen != dbNode.Frozen {
			slog.Debug(fmt.Sprintf("dataToNodeFrozen: updating node %s: %s frozen from %s -> %s", nodename, nodeID, dbNode.Frozen, frozen))
			if err := d.oDb.NodeUpdateFrozen(ctx, nodeID, frozen); err != nil {
				return fmt.Errorf("dataToNodeFrozen node %s (%s): %w", nodename, dbNode.NodeID, err)
			}
		}
	}
	return nil
}

func (d *jobFeedDaemonStatus) dataToNodeHeartbeat(_ context.Context) error {
	for nodeID, dbNode := range d.byNodeID {
		nodename := dbNode.Nodename
		l, err := d.data.nodeHeartbeat(nodename)
		if err != nil {
			if nodeID == d.nodeID {
				// accept missing caller node in initial data
				continue
			} else {
				return fmt.Errorf("dataToNodeHeartbeat %s (%s): %w", nodename, nodeID, err)
			}
		}
		for _, hb := range l {
			hb.DBHeartbeat.ClusterID = d.clusterID
			if hb.nodename != "" {
				if n := d.byNodename[hb.nodename]; n != nil {
					hb.DBHeartbeat.NodeID = n.NodeID
				} else {
					slog.Debug(fmt.Sprintf("dataToNodeHeartbeat: skipped because of unregistered node %s for %s", hb.nodename, hb))
					continue
				}
			}

			if hb.peerNodename != "" {
				if n := d.byNodename[hb.peerNodename]; n != nil {
					hb.DBHeartbeat.PeerNodeID = n.NodeID
				} else {
					slog.Debug(fmt.Sprintf("dataToNodeHeartbeat: skipped because of unregistered peer node %s for %s", hb.peerNodename, hb))
					continue
				}
			}
			d.heartbeats = append(d.heartbeats, hb)
		}
	}
	return nil
}

func (d *jobFeedDaemonStatus) heartbeatToDB(ctx context.Context) error {
	heartbeats := make([]*cdb.DBHeartbeat, len(d.heartbeats))
	hbLogLastM := make(map[string]*cdb.DBHeartbeatLog)
	hbLogLastExtentL := make([]*cdb.DBHeartbeatLog, 0)
	hbLogLastSetL := make([]*cdb.DBHeartbeatLog, 0)
	hbLogAddL := make([]*cdb.DBHeartbeatLog, 0)
	for i, v := range d.heartbeats {
		heartbeats[i] = &v.DBHeartbeat
	}
	if hbLogLastL, err := d.oDb.HBLogLastFromClusterID(ctx, d.clusterID); err != nil {
		return fmt.Errorf("heartbeatToDB hbLogLastByClusterID: %w", err)
	} else {
		for _, hbLog := range hbLogLastL {
			hbLogLastM[hbLog.NodeID+"->"+hbLog.PeerNodeID+":"+hbLog.Name] = hbLog
		}
	}
	for _, hb := range heartbeats {
		// Prepare the hb log transition
		id := hb.NodeID + "->" + hb.PeerNodeID + ":" + hb.Name
		if prev, ok := hbLogLastM[id]; ok {
			if hb.SameAsLog(prev) {
				// extend hbmon_log_last
				hbLogLastExtentL = append(hbLogLastExtentL, prev)
			} else {
				// prev hbmon log last will change its value, add prev as a new hb log transition
				hbLogAddL = append(hbLogAddL, prev)
				// update the last hb log value
				hbLogLastSetL = append(hbLogLastSetL, hb.AsLog())
			}
		} else {
			// create the last hb log value
			hbLogLastSetL = append(hbLogLastSetL, hb.AsLog())
		}
	}
	if err := d.oDb.HBUpdate(ctx, heartbeats...); err != nil {
		return fmt.Errorf("heartbeatToDB hbUpdate: %w", err)
	}

	if len(hbLogAddL) > 0 {
		if err := d.oDb.HBLogUpdate(ctx, hbLogAddL...); err != nil {
			return fmt.Errorf("heartbeatToDB HBLogUpdate: %w", err)
		}
	}
	if len(hbLogLastExtentL) > 0 {
		if err := d.oDb.HBLogLastExtend(ctx, hbLogLastExtentL...); err != nil {
			return fmt.Errorf("heartbeatToDB HBLogUpdate: %w", err)
		}
	}
	if len(hbLogLastSetL) > 0 {
		if err := d.oDb.HBLogLastUpdate(ctx, hbLogLastSetL...); err != nil {
			return fmt.Errorf("heartbeatToDB HBLogLastUpdate: %w", err)
		}
	}

	if err := d.oDb.HBDeleteOutDatedByClusterID(ctx, d.clusterID, d.now); err != nil {
		return fmt.Errorf("heartbeatToDB purge outdated %s: %w", d.clusterID, err)
	}
	return nil
}

func (d *jobFeedDaemonStatus) dbFindServices(ctx context.Context) error {
	var (
		objects []*cdb.DBObject
	)
	objectNames, err := d.data.objectNames()
	if err != nil {
		return fmt.Errorf("dbFindServices %s: %w", d.nodeID, err)
	}
	if len(objectNames) == 0 {
		slog.Debug(fmt.Sprintf("dbFindServices: no services for %s", d.nodeID))
		return nil
	}
	if objects, err = d.oDb.ObjectsFromClusterIDAndObjectNames(ctx, d.clusterID, objectNames); err != nil {
		return fmt.Errorf("dbFindServices query nodeID: %s clusterID: %s [%s]: %w", d.nodeID, d.clusterID, objectNames, err)
	}
	for _, o := range objects {
		d.byObjectName[o.Svcname] = o
		d.byObjectID[o.SvcID] = o
	}
	return nil
}

func (d *jobFeedDaemonStatus) dbFindServicesLog(ctx context.Context) error {
	var (
		objectIDs = make([]string, 0, len(d.byObjectID))
	)
	if len(d.byObjectID) == 0 {
		return nil
	}
	for objectID := range d.byObjectID {
		objectIDs = append(objectIDs, objectID)
	}

	l, err := d.oDb.ObjectStatusLogLastFromObjectIDs(ctx, objectIDs...)
	if err != nil {
		return fmt.Errorf("dbFindServicesLog ObjectStatusLogLastFromObjectIDs: %w", err)
	}

	for _, e := range l {
		d.objmonLogByObjectID[e.SvcID] = e
	}

	return nil
}

func (d *jobFeedDaemonStatus) dbFindInstances(ctx context.Context) error {
	var (
		objectIDs = make([]string, 0)
	)
	if len(d.byObjectID) == 0 {
		return nil
	}
	for objectID := range d.byObjectID {
		objectIDs = append(objectIDs, objectID)
	}
	instances, err := d.oDb.InstancesFromObjectIDs(ctx, objectIDs...)
	if err != nil {
		return fmt.Errorf("dbFindInstances: %w", err)
	}

	for _, mon := range instances {
		if n, ok := d.byNodeID[mon.NodeID]; ok {
			// Only pickup instances from known nodes
			if s, ok := d.byObjectID[mon.SvcID]; ok {
				// Only pickup instances from known objects
				d.byInstanceName[s.Svcname+"@"+n.Nodename] = mon
				d.byInstanceID[s.SvcID+"@"+n.NodeID] = mon
			}
		}
	}

	iStatusLogL, err := d.oDb.SvcmonLogLastFromObjectIDs(ctx, objectIDs...)
	if err != nil {
		return fmt.Errorf("dbFindInstances SvcmonLogLastFromObjectIDs: %w", err)
	}

	for _, e := range iStatusLogL {
		if n, ok := d.byNodeID[e.NodeID]; ok {
			// Only pickup from known nodes
			if s, ok := d.byObjectID[e.SvcID]; ok {
				// Only pickup from known objects
				d.svcmonLogByInstanceID[s.SvcID+"@"+n.NodeID] = e
			}
		}
	}

	// The rows of the nodes of the cluster only: the others are of no use here,
	// the instances being looked up by node, and reading them scanned the table.
	nodeIDs := make([]string, 0, len(d.byNodeID))
	for nodeID := range d.byNodeID {
		nodeIDs = append(nodeIDs, nodeID)
	}
	if l, err := d.oDb.ResmonInstancesFromObjectIDs(ctx, nodeIDs, objectIDs...); err != nil {
		return fmt.Errorf("dbFindInstances ResmonInstancesFromObjectIDs: %w", err)
	} else {
		for _, id := range l {
			d.withResources[id] = struct{}{}
		}
	}
	if resmonLogLastL, err := d.oDb.ResmonLogLastFromObjectIDs(ctx, nodeIDs, objectIDs...); err != nil {
		return fmt.Errorf("dbFindInstances ResmonLogLastFromObjectIDs: %w", err)
	} else {
		for _, r := range resmonLogLastL {
			d.resmonLogByResID[r.IDx()] = r
		}
	}
	return nil
}

// dbCreateServices creates missing services
func (d *jobFeedDaemonStatus) dbCreateServices(ctx context.Context) error {
	objectNames, err := d.data.objectNames()
	if err != nil {
		return fmt.Errorf("dbCreateServices: %w", err)
	}
	missing := make([]string, 0)
	for _, objectName := range objectNames {
		if _, ok := d.byObjectName[objectName]; ok {
			continue
		}
		missing = append(missing, objectName)
	}
	if len(missing) == 0 {
		return nil
	}
	slog.Debug(fmt.Sprintf("dbCreateServices: need create services: %v", missing))
	for _, objectName := range missing {
		app := d.data.appFromObjectName(objectName, d.nodes...)
		slog.Debug(fmt.Sprintf("dbCreateServices: creating service %s with app %s", objectName, app))
		obj, err := d.oDb.ObjectCreate(ctx, objectName, d.clusterID, app, d.byNodeID[d.nodeID])
		if err != nil {
			return fmt.Errorf("dbCreateServices objectCreate %s: %w", objectName, err)
		}
		slog.Debug(fmt.Sprintf("created service %s with app %s new id: %s", objectName, app, obj.SvcID))
		d.byObjectName[objectName] = obj
		d.byObjectName[obj.SvcID] = obj
	}
	return nil
}

func (d *jobFeedDaemonStatus) dbUpdateServices(ctx context.Context) error {
	objectIDPingM := make(map[string]struct{})
	objectL := make([]*cdb.DBObject, 0, len(d.byObjectID))
	statusLogAddL := make([]*cdb.DBObjectStatusLog, 0, len(d.byObjectID))
	statusLogLastSetL := make([]*cdb.DBObjectStatusLog, 0, len(d.byObjectID))
	statusLogLastExtentL := make([]string, 0, len(d.byObjectID))

	for objectID, obj := range d.byObjectID {
		objectName := obj.Svcname
		_, isChanged := d.changes[objectName]
		// freshly created services have availStatus "undef" and needs full update
		// even if not present in changes
		if !isChanged && obj.AvailStatus != "undef" {
			objectIDPingM[objectID] = struct{}{}
		} else {
			d.updatedSvcIDs = append(d.updatedSvcIDs, objectID)
			oStatus := d.data.objectStatus(objectName)
			if oStatus != nil {
				// refresh local cache
				obj.DBObjStatus = *oStatus
				objectL = append(objectL, obj)
				d.byObjectID[objectID].DBObjStatus = *oStatus
			}
		}

		// Prepare the svcmon log transition
		if prev, ok := d.objmonLogByObjectID[objectID]; ok {
			if obj.SameAsLog(prev) {
				// extend services_log_last
				statusLogLastExtentL = append(statusLogLastExtentL, objectID)
			} else {
				// prev services log last will change its value, add prev as a new services log transition
				statusLogAddL = append(statusLogAddL, prev)
				// update the last services log value
				statusLogLastSetL = append(statusLogLastSetL, obj.AsLog())
			}
		} else {
			// create the last svcmon log value
			statusLogLastSetL = append(statusLogLastSetL, obj.AsLog())
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
	if len(objectL) > 0 {
		if err := d.oDb.ObjectStatusUpdate(ctx, objectL...); err != nil {
			return fmt.Errorf("dbUpdateServices ObjectStatusUpdate: %w", err)
		}
	}

	// call batch services log updates
	if len(statusLogAddL) > 0 {
		if err := d.oDb.ObjectStatusLogUpdate(ctx, statusLogAddL...); err != nil {
			return fmt.Errorf("dbUpdateInstances ObjectStatusLogUpdate: %w", err)
		}
	}
	if len(statusLogLastExtentL) > 0 {
		if err := d.oDb.ObjectStatusLogLastExtend(ctx, statusLogLastExtentL...); err != nil {
			return fmt.Errorf("dbUpdateInstances ObjectStatusLogLastExtend: %w", err)
		}
	}
	if len(statusLogLastSetL) > 0 {
		if err := d.oDb.ObjectStatusLogLastUpdate(ctx, statusLogLastSetL...); err != nil {
			return fmt.Errorf("dbUpdateInstances ObjectStatusLogLastUpdate: %w", err)
		}
	}

	return nil
}

func (d *jobFeedDaemonStatus) dbUpdateInstances(ctx context.Context) error {
	count := 0
	objectIDsToPingByNodeID := make(map[string][]string)
	objectIDsToDropByNodeID := make(map[string][]string)

	svcmonL := make([]*cdb.DBInstanceStatus, 0)
	svcmonLogL := make([]*cdb.DBInstanceStatusLog, 0, len(d.byObjectName)*len(d.byNodeID))
	svcmonLogLastL := make([]*cdb.DBInstanceStatusLog, 0, len(d.byObjectName)*len(d.byNodeID))
	svcmonLogLastExtentM := make(map[string][]string)

	resmonL := make([]*cdb.DBInstanceResource, 0)
	resmonLogL := make([]*cdb.DBResmonLog, 0)
	resmonLogLastL := make([]*cdb.DBResmonLog, 0)
	resmonLogLastExtentL := make([]*cdb.DBInstanceResource, 0)

	dotDeleteL := make([]*cdb.DashboardObjectType, 0)
	dashboardObjectUpdateL := make([]*cdb.Dashboard, 0)

	containerNodeL := make([]*cdb.ContainerNode, 0)

	inAckPeriodM := make(map[string]struct{})

	dashboardUpdateObjectFlexStartedL := make([]*cdb.DashboardUpdateObjectFlexStartedParams, 0)

	objectIDL := make([]string, 0, len(d.byInstanceID))
	for i := range d.byInstanceID {
		objectIDL = append(objectIDL, i)
	}
	if inAckPeriodL, err := d.oDb.ObjectInAckUnavailabilityPeriod(ctx, d.nodeID); err != nil {
		return fmt.Errorf("dbUpdateInstances ObjectInAckUnavailabilityPeriod: %w", err)
	} else {
		for _, i := range inAckPeriodL {
			inAckPeriodM[i] = struct{}{}
		}
	}

	for objectName, obj := range d.byObjectName {
		count++
		objID := obj.SvcID
		instanceMonitorStates := make(map[string]bool)
		for nodeID, node := range d.byNodeID {
			if node == nil {
				return fmt.Errorf("dbUpdateInstances unexpected nil value for byNodeID(%s)", nodeID)
			}
			nodename := node.Nodename
			iStatus := d.data.InstanceStatus(objectName, nodename)
			if iStatus == nil {
				continue
			}
			// An instance the node does not report changed only has its
			// timestamps renewed, as in the legacy collector: rewriting its rows
			// at each report would consume an auto-increment value per row. One
			// not yet recorded, or of an object with no status yet, is written
			// in full, as is one whose resources are missing from resmon
			// (purged while the instance was absent from a report): renewing
			// timestamps would not bring them back.
			_, isChanged := d.changes[objectName+"@"+nodename]
			_, isKnown := d.byInstanceID[objID+"@"+nodeID]
			_, hasResources := d.withResources[objID+"@"+nodeID]
			isComplete := hasResources || len(iStatus.resources) == 0
			if !isChanged && isKnown && isComplete && obj.AvailStatus != "undef" {
				objectIDsToPingByNodeID[nodeID] = append(objectIDsToPingByNodeID[nodeID], objID)
				continue
			}
			result, err := d.extractInstanceDetails(ctx, iStatus, obj, node)
			if err != nil {
				return err
			}
			if len(result.svcmonL) > 0 {
				svcmonL = append(svcmonL, result.svcmonL...)

				// Prepare the svcmon log transition
				for _, instanceStatus := range result.svcmonL {
					if prev, ok := d.svcmonLogByInstanceID[objID+"@"+nodeID]; ok {
						if instanceStatus.SameAsLog(prev) {
							// extend svcmon_log_last
							svcmonLogLastExtentM[nodeID] = append(svcmonLogLastExtentM[nodeID], objID)
						} else {
							// prev svcmon log last will change its value, add prev as a new svcmon log transition
							svcmonLogL = append(svcmonLogL, prev)
							// update the last svcmon log value
							svcmonLogLastL = append(svcmonLogLastL, instanceStatus.AsLog())
						}
					} else {
						// create the last svcmon log value
						svcmonLogLastL = append(svcmonLogLastL, instanceStatus.AsLog())
					}
				}

			}
			if len(result.resmonL) > 0 {
				// Prepare resmon log transition
				for _, r := range result.resmonL {
					idx := r.IDx()
					if prev, ok := d.resmonLogByResID[idx]; ok {
						if r.SameAsLog(prev) {
							// extend resmon_log_last
							r.Changed = prev.BeginAt
							resmonLogLastExtentL = append(resmonLogLastExtentL, r)
						} else {
							// prev resmon log last will change its value, add prev as a new resmon log transition
							resmonLogL = append(resmonLogL, prev)
							// update the last resmon log value
							r.Changed = time.Now()
							resmonLogLastL = append(resmonLogLastL, r.AsLog())
						}
					} else {
						// create the last resmon log value
						r.Changed = time.Now()
						resmonLogLastL = append(resmonLogLastL, r.AsLog())
					}
					resmonL = append(resmonL, r)
				}
			}

			if result.dropInstance {
				objectIDsToDropByNodeID[nodeID] = append(objectIDsToDropByNodeID[nodeID], objID)
			}

			if result.MonSmonStatus != "" {
				instanceMonitorStates[result.MonSmonStatus] = true
			}

			containerNodeL = append(containerNodeL, result.cVmNameL...)
		}

		var dashObj dashboarder
		if len(instanceMonitorStates) == 1 && instanceMonitorStates["idle"] {
			_, inAckPeriod := inAckPeriodM[objID]

			dashObj = &DashboardObjectUnavailable{obj: obj}
			if inAckPeriod || slices.Contains([]string{"up", "n/a"}, obj.AvailStatus) {
				dotDeleteL = append(dotDeleteL, &cdb.DashboardObjectType{ObjectID: objID, DashType: dashObj.Type()})
			} else {
				dashboardObjectUpdateL = append(dashboardObjectUpdateL, newDashboardObjectUpdate(obj, dashObj))
			}

			dashObj = &DashboardObjectPlacement{obj: obj}
			if inAckPeriod || slices.Contains([]string{"optimal", "n/a"}, obj.Placement) {
				dotDeleteL = append(dotDeleteL, &cdb.DashboardObjectType{ObjectID: objID, DashType: dashObj.Type()})
			} else {
				dashboardObjectUpdateL = append(dashboardObjectUpdateL, newDashboardObjectUpdate(obj, dashObj))
			}

			dashObj = &DashboardObjectDegraded{obj: obj}
			if inAckPeriod || (slices.Contains([]string{"up", "n/a"}, obj.AvailStatus) && slices.Contains([]string{"up", "n/a"}, obj.OverallStatus)) {
				dotDeleteL = append(dotDeleteL, &cdb.DashboardObjectType{ObjectID: objID, DashType: dashObj.Type()})
			} else {
				dashboardObjectUpdateL = append(dashboardObjectUpdateL, newDashboardObjectUpdate(obj, dashObj))
			}

			dashboardUpdateObjectFlexStartedL = append(dashboardUpdateObjectFlexStartedL, &cdb.DashboardUpdateObjectFlexStartedParams{
				SvcID: objID,
				Sev:   severityFromEnv(dashObjObjectFlexError, obj.Env),
				Env:   obj.Env,
			})

			// Dropped feature: update_dash_flex_cpu
		}
	}

	if len(svcmonL) > 0 {
		if err := d.oDb.SvcmonUpdate(ctx, svcmonL...); err != nil {
			return fmt.Errorf("dbUpdateInstances SvcmonUpdate: %w", err)
		}
	}

	// call batch svcmon log updates
	if len(svcmonLogL) > 0 {
		if err := d.oDb.SvcmonLogUpdate(ctx, svcmonLogL...); err != nil {
			return fmt.Errorf("dbUpdateInstances SvcmonLogUpdate: %w", err)
		}
	}
	if len(svcmonLogLastExtentM) > 0 {
		for nodeID, objectIDs := range svcmonLogLastExtentM {
			if err := d.oDb.SvcmonLogLastExtend(ctx, nodeID, objectIDs...); err != nil {
				return fmt.Errorf("dbUpdateInstances SvcmonLogLastExtend: %w", err)
			}
		}
	}
	if len(svcmonLogLastL) > 0 {
		if err := d.oDb.SvcmonLogLastUpdate(ctx, svcmonLogLastL...); err != nil {
			return fmt.Errorf("dbUpdateInstances SvcmonLogLastUpdate: %w", err)
		}
	}

	// resmon
	if len(resmonL) > 0 {
		if err := d.oDb.ResmonUpdate(ctx, resmonL...); err != nil {
			return fmt.Errorf("dbUpdateInstances ResmonUpdate: %w", err)
		}
	}

	for _, a := range containerNodeL {
		if err := d.oDb.NodeContainerUpdateFromParentNode(ctx, a.MonVmName, a.ObjApp, a.DBNode); err != nil {
			return fmt.Errorf("dbUpdateInstances NodeContainerUpdateFromParentNode %s %s@%s encap hostname %s: %w",
				a.ObjName, a.ObjID, a.NodeID, a.MonVmName, err)
		}
	}
	// call batch resmon log updates
	if len(resmonLogL) > 0 {
		if err := d.oDb.ResmonLogUpdate(ctx, resmonLogL...); err != nil {
			return fmt.Errorf("dbUpdateInstances ResmonLogUpdate: %w", err)
		}
		if len(resmonLogLastL) > 0 {
			// detect changes => notify change
			d.oDb.SetChange("resmon", "svcmon", "services")
		}
	}
	if len(resmonLogLastExtentL) > 0 {
		if err := d.oDb.ResmonLogLastExtend(ctx, resmonLogLastExtentL...); err != nil {
			return fmt.Errorf("dbUpdateInstances ResmonLogLastExtend: %w", err)
		}
	}
	if len(resmonLogLastL) > 0 {
		if err := d.oDb.ResmonLogLastUpdate(ctx, resmonLogLastL...); err != nil {
			return fmt.Errorf("dbUpdateInstances ResmonLogLastUpdate: %w", err)
		}
	}

	for nodeID, objectIDs := range objectIDsToPingByNodeID {
		slog.Debug("ping instance: objectIDsToPingByNodeID")
		if _, err := d.oDb.SvcmonRefreshTimestamp(ctx, nodeID, objectIDs...); err != nil {
			return fmt.Errorf("dbUpdateInstances can't refresh node id: %s instances svcmon timestamp[%v]: %w", nodeID, objectIDs, err)
		}
		if _, err := d.oDb.ResmonRefreshTimestamp(ctx, nodeID, objectIDs...); err != nil {
			return fmt.Errorf("dbUpdateInstances can't refresh node id: %s instances resmon timestamp[%v]: %w", nodeID, objectIDs, err)
		}
	}
	for nodeID, objectIDs := range objectIDsToDropByNodeID {
		slog.Debug("drop instances: objectIDsToDropByNodeID")
		if err := d.oDb.DeleteNodeIDSvcmonInstances(ctx, nodeID, objectIDs...); err != nil {
			return fmt.Errorf("dbUpdateInstances can't delete node id: %s instances svcmon [%v]: %w", nodeID, objectIDs, err)
		}
		if err := d.oDb.DeleteNodeIDResmonInstances(ctx, nodeID, objectIDs...); err != nil {
			return fmt.Errorf("dbUpdateInstances can't delete node id: %s instances resmon [%v]: %w", nodeID, objectIDs, err)
		}
	}

	nodeIDs := make([]string, 0, len(d.byNodeID))
	for nodeID := range d.byNodeID {
		nodeIDs = append(nodeIDs, nodeID)
	}
	if err := d.oDb.ResmonPurgeExpired(ctx, d.now.Add(-time.Second), nodeIDs...); err != nil {
		return fmt.Errorf("dbUpdateInstances ResmonPurgeExpired: %w", err)
	}

	if err := d.oDb.DashboardObjectWithTypeDelete(ctx, dotDeleteL...); err != nil {
		return fmt.Errorf("dbUpdateInstances DashboardObjectWithTypeDelete: %w", err)
	}
	if err := d.oDb.DashboardUpdateObject(ctx, dashboardObjectUpdateL...); err != nil {
		return fmt.Errorf("dbUpdateInstances DashboardUpdateObject: %w", err)
	}

	if err := d.oDb.DashboardUpdateObjectFlexStartedBatch(ctx, dashboardUpdateObjectFlexStartedL...); err != nil {
		return fmt.Errorf("dbUpdateInstances DashboardUpdateObjectFlexStartedBatch: %w", err)
	}

	// TODO:
	// 	d.oDb.DashboardInstanceFrozenUpdate(ctx, objID, nodeID, obj.Env, iStatus.MonFrozen > 0)
	//	d.oDb.DashboardDeleteInstanceNotUpdated(ctx, objID, nodeID)

	// TODO add metrics
	d.Logger().Debug(fmt.Sprintf("dbUpdateInstances objects count %d", count))

	return nil
}

func (d *jobFeedDaemonStatus) dbPurgeInstances(ctx context.Context) error {
	var nodeIDs, objectNames []string
	for objectName := range d.byObjectName {
		objectNames = append(objectNames, objectName)
	}
	for nodeID := range d.byNodeID {
		nodeIDs = append(nodeIDs, nodeID)
	}
	instanceIDs, err := d.oDb.GetOrphanInstances(ctx, nodeIDs, objectNames)
	if err != nil {
		return fmt.Errorf("dbPurgeInstances: getOrphanInstances: %w", err)
	}
	for _, instanceID := range instanceIDs {
		d.purgedSvcIDs = append(d.purgedSvcIDs, instanceID.SvcID())
		d.purgedNodeIDs = append(d.purgedNodeIDs, instanceID.NodeID())
		if err1 := d.oDb.PurgeInstance(ctx, instanceID); err1 != nil {
			err = errors.Join(err, fmt.Errorf("purge instance %v: %w", instanceID, err1))
		}
	}
	if err != nil {
		return fmt.Errorf("dbPurgeInstances: %w", err)
	}
	return nil
}

func (d *jobFeedDaemonStatus) dbPurgeServices(ctx context.Context) error {
	objectIDs, err := d.oDb.ObjectIDsFromClusterIDWithPurgeTag(ctx, d.clusterID)
	if err != nil {
		err = fmt.Errorf("dbPurgeServices objectIDsFromClusterIDWithPurgeTag: %w", err)
		return err
	}
	for _, objectID := range objectIDs {
		d.purgedSvcIDs = append(d.purgedSvcIDs, objectID)
		if err1 := d.oDb.PurgeTablesFromObjectID(ctx, objectID); err1 != nil {
			err = errors.Join(err, fmt.Errorf("purge object %s: %w", objectID, err1))
		}
	}
	if err != nil {
		return fmt.Errorf("dbPurgeServices: %w", err)
	}
	return nil
}

// cacheObjectsWithoutConfig populate FeedObjectConfigForClusterIDH with names of objects without config
func (d *jobFeedDaemonStatus) cacheObjectsWithoutConfig(ctx context.Context) error {
	objects, err := d.populateFeedObjectConfigForClusterIDH(ctx, d.clusterID, d.byObjectID)
	if len(objects) > 0 {
		// TODO: add metrics
		slog.Debug(fmt.Sprintf("daemonStatus nodeID: %s need object config: %s", d.nodeID, objects))
	}
	return err
}
