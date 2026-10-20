package serverhandlers

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/logkey"
)

// clusterActionWords are the actions accepted on a whole cluster, and the om3
// agent command each queues. om3 has three cluster-wide actions, `om cluster
// freeze`, `unfreeze` (alias thaw) and `abort` (core/om/kind_ccfg.go), but its
// collector queue runner prefixes every command of a node action with `node`
// (core/object/node_deueue.go). The same actions are reached there as node
// actions on every node of the cluster, selected by `--node *`, which the agent
// expands itself (core/nodeselector, no shell involved). The other cluster
// commands of om3 — join, leave, enroll, evict, register, ssh trust — need
// arguments or a local session, and status and logs are reads.
var clusterActionWords = map[string]string{
	"freeze": "freeze",
	"thaw":   "unfreeze",
	"abort":  "abort",
}

// clusterActionCommand is the queued command of a cluster action: the node
// runner of om3 reads it after `om node`.
func clusterActionCommand(action string) string {
	return clusterActionWords[action] + " --node *"
}

// queueClusterAction queues an action on a whole cluster: NodeExec privilege and
// responsibility for every node of the cluster the collector knows, as the action
// acts on all of them. It is posted for a node of the cluster seen alive in the
// last 15 minutes and running the om3 agent, the only one with a node selector.
func (a *Api) queueClusterAction(act actionActor, log *slog.Logger, ctx context.Context, clusterID, action string) (*queuedAction, error) {
	odb := a.ODB
	if _, ok := clusterActionWords[action]; !ok {
		return nil, refuseAction(http.StatusBadRequest, "unsupported action %q", action)
	}
	if !act.manager() && !act.has("NodeExec") {
		return nil, refuseAction(http.StatusForbidden, "user has no NodeExec privilege")
	}

	clusterName, found, err := odb.ClusterName(ctx, clusterID)
	if err != nil {
		log.Error("cannot lookup cluster", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot lookup cluster")
	}
	if !found {
		return nil, refuseAction(http.StatusNotFound, "cluster %s not found", clusterID)
	}

	nodes, notResponsible, err := odb.ClusterNodesResponsibility(ctx, clusterID, act.groups, act.manager())
	if err != nil {
		log.Error("cannot check cluster responsibility", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot check cluster responsibility")
	}
	if nodes == 0 {
		return nil, refuseAction(http.StatusConflict, "cluster %s has no node in the collector", clusterName)
	}
	if len(notResponsible) > 0 {
		return nil, refuseAction(http.StatusForbidden, "user is not responsible for every node of cluster %s", clusterName)
	}

	target, err := odb.ClusterLiveNode(ctx, clusterID)
	if err != nil {
		log.Error("cannot look for a live node", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot look for a live node of cluster %s", clusterName)
	}
	if target == nil {
		return nil, refuseAction(http.StatusConflict,
			"no node of cluster %s running the om3 agent has been seen in the last 15 minutes", clusterName)
	}
	// A node feeding another collector queues its actions there; oc3 has no such
	// push channel, so the caller is told rather than left with a dead entry.
	if target.Collector != "" {
		return nil, refuseAction(http.StatusBadRequest,
			"node %s is attached to collector %s: queue the action there", target.Nodename, target.Collector)
	}

	connectTo, err := odb.NodeReachableAddress(ctx, target)
	if err != nil {
		log.Error("cannot find a reachable address", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot find an address for node %s", target.Nodename)
	}
	actionType := actionTypeOf(target)
	command := clusterActionCommand(action)

	// A dry run stops here, every check passed.
	if act.dryRun {
		return &queuedAction{Command: command, NodeID: target.NodeID}, nil
	}

	id, err := odb.EnqueueNodeAction(ctx, target.NodeID, actionType, command, connectTo, act.userID)
	if err != nil {
		log.Error("cannot queue the action", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot queue action %s", action)
	}

	log.Info("action queued", "cluster_id", clusterID, logkey.NodeID, target.NodeID, "action", action, "action_id", id)
	// Announced at once: the action queue views follow it live.
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Debug("cannot notify changes", logkey.Error, err)
	}

	userEmail := act.email
	logEntry := cdb.LogEntry{
		Action: "cluster.action",
		User:   userEmail,
		Fmt:    "run %(action)s on cluster %(cluster)s",
		Dict:   map[string]any{"action": action, "cluster": clusterName},
		Level:  "info",
	}
	if parsed, parseErr := uuid.Parse(target.NodeID); parseErr == nil {
		logEntry.NodeID = &parsed
	}
	if logErr := odb.Log(ctx, logEntry); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	return &queuedAction{
		ID:      id,
		Command: command,
		NodeID:  target.NodeID,
		Info:    "action " + action + " queued on cluster " + clusterName + " from node " + target.Nodename,
	}, nil
}
