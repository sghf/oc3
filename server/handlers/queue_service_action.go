package serverhandlers

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/logkey"
)

// serviceActions are the agent actions accepted on a whole service, the service
// entries of the historical collector menu (am_svc_agent_leafs in
// action_menu.js).
var serviceActions = map[string]bool{
	"push resinfo": true,
	"push config":  true,
	"start":        true,
	"stop":         true,
	"giveback":     true,
	"switch":       true,
	"abort":        true,
	"clear":        true,
	"freeze":       true,
	"thaw":         true,
}

// instanceActions are the agent actions accepted on an instance, or on some of
// its resources, the instance entries of the historical collector menu
// (on_services_instances), the compliance runs on every module attached to the
// service included.
var instanceActions = map[string]bool{
	"push resinfo":     true,
	"push config":      true,
	"start":            true,
	"stop":             true,
	"restart":          true,
	"takeover":         true,
	"giveback":         true,
	"switch":           true,
	"syncall":          true,
	"syncnodes":        true,
	"syncdrp":          true,
	"abort":            true,
	"clear":            true,
	"enable":           true,
	"disable":          true,
	"freeze":           true,
	"thaw":             true,
	"compliance_check": true,
	"compliance_fix":   true,
}

// ridPattern bounds a resource id list ("fs#1,ip#0"): the command reaches a push
// node through a remote shell, where anything else could be interpreted.
var ridPattern = regexp.MustCompile(`^[A-Za-z0-9_#.:,-]+$`)

// agentAtLeast19 reports whether the agent understands --local, as the python
// collector decides it from the leading digits of the reported version. The om3
// agent reports "v3.0.0-...", whose leading "v" the python collector could not
// read: it then left --local out, and an instance freeze froze the whole object.
// The prefix is skipped here, om3 accepting --local on these actions.
func agentAtLeast19(version string) bool {
	version = strings.TrimPrefix(strings.TrimPrefix(version, "v"), "V")
	if len(version) < 3 {
		return false
	}
	v, err := strconv.ParseFloat(version[:3], 64)
	if err != nil {
		return false
	}
	return v >= 1.9
}

// serviceActionCommand builds the queued command, as fmt_svc_action() does. A pull
// or feed node reads the bare action from its queue; a push node is reached over
// ssh. A resource id limits the action to those resources; otherwise `local`
// restricts it to the instance of that node, except for the actions that only
// make sense cluster-wide.
func serviceActionCommand(action, svcname, actionType, connectTo, agentVersion, rid string, local bool) string {
	var cmd []string
	if actionType != "pull" && actionType != "feed" {
		cmd = append(cmd, sshCmd...)
		cmd = append(cmd, "opensvc@"+connectTo, "--", "sudo", "svcmgr", "--service", svcname)
	}
	if words, ok := nodeActionWords[action]; ok {
		// A compliance run, on the modules attached to the service, as
		// fmt_svc_comp_action() builds it: never limited to an instance.
		cmd = append(cmd, words...)
		return strings.Join(cmd, " ")
	}
	cmd = append(cmd, action)
	switch {
	case rid != "":
		cmd = append(cmd, "--rid", rid)
	case local && agentAtLeast19(agentVersion) && action != "create" && action != "takeover" && action != "switch":
		cmd = append(cmd, "--local")
	}
	return strings.Join(cmd, " ")
}

// enqueueServiceCommand is the part shared by service and instance actions: build
// the command for the target node and post it to the queue, as enqueue_svc_action()
// does, then write the audit log entry.
func (a *Api) enqueueServiceCommand(
	act actionActor, log *slog.Logger, ctx context.Context,
	svc *cdb.DBService, target *cdb.NodeActionTarget, agentVersion, action, rid string, local bool,
) (*queuedAction, error) {
	odb := a.ODB

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
	command := serviceActionCommand(action, svc.Svcname, actionType, connectTo, agentVersion, rid, local)

	// A dry run stops here, every check passed.
	if act.dryRun {
		return &queuedAction{Command: command, NodeID: target.NodeID}, nil
	}

	id, err := odb.EnqueueServiceAction(ctx, target.NodeID, svc.SvcID, actionType, command, connectTo, act.userID)
	if err != nil {
		log.Error("cannot queue the action", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot queue action %s", action)
	}

	log.Info("action queued", "svc_id", svc.SvcID, logkey.NodeID, target.NodeID, "action", action, "rid", rid, "action_id", id)
	// Announced at once: the action queue views follow it live.
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Debug("cannot notify changes", logkey.Error, err)
	}

	userEmail := act.email
	logEntry := cdb.LogEntry{
		Action: "service.action",
		User:   userEmail,
		Fmt:    "run %(action)s",
		Dict:   map[string]any{"action": action},
		Level:  "info",
	}
	if rid != "" {
		logEntry.Action = "service.resource.action"
		logEntry.Fmt = "run %(action)s on rid %(rid)s"
		logEntry.Dict["rid"] = rid
	}
	if parsed, parseErr := uuid.Parse(svc.SvcID); parseErr == nil {
		logEntry.SvcID = &parsed
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
		Info:    "action " + action + " queued on service " + svc.Svcname + " from node " + target.Nodename,
	}, nil
}

// checkServiceAction validates the action, among those allowed, and the caller
// rights shared by service and instance actions: NodeExec privilege (CompExec
// for a compliance run) and responsibility for the service.
func (a *Api) checkServiceAction(act actionActor, log *slog.Logger, ctx context.Context, svcID, action string, allowed map[string]bool) (*cdb.DBService, error) {
	if !allowed[action] {
		return nil, refuseAction(http.StatusBadRequest, "unsupported action %q", action)
	}
	if isCompAction(action) {
		if !act.manager() && !act.has("CompExec") {
			return nil, refuseAction(http.StatusForbidden, "user has no CompExec privilege")
		}
	} else if !act.manager() && !act.has("NodeExec") {
		return nil, refuseAction(http.StatusForbidden, "user has no NodeExec privilege")
	}
	if svcID == "" {
		return nil, refuseAction(http.StatusBadRequest, "invalid svc_id: ''")
	}

	svc, err := a.ODB.ServiceBySvcIDOrName(ctx, svcID, act.groups, act.manager())
	if errors.Is(err, cdb.ErrServiceAmbiguous) {
		return nil, refuseAction(http.StatusConflict, "%s", err)
	}
	if err != nil {
		log.Error("cannot resolve service", "svc_id", svcID, logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot resolve service %s", svcID)
	}
	if svc == nil {
		return nil, refuseAction(http.StatusNotFound, "service %s not found", svcID)
	}

	responsible, err := a.ODB.ServiceResponsible(ctx, svc.SvcID, act.groups, act.manager())
	if err != nil {
		log.Error("cannot check service responsibility", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot check service responsibility")
	}
	if !responsible {
		return nil, refuseAction(http.StatusForbidden, "user is not responsible for service %s", svcID)
	}
	return svc, nil
}

// queueServiceAction queues an action on a whole service, as do_svc_action()
// does: posted for a node of the service seen alive in the last 15 minutes,
// without --local.
func (a *Api) queueServiceAction(act actionActor, log *slog.Logger, ctx context.Context, svcID, action string) (*queuedAction, error) {
	svc, err := a.checkServiceAction(act, log, ctx, svcID, action, serviceActions)
	if err != nil {
		return nil, err
	}

	target, agentVersion, err := a.ODB.ServiceLiveNode(ctx, svc.SvcID)
	if err != nil {
		log.Error("cannot look for a live node", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot look for a live node of service %s", svcID)
	}
	if target == nil {
		return nil, refuseAction(http.StatusConflict,
			"no node of service %s has been seen in the last 15 minutes", svc.Svcname)
	}

	return a.enqueueServiceCommand(act, log, ctx, svc, target, agentVersion, action, "", false)
}

// queueInstanceAction queues an action on the instance of a service on one node,
// as do_instance_action() does: NodeExec privilege and responsibility for the
// service; the node only has to exist. The action acts on that instance only
// (--local), or on the given resources (--rid).
func (a *Api) queueInstanceAction(act actionActor, log *slog.Logger, ctx context.Context, nodeID, svcID, action, rid string) (*queuedAction, error) {
	if rid != "" && !ridPattern.MatchString(rid) {
		return nil, refuseAction(http.StatusBadRequest, "invalid rid %q", rid)
	}
	svc, err := a.checkServiceAction(act, log, ctx, svcID, action, instanceActions)
	if err != nil {
		return nil, err
	}

	node, err := a.ODB.NodeByNodeIDOrNodename(ctx, nodeID)
	if err != nil {
		log.Error("cannot lookup node", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot lookup node")
	}
	if node == nil {
		return nil, refuseAction(http.StatusNotFound, "node %s not found", nodeID)
	}

	target, err := a.ODB.NodeActionTargetByID(ctx, node.NodeID)
	if err != nil {
		log.Error("cannot read node action fields", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot read node %s", nodeID)
	}
	if target == nil {
		return nil, refuseAction(http.StatusNotFound, "node %s not found", nodeID)
	}

	agentVersion, err := a.ODB.NodeAgentVersion(ctx, node.NodeID)
	if err != nil {
		log.Error("cannot read the agent version", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot read node %s", nodeID)
	}

	return a.enqueueServiceCommand(act, log, ctx, svc, target, agentVersion, action, rid, true)
}
