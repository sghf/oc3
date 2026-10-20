package serverhandlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/logkey"
)

// nodeActions are the agent actions the API accepts, the node agent entries of
// the historical collector action menu (am_node_agent_leafs in action_menu.js),
// but Wake On LAN, run through a proxy node of the same network, the
// provisioning from a template, and the root password rotation, which is not
// offered.
var nodeActions = map[string]bool{
	"pushasset":         true,
	"pushdisks":         true,
	"pushpkg":           true,
	"pushpatch":         true,
	"pushstats":         true,
	"checks":            true,
	"sysreport":         true,
	"updatecomp":        true,
	"updatepkg":         true,
	"scanscsi":          true,
	"reboot":            true,
	"schedule_reboot":   true,
	"unschedule_reboot": true,
	"shutdown":          true,
	"drain":             true,
	"compliance_check":  true,
	"compliance_fix":    true,
	"freeze":            true,
	"thaw":              true,
}

// nodeActionWords are the agent commands of the actions whose name is not the
// command: the compliance runs, on every module attached to the node, as the
// agent runs them without a module or moduleset.
var nodeActionWords = map[string][]string{
	"compliance_check": {"compliance", "check"},
	"compliance_fix":   {"compliance", "fix"},
}

// isCompAction tells the compliance runs, which need the CompExec privilege, as
// do_node_comp_action() requires, rather than NodeExec.
func isCompAction(action string) bool {
	return action == "compliance_check" || action == "compliance_fix"
}

// sshCmd is the ssh invocation the python collector uses to reach a push node
// (get_ssh_cmd, with the default of its ssh_cmd).
var sshCmd = []string{
	"ssh",
	"-o", "StrictHostKeyChecking=no",
	"-o", "CheckHostIP=no",
	"-o", "ForwardX11=no",
	"-o", "ConnectTimeout=5",
	"-o", "PasswordAuthentication=no",
}

// nodeActionCommand builds the queued command, as fmt_node_action() does: a pull or
// feed node reads the bare action from its queue, a push node is reached over ssh.
func nodeActionCommand(action, actionType, connectTo string) string {
	var cmd []string
	if actionType != "pull" && actionType != "feed" {
		cmd = append(cmd, sshCmd...)
		cmd = append(cmd, "opensvc@"+connectTo, "--", "sudo", "nodemgr")
	}
	if words, ok := nodeActionWords[action]; ok {
		cmd = append(cmd, words...)
	} else {
		cmd = append(cmd, action)
	}
	if action == "freeze" || action == "thaw" {
		cmd = append(cmd, "--local")
	}
	return strings.Join(cmd, " ")
}

// queuedAction is an action posted to the action queue.
type queuedAction struct {
	ID      int64
	Command string
	NodeID  string
	Info    string
}

// actionRefusal is a queueing request that did not go through, with the HTTP
// status that says why. Internal failures are logged where they happen and
// refused with a 500 and a generic message.
type actionRefusal struct {
	status int
	msg    string
}

func (e *actionRefusal) Error() string { return e.msg }

func refuseAction(status int, format string, args ...any) error {
	return &actionRefusal{status: status, msg: fmt.Sprintf(format, args...)}
}

// actionProblem writes the problem response of a refused queueing request.
func actionProblem(c echo.Context, err error) error {
	var refusal *actionRefusal
	if errors.As(err, &refusal) {
		return JSONProblem(c, refusal.status, refusal.msg)
	}
	return JSONProblem(c, http.StatusInternalServerError, err.Error())
}

// actionTypeOf returns how the node reads its queue; an unset action_type means
// pull, as get_action_type() does.
func actionTypeOf(target *cdb.NodeActionTarget) string {
	if target.ActionType == "" {
		return "pull"
	}
	return target.ActionType
}

// queueNodeAction queues an agent action on a node, as do_node_action() does:
// NodeExec privilege (CompExec for a compliance run), responsibility for the
// node, and a command run by nodemgr.
func (a *Api) queueNodeAction(act actionActor, log *slog.Logger, ctx context.Context, nodeID, action string) (*queuedAction, error) {
	odb := a.ODB
	if !nodeActions[action] {
		return nil, refuseAction(http.StatusBadRequest, "unsupported action %q", action)
	}
	if isCompAction(action) {
		if !act.manager() && !act.has("CompExec") {
			return nil, refuseAction(http.StatusForbidden, "user has no CompExec privilege")
		}
	} else if !act.manager() && !act.has("NodeExec") {
		return nil, refuseAction(http.StatusForbidden, "user has no NodeExec privilege")
	}

	node, err := odb.NodeByNodeIDOrNodename(ctx, nodeID)
	if err != nil {
		log.Error("cannot lookup node", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot lookup node")
	}
	if node == nil {
		return nil, refuseAction(http.StatusNotFound, "node %s not found", nodeID)
	}

	responsible, err := odb.NodeResponsible(ctx, node.NodeID, act.groups, act.manager())
	if err != nil {
		log.Error("cannot check node responsibility", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot check node responsibility")
	}
	if !responsible {
		return nil, refuseAction(http.StatusForbidden, "user is not responsible for node %s", nodeID)
	}

	target, err := odb.NodeActionTargetByID(ctx, node.NodeID)
	if err != nil {
		log.Error("cannot read node action fields", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot read node %s", nodeID)
	}
	if target == nil {
		return nil, refuseAction(http.StatusNotFound, "node %s not found", nodeID)
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
		return nil, refuseAction(http.StatusInternalServerError, "cannot find an address for node %s", nodeID)
	}

	actionType := actionTypeOf(target)
	command := nodeActionCommand(action, actionType, connectTo)

	// A dry run stops here, every check passed.
	if act.dryRun {
		return &queuedAction{Command: command, NodeID: node.NodeID}, nil
	}

	// Who asked, as the python collector records it: the queue keeps the caller.
	id, err := odb.EnqueueNodeAction(ctx, node.NodeID, actionType, command, connectTo, act.userID)
	if err != nil {
		log.Error("cannot queue the action", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot queue action %s", action)
	}

	log.Info("action queued", logkey.NodeID, node.NodeID, "action", action, "action_id", id)
	// Announced at once: the action queue views follow it live.
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Debug("cannot notify changes", logkey.Error, err)
	}

	userEmail := act.email
	logEntry := cdb.LogEntry{
		Action: "node.action",
		User:   userEmail,
		Fmt:    "run %(action)s",
		Dict:   map[string]any{"action": action},
		Level:  "info",
	}
	if parsed, parseErr := uuid.Parse(node.NodeID); parseErr == nil {
		logEntry.NodeID = &parsed
	}
	if logErr := odb.Log(ctx, logEntry); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	return &queuedAction{
		ID:      id,
		Command: command,
		NodeID:  node.NodeID,
		Info:    "action " + action + " queued on node " + target.Nodename,
	}, nil
}
