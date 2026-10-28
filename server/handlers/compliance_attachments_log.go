package serverhandlers

import (
	"context"
	"log/slog"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
)

// compAttachLog writes the audit entry of a moduleset or a ruleset (object)
// attached to, or detached from, a node or a service, with the actions and
// messages of the historical collector (lib_compliance.py): the object by its
// name, the node by its nodename, the service by its id on the entry. Every
// route attaching or detaching one, single, by list or by batch, ends here.
func (a *Api) compAttachLog(c echo.Context, ctx context.Context, log *slog.Logger, object, objID, nodeID, svcID string, attach bool) {
	odb := a.ODB
	name, err := odb.CompModulesetName(ctx, objID)
	if object == "ruleset" {
		name, err = odb.CompRulesetName(ctx, objID)
	}
	if err != nil || name == "" {
		name = objID
	}
	verb, phrase := "detach", "detached from"
	if attach {
		verb, phrase = "attach", "attached to"
	}
	entry := cdb.LogEntry{Dict: map[string]any{object: name}}
	if nodeID != "" {
		nodename := nodeID
		if node, err := odb.NodeByNodeIDOrNodename(ctx, nodeID); err == nil && node != nil {
			nodename, nodeID = node.Nodename, node.NodeID
		}
		entry.Action = "compliance." + object + ".node." + verb
		entry.Fmt = "%(" + object + ")s " + phrase + " node %(node)s"
		entry.Dict["node"] = nodename
		entry.NodeID = uuidRef(nodeID)
	} else {
		entry.Action = "compliance." + object + ".service." + verb
		entry.Fmt = "%(" + object + ")s " + phrase + " service"
		entry.SvcID = uuidRef(svcID)
	}
	a.auditLog(c, ctx, log, entry)
}
