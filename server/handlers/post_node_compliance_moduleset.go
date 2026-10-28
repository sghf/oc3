package serverhandlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// PostNodeComplianceModuleset handles POST /nodes/{node_id}/compliance/modulesets/{mset_id}
func (a *Api) PostNodeComplianceModuleset(c echo.Context, nodeId string, msetId string) error {
	return writeCompOutcome(c, a.attachNodeModuleset(c, nodeId, msetId))
}

// attachNodeModuleset attaches a moduleset to a node, with the checks of the
// single route: the responsibility of the caller, the moduleset, its current
// attachment, and whether the moduleset may be attached there. POST
// /compliance/attachments applies it to each pair of a batch.
func (a *Api) attachNodeModuleset(c echo.Context, nodeId string, msetId string) compOutcome {
	log := echolog.GetLogHandler(c, "PostNodeComplianceModuleset")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	log.Info("called", logkey.NodeID, nodeId, logkey.MSetID, msetId)

	responsible, err := odb.NodeResponsible(ctx, nodeId, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		log.Error("cannot check if user is responsible for the node", logkey.NodeID, nodeId, logkey.Error, err)
		return compFailf(http.StatusInternalServerError, "cannot check if user is responsible for node %s", nodeId)
	}
	if !responsible {
		log.Info("user is not responsible for this node", logkey.NodeID, nodeId)
		return compFailf(http.StatusForbidden, "user is not responsible for node %s", nodeId)
	}

	_, err = odb.CompModulesetName(ctx, msetId)
	if err != nil {
		log.Error("cannot find moduleset", logkey.MSetID, msetId, logkey.Error, err)
		return compFailf(http.StatusNotFound, "moduleset %s not found", msetId)
	}

	// check if the moduleset is already attached to the node
	attached, err := odb.CompModulesetAttached(ctx, nodeId, msetId)
	if err != nil {
		log.Error("cannot check if moduleset is attached", logkey.NodeID, nodeId, logkey.MSetID, msetId, logkey.Error, err)
		return compFailf(http.StatusInternalServerError, "cannot check if moduleset %s is attached to node %s", msetId, nodeId)
	}
	if attached {
		log.Info("moduleset is already attached to this node", logkey.NodeID, nodeId, logkey.MSetID, msetId)
		return compFailf(http.StatusConflict, "moduleset %s is already attached to this node", msetId)
	}

	// check if the moduleset is attachable to the node
	attachable, err := odb.CompModulesetAttachable(ctx, nodeId, msetId)
	if err != nil {
		log.Error("cannot check if moduleset is attachable", logkey.NodeID, nodeId, logkey.MSetID, msetId, logkey.Error, err)
		return compFailf(http.StatusInternalServerError, "cannot check if moduleset %s is attachable to node %s", msetId, nodeId)
	}
	if !attachable {
		log.Info("moduleset is not attachable to this node", logkey.NodeID, nodeId, logkey.MSetID, msetId)
		return compFailf(http.StatusForbidden, "moduleset %s is not attachable to this node", msetId)
	}

	// attach moduleset to node
	_, err = odb.CompModulesetAttachNode(ctx, nodeId, msetId)
	if err != nil {
		log.Error("cannot attach moduleset to node", logkey.NodeID, nodeId, logkey.MSetID, msetId, logkey.Error, err)
		return compFailf(http.StatusInternalServerError, "cannot attach moduleset %s to node %s", msetId, nodeId)
	}

	a.compAttachLog(c, ctx, log, "moduleset", msetId, nodeId, "", true)
	return compDone(fmt.Sprintf("moduleset %s attached to node %s", msetId, nodeId))
}
