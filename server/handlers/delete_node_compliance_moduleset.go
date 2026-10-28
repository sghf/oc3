package serverhandlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteNodeComplianceModuleset handles DELETE /nodes/{node_id}/compliance/modulesets/{mset_id}
func (a *Api) DeleteNodeComplianceModuleset(c echo.Context, nodeId string, msetId string) error {
	return writeCompOutcome(c, a.detachNodeModuleset(c, nodeId, msetId))
}

// detachNodeModuleset detaches a moduleset from a node, with the checks of the
// single route: the responsibility of the caller, the moduleset, its current
// attachment. POST /compliance/attachments applies it to each pair of a batch.
func (a *Api) detachNodeModuleset(c echo.Context, nodeId string, msetId string) compOutcome {
	log := echolog.GetLogHandler(c, "DeleteNodeComplianceModuleset")
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

	// get moduleset name
	_, err = odb.CompModulesetName(ctx, msetId)
	if err != nil {
		log.Error("cannot find moduleset", logkey.MSetID, msetId, logkey.Error, err)
		return compFailf(http.StatusNotFound, "moduleset %s not found", msetId)
	}

	// check if the moduleset is attached to the node
	attached, err := odb.CompModulesetAttached(ctx, nodeId, msetId)
	if err != nil {
		log.Error("cannot check if moduleset is attached", logkey.NodeID, nodeId, logkey.MSetID, msetId, logkey.Error, err)
		return compFailf(http.StatusInternalServerError, "cannot check if moduleset %s is attached to node %s", msetId, nodeId)
	}
	if !attached {
		log.Info("moduleset is not attached to this node", logkey.NodeID, nodeId, logkey.MSetID, msetId)
		return compFailf(http.StatusConflict, "moduleset %s is not attached to this node", msetId)
	}

	// detach moduleset from node
	_, err = odb.CompModulesetDetachNode(ctx, nodeId, []string{msetId})
	if err != nil {
		log.Error("cannot detach moduleset from node", logkey.NodeID, nodeId, logkey.MSetID, msetId, logkey.Error, err)
		return compFailf(http.StatusInternalServerError, "cannot detach moduleset %s from node %s", msetId, nodeId)
	}

	a.compAttachLog(c, ctx, log, "moduleset", msetId, nodeId, "", false)
	return compDone(fmt.Sprintf("moduleset %s detached from node %s", msetId, nodeId))
}
