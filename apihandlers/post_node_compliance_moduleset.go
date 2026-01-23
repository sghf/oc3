package apihandlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"
)

// PostNodeComplianceModuleset handles POST /nodes/{node_id}/compliance/modulesets/{mset_id}
func (a *Api) PostNodeComplianceModuleset(c echo.Context, nodeId string, msetId string) error {
	log := getLog(c)
	odb := a.cdbSession()
	ctx := c.Request().Context()
	odb.CreateTx(ctx, nil)
	ctx, cancel := context.WithTimeout(ctx, a.SyncTimeout)
	defer cancel()

	var success bool

	defer func() {
		if success {
			odb.Commit()
		} else {
			odb.Rollback()
		}
	}()

	log.Info("PostNodeComplianceModuleset called", "node_id", nodeId, "mset_id", msetId)

	_, err := odb.CompModulesetName(ctx, msetId)
	if err != nil {
		log.Error("PostNodeComplianceModuleset: cannot find moduleset", "mset_id", msetId, "error", err)
		return JSONProblemf(c, http.StatusNotFound, "NotFound", "moduleset %s not found", msetId)
	}

	// check if the moduleset is already attached to the node
	attached, err := odb.CompModulesetAttached(ctx, nodeId, msetId)
	if err != nil {
		log.Error("PostNodeComplianceModuleset: cannot check if moduleset is attached", "node_id", nodeId, "mset_id", msetId, "error", err)
		return JSONProblemf(c, http.StatusInternalServerError, "InternalError", "cannot check if moduleset %s is attached to node %s", msetId, nodeId)
	}
	if attached {
		log.Info("PostNodeComplianceModuleset: moduleset is already attached to this node", "node_id", nodeId, "mset_id", msetId)
		return JSONProblemf(c, http.StatusConflict, "Conflict", "moduleset %s is already attached to this node", msetId)
	}

	// check if the moduleset is attachable to the node
	attachable, err := odb.CompModulesetAttachable(ctx, nodeId, msetId)
	if err != nil {
		log.Error("PostNodeComplianceModuleset: cannot check if moduleset is attachable", "node_id", nodeId, "mset_id", msetId, "error", err)
		return JSONProblemf(c, http.StatusInternalServerError, "InternalError", "cannot check if moduleset %s is attachable to node %s", msetId, nodeId)
	}
	if !attachable {
		log.Info("PostNodeComplianceModuleset: moduleset is not attachable to this node", "node_id", nodeId, "mset_id", msetId)
		return JSONProblemf(c, http.StatusForbidden, "Forbidden", "moduleset %s is not attachable to this node", msetId)
	}

	// attach moduleset to node
	_, err = odb.CompModulesetAttachNode(ctx, nodeId, msetId)
	if err != nil {
		log.Error("PostNodeComplianceModuleset: cannot attach moduleset to node", "node_id", nodeId, "mset_id", msetId, "error", err)
		return JSONProblemf(c, http.StatusInternalServerError, "InternalError", "cannot attach moduleset %s to node %s", msetId, nodeId)
	}

	success = true

	response := map[string]string{
		"info": fmt.Sprintf("moduleset %s attached to node %s", msetId, nodeId),
	}

	return c.JSON(http.StatusAccepted, response)
}
