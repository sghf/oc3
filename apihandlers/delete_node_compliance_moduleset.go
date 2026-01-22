package apihandlers

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"
)

// DeleteNodeComplianceModuleset handles DELETE /nodes/{node_id}/compliance/modulesets/{mset_id}
func (a *Api) DeleteNodeComplianceModuleset(c echo.Context, nodeId string, msetId string) error {
	log := getLog(c)
	odb := a.cdbSession()

	log.Info("DeleteNodeComplianceModuleset called", "node_id", nodeId, "mset_id", msetId)

	// get moduleset name
	_, err := odb.CompModulesetName(c.Request().Context(), msetId)
	if err != nil {
		log.Error("DeleteNodeComplianceModuleset: cannot find moduleset", "mset_id", msetId, "error", err)
		return JSONProblemf(c, http.StatusNotFound, "NotFound", "moduleset %s not found", msetId)
	}

	// check if the moduleset is attached to the node
	attached, err := odb.CompModulesetAttached(c.Request().Context(), nodeId, msetId)
	if err != nil {
		log.Error("DeleteNodeComplianceModuleset: cannot check if moduleset is attached", "node_id", nodeId, "mset_id", msetId, "error", err)
		return JSONProblemf(c, http.StatusInternalServerError, "InternalError", "cannot check if moduleset %s is attached to node %s", msetId, nodeId)
	}
	if !attached {
		log.Info("DeleteNodeComplianceModuleset: moduleset is not attached to this node", "node_id", nodeId, "mset_id", msetId)
		return JSONProblemf(c, http.StatusConflict, "Conflict", "moduleset %s is not attached to this node", msetId)
	}

	// detach moduleset from node
	_, err = odb.CompModulesetDetachNode(c.Request().Context(), nodeId, []string{msetId})
	if err != nil {
		log.Error("DeleteNodeComplianceModuleset: cannot detach moduleset from node", "node_id", nodeId, "mset_id", msetId, "error", err)
		return JSONProblemf(c, http.StatusInternalServerError, "InternalError", "cannot detach moduleset %s from node %s", msetId, nodeId)
	}

	response := map[string]string{
		"info": fmt.Sprintf("moduleset %s detached from node %s", msetId, nodeId),
	}

	return c.JSON(http.StatusAccepted, response)
}
