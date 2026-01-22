package apihandlers

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"
)

// DeleteNodeComplianceRuleset handles DELETE /nodes/{node_id}/compliance/rulesets/{rset_id}
func (a *Api) DeleteNodeComplianceRuleset(c echo.Context, nodeId string, rsetId string) error {
	log := getLog(c)
	odb := a.cdbSession()

	log.Info("DeleteNodeComplianceRuleset called", "node_id", nodeId, "rset_id", rsetId)

	// get ruleset name
	rset, err := odb.CompRulesetName(c.Request().Context(), rsetId)
	if err != nil {
		log.Error("PostNodeComplianceRuleset: cannot find ruleset", "rset_id", rsetId, "error", err)
		return JSONProblemf(c, http.StatusNotFound, "NotFound", "ruleset %s not found", rsetId)
	} else {
		log.Info("Detaching ruleset from node", "ruleset", rset, "node_id", nodeId)
	}

	// check if the ruleset is attached to the node
	attached, err := odb.CompRulesetAttached(c.Request().Context(), nodeId, rsetId)
	if err != nil {
		log.Error("DeleteNodeComplianceRuleset: cannot check if ruleset is attached", "node_id", nodeId, "rset_id", rsetId, "error", err)
		return JSONProblemf(c, http.StatusInternalServerError, "InternalError", "cannot check if ruleset %s is attached to node %s", rsetId, nodeId)
	}
	if !attached {
		log.Info("DeleteNodeComplianceRuleset: ruleset is not attached to this node", "node_id", nodeId, "rset_id", rsetId)
		return JSONProblemf(c, http.StatusConflict, "Conflict", "ruleset %s is not attached to this node", rsetId)
	} else {
		log.Info("DeleteNodeComplianceRuleset: ruleset is attached to this node, proceeding to detach", "node_id", nodeId, "rset_id", rsetId)
	}

	// detach ruleset from node
	_, err = odb.CompRulesetDetachNode(c.Request().Context(), nodeId, []string{rsetId})
	if err != nil {
		log.Error("DeleteNodeComplianceRuleset: cannot detach ruleset from node", "node_id", nodeId, "rset_id", rsetId, "error", err)
		return JSONProblemf(c, http.StatusInternalServerError, "InternalError", "cannot detach ruleset %s from node %s", rsetId, nodeId)
	}

	response := map[string]string{
		"info": fmt.Sprintf("ruleset %s detached from node %s", rsetId, nodeId),
	}

	return c.JSON(http.StatusAccepted, response)
}
