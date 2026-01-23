package apihandlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"
)

// PostNodeComplianceRuleset handles POST /nodes/{node_id}/compliance/rulesets/{rset_id}
func (a *Api) PostNodeComplianceRuleset(c echo.Context, nodeId string, rsetId string) error {
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

	node, err := odb.NodeByNodeIDOrNodename(ctx, nodeId)
	if err != nil {
		log.Error("PostNodeComplianceRuleset: cannot find node", "node", nodeId, "error", err)
		return JSONProblemf(c, http.StatusNotFound, "NotFound", "node %s not found", nodeId)
	}

	rset, err := odb.CompRulesetName(ctx, rsetId)
	if err != nil {
		log.Error("PostNodeComplianceRuleset: cannot find ruleset", "rset_id", rsetId, "error", err)
		return JSONProblemf(c, http.StatusNotFound, "NotFound", "ruleset %s not found", rsetId)
	}

	// check if the ruleset is already attached
	attached, err := odb.CompRulesetAttached(ctx, node.NodeID, rsetId)
	if err != nil {
		log.Error("PostNodeComplianceRuleset: cannot check if ruleset is already attached", "node_id", node.NodeID, "rset_id", rsetId, "error", err)
		return JSONProblemf(c, http.StatusInternalServerError, "InternalError", "cannot check if ruleset %s is already attached to node %s", rsetId, node.NodeID)
	}
	if attached {
		log.Info("PostNodeComplianceRuleset: ruleset is already attached to this node", "node_id", node.NodeID, "rset_id", rsetId)
		return JSONProblemf(c, http.StatusConflict, "Conflict", "ruleset %s is already attached to this node", rsetId)
	}

	// check if the ruleset is attachable to the node
	attachable, err := odb.CompRulesetAttachable(ctx, node.NodeID, rsetId)
	if err != nil {
		log.Error("PostNodeComplianceRuleset: cannot check if ruleset is attachable", "node_id", node.NodeID, "rset_id", rsetId, "error", err)
		return JSONProblemf(c, http.StatusInternalServerError, "InternalError", "cannot check if ruleset %s is attachable to node %s", rsetId, node.NodeID)
	}
	if !attachable {
		log.Info("PostNodeComplianceRuleset: ruleset is not attachable to this node", "node_id", node.NodeID, "rset_id", rsetId)
		return JSONProblemf(c, http.StatusForbidden, "Forbidden", "ruleset %s is not attachable to this node", rsetId)
	}

	// attach ruleset to node
	_, err = odb.CompRulesetAttachNode(ctx, node.NodeID, rsetId)
	if err != nil {
		log.Error("PostNodeComplianceRuleset: cannot attach ruleset to node", "node_id", node.NodeID, "rset_id", rsetId, "error", err)
		return JSONProblemf(c, http.StatusInternalServerError, "InternalError", "cannot attach ruleset %s to node %s", rsetId, node.NodeID)
	}

	success = true

	response := map[string]string{
		"info": fmt.Sprintf("ruleset %s(%s) attached", rset, rsetId),
	}

	return c.JSON(http.StatusAccepted, response)
}
