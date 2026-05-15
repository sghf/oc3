package serverhandlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// PostNodeComplianceRuleset handles POST /nodes/{node_id}/compliance/rulesets/{rset_id}
func (a *Api) PostNodeComplianceRuleset(c echo.Context, nodeId string, rsetId string) error {
	log := echolog.GetLogHandler(c, "PostNodeComplianceRuleset")
	odb := a.ODB
	ctx := c.Request().Context()
	ctx, cancel := context.WithTimeout(ctx, a.SyncTimeout)
	defer cancel()

	responsible, err := odb.NodeResponsible(ctx, nodeId, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		log.Error("cannot check if user is responsible for the node", logkey.NodeID, nodeId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check if user is responsible for node %s", nodeId)
	}
	if !responsible {
		log.Info("user is not responsible for this node", logkey.NodeID, nodeId)
		return JSONProblemf(c, http.StatusForbidden, "user is not responsible for node %s", nodeId)
	}

	log.Info("PostNodeComplianceRuleset called", logkey.NodeID, nodeId, logkey.RSetID, rsetId)

	node, err := odb.NodeByNodeIDOrNodename(ctx, nodeId)
	if err != nil {
		log.Error("cannot find node", "node", nodeId, logkey.Error, err)
		return JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}

	rset, err := odb.CompRulesetName(ctx, rsetId)
	if err != nil {
		log.Error("cannot find ruleset", logkey.RSetID, rsetId, logkey.Error, err)
		return JSONProblemf(c, http.StatusNotFound, "ruleset %s not found", rsetId)
	}

	// check if the ruleset is already attached
	attached, err := odb.CompRulesetAttached(ctx, node.NodeID, rsetId)
	if err != nil {
		log.Error("cannot check if ruleset is already attached", logkey.NodeID, node.NodeID, logkey.RSetID, rsetId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check if ruleset %s is already attached to node %s", rsetId, node.NodeID)
	}
	if attached {
		log.Info("ruleset is already attached to this node", logkey.NodeID, node.NodeID, logkey.RSetID, rsetId)
		return JSONProblemf(c, http.StatusConflict, "ruleset %s is already attached to this node", rsetId)
	}

	// check if the ruleset is attachable to the node
	attachable, err := odb.CompRulesetAttachable(ctx, node.NodeID, rsetId)
	if err != nil {
		log.Error("cannot check if ruleset is attachable", logkey.NodeID, node.NodeID, logkey.RSetID, rsetId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check if ruleset %s is attachable to node %s", rsetId, node.NodeID)
	}
	if !attachable {
		log.Info("ruleset is not attachable to this node", logkey.NodeID, node.NodeID, logkey.RSetID, rsetId)
		return JSONProblemf(c, http.StatusForbidden, "ruleset %s is not attachable to this node", rsetId)
	}

	// attach ruleset to node
	_, err = odb.CompRulesetAttachNode(ctx, node.NodeID, rsetId)
	if err != nil {
		log.Error("cannot attach ruleset to node", logkey.NodeID, node.NodeID, logkey.RSetID, rsetId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot attach ruleset %s to node %s", rsetId, node.NodeID)
	}

	response := map[string]string{
		"info": fmt.Sprintf("ruleset %s(%s) attached", rset, rsetId),
	}

	return c.JSON(http.StatusAccepted, response)
}
