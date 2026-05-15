package serverhandlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteNodeComplianceRuleset handles DELETE /nodes/{node_id}/compliance/rulesets/{rset_id}
func (a *Api) DeleteNodeComplianceRuleset(c echo.Context, nodeId string, rsetId string) error {
	log := echolog.GetLogHandler(c, "DeleteNodeComplianceRuleset")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	log.Info("called", logkey.NodeID, nodeId, logkey.RSetID, rsetId)

	responsible, err := odb.NodeResponsible(ctx, nodeId, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		log.Error("cannot check if user is responsible for the node", logkey.NodeID, nodeId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check if user is responsible for node %s", nodeId)
	}
	if !responsible {
		log.Info("user is not responsible for this node", logkey.NodeID, nodeId)
		return JSONProblemf(c, http.StatusForbidden, "user is not responsible for node %s", nodeId)
	}

	// get ruleset name
	rset, err := odb.CompRulesetName(ctx, rsetId)
	if err != nil {
		log.Error("PostNodeComplianceRuleset: cannot find ruleset", logkey.RSetID, rsetId, logkey.Error, err)
		return JSONProblemf(c, http.StatusNotFound, "ruleset %s not found", rsetId)
	} else {
		log.Info("Detaching ruleset from node", "ruleset", rset, logkey.NodeID, nodeId)
	}

	// check if the ruleset is attached to the node
	attached, err := odb.CompRulesetAttached(ctx, nodeId, rsetId)
	if err != nil {
		log.Error("cannot check if ruleset is attached", logkey.NodeID, nodeId, logkey.RSetID, rsetId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check if ruleset %s is attached to node %s", rsetId, nodeId)
	}
	if !attached {
		log.Info("ruleset is not attached to this node", logkey.NodeID, nodeId, logkey.RSetID, rsetId)
		return JSONProblemf(c, http.StatusConflict, "ruleset %s is not attached to this node", rsetId)
	} else {
		log.Info("ruleset is attached to this node, proceeding to detach", logkey.NodeID, nodeId, logkey.RSetID, rsetId)
	}

	// detach ruleset from node
	_, err = odb.CompRulesetDetachNode(c.Request().Context(), nodeId, []string{rsetId})
	if err != nil {
		log.Error("cannot detach ruleset from node", logkey.NodeID, nodeId, logkey.RSetID, rsetId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot detach ruleset %s from node %s", rsetId, nodeId)
	}

	response := map[string]string{
		"info": fmt.Sprintf("ruleset %s detached from node %s", rsetId, nodeId),
	}

	return c.JSON(http.StatusAccepted, response)
}
