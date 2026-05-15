package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetNodeComplianceRulesets handles GET /nodes/{node_id}/compliance/rulesets
func (a *Api) GetNodeComplianceRulesets(c echo.Context, nodeId string, params server.GetNodeComplianceRulesetsParams) error {
	query, err := buildListQueryParameters(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, propsMapping["ruleset"])
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	log := echolog.GetLogHandler(c, "GetNodeComplianceRulesets")
	odb := a.ODB
	ctx := c.Request().Context()

	log.Info("called", logkey.NodeID, nodeId, "limit", query.Page.Limit, "offset", query.Page.Offset, "props", query.Props)

	// get node ID
	node, err := odb.NodeByNodeIDOrNodename(ctx, nodeId)
	if err != nil {
		log.Error("cannot find node", "node", nodeId, logkey.Error, err)
		return JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}

	// get attached rulesets with details
	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)
	rulesets, err := odb.CompNodeAttachedRulesets(ctx, node.NodeID, groups, isManager, query.Page.Limit, query.Page.Offset)
	if err != nil {
		log.Error("cannot get attached rulesets", logkey.NodeID, node.NodeID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get attached rulesets for node %s", node.NodeID)
	}

	filteredItems, err := filterItemsFields(rulesets, query.Props)
	if err != nil {
		log.Error("cannot filter ruleset props", logkey.NodeID, node.NodeID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot filter rulesets fields for node %s", node.NodeID)
	}

	return c.JSON(http.StatusOK, newListResponse(filteredItems, propsMapping["ruleset"], query))
}
