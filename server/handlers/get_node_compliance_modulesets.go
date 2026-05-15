package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetNodeComplianceModulesets handles GET /nodes/{node_id}/compliance/modulesets
func (a *Api) GetNodeComplianceModulesets(c echo.Context, nodeId string, params server.GetNodeComplianceModulesetsParams) error {
	query, err := buildListQueryParameters(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, propsMapping["moduleset"])
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	log := echolog.GetLogHandler(c, "GetNodeComplianceModulesets")
	odb := a.ODB
	ctx := c.Request().Context()

	log.Info("called", logkey.NodeID, nodeId, "limit", query.Page.Limit, "offset", query.Page.Offset, "props", query.Props)

	// get node ID
	node, err := odb.NodeByNodeIDOrNodename(ctx, nodeId)
	if err != nil {
		log.Error("cannot find node", "node", nodeId, logkey.Error, err)
		return JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}

	// get attached modulesets with details
	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)
	modulesets, err := odb.CompNodeAttachedModulesets(ctx, node.NodeID, groups, isManager, query.Page.Limit, query.Page.Offset)
	if err != nil {
		log.Error("cannot get attached modulesets", logkey.NodeID, node.NodeID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get attached modulesets for node %s", node.NodeID)
	}

	filteredItems, err := filterItemsFields(modulesets, query.Props)
	if err != nil {
		log.Error("cannot filter moduleset props", logkey.NodeID, node.NodeID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot filter modulesets fields for node %s", node.NodeID)
	}

	return c.JSON(http.StatusOK, newListResponse(filteredItems, propsMapping["moduleset"], query))
}
