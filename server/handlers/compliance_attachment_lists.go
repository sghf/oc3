package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// compNodeAttachments answers with the nodes and the objects of a kind attached
// to them, a row of its own for a node that has none.
func (a *Api) compNodeAttachments(c echo.Context, name, mappingKey string, k cdb.CompKind, params listEndpointParams) error {
	return a.handleList(c, name, mappingKey, params, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetCompNodeAttachments(ctx, k, p)
	})
}

// compServiceAttachments answers with the services and the objects of a kind
// attached to them or to their encapsulated part.
func (a *Api) compServiceAttachments(c echo.Context, name, mappingKey string, k cdb.CompKind, params listEndpointParams) error {
	return a.handleList(c, name, mappingKey, params, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetCompServiceAttachments(ctx, k, p)
	})
}

// GetComplianceModulesetsNodes handles GET /compliance/modulesets_nodes.
func (a *Api) GetComplianceModulesetsNodes(c echo.Context, params server.GetComplianceModulesetsNodesParams) error {
	return a.compNodeAttachments(c, "GetComplianceModulesetsNodes", "compModulesetNode", cdb.CompModulesetKind,
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter))
}

// GetComplianceRulesetsNodes handles GET /compliance/rulesets_nodes.
func (a *Api) GetComplianceRulesetsNodes(c echo.Context, params server.GetComplianceRulesetsNodesParams) error {
	return a.compNodeAttachments(c, "GetComplianceRulesetsNodes", "compRulesetNode", cdb.CompRulesetKind,
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter))
}

// GetComplianceModulesetsServices handles GET /compliance/modulesets_services.
func (a *Api) GetComplianceModulesetsServices(c echo.Context, params server.GetComplianceModulesetsServicesParams) error {
	return a.compServiceAttachments(c, "GetComplianceModulesetsServices", "compModulesetService", cdb.CompModulesetKind,
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter))
}

// GetComplianceRulesetsServices handles GET /compliance/rulesets_services.
func (a *Api) GetComplianceRulesetsServices(c echo.Context, params server.GetComplianceRulesetsServicesParams) error {
	return a.compServiceAttachments(c, "GetComplianceRulesetsServices", "compRulesetService", cdb.CompRulesetKind,
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter))
}
