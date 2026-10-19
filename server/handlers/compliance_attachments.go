package serverhandlers

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// compObjectNodes answers with the nodes a compliance object is attached to, or
// its candidate nodes.
func (a *Api) compObjectNodes(c echo.Context, name string, k cdb.CompKind, objID int64, candidates bool, params listEndpointParams) error {
	return a.handleList(c, name, "node", params, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetCompObjectNodes(ctx, k, objID, candidates, p)
	})
}

// compObjectServices answers with the services a compliance object is attached
// to, or its candidate services.
func (a *Api) compObjectServices(c echo.Context, name string, k cdb.CompKind, objID int64, slave *bool, candidates bool, params listEndpointParams) error {
	isSlave := slave != nil && *slave
	return a.handleList(c, name, "service", params, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetCompObjectServices(ctx, k, objID, isSlave, candidates, p)
	})
}

// publishedRuleset resolves a ruleset the caller may read: published to one of
// their groups or to Everybody.
func (a *Api) publishedRuleset(c echo.Context, rsetID string) (int64, error) {
	ctx := c.Request().Context()
	id, err := a.resolveRuleset(ctx, rsetID)
	if err == nil {
		err = a.requireRulesetPublished(ctx, c, id)
	}
	return id, err
}

// GetComplianceRulesetNodes handles GET /compliance/rulesets/{rset_id}/nodes.
func (a *Api) GetComplianceRulesetNodes(c echo.Context, rsetId string, params server.GetComplianceRulesetNodesParams) error {
	id, err := a.publishedRuleset(c, rsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.compObjectNodes(c, "GetComplianceRulesetNodes", cdb.CompRulesetKind, id, false,
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter))
}

// GetComplianceRulesetCandidateNodes handles GET /compliance/rulesets/{rset_id}/candidate_nodes.
func (a *Api) GetComplianceRulesetCandidateNodes(c echo.Context, rsetId string, params server.GetComplianceRulesetCandidateNodesParams) error {
	id, err := a.publishedRuleset(c, rsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.compObjectNodes(c, "GetComplianceRulesetCandidateNodes", cdb.CompRulesetKind, id, true,
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter))
}

// GetComplianceRulesetServices handles GET /compliance/rulesets/{rset_id}/services.
func (a *Api) GetComplianceRulesetServices(c echo.Context, rsetId string, params server.GetComplianceRulesetServicesParams) error {
	id, err := a.publishedRuleset(c, rsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.compObjectServices(c, "GetComplianceRulesetServices", cdb.CompRulesetKind, id, params.Slave, false,
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter))
}

// GetComplianceRulesetCandidateServices handles GET /compliance/rulesets/{rset_id}/candidate_services.
func (a *Api) GetComplianceRulesetCandidateServices(c echo.Context, rsetId string, params server.GetComplianceRulesetCandidateServicesParams) error {
	id, err := a.publishedRuleset(c, rsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.compObjectServices(c, "GetComplianceRulesetCandidateServices", cdb.CompRulesetKind, id, params.Slave, true,
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter))
}

// compAttachBody reads the bulk attachment forms: the object by id or name, the
// node or service, and for a service whether it is the encapsulated one (slave,
// or encap as the historical ruleset examples post it).
type compAttachBody struct {
	obj, target string
	slave       bool
}

func readCompAttachBody(c echo.Context, idKey, nameKey, targetKey string) (compAttachBody, error) {
	entry, err := oneEntry(c)
	if err != nil {
		return compAttachBody{}, err
	}
	var b compAttachBody
	target, ok := entry[targetKey]
	if !ok {
		return b, httpErrorf(http.StatusBadRequest, "'%s' must be specified", targetKey)
	}
	b.target = fmt.Sprint(target)
	if v, ok := entry[idKey]; ok {
		b.obj = fmt.Sprint(v)
	} else if v, ok := entry[nameKey]; ok {
		b.obj = fmt.Sprint(v)
	} else {
		return b, httpErrorf(http.StatusBadRequest, "Either '%s' or '%s' must be specified", idKey, nameKey)
	}
	for _, key := range []string{"slave", "encap"} {
		if v, ok := entry[key]; ok {
			flag, err := compBool(key, v)
			if err != nil {
				return b, err
			}
			b.slave = flag == "T"
		}
	}
	return b, nil
}

// rulesetsNodes attaches or detaches a ruleset named in the body to a node, as
// POST and DELETE /nodes/{node_id}/compliance/rulesets/{rset_id} do.
func (a *Api) rulesetsNodes(c echo.Context, attach bool) error {
	b, err := readCompAttachBody(c, "ruleset_id", "ruleset_name", "node_id")
	if err != nil {
		return httpProblem(c, err)
	}
	id, err := a.resolveRuleset(c.Request().Context(), b.obj)
	if err != nil {
		return httpProblem(c, err)
	}
	if attach {
		return a.PostNodeComplianceRuleset(c, b.target, strconv.FormatInt(id, 10))
	}
	return a.DeleteNodeComplianceRuleset(c, b.target, strconv.FormatInt(id, 10))
}

// PostComplianceRulesetsNodes handles POST /compliance/rulesets_nodes.
func (a *Api) PostComplianceRulesetsNodes(c echo.Context) error { return a.rulesetsNodes(c, true) }

// DeleteComplianceRulesetsNodes handles DELETE /compliance/rulesets_nodes.
func (a *Api) DeleteComplianceRulesetsNodes(c echo.Context) error { return a.rulesetsNodes(c, false) }

// rulesetsServices attaches or detaches a ruleset named in the body to a
// service, as POST and DELETE /services/{svc_id}/compliance/rulesets/{rset_id} do.
func (a *Api) rulesetsServices(c echo.Context, attach bool) error {
	b, err := readCompAttachBody(c, "ruleset_id", "ruleset_name", "svc_id")
	if err != nil {
		return httpProblem(c, err)
	}
	id, err := a.resolveRuleset(c.Request().Context(), b.obj)
	if err != nil {
		return httpProblem(c, err)
	}
	slave := b.slave
	if attach {
		return a.PostServiceComplianceRuleset(c, b.target, strconv.FormatInt(id, 10), server.PostServiceComplianceRulesetParams{Slave: &slave})
	}
	return a.DeleteServiceComplianceRuleset(c, b.target, strconv.FormatInt(id, 10), server.DeleteServiceComplianceRulesetParams{Slave: &slave})
}

// PostComplianceRulesetsServices handles POST /compliance/rulesets_services.
func (a *Api) PostComplianceRulesetsServices(c echo.Context) error {
	return a.rulesetsServices(c, true)
}

// DeleteComplianceRulesetsServices handles DELETE /compliance/rulesets_services.
func (a *Api) DeleteComplianceRulesetsServices(c echo.Context) error {
	return a.rulesetsServices(c, false)
}

// GetComplianceModulesetNodes handles GET /compliance/modulesets/{modset_id}/nodes.
func (a *Api) GetComplianceModulesetNodes(c echo.Context, modsetId string, params server.GetComplianceModulesetNodesParams) error {
	id, err := a.publishedModuleset(c, modsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.compObjectNodes(c, "GetComplianceModulesetNodes", cdb.CompModulesetKind, id, false,
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter))
}

// GetComplianceModulesetCandidateNodes handles GET /compliance/modulesets/{modset_id}/candidate_nodes.
func (a *Api) GetComplianceModulesetCandidateNodes(c echo.Context, modsetId string, params server.GetComplianceModulesetCandidateNodesParams) error {
	id, err := a.publishedModuleset(c, modsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.compObjectNodes(c, "GetComplianceModulesetCandidateNodes", cdb.CompModulesetKind, id, true,
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter))
}

// GetComplianceModulesetServices handles GET /compliance/modulesets/{modset_id}/services.
func (a *Api) GetComplianceModulesetServices(c echo.Context, modsetId string, params server.GetComplianceModulesetServicesParams) error {
	id, err := a.publishedModuleset(c, modsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.compObjectServices(c, "GetComplianceModulesetServices", cdb.CompModulesetKind, id, params.Slave, false,
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter))
}

// GetComplianceModulesetCandidateServices handles GET /compliance/modulesets/{modset_id}/candidate_services.
func (a *Api) GetComplianceModulesetCandidateServices(c echo.Context, modsetId string, params server.GetComplianceModulesetCandidateServicesParams) error {
	id, err := a.publishedModuleset(c, modsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.compObjectServices(c, "GetComplianceModulesetCandidateServices", cdb.CompModulesetKind, id, params.Slave, true,
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter))
}

// modulesetsNodes attaches or detaches a moduleset named in the body to a node,
// as POST and DELETE /nodes/{node_id}/compliance/modulesets/{mset_id} do.
func (a *Api) modulesetsNodes(c echo.Context, attach bool) error {
	b, err := readCompAttachBody(c, "modset_id", "modset_name", "node_id")
	if err != nil {
		return httpProblem(c, err)
	}
	id, err := a.resolveModuleset(c.Request().Context(), b.obj)
	if err != nil {
		return httpProblem(c, err)
	}
	if attach {
		return a.PostNodeComplianceModuleset(c, b.target, strconv.FormatInt(id, 10))
	}
	return a.DeleteNodeComplianceModuleset(c, b.target, strconv.FormatInt(id, 10))
}

// PostComplianceModulesetsNodes handles POST /compliance/modulesets_nodes.
func (a *Api) PostComplianceModulesetsNodes(c echo.Context) error { return a.modulesetsNodes(c, true) }

// DeleteComplianceModulesetsNodes handles DELETE /compliance/modulesets_nodes.
func (a *Api) DeleteComplianceModulesetsNodes(c echo.Context) error {
	return a.modulesetsNodes(c, false)
}

// modulesetsServices attaches or detaches a moduleset named in the body to a
// service, as POST and DELETE /services/{svc_id}/compliance/modulesets/{mset_id} do.
func (a *Api) modulesetsServices(c echo.Context, attach bool) error {
	b, err := readCompAttachBody(c, "modset_id", "modset_name", "svc_id")
	if err != nil {
		return httpProblem(c, err)
	}
	id, err := a.resolveModuleset(c.Request().Context(), b.obj)
	if err != nil {
		return httpProblem(c, err)
	}
	slave := b.slave
	if attach {
		return a.PostServiceComplianceModuleset(c, b.target, strconv.FormatInt(id, 10), server.PostServiceComplianceModulesetParams{Slave: &slave})
	}
	return a.DeleteServiceComplianceModuleset(c, b.target, strconv.FormatInt(id, 10), server.DeleteServiceComplianceModulesetParams{Slave: &slave})
}

// PostComplianceModulesetsServices handles POST /compliance/modulesets_services.
func (a *Api) PostComplianceModulesetsServices(c echo.Context) error {
	return a.modulesetsServices(c, true)
}

// DeleteComplianceModulesetsServices handles DELETE /compliance/modulesets_services.
func (a *Api) DeleteComplianceModulesetsServices(c echo.Context) error {
	return a.modulesetsServices(c, false)
}

// compOutcome is the result of attaching or detaching one compliance object: the
// status the single routes answer with, and their message.
type compOutcome struct {
	status int
	text   string
}

func compFailf(code int, format string, args ...any) compOutcome {
	return compOutcome{status: code, text: fmt.Sprintf(format, args...)}
}

func compDone(text string) compOutcome {
	return compOutcome{status: http.StatusAccepted, text: text}
}

// writeCompOutcome answers a single route with an outcome: a problem, or the
// message of the change.
func writeCompOutcome(c echo.Context, o compOutcome) error {
	if o.status >= http.StatusBadRequest {
		return JSONProblem(c, o.status, o.text)
	}
	return c.JSON(o.status, map[string]string{"info": o.text})
}
