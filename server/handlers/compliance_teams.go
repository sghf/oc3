package serverhandlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// compTeamList answers with the publication or responsible groups of a
// compliance object; a non-manager sees only their own among them.
func (a *Api) compTeamList(c echo.Context, name string, k cdb.CompKind, objID int64, gtype string, params listEndpointParams) error {
	return a.handleList(c, name, "auth_group", params, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetCompTeamGroups(ctx, k, objID, gtype, p)
	})
}

// compTeamChange attaches or detaches a publication or responsible group of a
// compliance object, as compWriter.teamChange.
func (a *Api) compTeamChange(c echo.Context, name string, k cdb.CompKind, objID int64, gtype, groupRef string, attach bool) error {
	err := a.compWrite(c, name, func(w *compWriter) error {
		return w.teamChange(k, objID, gtype, groupRef, attach)
	})
	switch {
	case errors.Is(err, errNothing):
		return compInfo(c, "%s group already attached", gtype)
	case err != nil:
		return httpProblem(c, err)
	case attach:
		return compInfo(c, "group attached")
	default:
		return compInfo(c, "group detached")
	}
}

// compTeamBody reads the object and the group a bulk form names in its body.
func compTeamBody(c echo.Context, objKey string) (string, string, error) {
	entry, err := oneEntry(c)
	if err != nil {
		return "", "", err
	}
	obj, ok := entry[objKey]
	if !ok {
		return "", "", httpErrorf(http.StatusBadRequest, "The '%s' key is mandatory", objKey)
	}
	group, ok := entry["group_id"]
	if !ok {
		return "", "", httpErrorf(http.StatusBadRequest, "The 'group_id' key is mandatory")
	}
	return fmt.Sprint(obj), fmt.Sprint(group), nil
}

func listParams(props *server.InQueryProps, limit *server.InQueryLimit, offset *server.InQueryOffset, meta *server.InQueryMeta, stats *server.InQueryStats, orderby *server.InQueryOrderby, groupby *server.InQueryGroupby, filter *server.InQueryFilter) listEndpointParams {
	return listEndpointParams{props: props, limit: limit, offset: offset, meta: meta, stats: stats, orderby: orderby, groupby: groupby, filter: filter}
}

// GetComplianceRulesetPublications handles GET /compliance/rulesets/{rset_id}/publications.
func (a *Api) GetComplianceRulesetPublications(c echo.Context, rsetId string, params server.GetComplianceRulesetPublicationsParams) error {
	id, err := a.resolveRuleset(c.Request().Context(), rsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.compTeamList(c, "GetComplianceRulesetPublications", cdb.CompRulesetKind, id, "publication",
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter))
}

// GetComplianceRulesetResponsibles handles GET /compliance/rulesets/{rset_id}/responsibles.
func (a *Api) GetComplianceRulesetResponsibles(c echo.Context, rsetId string, params server.GetComplianceRulesetResponsiblesParams) error {
	id, err := a.resolveRuleset(c.Request().Context(), rsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.compTeamList(c, "GetComplianceRulesetResponsibles", cdb.CompRulesetKind, id, "responsible",
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter))
}

func (a *Api) rulesetTeamChange(c echo.Context, name, rsetRef, groupRef, gtype string, attach bool) error {
	id, err := a.resolveRuleset(c.Request().Context(), rsetRef)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.compTeamChange(c, name, cdb.CompRulesetKind, id, gtype, groupRef, attach)
}

// PostComplianceRulesetPublication handles POST /compliance/rulesets/{rset_id}/publications/{group_id}.
func (a *Api) PostComplianceRulesetPublication(c echo.Context, rsetId, groupId string) error {
	return a.rulesetTeamChange(c, "PostComplianceRulesetPublication", rsetId, groupId, "publication", true)
}

// DeleteComplianceRulesetPublication handles DELETE /compliance/rulesets/{rset_id}/publications/{group_id}.
func (a *Api) DeleteComplianceRulesetPublication(c echo.Context, rsetId, groupId string) error {
	return a.rulesetTeamChange(c, "DeleteComplianceRulesetPublication", rsetId, groupId, "publication", false)
}

// PostComplianceRulesetResponsible handles POST /compliance/rulesets/{rset_id}/responsibles/{group_id}.
func (a *Api) PostComplianceRulesetResponsible(c echo.Context, rsetId, groupId string) error {
	return a.rulesetTeamChange(c, "PostComplianceRulesetResponsible", rsetId, groupId, "responsible", true)
}

// DeleteComplianceRulesetResponsible handles DELETE /compliance/rulesets/{rset_id}/responsibles/{group_id}.
func (a *Api) DeleteComplianceRulesetResponsible(c echo.Context, rsetId, groupId string) error {
	return a.rulesetTeamChange(c, "DeleteComplianceRulesetResponsible", rsetId, groupId, "responsible", false)
}

func (a *Api) rulesetsTeamBulk(c echo.Context, name, gtype string, attach bool) error {
	rset, group, err := compTeamBody(c, "ruleset_id")
	if err != nil {
		return httpProblem(c, err)
	}
	return a.rulesetTeamChange(c, name, rset, group, gtype, attach)
}

// PostComplianceRulesetsPublications handles POST /compliance/rulesets_publications.
func (a *Api) PostComplianceRulesetsPublications(c echo.Context) error {
	return a.rulesetsTeamBulk(c, "PostComplianceRulesetsPublications", "publication", true)
}

// DeleteComplianceRulesetsPublications handles DELETE /compliance/rulesets_publications.
func (a *Api) DeleteComplianceRulesetsPublications(c echo.Context) error {
	return a.rulesetsTeamBulk(c, "DeleteComplianceRulesetsPublications", "publication", false)
}

// PostComplianceRulesetsResponsibles handles POST /compliance/rulesets_responsibles.
func (a *Api) PostComplianceRulesetsResponsibles(c echo.Context) error {
	return a.rulesetsTeamBulk(c, "PostComplianceRulesetsResponsibles", "responsible", true)
}

// DeleteComplianceRulesetsResponsibles handles DELETE /compliance/rulesets_responsibles.
func (a *Api) DeleteComplianceRulesetsResponsibles(c echo.Context) error {
	return a.rulesetsTeamBulk(c, "DeleteComplianceRulesetsResponsibles", "responsible", false)
}

// GetComplianceModulesetPublications handles GET /compliance/modulesets/{modset_id}/publications.
func (a *Api) GetComplianceModulesetPublications(c echo.Context, modsetId string, params server.GetComplianceModulesetPublicationsParams) error {
	id, err := a.resolveModuleset(c.Request().Context(), modsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.compTeamList(c, "GetComplianceModulesetPublications", cdb.CompModulesetKind, id, "publication",
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter))
}

// GetComplianceModulesetResponsibles handles GET /compliance/modulesets/{modset_id}/responsibles.
func (a *Api) GetComplianceModulesetResponsibles(c echo.Context, modsetId string, params server.GetComplianceModulesetResponsiblesParams) error {
	id, err := a.resolveModuleset(c.Request().Context(), modsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.compTeamList(c, "GetComplianceModulesetResponsibles", cdb.CompModulesetKind, id, "responsible",
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter))
}

func (a *Api) modulesetTeamChange(c echo.Context, name, modsetRef, groupRef, gtype string, attach bool) error {
	id, err := a.resolveModuleset(c.Request().Context(), modsetRef)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.compTeamChange(c, name, cdb.CompModulesetKind, id, gtype, groupRef, attach)
}

// PostComplianceModulesetPublication handles POST /compliance/modulesets/{modset_id}/publications/{group_id}.
func (a *Api) PostComplianceModulesetPublication(c echo.Context, modsetId, groupId string) error {
	return a.modulesetTeamChange(c, "PostComplianceModulesetPublication", modsetId, groupId, "publication", true)
}

// DeleteComplianceModulesetPublication handles DELETE /compliance/modulesets/{modset_id}/publications/{group_id}.
func (a *Api) DeleteComplianceModulesetPublication(c echo.Context, modsetId, groupId string) error {
	return a.modulesetTeamChange(c, "DeleteComplianceModulesetPublication", modsetId, groupId, "publication", false)
}

// PostComplianceModulesetResponsible handles POST /compliance/modulesets/{modset_id}/responsibles/{group_id}.
func (a *Api) PostComplianceModulesetResponsible(c echo.Context, modsetId, groupId string) error {
	return a.modulesetTeamChange(c, "PostComplianceModulesetResponsible", modsetId, groupId, "responsible", true)
}

// DeleteComplianceModulesetResponsible handles DELETE /compliance/modulesets/{modset_id}/responsibles/{group_id}.
func (a *Api) DeleteComplianceModulesetResponsible(c echo.Context, modsetId, groupId string) error {
	return a.modulesetTeamChange(c, "DeleteComplianceModulesetResponsible", modsetId, groupId, "responsible", false)
}

func (a *Api) modulesetsTeamBulk(c echo.Context, name, gtype string, attach bool) error {
	modset, group, err := compTeamBody(c, "modset_id")
	if err != nil {
		return httpProblem(c, err)
	}
	return a.modulesetTeamChange(c, name, modset, group, gtype, attach)
}

// PostComplianceModulesetsPublications handles POST /compliance/modulesets_publications.
func (a *Api) PostComplianceModulesetsPublications(c echo.Context) error {
	return a.modulesetsTeamBulk(c, "PostComplianceModulesetsPublications", "publication", true)
}

// DeleteComplianceModulesetsPublications handles DELETE /compliance/modulesets_publications.
func (a *Api) DeleteComplianceModulesetsPublications(c echo.Context) error {
	return a.modulesetsTeamBulk(c, "DeleteComplianceModulesetsPublications", "publication", false)
}

// PostComplianceModulesetsResponsibles handles POST /compliance/modulesets_responsibles.
func (a *Api) PostComplianceModulesetsResponsibles(c echo.Context) error {
	return a.modulesetsTeamBulk(c, "PostComplianceModulesetsResponsibles", "responsible", true)
}

// DeleteComplianceModulesetsResponsibles handles DELETE /compliance/modulesets_responsibles.
func (a *Api) DeleteComplianceModulesetsResponsibles(c echo.Context) error {
	return a.modulesetsTeamBulk(c, "DeleteComplianceModulesetsResponsibles", "responsible", false)
}
