package serverhandlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// requireRulesetPublished refuses a caller the ruleset is not published to.
func (a *Api) requireRulesetPublished(ctx context.Context, c echo.Context, id int64) error {
	ok, err := a.ODB.CompRulesetPublished(ctx, id, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		return compDesignerError(c, "check the ruleset publication", err)
	}
	if !ok {
		return httpErrorf(http.StatusForbidden, "you are not member of one of the ruleset publication groups")
	}
	return nil
}

// resolveRulesetVariable returns the ids of a ruleset and of one of its
// variables, each given by id or by name.
func (a *Api) resolveRulesetVariable(ctx context.Context, rsetID, varID string) (int64, int64, error) {
	rid, err := a.resolveRuleset(ctx, rsetID)
	if err != nil {
		return 0, 0, err
	}
	vid, found, err := a.ODB.CompRulesetVariableID(ctx, rid, varID)
	if err != nil {
		return 0, 0, err
	}
	if !found {
		return 0, 0, httpErrorf(http.StatusNotFound, "variable %s not found", varID)
	}
	return rid, vid, nil
}

// variableFields checks the settable properties of a variable. var_value may be
// given as a string, or as any JSON value, stored serialized.
func variableFields(entry map[string]any) (map[string]any, error) {
	fields := map[string]any{}
	for k, v := range entry {
		switch k {
		case "var_name":
			name, err := compString(k, v, true)
			if err != nil {
				return nil, err
			}
			fields[k] = name
		case "var_class":
			class, err := compString(k, v, false)
			if err != nil {
				return nil, err
			}
			fields[k] = class
		case "var_value":
			if s, ok := v.(string); ok {
				fields[k] = s
			} else if v == nil {
				fields[k] = nil
			} else {
				b, err := json.Marshal(v)
				if err != nil {
					return nil, httpErrorf(http.StatusBadRequest, "invalid var_value: %s", err)
				}
				fields[k] = string(b)
			}
		case "id", "ruleset_id", "ruleset_name", "var_author", "var_updated":
		default:
			return nil, httpErrorf(http.StatusBadRequest, "unknown variable property %s", k)
		}
	}
	return fields, nil
}

// GetComplianceRulesetVariables handles GET /compliance/rulesets/{rset_id}/variables:
// the variables of a ruleset published to the caller.
func (a *Api) GetComplianceRulesetVariables(c echo.Context, rsetId string, params server.GetComplianceRulesetVariablesParams) error {
	ctx := c.Request().Context()
	rid, err := a.resolveRuleset(ctx, rsetId)
	if err == nil {
		err = a.requireRulesetPublished(ctx, c, rid)
	}
	if err != nil {
		return httpProblem(c, err)
	}
	return a.handleList(c, "GetComplianceRulesetVariables", "ruleset_variable", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetComplianceRulesetVariables(ctx, rid, nil, p)
	})
}

// GetComplianceRulesetVariable handles GET
// /compliance/rulesets/{rset_id}/variables/{var_id}.
func (a *Api) GetComplianceRulesetVariable(c echo.Context, rsetId, varId string, params server.GetComplianceRulesetVariableParams) error {
	ctx := c.Request().Context()
	rid, vid, err := a.resolveRulesetVariable(ctx, rsetId, varId)
	if err == nil {
		err = a.requireRulesetPublished(ctx, c, rid)
	}
	if err != nil {
		return httpProblem(c, err)
	}
	return a.variableResponse(c, rid, vid, params.Props)
}

func (a *Api) variableResponse(c echo.Context, rid, vid int64, props *server.InQueryProps) error {
	return a.handleItem(c, "GetComplianceRulesetVariable", "ruleset_variable", "id", strconv.FormatInt(vid, 10), listEndpointParams{
		props: props,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetComplianceRulesetVariables(ctx, rid, &vid, p)
	})
}

// PostComplianceRulesetVariables handles POST /compliance/rulesets/{rset_id}/variables,
// as the historical rest_post_compliance_ruleset_variables: a variable named
// var_name is created, or updated when the ruleset has one of that name.
func (a *Api) PostComplianceRulesetVariables(c echo.Context, rsetId string) error {
	entry, err := oneEntry(c)
	if err != nil {
		return httpProblem(c, err)
	}
	rid, err := a.resolveRuleset(c.Request().Context(), rsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.createOrUpdateVariable(c, rid, entry)
}

// PostComplianceRulesetsVariables handles POST /compliance/rulesets_variables,
// GetComplianceRulesetsVariables handles GET /compliance/rulesets_variables.
func (a *Api) GetComplianceRulesetsVariables(c echo.Context, params server.GetComplianceRulesetsVariablesParams) error {
	return a.handleList(c, "GetComplianceRulesetsVariables", "rulesets_variable",
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter),
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetComplianceRulesetsVariables(ctx, p)
		})
}

// the form naming the ruleset by ruleset_id or ruleset_name in the body.
func (a *Api) PostComplianceRulesetsVariables(c echo.Context) error {
	entry, err := oneEntry(c)
	if err != nil {
		return httpProblem(c, err)
	}
	ref, ok := entry["ruleset_id"]
	if !ok {
		ref, ok = entry["ruleset_name"]
	}
	if !ok {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "The 'ruleset_id' or 'ruleset_name' is mandatory"))
	}
	rid, err := a.resolveRuleset(c.Request().Context(), fmt.Sprint(ref))
	if err != nil {
		return httpProblem(c, err)
	}
	return a.createOrUpdateVariable(c, rid, entry)
}

func (a *Api) createOrUpdateVariable(c echo.Context, rid int64, entry map[string]any) error {
	if err := checkCompManager(c); err != nil {
		return httpProblem(c, err)
	}
	if err := a.requireRulesetResponsible(c.Request().Context(), c, rid); err != nil {
		return httpProblem(c, err)
	}
	fields, err := variableFields(entry)
	if err != nil {
		return httpProblem(c, err)
	}
	var vid int64
	if err := a.compWrite(c, "PostComplianceRulesetVariables", func(w *compWriter) (err error) {
		vid, err = w.createOrUpdateVariable(rid, fields)
		return err
	}); err != nil {
		return httpProblem(c, err)
	}
	return a.variableResponse(c, rid, vid, nil)
}

// PostComplianceRulesetVariable handles POST
// /compliance/rulesets/{rset_id}/variables/{var_id}: a change of the variable
// properties, by a CompManager responsible for the ruleset.
func (a *Api) PostComplianceRulesetVariable(c echo.Context, rsetId, varId string) error {
	entry, err := oneEntry(c)
	if err != nil {
		return httpProblem(c, err)
	}
	ctx := c.Request().Context()
	rid, vid, err := a.resolveRulesetVariable(ctx, rsetId, varId)
	if err == nil {
		err = checkCompManager(c)
	}
	if err == nil {
		err = a.requireRulesetResponsible(ctx, c, rid)
	}
	if err != nil {
		return httpProblem(c, err)
	}
	fields, err := variableFields(entry)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.updateVariable(c, rid, vid, fields)
}

func (a *Api) updateVariable(c echo.Context, rid, vid int64, fields map[string]any) error {
	if err := a.compWrite(c, "PostComplianceRulesetVariable", func(w *compWriter) error {
		return w.updateVariable(rid, vid, fields)
	}); err != nil {
		return httpProblem(c, err)
	}
	return a.variableResponse(c, rid, vid, nil)
}

// DeleteComplianceRulesetVariable handles DELETE
// /compliance/rulesets/{rset_id}/variables/{var_id}, by a CompManager responsible
// for the ruleset, as the historical handler describes it.
func (a *Api) DeleteComplianceRulesetVariable(c echo.Context, rsetId, varId string) error {
	ctx := c.Request().Context()
	rid, vid, err := a.resolveRulesetVariable(ctx, rsetId, varId)
	if err == nil {
		err = checkCompManager(c)
	}
	if err == nil {
		err = a.requireRulesetResponsible(ctx, c, rid)
	}
	if err != nil {
		return httpProblem(c, err)
	}
	if err := a.compWrite(c, "DeleteComplianceRulesetVariable", func(w *compWriter) error {
		return w.deleteVariable(rid, vid)
	}); err != nil {
		return httpProblem(c, err)
	}
	return compInfo(c, "variable deleted")
}

// PutComplianceRulesetVariable handles PUT
// /compliance/rulesets/{rset_id}/variables/{var_id}, the special actions on a
// variable, as the historical rest_put_compliance_ruleset_variable: copy or move
// to dst_ruleset. A copy requires the source ruleset to be published to the
// caller, a move its responsibility; both the responsibility of the destination.
func (a *Api) PutComplianceRulesetVariable(c echo.Context, rsetId, varId string) error {
	entry, err := oneEntry(c)
	if err != nil {
		return httpProblem(c, err)
	}
	ctx := c.Request().Context()
	rid, vid, err := a.resolveRulesetVariable(ctx, rsetId, varId)
	if err != nil {
		return httpProblem(c, err)
	}
	action, _ := entry["action"].(string)
	if action != "copy" && action != "move" {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "unsupported action %q", action))
	}
	dst, ok := entry["dst_ruleset"]
	if !ok {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "dst_ruleset not found in data"))
	}
	dstID, err := a.resolveRuleset(ctx, fmt.Sprint(dst))
	if err != nil {
		return httpProblem(c, err)
	}
	if err := a.compWrite(c, "PutComplianceRulesetVariable", func(w *compWriter) error {
		return w.copyVariable(rid, vid, dstID, action == "move")
	}); err != nil {
		return httpProblem(c, err)
	}
	return compInfo(c, "variable %s done", action)
}

// oneEntry reads a body holding one object, JSON or form-encoded.
func oneEntry(c echo.Context) (map[string]any, error) {
	entries, _, err := decodeEntries(c)
	if err != nil {
		return nil, httpErrorf(http.StatusBadRequest, "%s", err)
	}
	if len(entries) != 1 {
		return nil, httpErrorf(http.StatusBadRequest, "one object is expected in the body")
	}
	return entries[0], nil
}
