package serverhandlers

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
)

// GetComplianceRulesets handles GET /compliance/rulesets, as the historical
// rest_get_compliance_rulesets: the rulesets published to the caller's groups.
func (a *Api) GetComplianceRulesets(c echo.Context, params server.GetComplianceRulesetsParams) error {
	return a.handleList(c, "GetComplianceRulesets", "ruleset", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetComplianceRulesets(ctx, nil, p)
	})
}

// GetComplianceRuleset handles GET /compliance/rulesets/{rset_id}, the ruleset
// given by id or by name, when published to one of the caller's groups.
func (a *Api) GetComplianceRuleset(c echo.Context, rsetId string, params server.GetComplianceRulesetParams) error {
	id, err := a.resolveRuleset(c.Request().Context(), rsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.rulesetResponse(c, id, params.Props)
}

// rulesetResponse answers with a ruleset, as the historical handlers end with
// rest_get_compliance_ruleset().handler(id).
func (a *Api) rulesetResponse(c echo.Context, id int64, props *server.InQueryProps) error {
	return a.handleItem(c, "GetComplianceRuleset", "ruleset", "id", strconv.FormatInt(id, 10), listEndpointParams{
		props: props,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetComplianceRulesets(ctx, &id, p)
	})
}

// PostComplianceRulesets handles POST /compliance/rulesets, as the historical
// rest_post_compliance_rulesets: a ruleset named ruleset_name is created,
// published to and under the responsibility of the caller's default group; when
// it exists, its other given properties are updated instead.
func (a *Api) PostComplianceRulesets(c echo.Context) error {
	entries, _, err := decodeEntries(c)
	if err != nil {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "%s", err))
	}
	if len(entries) != 1 {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "one ruleset is expected"))
	}
	entry := entries[0]
	name, err := compString("ruleset_name", entry["ruleset_name"], true)
	if err != nil {
		return httpProblem(c, err)
	}
	if id, found, err := a.ODB.CompRulesetID(c.Request().Context(), name); err != nil {
		return httpProblem(c, compDesignerError(c, "PostComplianceRulesets", err))
	} else if found {
		delete(entry, "ruleset_name")
		if len(entry) == 0 {
			return compInfo(c, "No fields to update")
		}
		return a.updateRuleset(c, id, entry)
	}
	rtype := "explicit"
	if v, ok := entry["ruleset_type"]; ok {
		if rtype, err = compString("ruleset_type", v, true); err != nil {
			return httpProblem(c, err)
		}
		if err := compOneOf("ruleset_type", rtype, "explicit", "contextual"); err != nil {
			return httpProblem(c, err)
		}
	}
	public := "T"
	if v, ok := entry["ruleset_public"]; ok {
		if public, err = compBool("ruleset_public", v); err != nil {
			return httpProblem(c, err)
		}
	}
	var id int64
	if err := a.compWrite(c, "PostComplianceRulesets", func(w *compWriter) (err error) {
		id, err = w.createRuleset(name, rtype, public)
		return err
	}); err != nil {
		return httpProblem(c, err)
	}
	return a.rulesetResponse(c, id, nil)
}

// PostComplianceRuleset handles POST /compliance/rulesets/{rset_id}, as the
// historical rest_post_compliance_ruleset: the given properties are updated, by a
// CompManager responsible for the ruleset.
func (a *Api) PostComplianceRuleset(c echo.Context, rsetId string) error {
	entries, _, err := decodeEntries(c)
	if err != nil {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "%s", err))
	}
	if len(entries) != 1 {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "one set of properties is expected"))
	}
	id, err := a.resolveRuleset(c.Request().Context(), rsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.updateRuleset(c, id, entries[0])
}

// updateRuleset checks and applies a change of ruleset properties.
func (a *Api) updateRuleset(c echo.Context, id int64, entry map[string]any) error {
	if err := checkCompManager(c); err != nil {
		return httpProblem(c, err)
	}
	fields, err := rulesetFields(entry)
	if err != nil {
		return httpProblem(c, err)
	}
	if len(fields) == 0 {
		return compInfo(c, "No fields to update")
	}
	if err := a.compWrite(c, "PostComplianceRuleset", func(w *compWriter) error {
		return w.updateRuleset(id, fields)
	}); err != nil {
		return httpProblem(c, err)
	}
	return a.rulesetResponse(c, id, nil)
}

// PutComplianceRuleset handles PUT /compliance/rulesets/{rset_id}, the special
// actions on a ruleset, as the historical rest_put_compliance_ruleset: clone.
func (a *Api) PutComplianceRuleset(c echo.Context, rsetId string) error {
	entries, _, err := decodeEntries(c)
	if err != nil || len(entries) != 1 {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "an action is expected"))
	}
	ctx := c.Request().Context()
	id, err := a.resolveRuleset(ctx, rsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	action, _ := entries[0]["action"].(string)
	if action != "clone" {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "unsupported action %q", action))
	}
	var cloneName string
	if err := a.compWrite(c, "PutComplianceRuleset", func(w *compWriter) (err error) {
		_, cloneName, err = w.cloneObject(cdb.CompRulesetKind, id, "")
		return err
	}); err != nil {
		return httpProblem(c, err)
	}
	return compInfo(c, "clone done. new ruleset name %s", cloneName)
}

// DeleteComplianceRuleset handles DELETE /compliance/rulesets/{rset_id}.
func (a *Api) DeleteComplianceRuleset(c echo.Context, rsetId string) error {
	id, err := a.resolveRuleset(c.Request().Context(), rsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.deleteRuleset(c, id)
}

// DeleteComplianceRulesets handles DELETE /compliance/rulesets, the bulk form
// naming the ruleset by the id key of the body.
func (a *Api) DeleteComplianceRulesets(c echo.Context) error {
	entries, _, err := decodeEntries(c)
	if err != nil || len(entries) != 1 {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "The 'id' key is mandatory"))
	}
	raw, ok := entries[0]["id"]
	if !ok {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "The 'id' key is mandatory"))
	}
	id, err := a.resolveRuleset(c.Request().Context(), fmt.Sprint(raw))
	if err != nil {
		return httpProblem(c, err)
	}
	return a.deleteRuleset(c, id)
}

// deleteRuleset deletes a ruleset and all its relations, as delete_ruleset(): by
// a CompManager whose group is responsible for it.
func (a *Api) deleteRuleset(c echo.Context, id int64) error {
	if err := a.compWrite(c, "DeleteComplianceRuleset", func(w *compWriter) error {
		return w.deleteObject(cdb.CompRulesetKind, id)
	}); err != nil {
		return httpProblem(c, err)
	}
	return compInfo(c, "Ruleset %d deleted", id)
}

// requireRulesetResponsible refuses a caller none of whose groups is responsible
// for the ruleset.
func (a *Api) requireRulesetResponsible(ctx context.Context, c echo.Context, id int64) error {
	ok, err := a.ODB.CompRulesetResponsible(ctx, id, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		return compDesignerError(c, "check the ruleset responsibility", err)
	}
	if !ok {
		return httpErrorf(http.StatusForbidden, "you are not responsible for this ruleset")
	}
	return nil
}

// afterRulesetChange rebuilds the ruleset chains when the hierarchy or the names
// changed, and announces the change.
func (a *Api) afterRulesetChange(c echo.Context, chains bool) {
	ctx := c.Request().Context()
	log := echolog.GetLogHandler(c, "compliance")
	if chains {
		if err := a.ODB.CompRulesetsChains(ctx); err != nil {
			log.Error("cannot rebuild the ruleset chains", "error", err)
		}
	}
	a.notifyChanges(log, ctx)
}

// GetComplianceRulesetUsage handles GET /compliance/rulesets/{rset_id}/usage, as
// the historical rest_get_compliance_ruleset_usage: the modulesets and parent
// rulesets holding the ruleset, the nodes and services it is attached to.
func (a *Api) GetComplianceRulesetUsage(c echo.Context, rsetId string) error {
	ctx := c.Request().Context()
	id, found, err := a.ODB.CompRulesetID(ctx, rsetId)
	if err != nil {
		return httpProblem(c, compDesignerError(c, "GetComplianceRulesetUsage", err))
	}
	if found {
		found, err = a.ODB.CompRulesetVisible(ctx, id, UserGroupsFromContext(c), IsManager(c))
		if err != nil {
			return httpProblem(c, compDesignerError(c, "GetComplianceRulesetUsage", err))
		}
	}
	if !found {
		return httpProblem(c, httpErrorf(http.StatusNotFound, "ruleset not found or not visible to your groups"))
	}
	usage, err := a.ODB.CompRulesetUsage(ctx, id)
	if err != nil {
		return httpProblem(c, compDesignerError(c, "GetComplianceRulesetUsage", err))
	}
	return c.JSON(http.StatusOK, map[string]any{"data": usage})
}

// GetComplianceRulesetAmIResponsible handles GET
// /compliance/rulesets/{rset_id}/am_i_responsible: whether one of the caller's
// groups is responsible for the ruleset.
func (a *Api) GetComplianceRulesetAmIResponsible(c echo.Context, rsetId string) error {
	ctx := c.Request().Context()
	id, found, err := a.ODB.CompRulesetID(ctx, rsetId)
	if err != nil {
		return httpProblem(c, compDesignerError(c, "GetComplianceRulesetAmIResponsible", err))
	}
	ok := false
	if found {
		if ok, err = a.ODB.CompRulesetResponsible(ctx, id, UserGroupsFromContext(c), IsManager(c)); err != nil {
			return httpProblem(c, compDesignerError(c, "GetComplianceRulesetAmIResponsible", err))
		}
	}
	return c.JSON(http.StatusOK, map[string]bool{"data": ok})
}
