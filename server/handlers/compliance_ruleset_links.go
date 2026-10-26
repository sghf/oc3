package serverhandlers

import (
	"github.com/labstack/echo/v4"
)

// PostComplianceRulesetRuleset handles POST
// /compliance/rulesets/{rset_id}/rulesets/{child_rset_id}, as the historical
// attach_ruleset_to_ruleset(): by a CompManager responsible for the parent, a
// ruleset becomes a child of another, unless that would close a loop.
func (a *Api) PostComplianceRulesetRuleset(c echo.Context, rsetId, childRsetId string) error {
	ctx := c.Request().Context()
	parent, err := a.resolveRuleset(ctx, rsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	child, err := a.resolveRuleset(ctx, childRsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	if err := a.compWrite(c, "PostComplianceRulesetRuleset", func(w *compWriter) error {
		return w.attachRulesetChild(parent, child)
	}); err != nil {
		return httpProblem(c, err)
	}
	return compInfo(c, "ruleset attached")
}

// DeleteComplianceRulesetRuleset handles DELETE
// /compliance/rulesets/{rset_id}/rulesets/{child_rset_id}, as the historical
// detach_ruleset_from_ruleset(): by a CompManager responsible for the parent, the
// child being published to them.
func (a *Api) DeleteComplianceRulesetRuleset(c echo.Context, rsetId, childRsetId string) error {
	ctx := c.Request().Context()
	parent, err := a.resolveRuleset(ctx, rsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	child, err := a.resolveRuleset(ctx, childRsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	if err := a.compWrite(c, "DeleteComplianceRulesetRuleset", func(w *compWriter) error {
		return w.detachRulesetChild(parent, child)
	}); err != nil {
		return httpProblem(c, err)
	}
	return compInfo(c, "ruleset detached")
}

// PostComplianceRulesetFilterset handles POST
// /compliance/rulesets/{rset_id}/filtersets/{fset_id}, as the historical
// attach_filterset_to_ruleset(): the filterset selecting the nodes and services a
// contextual ruleset applies to, replacing the previous one.
func (a *Api) PostComplianceRulesetFilterset(c echo.Context, rsetId, fsetId string) error {
	ctx := c.Request().Context()
	rid, err := a.resolveRuleset(ctx, rsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	if err := a.compWrite(c, "PostComplianceRulesetFilterset", func(w *compWriter) error {
		return w.setRulesetFilterset(rid, fsetId)
	}); err != nil {
		return httpProblem(c, err)
	}
	return compInfo(c, "filterset attached")
}

// DeleteComplianceRulesetFilterset handles DELETE
// /compliance/rulesets/{rset_id}/filtersets/{fset_id}, as the historical
// detach_filterset_from_ruleset(): the ruleset's filterset is removed, whichever
// it is.
func (a *Api) DeleteComplianceRulesetFilterset(c echo.Context, rsetId, fsetId string) error {
	ctx := c.Request().Context()
	rid, err := a.resolveRuleset(ctx, rsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	if err := a.compWrite(c, "DeleteComplianceRulesetFilterset", func(w *compWriter) error {
		return w.detachRulesetFilterset(rid, nil)
	}); err != nil {
		return httpProblem(c, err)
	}
	return compInfo(c, "filterset detached")
}
