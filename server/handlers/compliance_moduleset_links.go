package serverhandlers

import (
	"errors"

	"github.com/labstack/echo/v4"
)

// modulesetParent resolves the moduleset a composition change applies to, and
// checks the caller is a CompManager responsible for it.
func (a *Api) modulesetParent(c echo.Context, modsetID string) (int64, error) {
	ctx := c.Request().Context()
	id, err := a.resolveModuleset(ctx, modsetID)
	if err == nil {
		err = checkCompManager(c)
	}
	if err == nil {
		err = a.requireModulesetResponsible(ctx, c, id)
	}
	return id, err
}

// PostComplianceModulesetModuleset handles POST
// /compliance/modulesets/{modset_id}/modulesets/{child_modset_id}, as the
// historical attach_moduleset_to_moduleset(): by a CompManager responsible for
// the parent, a moduleset published to them becomes a child of another, unless
// that would close a loop.
func (a *Api) PostComplianceModulesetModuleset(c echo.Context, modsetId, childModsetId string) error {
	parent, err := a.modulesetParent(c, modsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	child, err := a.publishedModuleset(c, childModsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	err = a.compWrite(c, "PostComplianceModulesetModuleset", func(w *compWriter) error {
		return w.attachModulesetChild(parent, child)
	})
	if errors.Is(err, errNothing) {
		return compInfo(c, "already attached")
	} else if err != nil {
		return httpProblem(c, err)
	}
	return compInfo(c, "moduleset attached")
}

// DeleteComplianceModulesetModuleset handles DELETE
// /compliance/modulesets/{modset_id}/modulesets/{child_modset_id}, as the
// historical detach_moduleset_from_moduleset(), by a CompManager responsible for
// the parent.
func (a *Api) DeleteComplianceModulesetModuleset(c echo.Context, modsetId, childModsetId string) error {
	ctx := c.Request().Context()
	parent, err := a.modulesetParent(c, modsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	child, err := a.resolveModuleset(ctx, childModsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	if err := a.compWrite(c, "DeleteComplianceModulesetModuleset", func(w *compWriter) error {
		return w.detachModulesetChild(parent, child)
	}); err != nil {
		return httpProblem(c, err)
	}
	return compInfo(c, "moduleset detached")
}

// PostComplianceModulesetRuleset handles POST
// /compliance/modulesets/{modset_id}/rulesets/{rset_id}, as the historical
// attach_ruleset_to_moduleset(): by a CompManager responsible for the moduleset,
// a ruleset published to them adds its variables to the modules' environment.
func (a *Api) PostComplianceModulesetRuleset(c echo.Context, modsetId, rsetId string) error {
	ctx := c.Request().Context()
	modset, err := a.modulesetParent(c, modsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	rset, err := a.resolveRuleset(ctx, rsetId)
	if err == nil {
		err = a.requireRulesetPublished(ctx, c, rset)
	}
	if err != nil {
		return httpProblem(c, err)
	}
	err = a.compWrite(c, "PostComplianceModulesetRuleset", func(w *compWriter) error {
		return w.attachModulesetRuleset(modset, rset)
	})
	if errors.Is(err, errNothing) {
		return compInfo(c, "already attached")
	} else if err != nil {
		return httpProblem(c, err)
	}
	return compInfo(c, "ruleset attached")
}

// DeleteComplianceModulesetRuleset handles DELETE
// /compliance/modulesets/{modset_id}/rulesets/{rset_id}, as the historical
// detach_ruleset_from_moduleset(): by a CompManager responsible for the
// moduleset, the ruleset being published to them.
func (a *Api) DeleteComplianceModulesetRuleset(c echo.Context, modsetId, rsetId string) error {
	ctx := c.Request().Context()
	modset, err := a.modulesetParent(c, modsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	rset, err := a.resolveRuleset(ctx, rsetId)
	if err == nil {
		err = a.requireRulesetPublished(ctx, c, rset)
	}
	if err != nil {
		return httpProblem(c, err)
	}
	if err := a.compWrite(c, "DeleteComplianceModulesetRuleset", func(w *compWriter) error {
		return w.detachModulesetRuleset(modset, rset)
	}); err != nil {
		return httpProblem(c, err)
	}
	return compInfo(c, "ruleset detached")
}
