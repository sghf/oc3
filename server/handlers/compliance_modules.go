package serverhandlers

import (
	"context"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// resolveModulesetModule returns the ids of a moduleset and of one of its
// modules, each given by id or by name.
func (a *Api) resolveModulesetModule(ctx context.Context, modsetID, modID string) (int64, int64, error) {
	mid, err := a.resolveModuleset(ctx, modsetID)
	if err != nil {
		return 0, 0, err
	}
	id, found, err := a.ODB.CompModulesetModuleID(ctx, mid, modID)
	if err != nil {
		return 0, 0, err
	}
	if !found {
		return 0, 0, httpErrorf(http.StatusNotFound, "module %s not found", modID)
	}
	return mid, id, nil
}

// moduleFields checks the settable properties of a module.
func moduleFields(entry map[string]any) (map[string]any, error) {
	fields := map[string]any{}
	for k, v := range entry {
		switch k {
		case "modset_mod_name":
			name, err := compString(k, v, true)
			if err != nil {
				return nil, err
			}
			fields[k] = name
		case "autofix":
			flag, err := compBool(k, v)
			if err != nil {
				return nil, err
			}
			fields[k] = flag
		case "id", "modset_id", "modset_mod_author", "modset_mod_updated":
		default:
			return nil, httpErrorf(http.StatusBadRequest, "unknown module property %s", k)
		}
	}
	return fields, nil
}

// GetComplianceModulesetModules handles GET /compliance/modulesets/{modset_id}/modules:
// the modules of a moduleset published to the caller.
func (a *Api) GetComplianceModulesetModules(c echo.Context, modsetId string, params server.GetComplianceModulesetModulesParams) error {
	mid, err := a.publishedModuleset(c, modsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.handleList(c, "GetComplianceModulesetModules", "moduleset_module",
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter),
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetComplianceModulesetModules(ctx, mid, nil, p)
		})
}

// GetComplianceModulesetsModules handles GET /compliance/modulesets_modules.
func (a *Api) GetComplianceModulesetsModules(c echo.Context, params server.GetComplianceModulesetsModulesParams) error {
	return a.handleList(c, "GetComplianceModulesetsModules", "modulesets_module",
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter),
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetComplianceModulesetsModules(ctx, p)
		})
}

// GetComplianceModulesetModule handles GET /compliance/modulesets/{modset_id}/modules/{mod_id}.
func (a *Api) GetComplianceModulesetModule(c echo.Context, modsetId, modId string, params server.GetComplianceModulesetModuleParams) error {
	ctx := c.Request().Context()
	mid, id, err := a.resolveModulesetModule(ctx, modsetId, modId)
	if err == nil {
		err = a.requireModulesetPublished(ctx, c, mid)
	}
	if err != nil {
		return httpProblem(c, err)
	}
	return a.moduleResponse(c, mid, id, params.Props)
}

func (a *Api) moduleResponse(c echo.Context, mid, id int64, props *server.InQueryProps) error {
	return a.handleItem(c, "GetComplianceModulesetModule", "moduleset_module", "id", strconv.FormatInt(id, 10), listEndpointParams{
		props: props,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetComplianceModulesetModules(ctx, mid, &id, p)
	})
}

// PostComplianceModulesetModules handles POST /compliance/modulesets/{modset_id}/modules,
// as the historical rest_post_compliance_moduleset_modules: a module named
// modset_mod_name is created, by a CompManager responsible for the moduleset.
func (a *Api) PostComplianceModulesetModules(c echo.Context, modsetId string) error {
	entry, err := oneEntry(c)
	if err != nil {
		return httpProblem(c, err)
	}
	ctx := c.Request().Context()
	mid, err := a.resolveModuleset(ctx, modsetId)
	if err == nil {
		err = checkCompManager(c)
	}
	if err == nil {
		err = a.requireModulesetResponsible(ctx, c, mid)
	}
	if err != nil {
		return httpProblem(c, err)
	}
	fields, err := moduleFields(entry)
	if err != nil {
		return httpProblem(c, err)
	}
	var id int64
	if err := a.compWrite(c, "PostComplianceModulesetModules", func(w *compWriter) (err error) {
		id, err = w.createModule(mid, fields)
		return err
	}); err != nil {
		return httpProblem(c, err)
	}
	return a.moduleResponse(c, mid, id, nil)
}

// PostComplianceModulesetModule handles POST
// /compliance/modulesets/{modset_id}/modules/{mod_id}: a change of the module
// name or autofix flag, by a CompManager responsible for the moduleset.
func (a *Api) PostComplianceModulesetModule(c echo.Context, modsetId, modId string) error {
	entry, err := oneEntry(c)
	if err != nil {
		return httpProblem(c, err)
	}
	ctx := c.Request().Context()
	mid, id, err := a.resolveModulesetModule(ctx, modsetId, modId)
	if err == nil {
		err = checkCompManager(c)
	}
	if err == nil {
		err = a.requireModulesetResponsible(ctx, c, mid)
	}
	if err != nil {
		return httpProblem(c, err)
	}
	fields, err := moduleFields(entry)
	if err != nil {
		return httpProblem(c, err)
	}
	if err := a.compWrite(c, "PostComplianceModulesetModule", func(w *compWriter) error {
		return w.updateModule(mid, id, fields)
	}); err != nil {
		return httpProblem(c, err)
	}
	return a.moduleResponse(c, mid, id, nil)
}

// DeleteComplianceModulesetModule handles DELETE
// /compliance/modulesets/{modset_id}/modules/{mod_id}, by a CompManager
// responsible for the moduleset, as the historical handler describes it.
func (a *Api) DeleteComplianceModulesetModule(c echo.Context, modsetId, modId string) error {
	ctx := c.Request().Context()
	mid, id, err := a.resolveModulesetModule(ctx, modsetId, modId)
	if err == nil {
		err = checkCompManager(c)
	}
	if err == nil {
		err = a.requireModulesetResponsible(ctx, c, mid)
	}
	if err != nil {
		return httpProblem(c, err)
	}
	if err := a.compWrite(c, "DeleteComplianceModulesetModule", func(w *compWriter) error {
		return w.deleteModule(mid, id)
	}); err != nil {
		return httpProblem(c, err)
	}
	return compInfo(c, "module deleted")
}
