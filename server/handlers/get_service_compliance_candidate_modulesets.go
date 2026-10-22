package serverhandlers

import (
	"errors"
	"net/http"

	"github.com/opensvc/oc3/cdb"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetServiceComplianceCandidateModulesets handles GET /services/{svc_id}/compliance/candidate_modulesets
func (a *Api) GetServiceComplianceCandidateModulesets(c echo.Context, svcId string, params server.GetServiceComplianceCandidateModulesetsParams) error {
	query, err := buildListQueryParameters(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, propsMapping["moduleset"])
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	log := echolog.GetLogHandler(c, "GetServiceComplianceCandidateModulesets")
	odb := a.ODB
	ctx := c.Request().Context()

	log.Info("called", "svc_id", svcId, "limit", query.Page.Limit, "offset", query.Page.Offset, "props", query.Props)

	svc, err := odb.ServiceBySvcIDOrName(ctx, svcId, UserGroupsFromContext(c), IsManager(c))
	if errors.Is(err, cdb.ErrServiceAmbiguous) {
		return JSONProblemf(c, http.StatusConflict, "%s", err)
	}
	if err != nil {
		log.Error("cannot resolve service", "svc_id", svcId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve service %s", svcId)
	}
	if svc == nil {
		return JSONProblemf(c, http.StatusNotFound, "service %s not found", svcId)
	}

	attachedModulesets, err := odb.CompServiceModulesets(ctx, svc.SvcID, false)
	if err != nil {
		log.Error("cannot get attached modulesets", "svc_id", svc.SvcID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get attached modulesets for service %s", svc.SvcID)
	}

	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)
	candidates, err := odb.CompServiceCandidateModulesets(ctx, svc.SvcID, attachedModulesets, groups, isManager, query.Page.Limit, query.Page.Offset)
	if err != nil {
		log.Error("cannot get candidate modulesets", "svc_id", svc.SvcID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get candidate modulesets for service %s", svc.SvcID)
	}

	filteredItems, err := filterItemsFields(candidates, query.Props)
	if err != nil {
		log.Error("cannot filter moduleset props", "svc_id", svc.SvcID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot filter modulesets fields for service %s", svc.SvcID)
	}

	return c.JSON(http.StatusOK, newListResponse(filteredItems, propsMapping["moduleset"], query))
}
