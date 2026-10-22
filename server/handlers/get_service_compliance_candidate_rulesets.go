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

// GetServiceComplianceCandidateRulesets handles GET /services/{svc_id}/compliance/candidate_rulesets
func (a *Api) GetServiceComplianceCandidateRulesets(c echo.Context, svcId string, params server.GetServiceComplianceCandidateRulesetsParams) error {
	query, err := buildListQueryParameters(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, propsMapping["ruleset"])
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	log := echolog.GetLogHandler(c, "GetServiceComplianceCandidateRulesets")
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

	slave := false
	if params.Slave != nil {
		slave = *params.Slave
	}
	attachedRulesets, err := odb.CompServiceRulesets(ctx, svc.SvcID, slave)
	if err != nil {
		log.Error("cannot get attached rulesets", "svc_id", svc.SvcID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get attached rulesets for service %s", svc.SvcID)
	}

	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)
	candidates, err := odb.CompServiceCandidateRulesets(ctx, svc.SvcID, attachedRulesets, groups, isManager, query.Page.Limit, query.Page.Offset)
	if err != nil {
		log.Error("cannot get candidate rulesets", "svc_id", svc.SvcID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get candidate rulesets for service %s", svc.SvcID)
	}

	filteredItems, err := filterItemsFields(candidates, query.Props)
	if err != nil {
		log.Error("cannot filter ruleset props", "svc_id", svc.SvcID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot filter rulesets fields for service %s", svc.SvcID)
	}

	return c.JSON(http.StatusOK, newListResponse(filteredItems, propsMapping["ruleset"], query))
}
