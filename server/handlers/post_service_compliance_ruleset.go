package serverhandlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// PostServiceComplianceRuleset handles POST /services/{svc_id}/compliance/rulesets/{rset_id}
func (a *Api) PostServiceComplianceRuleset(c echo.Context, svcId string, rsetId string, params server.PostServiceComplianceRulesetParams) error {
	slave := false
	if params.Slave != nil {
		slave = *params.Slave
	}
	return writeCompOutcome(c, a.attachServiceRuleset(c, svcId, rsetId, slave))
}

// attachServiceRuleset attaches a ruleset to a service, its encapsulated part when
// slave, with the checks of the single route: the responsibility of the caller,
// the ruleset, its current attachment, and whether the ruleset may be attached
// there. POST /compliance/attachments applies it to each pair of a batch.
func (a *Api) attachServiceRuleset(c echo.Context, svcId string, rsetId string, slave bool) compOutcome {
	log := echolog.GetLogHandler(c, "PostServiceComplianceRuleset")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	log.Info("called", "svc_id", svcId, logkey.RSetID, rsetId, "slave", slave)

	svc, err := odb.ServiceBySvcIDOrName(ctx, svcId)
	if err != nil {
		log.Error("cannot resolve service", "svc_id", svcId, logkey.Error, err)
		return compFailf(http.StatusInternalServerError, "cannot resolve service %s", svcId)
	}
	if svc == nil {
		return compFailf(http.StatusNotFound, "service %s not found", svcId)
	}

	responsible, err := odb.ServiceResponsible(ctx, svc.SvcID, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		log.Error("cannot check if user is responsible for the service", "svc_id", svc.SvcID, logkey.Error, err)
		return compFailf(http.StatusInternalServerError, "cannot check if user is responsible for service %s", svc.SvcID)
	}
	if !responsible {
		log.Info("user is not responsible for this service", "svc_id", svc.SvcID)
		return compFailf(http.StatusForbidden, "user is not responsible for service %s", svc.SvcID)
	}

	_, err = odb.CompRulesetName(ctx, rsetId)
	if err != nil {
		log.Error("cannot find ruleset", logkey.RSetID, rsetId, logkey.Error, err)
		return compFailf(http.StatusNotFound, "ruleset %s not found", rsetId)
	}

	// check if the ruleset is already attached to the service
	attached, err := odb.CompRulesetSvcAttached(ctx, svc.SvcID, rsetId, slave)
	if err != nil {
		log.Error("cannot check if ruleset is attached", "svc_id", svc.SvcID, logkey.RSetID, rsetId, logkey.Error, err)
		return compFailf(http.StatusInternalServerError, "cannot check if ruleset %s is attached to service %s", rsetId, svc.SvcID)
	}
	if attached {
		log.Info("ruleset is already attached to this service", "svc_id", svc.SvcID, logkey.RSetID, rsetId)
		return compFailf(http.StatusConflict, "ruleset %s is already attached to this service", rsetId)
	}

	// check if the ruleset is attachable to the service
	attachable, err := odb.CompRulesetSvcAttachable(ctx, svc.SvcID, rsetId)
	if err != nil {
		log.Error("cannot check if ruleset is attachable", "svc_id", svc.SvcID, logkey.RSetID, rsetId, logkey.Error, err)
		return compFailf(http.StatusInternalServerError, "cannot check if ruleset %s is attachable to service %s", rsetId, svc.SvcID)
	}
	if !attachable {
		log.Info("ruleset is not attachable to this service", "svc_id", svc.SvcID, logkey.RSetID, rsetId)
		return compFailf(http.StatusForbidden, "ruleset %s is not attachable to this service", rsetId)
	}

	// attach ruleset to service
	_, err = odb.CompRulesetAttachService(ctx, svc.SvcID, rsetId, slave)
	if err != nil {
		log.Error("cannot attach ruleset to service", "svc_id", svc.SvcID, logkey.RSetID, rsetId, logkey.Error, err)
		return compFailf(http.StatusInternalServerError, "cannot attach ruleset %s to service %s", rsetId, svc.SvcID)
	}

	return compDone(fmt.Sprintf("ruleset %s attached to service %s", rsetId, svc.SvcID))
}
