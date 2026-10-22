package serverhandlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/opensvc/oc3/cdb"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteServiceComplianceModuleset handles DELETE /services/{svc_id}/compliance/modulesets/{mset_id}
func (a *Api) DeleteServiceComplianceModuleset(c echo.Context, svcId string, msetId string, params server.DeleteServiceComplianceModulesetParams) error {
	slave := false
	if params.Slave != nil {
		slave = *params.Slave
	}
	return writeCompOutcome(c, a.detachServiceModuleset(c, svcId, msetId, slave))
}

// detachServiceModuleset detaches a moduleset from a service, its encapsulated
// part when slave, with the checks of the single route: the responsibility of the
// caller, the moduleset, its current attachment. POST /compliance/attachments
// applies it to each pair of a batch.
func (a *Api) detachServiceModuleset(c echo.Context, svcId string, msetId string, slave bool) compOutcome {
	log := echolog.GetLogHandler(c, "DeleteServiceComplianceModuleset")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	log.Info("called", "svc_id", svcId, logkey.MSetID, msetId, "slave", slave)

	svc, err := odb.ServiceBySvcIDOrName(ctx, svcId, UserGroupsFromContext(c), IsManager(c))
	if errors.Is(err, cdb.ErrServiceAmbiguous) {
		return compFailf(http.StatusConflict, "%s", err)
	}
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

	_, err = odb.CompModulesetName(ctx, msetId)
	if err != nil {
		log.Error("cannot find moduleset", logkey.MSetID, msetId, logkey.Error, err)
		return compFailf(http.StatusNotFound, "moduleset %s not found", msetId)
	}

	// check if the moduleset is attached to the service
	attached, err := odb.CompModulesetSvcAttached(ctx, svc.SvcID, msetId, slave)
	if err != nil {
		log.Error("cannot check if moduleset is attached", "svc_id", svc.SvcID, logkey.MSetID, msetId, logkey.Error, err)
		return compFailf(http.StatusInternalServerError, "cannot check if moduleset %s is attached to service %s", msetId, svc.SvcID)
	}
	if !attached {
		log.Info("moduleset is not attached to this service", "svc_id", svc.SvcID, logkey.MSetID, msetId)
		return compFailf(http.StatusConflict, "moduleset %s is not attached to this service", msetId)
	}

	// detach moduleset from service
	_, err = odb.CompModulesetDetachService(ctx, svc.SvcID, msetId, slave)
	if err != nil {
		log.Error("cannot detach moduleset from service", "svc_id", svc.SvcID, logkey.MSetID, msetId, logkey.Error, err)
		return compFailf(http.StatusInternalServerError, "cannot detach moduleset %s from service %s", msetId, svc.SvcID)
	}

	return compDone(fmt.Sprintf("moduleset %s detached from service %s", msetId, svc.SvcID))
}
