package serverhandlers

import (
	"context"
	"errors"
	"net/http"

	"github.com/opensvc/oc3/cdb"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetServiceAmIResponsible handles GET /services/{svc_id}/am_i_responsible
func (a *Api) GetServiceAmIResponsible(c echo.Context, svcId string) error {
	log := echolog.GetLogHandler(c, "GetServiceAmIResponsible")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)

	log.Info("called", "svc_id", svcId, "is_manager", isManager)

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

	responsible, err := odb.ServiceResponsible(ctx, svc.SvcID, groups, isManager)
	if err != nil {
		log.Error("cannot check service responsibility", "svc_id", svcId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check responsibility for service %s", svcId)
	}
	if !responsible {
		return JSONProblemf(c, http.StatusForbidden, "you are not responsible for service %s", svc.Svcname)
	}

	return c.JSON(http.StatusOK, map[string]any{"data": true})
}
