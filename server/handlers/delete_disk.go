package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteDisk handles DELETE /disks/{disk_id}
func (a *Api) DeleteDisk(c echo.Context, diskId string) error {
	return a.deleteDiskByID(c, "DeleteDisk", diskId)
}

func (a *Api) deleteDiskByID(c echo.Context, handlerName, diskId string) error {
	log := echolog.GetLogHandler(c, handlerName)
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) && !IsAuthByNode(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "authentication required")
	}

	disks, err := odb.DiskinfoByDiskID(ctx, diskId)
	if err != nil {
		log.Error("cannot lookup disk", "disk_id", diskId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup disk")
	}
	if len(disks) == 0 {
		return c.JSON(http.StatusOK, map[string]any{"info": "Disk " + diskId + " does not exist"})
	}
	disk := disks[0]

	manager := IsManager(c)
	callerNodeID, _ := c.Get(XNodeID).(string)
	proxyOK := false
	var proxyNodeIDs []string
	if !manager && IsAuthByNode(c) && callerNodeID != "" && disk.ArrayID != "" {
		proxyNodeIDs, err = odb.ArrayProxyNodeIDs(ctx, disk.ArrayID)
		if err != nil {
			log.Error("cannot fetch array proxies", "disk_arrayid", disk.ArrayID, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot fetch array proxies")
		}
		for _, id := range proxyNodeIDs {
			if id == callerNodeID {
				proxyOK = true
				break
			}
		}
	}
	if !manager && !proxyOK {
		return JSONProblemf(c, http.StatusForbidden,
			"you are not allowed to use this handler. you are node %q, array %q allowed proxies are %v",
			callerNodeID, disk.ArrayID, proxyNodeIDs)
	}

	if _, err := odb.DeleteDiskinfoByDiskID(ctx, diskId); err != nil {
		log.Error("cannot delete disk", "disk_id", diskId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete disk")
	}

	a.auditLog(c, ctx, log, cdb.LogEntry{
		Action: "disk.delete",
		Fmt:    "disk %(disk_id)s of array %(disk_arrayid)s deleted",
		Dict:   map[string]any{"disk_id": diskId, "disk_arrayid": disk.ArrayID},
	})
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"info": "Disk " + diskId + " deleted"})
}
