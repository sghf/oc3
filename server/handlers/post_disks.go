package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// PostDisks handles POST /disks
func (a *Api) PostDisks(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostDisks")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) && !IsAuthByNode(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "authentication required")
	}

	var body server.PostDisksJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if body.DiskId == "" {
		return JSONProblemf(c, http.StatusBadRequest, "the 'disk_id' key is mandatory")
	}
	if body.DiskArrayid == "" {
		return JSONProblemf(c, http.StatusBadRequest, "the 'disk_arrayid' key is mandatory")
	}

	proxyNodeIDs, err := odb.ArrayProxyNodeIDs(ctx, body.DiskArrayid)
	if err != nil {
		log.Error("cannot fetch array proxies", "disk_arrayid", body.DiskArrayid, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot fetch array proxies")
	}

	manager := IsManager(c)
	callerNodeID, _ := c.Get(XNodeID).(string)
	proxyOK := false
	if IsAuthByNode(c) && callerNodeID != "" {
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
			callerNodeID, body.DiskArrayid, proxyNodeIDs)
	}

	fields := map[string]any{
		"disk_id":      body.DiskId,
		"disk_arrayid": body.DiskArrayid,
	}
	if body.DiskDevid != nil {
		fields["disk_devid"] = *body.DiskDevid
	}
	if body.DiskRaid != nil {
		fields["disk_raid"] = *body.DiskRaid
	}
	if body.DiskGroup != nil {
		fields["disk_group"] = *body.DiskGroup
	}
	if body.DiskName != nil {
		fields["disk_name"] = *body.DiskName
	}
	if body.DiskController != nil {
		fields["disk_controller"] = *body.DiskController
	}
	if body.DiskSize != nil {
		fields["disk_size"] = *body.DiskSize
	}
	if body.DiskAlloc != nil {
		fields["disk_alloc"] = *body.DiskAlloc
	}
	if body.DiskLevel != nil {
		fields["disk_level"] = *body.DiskLevel
	}
	if body.DiskUpdated != nil {
		fields["disk_updated"] = *body.DiskUpdated
	}

	if err := odb.UpsertDiskinfo(ctx, fields); err != nil {
		log.Error("cannot upsert diskinfo", "disk_id", body.DiskId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot upsert disk")
	}

	// The disks an array proxy feeds are not logged, one entry per disk at each
	// run, as the historical collector does not; a user setting one is.
	if IsAuthByUser(c) {
		a.auditLog(c, ctx, log, cdb.LogEntry{
			Action: "disk.change",
			Fmt:    "disk %(disk_id)s of array %(disk_arrayid)s set: %(data)s",
			Dict:   map[string]any{"disk_id": body.DiskId, "disk_arrayid": body.DiskArrayid, "data": fields},
		})
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return fetchAndReturnDisk(c, odb, ctx, body.DiskId)
}

func fetchAndReturnDisk(c echo.Context, odb *cdb.DB, ctx context.Context, diskID string) error {
	mapping := propsMapping["disk"]
	props := defaultProps(mapping)
	selectExprs, err := buildSelectClause(props, mapping)
	if err != nil {
		return JSONProblemf(c, http.StatusInternalServerError, "cannot fetch disk")
	}
	rows, err := odb.GetDisk(ctx, diskID, cdb.ListParams{
		IsManager:   true,
		Props:       props,
		SelectExprs: selectExprs,
	})
	if err != nil || len(rows) == 0 {
		return JSONProblemf(c, http.StatusInternalServerError, "cannot fetch disk")
	}
	return c.JSON(http.StatusOK, rows[0])
}
