package serverhandlers

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteNode handles DELETE /nodes/{node_id}
func (a *Api) DeleteNode(c echo.Context, nodeId string) error {
	return a.deleteNodeByID(c, "DeleteNode", nodeId)
}

func (a *Api) deleteNodeByID(c echo.Context, handlerName, nodeId string) error {
	log := echolog.GetLogHandler(c, handlerName)
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	if err := checkNodeDeleter(c); err != nil {
		return err
	}
	log.Info("called", logkey.NodeID, nodeId)
	o := a.deleteNodeOne(c, log, ctx, nodeId)
	if o.ok() {
		if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
			log.Error("cannot notify changes", logkey.Error, err)
		}
	}
	return writeOutcome(c, o)
}

// checkNodeDeleter answers 401 or 403 unless the caller may delete nodes: a
// user with NodeManager, as the historical collector, which a Manager holds
// implicitly.
func checkNodeDeleter(c echo.Context) error {
	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	if !IsNodeManager(c) {
		return JSONProblemf(c, http.StatusForbidden, "NodeManager privilege required")
	}
	return nil
}

// deleteNodeOne deletes a node and what refers to it, in a transaction of its
// own, after checking the caller is responsible for it.
func (a *Api) deleteNodeOne(c echo.Context, log *slog.Logger, ctx context.Context, nodeId string) batchOutcome {
	odb := a.ODB
	node, err := odb.NodeByNodeIDOrNodename(ctx, nodeId)
	if err != nil {
		log.Error("cannot lookup node", logkey.NodeID, nodeId, logkey.Error, err)
		return batchFail(http.StatusInternalServerError, "cannot lookup node %s", nodeId)
	}
	if node == nil {
		return batchFail(http.StatusNotFound, "node %s not found", nodeId)
	}
	responsible, err := odb.NodeResponsible(ctx, node.NodeID, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		log.Error("cannot check node responsibility", logkey.NodeID, node.NodeID, logkey.Error, err)
		return batchFail(http.StatusInternalServerError, "cannot check node responsibility")
	}
	if !responsible {
		return batchFail(http.StatusForbidden, "you are not responsible for node %s", node.Nodename)
	}
	tx, markSuccess, endTx, err := odb.BeginTxWithControl(ctx, log, &sql.TxOptions{})
	if err != nil {
		log.Error("cannot start transaction", logkey.Error, err)
		return batchFail(http.StatusInternalServerError, "cannot delete node")
	}
	defer endTx()
	if err := tx.DeleteNodeCascade(ctx, node.NodeID); err != nil {
		log.Error("cannot delete node", logkey.NodeID, node.NodeID, logkey.Error, err)
		return batchFail(http.StatusInternalServerError, "cannot delete node %s", node.Nodename)
	}
	userEmail, _ := c.Get(XUserEmail).(string)
	logEntry := cdb.LogEntry{
		Action: "node.delete",
		User:   userEmail,
		Fmt:    "delete node %(data)s",
		Dict:   map[string]any{"data": node.Nodename},
		Level:  "info",
	}
	if parsed, err := uuid.Parse(node.NodeID); err == nil {
		logEntry.NodeID = &parsed
	}
	if logErr := tx.Log(ctx, logEntry); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
		return batchFail(http.StatusInternalServerError, "cannot write audit log")
	}
	markSuccess()
	return batchOK("delete node %s", node.Nodename)
}
