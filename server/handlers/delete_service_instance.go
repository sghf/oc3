package serverhandlers

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteServiceInstance handles DELETE /services/{svc_id}/instances/{node_id}
func (a *Api) DeleteServiceInstance(c echo.Context, svcId string, nodeId string) error {
	log := echolog.GetLogHandler(c, "DeleteServiceInstance")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}

	log.Info("called", "svc_id", svcId, logkey.NodeID, nodeId)

	return a.deleteServiceInstanceByKeys(c, log, ctx, svcId, nodeId)
}

// deleteServiceInstanceByKeys deletes the instance of a service on a node, and
// answers.
func (a *Api) deleteServiceInstanceByKeys(c echo.Context, log *slog.Logger, ctx context.Context, svcId, nodeId string) error {
	o := a.deleteServiceInstanceOne(c, log, ctx, svcId, nodeId)
	if o.ok() {
		if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
			log.Error("cannot notify changes", logkey.Error, err)
		}
	}
	return writeOutcome(c, o)
}

// deleteServiceInstanceOne deletes the instance of a service on a node, in a
// transaction of its own, after checking the caller is responsible for the
// service.
func (a *Api) deleteServiceInstanceOne(c echo.Context, log *slog.Logger, ctx context.Context, svcId, nodeId string) batchOutcome {
	outcomes := a.deleteServiceInstances(c, log, ctx, [][2]string{{svcId, nodeId}})
	return outcomes[0]
}

// instanceToDelete is an instance the caller may delete, and its names.
type instanceToDelete struct {
	svcID, nodeID     string
	svcname, nodename string
}

// deleteServiceInstances deletes instances, given as (service, node) pairs by
// id or name, and returns their outcomes in order. Each is checked on its own:
// its service and node exist, the caller is responsible for the service (read
// once per service), the instance exists (read once per node). Those accepted
// are deleted by groups of instancesDeleteGroup, a transaction each, an audit
// entry per instance and a statement per table and node for the group.
func (a *Api) deleteServiceInstances(c echo.Context, log *slog.Logger, ctx context.Context, pairs [][2]string) []batchOutcome {
	odb := a.ODB
	outcomes := make([]batchOutcome, len(pairs))
	targets := make([]*instanceToDelete, len(pairs))
	responsible := map[string]bool{}
	for i, pair := range pairs {
		svcId, nodeId := pair[0], pair[1]
		if svcId == "" {
			outcomes[i] = batchFail(http.StatusBadRequest, "invalid svc_id: ''")
			continue
		}
		svc, err := odb.ServiceBySvcIDOrName(ctx, svcId, UserGroupsFromContext(c), IsManager(c))
		if errors.Is(err, cdb.ErrServiceAmbiguous) {
			outcomes[i] = batchFail(http.StatusConflict, "%s", err)
		}
		if err != nil {
			log.Error("cannot resolve service", "svc_id", svcId, logkey.Error, err)
			outcomes[i] = batchFail(http.StatusInternalServerError, "cannot resolve service %s", svcId)
			continue
		}
		if svc == nil {
			outcomes[i] = batchFail(http.StatusNotFound, "service %s not found", svcId)
			continue
		}
		node, err := odb.NodeByNodeIDOrNodename(ctx, nodeId)
		if err != nil {
			log.Error("cannot lookup node", logkey.NodeID, nodeId, logkey.Error, err)
			outcomes[i] = batchFail(http.StatusInternalServerError, "cannot lookup node %s", nodeId)
			continue
		}
		if node == nil {
			outcomes[i] = batchFail(http.StatusNotFound, "node %s not found", nodeId)
			continue
		}
		ok, known := responsible[svc.SvcID]
		if !known {
			ok, err = odb.ServiceResponsible(ctx, svc.SvcID, UserGroupsFromContext(c), IsManager(c))
			if err != nil {
				log.Error("cannot check service responsibility", "svc_id", svc.SvcID, logkey.Error, err)
				outcomes[i] = batchFail(http.StatusInternalServerError, "cannot check service responsibility")
				continue
			}
			responsible[svc.SvcID] = ok
		}
		if !ok {
			outcomes[i] = batchFail(http.StatusForbidden, "you are not responsible for service %s", svc.Svcname)
			continue
		}
		targets[i] = &instanceToDelete{svcID: svc.SvcID, nodeID: node.NodeID, svcname: svc.Svcname, nodename: node.Nodename}
	}
	// The instances that exist, read node by node.
	byNode := map[string][]string{}
	for _, t := range targets {
		if t != nil {
			byNode[t.nodeID] = append(byNode[t.nodeID], t.svcID)
		}
	}
	existing := map[string]map[string]bool{}
	for nodeID, svcIDs := range byNode {
		found, err := odb.ExistingInstances(ctx, nodeID, svcIDs)
		if err != nil {
			log.Error("cannot read the instances", logkey.NodeID, nodeID, logkey.Error, err)
			found = nil
		}
		existing[nodeID] = found
	}
	var accepted []int
	seen := map[[2]string]bool{}
	for i, t := range targets {
		if t == nil {
			continue
		}
		switch found := existing[t.nodeID]; {
		case found == nil:
			outcomes[i] = batchFail(http.StatusInternalServerError, "cannot delete service instance")
		case !found[t.svcID] || seen[[2]string{t.svcID, t.nodeID}]:
			outcomes[i] = batchFail(http.StatusNotFound, "service instance %s on node %s does not exist", t.svcname, t.nodename)
		default:
			seen[[2]string{t.svcID, t.nodeID}] = true
			accepted = append(accepted, i)
		}
	}
	for start := 0; start < len(accepted); start += instancesDeleteGroup {
		group := accepted[start:min(start+instancesDeleteGroup, len(accepted))]
		list := make([]instanceToDelete, len(group))
		for j, i := range group {
			list[j] = *targets[i]
		}
		for j, o := range a.deleteInstancesChecked(c, log, ctx, list) {
			outcomes[group[j]] = o
		}
	}
	return outcomes
}

// instancesDeleteGroup is the number of instances deleted in one transaction.
const instancesDeleteGroup = 100

// deleteInstancesChecked deletes instances already checked, in one transaction:
// an audit entry each, then their rows (DeleteServiceInstancesCascade). It
// returns their outcomes, in order; a failure fails them all, rolled back.
func (a *Api) deleteInstancesChecked(c echo.Context, log *slog.Logger, ctx context.Context, list []instanceToDelete) []batchOutcome {
	outcomes := make([]batchOutcome, len(list))
	failAll := func(o batchOutcome) []batchOutcome {
		for i := range outcomes {
			outcomes[i] = o
		}
		return outcomes
	}
	tx, markSuccess, endTx, err := a.ODB.BeginTxWithControl(ctx, log, &sql.TxOptions{})
	if err != nil {
		log.Error("cannot start transaction", logkey.Error, err)
		return failAll(batchFail(http.StatusInternalServerError, "cannot delete service instance"))
	}
	defer endTx()
	userEmail, _ := c.Get(XUserEmail).(string)
	byNode := map[string][]string{}
	for _, t := range list {
		byNode[t.nodeID] = append(byNode[t.nodeID], t.svcID)
		if logErr := tx.Log(ctx, cdb.LogEntry{
			Action: "service_instance.delete",
			User:   userEmail,
			Fmt:    "delete service %(svcname)s instance on node %(nodename)s",
			Dict:   map[string]any{"svcname": t.svcname, "nodename": t.nodename},
			Level:  "info",
		}); logErr != nil {
			log.Error("cannot write audit log", logkey.Error, logErr)
			return failAll(batchFail(http.StatusInternalServerError, "cannot write audit log"))
		}
	}
	if err := tx.DeleteServiceInstancesCascade(ctx, byNode); err != nil {
		log.Error("cannot delete service instances", "count", len(list), logkey.Error, err)
		return failAll(batchFail(http.StatusInternalServerError, "cannot delete service instance"))
	}
	markSuccess()
	for i, t := range list {
		outcomes[i] = batchOK("delete service %s instance on node %s", t.svcname, t.nodename)
	}
	return outcomes
}
