package serverhandlers

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// batchTimeout bounds a batch request: longer than a single write, each object
// costing a few queries.
const batchTimeout = 2 * time.Minute

// runBatch applies one to each id in order, stopping when the caller is gone,
// and answers with the outcomes; done runs once after, when one at least
// succeeded, then the change event is sent.
func (a *Api) runBatch(c echo.Context, log *slog.Logger, ctx context.Context, ids []string, one func(id string) batchOutcome, done func()) error {
	outcomes := make([]server.BatchOutcome, 0, len(ids))
	succeeded := 0
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			log.Info("stopped", "applied", len(outcomes), "objects", len(ids), logkey.Error, err)
			break
		}
		o := one(id)
		if o.ok() {
			succeeded++
		}
		outcomes = append(outcomes, batchItem(id, o))
	}
	if succeeded > 0 {
		if done != nil {
			done()
		}
		a.notifyChanges(log, ctx)
	}
	log.Info("applied", "objects", len(ids), "succeeded", succeeded)
	return c.JSON(http.StatusOK, server.BatchResult{Data: outcomes})
}

// PostNodesDelete handles POST /nodes/delete.
func (a *Api) PostNodesDelete(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostNodesDelete")
	if err := checkNodeDeleter(c); err != nil {
		return err
	}
	var body server.IdsInput
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if err := checkBatchSize(len(body.Ids)); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), batchTimeout)
	defer cancel()
	return a.runBatch(c, log, ctx, body.Ids, func(id string) batchOutcome {
		return a.deleteNodeOne(c, log, ctx, id)
	}, nil)
}

// PostServicesDelete handles POST /services/delete.
func (a *Api) PostServicesDelete(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostServicesDelete")
	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	var body server.IdsInput
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if err := checkBatchSize(len(body.Ids)); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), batchTimeout)
	defer cancel()
	// Each service checked on its own, those accepted deleted by groups: a
	// transaction and a statement per table for each group, not for each service.
	outcomes := make([]batchOutcome, len(body.Ids))
	var accepted []serviceToDelete
	var positions []int
	for i, id := range body.Ids {
		target, o := a.checkServiceDeletion(c, log, ctx, id)
		if target == nil {
			outcomes[i] = o
			continue
		}
		accepted = append(accepted, *target)
		positions = append(positions, i)
	}
	for start := 0; start < len(accepted); start += servicesDeleteGroup {
		end := min(start+servicesDeleteGroup, len(accepted))
		for j, o := range a.deleteServicesChecked(c, log, ctx, accepted[start:end]) {
			outcomes[positions[start+j]] = o
		}
	}
	return a.runBatch(c, log, ctx, body.Ids, func(id string) batchOutcome {
		o := outcomes[0]
		outcomes = outcomes[1:]
		return o
	}, nil)
}

// servicesDeleteGroup is the number of services deleted in one transaction by
// POST /services/delete.
const servicesDeleteGroup = 100

// PostServicesInstancesDelete handles POST /services_instances/delete.
func (a *Api) PostServicesInstancesDelete(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostServicesInstancesDelete")
	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	var body server.InstancesInput
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if err := checkBatchSize(len(body.Instances)); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	ids := make([]string, len(body.Instances))
	pairs := make([][2]string, len(body.Instances))
	for i, inst := range body.Instances {
		ids[i] = inst.SvcId + "@" + inst.NodeId
		pairs[i] = [2]string{inst.SvcId, inst.NodeId}
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), batchTimeout)
	defer cancel()
	// Checked one by one, deleted by groups (deleteServiceInstances).
	outcomes := a.deleteServiceInstances(c, log, ctx, pairs)
	return a.runBatch(c, log, ctx, ids, func(string) batchOutcome {
		o := outcomes[0]
		outcomes = outcomes[1:]
		return o
	}, nil)
}

// PostServicesActionsAck handles POST /services_actions/ack: each action as its
// single route, the errors of each instance concerned counted once after.
func (a *Api) PostServicesActionsAck(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostServicesActionsAck")
	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	var body server.ActionsAckInput
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if err := checkBatchSize(len(body.Ids)); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	text := ""
	if body.AckedComment != nil {
		text = *body.AckedComment
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), batchTimeout)
	defer cancel()
	ids := make([]string, len(body.Ids))
	for i, id := range body.Ids {
		ids[i] = fmt.Sprint(id)
	}
	byKey := map[string]int64{}
	for i, id := range body.Ids {
		byKey[ids[i]] = id
	}
	instances := map[actionInstance]bool{}
	return a.runBatch(c, log, ctx, ids, func(id string) batchOutcome {
		o, instance := a.ackServiceActionOne(c, log, ctx, byKey[id], text)
		if instance != nil {
			instances[*instance] = true
		}
		return o
	}, func() {
		for instance := range instances {
			a.recountActionErrors(log, ctx, instance)
		}
	})
}

// PostChecksLiveBatch handles POST /checks/live/batch: each check as its single
// route, the checks of each node concerned refreshed once after.
func (a *Api) PostChecksLiveBatch(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostChecksLiveBatch")
	if err := requirePrivilege(c, "CheckExec"); err != nil {
		return err
	}
	var body server.ChecksBatchInput
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if !body.Action.Valid() {
		return JSONProblemf(c, http.StatusBadRequest, "unknown action %q", body.Action)
	}
	if err := checkBatchSize(len(body.Ids)); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if body.Action == server.SetThresholds && body.ChkLow == nil && body.ChkHigh == nil {
		return JSONProblemf(c, http.StatusBadRequest, "set_thresholds needs chk_low or chk_high")
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), batchTimeout)
	defer cancel()
	ids := make([]string, len(body.Ids))
	byKey := map[string]int64{}
	for i, id := range body.Ids {
		ids[i] = fmt.Sprint(id)
		byKey[ids[i]] = id
	}
	nodes := map[string]bool{}
	return a.runBatch(c, log, ctx, ids, func(id string) batchOutcome {
		check, o := a.checkLiveByID(log, ctx, byKey[id])
		if check == nil {
			return o
		}
		switch body.Action {
		case server.SetThresholds:
			o = a.setCheckThresholdsOne(c, log, ctx, check, body.ChkLow, body.ChkHigh)
		case server.ResetThresholds:
			o = a.resetCheckThresholdsOne(c, log, ctx, keyOfCheck(check))
		default:
			// A deleted check needs no refresh of its node.
			return a.deleteCheckOne(c, log, ctx, check)
		}
		if o.ok() {
			nodes[check.NodeID] = true
		}
		return o
	}, func() {
		for nodeID := range nodes {
			if err := a.ODB.ChecksRefreshNode(ctx, nodeID); err != nil {
				log.Error("cannot set the thresholds of the checks again", logkey.NodeID, nodeID, logkey.Error, err)
			}
		}
	})
}
