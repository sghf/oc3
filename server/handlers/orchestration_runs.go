package serverhandlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

const (
	// orchestrationTick is how often the engine looks at the running runs,
	// besides the wake-ups of the requests that start or resume one.
	orchestrationTick = 3 * time.Second

	// orchestrationLock is the MariaDB named lock the engine holds while it
	// advances the runs: with several api servers, one of them does.
	orchestrationLock = "oc3.orchestrations"
)

// orchestrationWake asks the engine for a tick now, a run having started.
var orchestrationWake = make(chan struct{}, 1)

func (a *Api) wakeOrchestrationEngine() {
	select {
	case orchestrationWake <- struct{}{}:
	default:
	}
}

// targetFailed tells the statuses of a target that fail its step.
func targetFailed(status string) bool {
	switch status {
	case "failed", "refused", "timed_out", "cancelled":
		return true
	}
	return false
}

// failedWords say in the message of a failed run what happened to the target.
var failedWords = map[string]string{
	"failed":    "failed",
	"refused":   "was refused",
	"timed_out": "timed out",
	"cancelled": "was cancelled",
}

func targetInFlight(status string) bool {
	return status == "queued" || status == "running"
}

func formatTime(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := t.Format(time.RFC3339)
	return &s
}

// toOrchestrationRun builds the api view of a run, with the targets of its
// steps when targets is not nil.
func toOrchestrationRun(r cdb.OrchestrationRun, steps []cdb.OrchestrationRunStep, counts map[int]map[string]int, targets []cdb.OrchestrationRunTarget) server.OrchestrationRun {
	plan := []server.OrchestrationStep{}
	_ = json.Unmarshal([]byte(r.PlanJSON), &plan)
	run := server.OrchestrationRun{
		Id: r.ID, OrchestrationId: r.OrchestrationID, Name: r.Name, Plan: plan, Status: server.OrchestrationRunStatus(r.Status),
		CurrentStep: r.CurrentStep, Started: r.Started.Format(time.RFC3339), Ended: formatTime(r.Ended),
		StartedBy: r.StartedBy, Message: r.Message, Steps: []server.OrchestrationRunStep{},
	}
	byStep := map[int][]server.OrchestrationRunTarget{}
	for _, t := range targets {
		v := server.OrchestrationRunTarget{
			Id: t.ID, Step: t.Step, Position: t.Position, NodeId: t.NodeID, SvcId: t.SvcID, ClusterId: t.ClusterID,
			Label: t.Label, Status: server.OrchestrationRunTargetStatus(t.Status), Ret: t.Ret, Message: t.Message,
			Command: t.Command, Stdout: t.Stdout, Stderr: t.Stderr,
			Queued: formatTime(t.Queued), Started: formatTime(t.Started), Ended: formatTime(t.Ended),
		}
		if t.ActionID != nil {
			id := int(*t.ActionID)
			v.ActionId = &id
		}
		byStep[t.Step] = append(byStep[t.Step], v)
	}
	for _, s := range steps {
		step := server.OrchestrationRunStep{
			Step: s.Step, Status: server.OrchestrationRunStepStatus(s.Status),
			Started: formatTime(s.Started), Ended: formatTime(s.Ended), Counts: map[string]int{},
		}
		for status, n := range counts[s.Step] {
			step.Counts[status] = n
		}
		if targets != nil {
			list := byStep[s.Step]
			if list == nil {
				list = []server.OrchestrationRunTarget{}
			}
			step.Targets = &list
		}
		run.Steps = append(run.Steps, step)
	}
	return run
}

func (a *Api) writeOrchestrationRun(c echo.Context, log *slog.Logger, id int) error {
	ctx := c.Request().Context()
	odb := a.ODB
	run, err := odb.OrchestrationRunByID(ctx, id)
	if err != nil {
		log.Error("cannot read the run", "run_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read run %d", id)
	}
	if run == nil {
		return JSONProblemf(c, http.StatusNotFound, "run %d not found", id)
	}
	steps, err := odb.OrchestrationRunSteps(ctx, []int{id})
	if err != nil {
		log.Error("cannot read the steps of the run", "run_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read run %d", id)
	}
	targets, err := odb.OrchestrationRunTargets(ctx, id, -1)
	if err != nil {
		log.Error("cannot read the targets of the run", "run_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read run %d", id)
	}
	if targets == nil {
		targets = []cdb.OrchestrationRunTarget{}
	}
	counts := map[int]map[string]int{}
	for _, t := range targets {
		if counts[t.Step] == nil {
			counts[t.Step] = map[string]int{}
		}
		counts[t.Step][t.Status]++
	}
	return c.JSON(http.StatusOK, toOrchestrationRun(*run, steps, counts, targets))
}

// GetOrchestrationRuns handles GET /orchestration_runs.
func (a *Api) GetOrchestrationRuns(c echo.Context, params server.GetOrchestrationRunsParams) error {
	log := echolog.GetLogHandler(c, "GetOrchestrationRuns")
	if err := requireManager(c); err != nil {
		return err
	}
	ctx := c.Request().Context()
	limit := 50
	if params.Limit != nil {
		limit = *params.Limit
	}
	runs, err := a.ODB.OrchestrationRuns(ctx, params.OrchestrationId, limit)
	if err != nil {
		log.Error("cannot list the runs", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot list the runs")
	}
	ids := make([]int, len(runs))
	for i, r := range runs {
		ids[i] = r.ID
	}
	steps, err := a.ODB.OrchestrationRunSteps(ctx, ids)
	if err != nil {
		log.Error("cannot read the steps of the runs", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot list the runs")
	}
	counts, err := a.ODB.OrchestrationRunTargetCounts(ctx, ids)
	if err != nil {
		log.Error("cannot count the targets of the runs", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot list the runs")
	}
	stepsOf := map[int][]cdb.OrchestrationRunStep{}
	for _, s := range steps {
		stepsOf[s.RunID] = append(stepsOf[s.RunID], s)
	}
	data := []server.OrchestrationRun{}
	for _, r := range runs {
		data = append(data, toOrchestrationRun(r, stepsOf[r.ID], counts[r.ID], nil))
	}
	return c.JSON(http.StatusOK, server.OrchestrationRunListResponse{Data: data})
}

// GetOrchestrationRun handles GET /orchestration_runs/{run_id}.
func (a *Api) GetOrchestrationRun(c echo.Context, id server.InPathOrchestrationRunId) error {
	log := echolog.GetLogHandler(c, "GetOrchestrationRun")
	if err := requireManager(c); err != nil {
		return err
	}
	return a.writeOrchestrationRun(c, log, id)
}

// PostOrchestrationRunStop handles POST /orchestration_runs/{run_id}/stop.
func (a *Api) PostOrchestrationRunStop(c echo.Context, id server.InPathOrchestrationRunId) error {
	log := echolog.GetLogHandler(c, "PostOrchestrationRunStop")
	if err := requireManager(c); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	odb := a.ODB
	run, err := odb.OrchestrationRunByID(ctx, id)
	if err != nil {
		log.Error("cannot read the run", "run_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read run %d", id)
	}
	if run == nil {
		return JSONProblemf(c, http.StatusNotFound, "run %d not found", id)
	}
	if run.Status != "running" {
		return JSONProblemf(c, http.StatusConflict, "run %d is %s, not running", id, run.Status)
	}
	author, _ := c.Get(XUserEmail).(string)
	if err := a.stopOrchestrationRun(ctx, run, "stopped by "+author); err != nil {
		log.Error("cannot stop the run", "run_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot stop run %d", id)
	}
	if err := odb.Log(ctx, cdb.LogEntry{
		Action: "orchestration.stop", User: author, Fmt: "stopped run %(run_id)s of orchestration %(name)s",
		Dict: map[string]any{"name": run.Name, "run_id": id}, Level: "info",
	}); err != nil {
		log.Error("cannot write audit log", logkey.Error, err)
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Debug("cannot notify changes", logkey.Error, err)
	}
	return a.writeOrchestrationRun(c, log, id)
}

// stopOrchestrationRun stops a run: the actions of its step still waiting in the
// queue cancelled, its targets not started and the next steps not run. The
// actions an agent took go on; the engine records their results.
func (a *Api) stopOrchestrationRun(ctx context.Context, run *cdb.OrchestrationRun, message string) error {
	odb := a.ODB
	targets, err := odb.OrchestrationRunTargets(ctx, run.ID, run.CurrentStep)
	if err != nil {
		return err
	}
	for _, t := range targets {
		if t.Status != "queued" || t.ActionID == nil {
			continue
		}
		cancelled, err := odb.CancelWaitingAction(ctx, *t.ActionID)
		if err != nil {
			return err
		}
		if cancelled {
			if err := odb.SetOrchestrationTargetDone(ctx, t.ID, "cancelled", nil, nil, nil, "the run was stopped"); err != nil {
				return err
			}
		}
	}
	if err := odb.SetOrchestrationTargetsStatus(ctx, run.ID, -1, run.CurrentStep, []string{"waiting"}, "not_run", "the run was stopped"); err != nil {
		return err
	}
	if err := odb.SetOrchestrationRunStep(ctx, run.ID, run.CurrentStep, "stopped"); err != nil {
		return err
	}
	if err := odb.SetOrchestrationRunStepsFrom(ctx, run.ID, run.CurrentStep+1, "not_run"); err != nil {
		return err
	}
	return odb.SetOrchestrationRun(ctx, run.ID, "stopped", run.CurrentStep, message)
}

// PostOrchestrationRunResume handles POST /orchestration_runs/{run_id}/resume.
func (a *Api) PostOrchestrationRunResume(c echo.Context, id server.InPathOrchestrationRunId) error {
	log := echolog.GetLogHandler(c, "PostOrchestrationRunResume")
	if err := requireManager(c); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	odb := a.ODB
	run, err := odb.OrchestrationRunByID(ctx, id)
	if err != nil {
		log.Error("cannot read the run", "run_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read run %d", id)
	}
	if run == nil {
		return JSONProblemf(c, http.StatusNotFound, "run %d not found", id)
	}
	if run.Status != "failed" && run.Status != "stopped" {
		return JSONProblemf(c, http.StatusConflict, "run %d is %s: only a failed or stopped run resumes", id, run.Status)
	}
	targets, err := odb.OrchestrationRunTargets(ctx, id, -1)
	if err != nil {
		log.Error("cannot read the targets of the run", "run_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read run %d", id)
	}
	for _, t := range targets {
		if targetInFlight(t.Status) {
			return JSONProblemf(c, http.StatusConflict, "an action of run %d still runs on %s: wait for its end", id, t.Label)
		}
	}
	if err := a.inTx(ctx, log, func(tx *cdb.DB) error {
		if err := tx.ResetOrchestrationTargets(ctx, id, run.CurrentStep); err != nil {
			return err
		}
		if err := tx.SetOrchestrationRunStepsFrom(ctx, id, run.CurrentStep, "waiting"); err != nil {
			return err
		}
		if err := tx.SetOrchestrationRunStep(ctx, id, run.CurrentStep, "running"); err != nil {
			return err
		}
		return tx.SetOrchestrationRun(ctx, id, "running", run.CurrentStep, "")
	}); err != nil {
		log.Error("cannot resume the run", "run_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resume run %d", id)
	}
	author, _ := c.Get(XUserEmail).(string)
	if err := odb.Log(ctx, cdb.LogEntry{
		Action: "orchestration.resume", User: author, Fmt: "resumed run %(run_id)s of orchestration %(name)s from step %(step)s",
		Dict: map[string]any{"name": run.Name, "run_id": id, "step": run.CurrentStep + 1}, Level: "info",
	}); err != nil {
		log.Error("cannot write audit log", logkey.Error, err)
	}
	a.wakeOrchestrationEngine()
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Debug("cannot notify changes", logkey.Error, err)
	}
	return a.writeOrchestrationRun(c, log, id)
}

// RunOrchestrationEngine advances the orchestration runs until ctx is done:
// every tick, or when a request wakes it, the one api server holding the named
// lock records the results of the queued actions, fails the targets out of
// time, stops a run at a failed step, and queues the actions of the next
// targets, step after step.
func (a *Api) RunOrchestrationEngine(ctx context.Context) {
	log := slog.With("component", "orchestration-engine")
	ticker := time.NewTicker(orchestrationTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-orchestrationWake:
		}
		if err := a.orchestrationTick(ctx, log); err != nil {
			log.Error("tick failed", logkey.Error, err)
		}
	}
}

func (a *Api) orchestrationTick(ctx context.Context, log *slog.Logger) error {
	conn, err := a.DB.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", orchestrationLock).Scan(&got); err != nil {
		return err
	}
	if !got.Valid || got.Int64 != 1 {
		// Another server advances the runs.
		return nil
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), "SELECT RELEASE_LOCK(?)", orchestrationLock)
	}()

	ids, err := a.ODB.OrchestrationRunIDsToAdvance(ctx)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := a.advanceOrchestrationRun(ctx, log, id); err != nil {
			log.Error("cannot advance the run", "run_id", id, logkey.Error, err)
		}
	}
	if len(ids) > 0 {
		if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
			log.Debug("cannot notify changes", logkey.Error, err)
		}
	}
	return nil
}

// advanceOrchestrationRun does what a tick does for one run.
func (a *Api) advanceOrchestrationRun(ctx context.Context, log *slog.Logger, id int) error {
	odb := a.ODB
	run, err := odb.OrchestrationRunByID(ctx, id)
	if err != nil || run == nil {
		return err
	}
	plan := []server.OrchestrationStep{}
	if err := json.Unmarshal([]byte(run.PlanJSON), &plan); err != nil {
		return fmt.Errorf("plan: %w", err)
	}
	if err := a.syncOrchestrationTargets(ctx, run.ID, plan); err != nil {
		return err
	}
	if run.Status != "running" {
		// Ended: only the results of the actions still running were awaited.
		return nil
	}
	// A step that succeeds lets the next one start in the same tick.
	for run.CurrentStep < len(plan) {
		advanced, err := a.advanceOrchestrationStep(ctx, log, run, plan)
		if err != nil || !advanced {
			return err
		}
	}
	return nil
}

// syncOrchestrationTargets records the state of the actions queued for the
// targets of a run, and fails those out of time.
func (a *Api) syncOrchestrationTargets(ctx context.Context, runID int, plan []server.OrchestrationStep) error {
	odb := a.ODB
	targets, err := odb.OrchestrationRunTargets(ctx, runID, -1)
	if err != nil {
		return err
	}
	var ids []int64
	for _, t := range targets {
		if targetInFlight(t.Status) && t.ActionID != nil {
			ids = append(ids, *t.ActionID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	states, err := odb.ActionQueueStates(ctx, ids)
	if err != nil {
		return err
	}
	now := time.Now()
	for _, t := range targets {
		if !targetInFlight(t.Status) || t.ActionID == nil {
			continue
		}
		state, ok := states[*t.ActionID]
		if !ok {
			if err := odb.SetOrchestrationTargetDone(ctx, t.ID, "cancelled", nil, nil, nil, "the action was deleted from the queue"); err != nil {
				return err
			}
			continue
		}
		switch state.Status {
		case "T":
			status := "ok"
			if state.Ret == nil || *state.Ret != 0 {
				status = "failed"
			}
			if err := odb.SetOrchestrationTargetDone(ctx, t.ID, status, state.Ret, state.Stdout, state.Stderr, ""); err != nil {
				return err
			}
			continue
		case "C":
			if err := odb.SetOrchestrationTargetDone(ctx, t.ID, "cancelled", nil, nil, nil, "the action was cancelled in the queue"); err != nil {
				return err
			}
			continue
		case "R", "S":
			if t.Status == "queued" {
				if err := odb.SetOrchestrationTargetRunning(ctx, t.ID); err != nil {
					return err
				}
			}
		}
		// Out of time: failed, its action cancelled if no agent took it yet. An
		// action an agent runs cannot be interrupted, and is left to end.
		if t.Step < len(plan) && plan[t.Step].Timeout != nil && *plan[t.Step].Timeout > 0 && t.Queued != nil &&
			now.Sub(*t.Queued) > time.Duration(*plan[t.Step].Timeout)*time.Second {
			message := fmt.Sprintf("not done after %s", time.Duration(*plan[t.Step].Timeout)*time.Second)
			if state.Status == "W" {
				if _, err := odb.CancelWaitingAction(ctx, *t.ActionID); err != nil {
					return err
				}
				message += ": cancelled in the queue"
			}
			if err := odb.SetOrchestrationTargetDone(ctx, t.ID, "timed_out", nil, nil, nil, message); err != nil {
				return err
			}
		}
	}
	return nil
}

// advanceOrchestrationStep advances the current step of a running run: fails the
// run when a target failed and nothing runs any longer, ends the step when all
// its targets succeeded, or queues the next of its targets its pace allows. It
// tells whether the run went on to the next step.
func (a *Api) advanceOrchestrationStep(ctx context.Context, log *slog.Logger, run *cdb.OrchestrationRun, plan []server.OrchestrationStep) (bool, error) {
	odb := a.ODB
	cur := run.CurrentStep
	step := plan[cur]
	targets, err := odb.OrchestrationRunTargets(ctx, run.ID, cur)
	if err != nil {
		return false, err
	}
	var (
		failed   *cdb.OrchestrationRunTarget
		inFlight int
		waiting  []cdb.OrchestrationRunTarget
	)
	for i, t := range targets {
		switch {
		case targetFailed(t.Status):
			if failed == nil {
				failed = &targets[i]
			}
		case targetInFlight(t.Status):
			inFlight++
		case t.Status == "waiting":
			waiting = append(waiting, t)
		}
	}

	if failed != nil {
		// The step failed: what did not start does not, and the run ends once
		// what runs is done.
		if len(waiting) > 0 {
			if err := odb.SetOrchestrationTargetsStatus(ctx, run.ID, cur, 0, []string{"waiting"}, "not_run", "the step failed"); err != nil {
				return false, err
			}
		}
		if inFlight > 0 {
			return false, nil
		}
		reason := failed.Message
		if reason == "" && failed.Ret != nil {
			reason = fmt.Sprintf("returned %d", *failed.Ret)
		}
		message := fmt.Sprintf("step %d: %s %s", cur+1, failed.Label, failedWords[failed.Status])
		if reason != "" {
			message += ": " + reason
		}
		if err := odb.SetOrchestrationRunStep(ctx, run.ID, cur, "failed"); err != nil {
			return false, err
		}
		if err := odb.SetOrchestrationRunStepsFrom(ctx, run.ID, cur+1, "not_run"); err != nil {
			return false, err
		}
		if err := odb.SetOrchestrationTargetsStatus(ctx, run.ID, -1, cur+1, []string{"waiting"}, "not_run", "an earlier step failed"); err != nil {
			return false, err
		}
		log.Info("orchestration run failed", "run_id", run.ID, "step", cur, "target", failed.Label)
		run.Status = "failed"
		return false, odb.SetOrchestrationRun(ctx, run.ID, "failed", cur, message)
	}

	if len(waiting) == 0 && inFlight == 0 {
		// Every target succeeded: the next step starts, or the run ends.
		if err := odb.SetOrchestrationRunStep(ctx, run.ID, cur, "succeeded"); err != nil {
			return false, err
		}
		if cur+1 >= len(plan) {
			log.Info("orchestration run succeeded", "run_id", run.ID)
			run.Status = "succeeded"
			run.CurrentStep = len(plan)
			return false, odb.SetOrchestrationRun(ctx, run.ID, "succeeded", cur, "")
		}
		if err := odb.SetOrchestrationRunStep(ctx, run.ID, cur+1, "running"); err != nil {
			return false, err
		}
		if err := odb.SetOrchestrationRun(ctx, run.ID, "running", cur+1, ""); err != nil {
			return false, err
		}
		run.CurrentStep = cur + 1
		return true, nil
	}

	capacity := len(targets)
	switch step.Pace {
	case server.One:
		capacity = 1
	case server.Batch:
		if step.BatchSize != nil && *step.BatchSize > 0 {
			capacity = *step.BatchSize
		}
	}
	// The run acts for the Manager who started it: only a Manager may.
	act := actionActor{userID: run.UserID, email: run.StartedBy, groups: []string{"Manager"}}
	for _, t := range waiting {
		if inFlight >= capacity {
			break
		}
		queued, err := a.queueOrchestrationTarget(act, log, ctx, step, orchestrationTarget{
			nodeID: t.NodeID, svcID: t.SvcID, clusterID: t.ClusterID, label: t.Label,
		})
		if err != nil {
			// Refused: the step fails, at the next tick, once what runs is done.
			return false, odb.SetOrchestrationTargetDone(ctx, t.ID, "refused", nil, nil, nil, refusalMessage(err))
		}
		if err := odb.SetOrchestrationTargetQueued(ctx, t.ID, queued.ID, queued.Command); err != nil {
			return false, err
		}
		inFlight++
	}
	return false, nil
}
