package runner

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/cdb"
)

type (
	ActionDaemon struct {
		DB *sql.DB
		Ev eventPublisher

		Ctx context.Context

		ids            []int
		nIds           []int
		invalidIds     []int
		unreachableIds []int
		runningIds     []int

		doneEntries []cmdSetDone

		SubSystem string
	}

	Worker struct {
		dispatchC chan cdb.ActionQueueEntry
		cmdC      chan any
		ctx       context.Context
	}

	cmdSetUnreachable struct {
		id         int
		actionType string
	}

	cmdSetNotified struct {
		id         int
		actionType string
	}

	cmdSetInvalid struct {
		id         int
		actionType string
	}

	cmdSetRunning struct {
		id int
	}

	cmdSetDone struct {
		id         int
		ret        int
		stdout     string
		stderr     string
		actionType string
	}

	dedupLog struct {
		notified bool
	}

	eventPublisher interface {
		EventPublish(eventName string, data map[string]any) error
	}
)

const (
	DefaultNbWorkers           = 5
	DefaultNotificationTimeout = 5 * time.Second
	DefaultCommandTimeout      = 10 * time.Second
	DefaultPurgeTimeout        = 24 * time.Hour
)

var (
	queueQueued = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "oc3",
			Subsystem: "runner",
			Name:      "queue_queued_total",
			Help:      "Total number of actions queued",
		})
	actionInProgress = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "oc3",
			Subsystem: "runner",
			Name:      "action_in_progress_total",
			Help:      "Total number of actions in progress by type",
		}, []string{"action_type"})
	actionRunning = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "oc3",
			Subsystem: "runner",
			Name:      "actions_running",
			Help:      "Number of actions running by type",
		}, []string{"action_type"})
	actionProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "oc3",
			Subsystem: "runner",
			Name:      "action_processed_total",
			Help:      "Total number of actions processed by type and result",
		}, []string{"action_type", "result"})
	actionPullReturnCode = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "oc3",
			Subsystem: "runner",
			Name:      "action_pull_return_code_total",
			Help:      "Total number of pull action return codes",
		}, []string{"ret"})
	dbErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "oc3",
			Subsystem: "runner",
			Name:      "db_errors_total",
			Help:      "Total number of database errors by operation",
		}, []string{"op"})
	dbRequests = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "oc3",
			Subsystem: "runner",
			Name:      "db_requests_total",
			Help:      "Total number of database requests by operation",
		}, []string{"op"})
)

func (d *ActionDaemon) Run() error {
	nbWorkers := getOptionInt("runner.nb_workers", DefaultNbWorkers)
	purgeTimeout := getOptionDuration("runner.purge_timeout", DefaultPurgeTimeout)
	odb := cdb.New(d.DB)
	dispatchC := make(chan cdb.ActionQueueEntry)
	cmdC := make(chan any)
	for i := 0; i < nbWorkers; i++ {
		w := Worker{
			dispatchC: dispatchC,
			cmdC:      cmdC,
			ctx:       d.Ctx,
		}
		go w.Run()
	}

	// pollTicker for polling waiting actions
	pollTicker := time.NewTicker(time.Second)
	defer pollTicker.Stop()

	// updateTicker for batching status updates
	updateTicker := time.NewTicker(5 * time.Second)
	defer updateTicker.Stop()

	purgeTicker := time.NewTicker(purgeTimeout)

	var nowErrorLogger, setDequeuedToNowErrorLogger, getQueuedErrorLogger dedupLog
	pollWaitingActions := func() {
		// The three steps share @now, a variable of the database session: they
		// run in one transaction, on one connection. Issued on odb, shared
		// with the workers, each could take another connection of the pool,
		// where @now is unset or left by an earlier poll: the actions marked
		// would never be read, or those of an earlier poll read again.
		tx, _, endTx, err := odb.BeginTxWithControl(d.Ctx, slog.Default(), nil)
		if err != nil {
			nowErrorLogger.warnf("begin transaction: %s", err)
			dbErrors.WithLabelValues("set_now").Inc()
			return
		}
		lines, ok := func() ([]cdb.ActionQueueEntry, bool) {
			// Rolls back unless committed below.
			defer endTx()

			// define SQL @now as the time we start processing the queue
			err := tx.ActionQSetNow(d.Ctx)
			dbRequests.WithLabelValues("set_now").Inc()
			if err != nil {
				nowErrorLogger.warnf("set now: %s", err)
				dbErrors.WithLabelValues("set_now").Inc()
				return nil, false
			}
			nowErrorLogger.reset()

			// mark all waiting actions with type push or pull as dequeued at @now
			err = tx.ActionQPushPullSetDequeuedToNow(d.Ctx)
			dbRequests.WithLabelValues("set_dequeued_to_now").Inc()
			if err != nil {
				setDequeuedToNowErrorLogger.warnf("set dequeued to now: %s", err)
				dbErrors.WithLabelValues("set_dequeued_to_now").Inc()
				return nil, false
			}
			setDequeuedToNowErrorLogger.reset()

			// fetch all actions with type push or pull marked as dequeued at @now
			lines, err := tx.ActionQPushPullGetQueued(d.Ctx)
			dbRequests.WithLabelValues("get_queued").Inc()
			if err != nil {
				getQueuedErrorLogger.warnf("get queued: %s", err)
				dbErrors.WithLabelValues("get_queued").Inc()
				return nil, false
			}
			getQueuedErrorLogger.reset()

			// Dispatched only once the marks are committed: dispatched, then
			// rolled back, they would be dispatched again by the next poll.
			if err := tx.Commit(); err != nil {
				getQueuedErrorLogger.warnf("commit: %s", err)
				dbErrors.WithLabelValues("get_queued").Inc()
				return nil, false
			}
			return lines, true
		}()
		if !ok {
			return
		}
		queueQueued.Add(float64(len(lines)))

		// dispatch each action push or pull to a worker
		for _, line := range lines {
			switch line.ActionType {
			case cdb.ActionQTypePull, cdb.ActionQTypePush:
				dispatchC <- line
			}
		}
	}
	for {
		select {
		case <-pollTicker.C:
			pollWaitingActions()
		case cmd := <-cmdC:
			switch cmd.(type) {
			case cmdSetUnreachable:
				c := cmd.(cmdSetUnreachable)
				d.unreachableIds = append(d.unreachableIds, c.id)
				actionProcessed.WithLabelValues(c.actionType, "unreachable").Inc()
			case cmdSetNotified:
				c := cmd.(cmdSetNotified)
				d.nIds = append(d.nIds, c.id)
				actionProcessed.WithLabelValues(c.actionType, "notified").Inc()
			case cmdSetInvalid:
				c := cmd.(cmdSetInvalid)
				d.invalidIds = append(d.invalidIds, c.id)
				actionProcessed.WithLabelValues(c.actionType, "invalid").Inc()
			case cmdSetRunning:
				c := cmd.(cmdSetRunning)
				d.runningIds = append(d.runningIds, c.id)
			case cmdSetDone:
				c := cmd.(cmdSetDone)
				d.doneEntries = append(d.doneEntries, c)
				result := "success"
				if c.ret != 0 {
					result = "failure"
				}
				actionProcessed.WithLabelValues(c.actionType, result).Inc()
			}
		case <-updateTicker.C:
			if len(d.unreachableIds) > 0 {
				err := odb.ActionQSetUnreachable(d.Ctx, d.unreachableIds)
				dbRequests.WithLabelValues("set_unreachable").Inc()
				if err != nil {
					slog.Warn(fmt.Sprintf("set unreachable: %s", err))
					dbErrors.WithLabelValues("set_unreachable").Inc()
				} else {
					slog.Debug(fmt.Sprintf("set unreachable: %v", d.unreachableIds))
					d.unreachableIds = []int{}
				}
			}
			if len(d.invalidIds) > 0 {
				err := odb.ActionQSetInvalid(d.Ctx, d.invalidIds)
				dbRequests.WithLabelValues("set_invalid").Inc()
				if err != nil {
					slog.Warn(fmt.Sprintf("set invalid: %s", err))
					dbErrors.WithLabelValues("set_invalid").Inc()
				} else {
					slog.Debug(fmt.Sprintf("set invalid: %v", d.invalidIds))
					d.invalidIds = []int{}
				}
			}
			if len(d.nIds) > 0 {
				err := odb.ActionQSetNotified(d.Ctx, d.nIds)
				dbRequests.WithLabelValues("set_notified").Inc()
				if err != nil {
					slog.Warn(fmt.Sprintf("set notified: %s", err))
					dbErrors.WithLabelValues("set_notified").Inc()
				} else {
					slog.Debug(fmt.Sprintf("set notified: %v", d.nIds))
					d.nIds = []int{}
				}
			}
			if len(d.ids) > 0 {
				err := odb.ActionQSetQueued(d.Ctx, d.ids)
				dbRequests.WithLabelValues("set_queued").Inc()
				if err != nil {
					slog.Warn(fmt.Sprintf("set queued: %s", err))
					dbErrors.WithLabelValues("set_queued").Inc()
				} else {
					slog.Debug(fmt.Sprintf("set queued: %v", d.ids))
					d.ids = []int{}
				}
			}
			if len(d.runningIds) > 0 {
				err := odb.ActionQSetRunning(d.Ctx, d.runningIds)
				dbRequests.WithLabelValues("set_running").Inc()
				if err != nil {
					slog.Warn(fmt.Sprintf("set running: %s", err))
					dbErrors.WithLabelValues("set_running").Inc()
				} else {
					slog.Debug(fmt.Sprintf("set running: %v", d.runningIds))
					d.runningIds = []int{}
				}
			}
			if len(d.doneEntries) > 0 {
				for _, entry := range d.doneEntries {
					err := odb.ActionQSetDone(d.Ctx, entry.id, entry.ret, entry.stdout, entry.stderr)
					dbRequests.WithLabelValues("set_done").Inc()
					if err != nil {
						slog.Warn(fmt.Sprintf("set done: %s", err))
						dbErrors.WithLabelValues("set_done").Inc()
					} else {
						slog.Debug(fmt.Sprintf("set done: id %d ret %d stdout %d stderr %d", entry.id, entry.ret, len(entry.stdout), len(entry.stderr)))
					}
				}
				d.doneEntries = []cmdSetDone{}
			}
			if len(d.ids) > 0 || len(d.nIds) > 0 || len(d.invalidIds) > 0 || len(d.unreachableIds) > 0 || len(d.runningIds) > 0 || len(d.doneEntries) > 0 {
				data, err := odb.ActionQEventData(d.Ctx)
				dbRequests.WithLabelValues("action_queue_event_data").Inc()
				if err != nil {
					slog.Warn(fmt.Sprintf("get action queue event data: %s", err))
					dbErrors.WithLabelValues("action_queue_event_data").Inc()
				}
				if err := odb.Session.NotifyTableChangeWithData(d.Ctx, "action_queue", data); err != nil {
					slog.Warn(fmt.Sprintf("notify changes: %s", err))
				}
			}
		case <-purgeTicker.C:
			if err := odb.ActionQPurge(d.Ctx); err != nil {
				slog.Warn(fmt.Sprintf("purge action queue: %s", err))
				dbErrors.WithLabelValues("purge").Inc()
			} else {
				slog.Debug("purge action queue: done")
			}
			dbRequests.WithLabelValues("purge").Inc()
		case <-d.Ctx.Done():
			return nil
		}
	}
}

func (w *Worker) Run() {
	for {
		select {
		case e := <-w.dispatchC:
			if err := w.work(e); err != nil {
				slog.Warn(err.Error())
			}
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *Worker) work(e cdb.ActionQueueEntry) error {
	if err := w.validateCommand(e.Command); err != nil {
		w.cmdC <- cmdSetInvalid{
			id:         e.ID,
			actionType: e.ActionType,
		}
		return fmt.Errorf("invalid command: %s", err)
	}
	switch e.ActionType {
	case cdb.ActionQTypePull:
		w.workPull(e)
	case cdb.ActionQTypePush:
		w.workPush(e)
	case cdb.ActionQTypeFeed:
		return fmt.Errorf("unexpected action type: %s", e.ActionType)
	default:
		return fmt.Errorf("unknown action type: %s", e.ActionType)
	}
	return nil
}

func notifyNode(nodename string, port int) error {
	addr := net.JoinHostPort(nodename, strconv.Itoa(port))
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		return err
	}

	_, err = conn.Write([]byte("dequeue_actions"))
	return err
}

func (w *Worker) workPull(e cdb.ActionQueueEntry) {

	notifTimeout := getOptionDuration("runner.notification_timeout", DefaultNotificationTimeout)

	actionInProgress.WithLabelValues(cdb.ActionQTypePull).Inc()
	actionRunning.WithLabelValues(cdb.ActionQTypePull).Inc()
	defer actionRunning.WithLabelValues(cdb.ActionQTypePull).Dec()

	ctx, cancel := context.WithTimeout(w.ctx, notifTimeout)
	defer cancel()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.cmdC <- cmdSetUnreachable{
				id:         e.ID,
				actionType: e.ActionType,
			}
			return
		case <-ticker.C:
			if e.ConnectTo == nil {
				continue
			}
			if err := notifyNode(*e.ConnectTo, e.ListenerPort); err != nil {
				continue
			}
			w.cmdC <- cmdSetNotified{
				id:         e.ID,
				actionType: e.ActionType,
			}
			return
		}
	}
}

func (w *Worker) workPush(e cdb.ActionQueueEntry) {
	actionInProgress.WithLabelValues(cdb.ActionQTypePush).Inc()
	actionRunning.WithLabelValues(cdb.ActionQTypePush).Inc()
	defer actionRunning.WithLabelValues(cdb.ActionQTypePush).Dec()
	w.cmdC <- cmdSetRunning{
		id: e.ID,
	}

	stdout, stderr, returnCode := executeCommand(w.ctx, e.Command)

	slog.Debug("command executed",
		"action_id", e.ID,
		"return_code", returnCode,
		"stdout_len", len(stdout),
		"stderr_len", len(stderr),
	)

	w.cmdC <- cmdSetDone{
		id:         e.ID,
		ret:        returnCode,
		stdout:     strings.TrimSpace(stdout),
		stderr:     strings.TrimSpace(stderr),
		actionType: e.ActionType,
	}

	actionPullReturnCode.WithLabelValues(fmt.Sprintf("%d", returnCode)).Inc()
}

func (w *Worker) validateCommand(cmd string) error {
	invalid := []string{
		"opensvc@localhost",
		"opensvc@localhost.localdomain",
	}

	for _, pattern := range invalid {
		if strings.Contains(cmd, pattern) {
			return fmt.Errorf("invalid command pattern: %s", pattern)
		}
	}

	if strings.TrimSpace(cmd) == "" {
		return fmt.Errorf("empty command")
	}

	return nil
}

func executeCommand(ctx context.Context, cmd string) (string, string, int) {
	cmdTimeout := getOptionDuration("runner.command_timeout", DefaultCommandTimeout)

	ctx, cancel := context.WithTimeout(ctx, cmdTimeout)
	defer cancel()

	args := strings.Fields(cmd)

	command := exec.CommandContext(ctx, args[0], args[1:]...)

	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr

	err := command.Run()

	switch {
	case err == nil:
		return stdout.String(), stderr.String(), 0
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return stdout.String(), "command timeout", 1
	default:
		if exitErr, ok := err.(*exec.ExitError); ok {
			return stdout.String(), stderr.String(), exitErr.ExitCode()
		}
		return stdout.String(), err.Error(), 1
	}
}

func (d *dedupLog) warnf(format string, args ...any) {
	if !d.notified {
		slog.Warn(fmt.Sprintf(format, args...))
		d.notified = true
	}
}

func (d *dedupLog) reset() {
	d.notified = false
}

func getOptionInt(name string, defaultValue int) int {
	value := viper.GetInt(name)
	if value == 0 {
		return defaultValue
	}
	return value
}

func getOptionDuration(name string, defaultValue time.Duration) time.Duration {
	value := viper.GetDuration(name)
	if value == 0 {
		return defaultValue
	}
	return value
}
