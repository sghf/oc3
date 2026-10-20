package serverhandlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// actionEntry is one action to enqueue, with the fields of the historical
// collector's PUT /actions (rest_put_action_queue). The keys it does not support
// are kept to be refused rather than silently dropped.
type actionEntry struct {
	NodeID    string `json:"node_id"`
	SvcID     string `json:"svc_id"`
	ClusterID string `json:"cluster_id"`
	Action    string `json:"action"`
	Rid       string `json:"rid"`
	VMName    string `json:"vmname"`

	unsupported []string
}

// unsupportedActionKeys are accepted by the historical collector but not ported:
// compliance runs and agent options.
var unsupportedActionKeys = []string{"module", "moduleset", "ruleset", "options"}

func (e *actionEntry) UnmarshalJSON(data []byte) error {
	type plain actionEntry
	var fields plain
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*e = actionEntry(fields)
	for _, key := range unsupportedActionKeys {
		if _, ok := raw[key]; ok {
			e.unsupported = append(e.unsupported, key)
		}
	}
	return nil
}

// queueActionEntry dispatches one entry as json_action_one() does: node and
// service make an instance action, a service alone a service action, a node alone
// a node action, a cluster alone a cluster action. vmname names the node in
// place of node_id.
func (a *Api) queueActionEntry(act actionActor, log *slog.Logger, ctx context.Context, e actionEntry) (*queuedAction, error) {
	if len(e.unsupported) > 0 {
		return nil, refuseAction(http.StatusBadRequest, "unsupported keys: %s", strings.Join(e.unsupported, ", "))
	}
	if e.Action == "" {
		return nil, refuseAction(http.StatusBadRequest, "no action specified")
	}
	nodeID := e.NodeID
	if e.VMName != "" {
		nodeID = e.VMName
	}
	if e.ClusterID != "" {
		// A cluster action names the cluster alone: the node it is posted for is
		// chosen among the live ones.
		if nodeID != "" || e.SvcID != "" || e.Rid != "" {
			return nil, refuseAction(http.StatusBadRequest, "cluster_id excludes node_id, vmname, svc_id and rid")
		}
		return a.queueClusterAction(act, log, ctx, e.ClusterID, e.Action)
	}
	switch {
	case e.SvcID != "" && nodeID != "":
		return a.queueInstanceAction(act, log, ctx, nodeID, e.SvcID, e.Action, e.Rid)
	case e.Rid != "":
		return nil, refuseAction(http.StatusBadRequest, "rid requires both node_id and svc_id")
	case nodeID != "":
		return a.queueNodeAction(act, log, ctx, nodeID, e.Action)
	case e.SvcID != "":
		return a.queueServiceAction(act, log, ctx, e.SvcID, e.Action)
	}
	return nil, refuseAction(http.StatusBadRequest, "node_id, svc_id or cluster_id must be specified")
}

// factorizeActionEntries merges the entries that target the same resources of an
// instance with the same action into one action on all of them, as
// factorize_actions() does: selecting three resources queues one agent run.
func factorizeActionEntries(entries []actionEntry) []actionEntry {
	type key struct{ svcID, nodeID, action string }
	var out []actionEntry
	var order []key
	rids := map[key][]string{}
	for _, e := range entries {
		nodeID := e.NodeID
		if e.VMName != "" {
			nodeID = e.VMName
		}
		if e.Rid == "" || e.SvcID == "" || nodeID == "" || e.ClusterID != "" || len(e.unsupported) > 0 {
			out = append(out, e)
			continue
		}
		k := key{e.SvcID, nodeID, e.Action}
		if _, seen := rids[k]; !seen {
			order = append(order, k)
		}
		rids[k] = append(rids[k], e.Rid)
	}
	for _, k := range order {
		out = append(out, actionEntry{NodeID: k.nodeID, SvcID: k.svcID, Action: k.action, Rid: strings.Join(rids[k], ",")})
	}
	return out
}

// queuedActionRows reads back the queued actions, with the default props of
// GET /actions/{id}. The caller has just created them: they are read without the
// node responsibility filter of the action list, which would hide an instance
// action queued on a node the caller is not responsible for.
func (a *Api) queuedActionRows(ctx context.Context, ids []int64) ([]map[string]any, ListQueryParameters, error) {
	mapping := propsMapping["action_queue"]
	query, err := buildListQueryParameters(nil, nil, nil, nil, nil, nil, nil, mapping)
	if err != nil {
		return nil, query, err
	}
	selectExprs, err := buildSelectClause(query.Props, mapping)
	if err != nil {
		return nil, query, err
	}
	rows := []map[string]any{}
	for _, id := range ids {
		items, err := a.ODB.GetActionOne(ctx, strconv.FormatInt(id, 10), cdb.ListParams{
			IsManager:   true,
			Limit:       1,
			Props:       query.Props,
			SelectExprs: selectExprs,
			TypeHints:   buildTypeHints(query.Props, mapping),
		})
		if err != nil {
			return nil, query, err
		}
		rows = append(rows, items...)
	}
	return rows, query, nil
}

// PutActions handles PUT /actions: enqueue agent actions, as the historical
// collector's REST API and action menu do.
func (a *Api) PutActions(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PutActions")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}

	var raw json.RawMessage
	if err := json.NewDecoder(c.Request().Body).Decode(&raw); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	// A list: every entry is tried, and the answer collects what was queued and
	// what was refused, as the historical handle_list() does.
	if trimmed := bytes.TrimSpace(raw); len(trimmed) > 0 && trimmed[0] == '[' {
		var entries []actionEntry
		if err := json.Unmarshal(raw, &entries); err != nil {
			return JSONProblem(c, http.StatusBadRequest, err.Error())
		}
		var ids []int64
		refusals := []string{}
		for _, e := range factorizeActionEntries(entries) {
			queued, err := a.queueActionEntry(actorOf(c), log, ctx, e)
			if err != nil {
				refusals = append(refusals, describeRefusal(e, err))
				continue
			}
			ids = append(ids, queued.ID)
		}
		if len(ids) > 0 {
			if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
				log.Error("cannot notify changes", logkey.Error, err)
			}
		}
		rows, _, err := a.queuedActionRows(ctx, ids)
		if err != nil {
			log.Error("cannot read the queued actions", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "actions queued, but cannot read them back")
		}
		return c.JSON(http.StatusOK, map[string]any{
			"info":  []string{fmt.Sprintf("%d action(s) queued", len(ids))},
			"error": refusals,
			"data":  rows,
		})
	}

	var entry actionEntry
	if err := json.Unmarshal(raw, &entry); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	queued, err := a.queueActionEntry(actorOf(c), log, ctx, entry)
	if err != nil {
		return actionProblem(c, err)
	}
	if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}
	rows, query, err := a.queuedActionRows(ctx, []int64{queued.ID})
	if err != nil {
		log.Error("cannot read the queued action", "action_id", queued.ID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "action %d queued, but cannot read it back", queued.ID)
	}
	return c.JSON(http.StatusOK, newListResponse(rows, propsMapping["action_queue"], query))
}

// describeRefusal names the refused entry in the message of a list answer.
func describeRefusal(e actionEntry, err error) string {
	var target []string
	for _, part := range []struct{ key, value string }{
		{"node_id", e.NodeID}, {"vmname", e.VMName}, {"svc_id", e.SvcID}, {"cluster_id", e.ClusterID}, {"rid", e.Rid},
	} {
		if part.value != "" {
			target = append(target, part.key+"="+part.value)
		}
	}
	msg := err.Error()
	var refusal *actionRefusal
	if errors.As(err, &refusal) {
		msg = fmt.Sprintf("%d: %s", refusal.status, refusal.msg)
	}
	return fmt.Sprintf("%s on %s: %s", e.Action, strings.Join(target, " "), msg)
}
