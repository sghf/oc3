package serverhandlers

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

const (
	// maxAlertsDelete bounds a batch, as the api says (maxItems).
	maxAlertsDelete = 10000
	// alertsDeleteChunk is the number of ids of one DELETE statement.
	alertsDeleteChunk = 1000
)

// PostAlertsDelete handles POST /alerts/delete: the alerts of a list of ids
// deleted in one transaction, with one audit entry and one change event, where
// DELETE /alerts/{id} takes a request, an entry and an event per alert.
func (a *Api) PostAlertsDelete(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostAlertsDelete")
	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	if !IsAlertsManager(c) {
		return JSONProblemf(c, http.StatusForbidden, "user has no AlertsManager privilege")
	}
	var body server.AlertsDeleteInput
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if len(body.Ids) > maxAlertsDelete {
		return JSONProblemf(c, http.StatusBadRequest, "%d alerts, more than %d at once", len(body.Ids), maxAlertsDelete)
	}
	// A large batch takes longer than a single request: a timeout of its own,
	// still bounded.
	ctx, cancel := context.WithTimeout(c.Request().Context(), max(a.SyncTimeout, 60*time.Second))
	defer cancel()

	seen := map[int64]bool{}
	var ids []int64
	for _, id := range body.Ids {
		if !seen[int64(id)] {
			seen[int64(id)] = true
			ids = append(ids, int64(id))
		}
	}
	types := map[int64]string{}
	var deleted int64
	if err := a.inTx(ctx, log, func(tx *cdb.DB) error {
		for start := 0; start < len(ids); start += alertsDeleteChunk {
			chunk := ids[start:min(start+alertsDeleteChunk, len(ids))]
			found, err := tx.AlertTypesByIDs(ctx, chunk)
			if err != nil {
				return err
			}
			for id, typ := range found {
				types[id] = typ
			}
			n, err := tx.DeleteAlertsByIDs(ctx, chunk)
			if err != nil {
				return err
			}
			deleted += n
		}
		if len(types) == 0 {
			return nil
		}
		author, _ := c.Get(XUserEmail).(string)
		return tx.Log(ctx, cdb.LogEntry{
			Action: "alert.del",
			User:   author,
			Fmt:    "%(count)s alerts deleted: %(types)s",
			Dict:   auditAlertsDict(types),
			Level:  "info",
		})
	}); err != nil {
		log.Error("cannot delete the alerts", "count", len(ids), logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete the alerts")
	}
	if deleted > 0 {
		if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
			log.Debug("cannot notify changes", logkey.Error, err)
		}
	}
	notFound := []int{}
	for _, id := range ids {
		if _, ok := types[id]; !ok {
			notFound = append(notFound, int(id))
		}
	}
	log.Info("alerts deleted", "count", deleted, "not_found", len(notFound))
	return c.JSON(http.StatusOK, server.AlertsDeleteResult{Deleted: int(deleted), NotFound: notFound})
}

// summarizeAlertTypes names the types of the alerts deleted, the most frequent
// first, with their counts: "action errors (12), node not updated (3)".
func summarizeAlertTypes(types map[int64]string) string {
	counts := map[string]int{}
	for _, typ := range types {
		counts[typ]++
	}
	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		if counts[names[i]] != counts[names[j]] {
			return counts[names[i]] > counts[names[j]]
		}
		return names[i] < names[j]
	})
	parts := make([]string, len(names))
	for i, name := range names {
		parts[i] = fmt.Sprintf("%s (%d)", name, counts[name])
	}
	return strings.Join(parts, ", ")
}

// auditIDs is the number of ids the audit entry lists: the log_dict column, a
// TEXT, would not hold 10000 of them.
const auditIDs = 500

// auditAlertsDict is the values of the audit entry of a batch: the count, the
// types, the first ids in order and how many more were deleted.
func auditAlertsDict(types map[int64]string) map[string]any {
	ids := make([]int64, 0, len(types))
	for id := range types {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	dict := map[string]any{"count": len(types), "types": summarizeAlertTypes(types)}
	if len(ids) > auditIDs {
		dict["ids"], dict["more_ids"] = ids[:auditIDs], len(ids)-auditIDs
	} else {
		dict["ids"] = ids
	}
	return dict
}
