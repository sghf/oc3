package serverhandlers

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// parseDuration converts a duration string like "1h", "30m", "2d", "1w" to a time.Duration.
// Supported units: w (week), d (day), h (hour), m (minute), s (second).
var durationUnitRe = regexp.MustCompile(`(\d+)([wdhms])`)

func parseDuration(s string) (time.Duration, error) {
	units := map[string]time.Duration{
		"w": 7 * 24 * time.Hour,
		"d": 24 * time.Hour,
		"h": time.Hour,
		"m": time.Minute,
		"s": time.Second,
	}
	matches := durationUnitRe.FindAllStringSubmatch(s, -1)
	if len(matches) == 0 {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q", s)
		}
		return time.Duration(n) * time.Minute, nil
	}
	var total time.Duration
	for _, m := range matches {
		n, _ := strconv.ParseInt(m[1], 10, 64)
		total += time.Duration(n) * units[m[2]]
	}
	return total, nil
}

// PostNodeSnooze handles POST /nodes/{node_id}/snooze
func (a *Api) PostNodeSnooze(c echo.Context, nodeId server.InPathNodeId) error {
	log := echolog.GetLogHandler(c, "PostNodeSnooze")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) && !IsAuthByNode(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "authentication required")
	}

	node, err := odb.NodeByNodeIDOrNodename(ctx, string(nodeId))
	if err != nil {
		log.Error("cannot lookup node", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup node")
	}
	if node == nil {
		return JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}

	if IsAuthByNode(c) {
		callerNodeID, _ := c.Get(XNodeID).(string)
		if callerNodeID != node.NodeID {
			return JSONProblemf(c, http.StatusForbidden, "a node can only snooze itself")
		}
	} else {
		if !IsManager(c) {
			return JSONProblemf(c, http.StatusForbidden, "NodeManager privilege required")
		}
		responsible, err := odb.NodeResponsible(ctx, node.NodeID, UserGroupsFromContext(c), false)
		if err != nil {
			log.Error("cannot check node responsibility", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot check node responsibility")
		}
		if !responsible {
			return JSONProblemf(c, http.StatusForbidden, "user is not responsible for node %s", nodeId)
		}
	}

	var body struct {
		Duration *string `json:"duration"`
	}
	_ = c.Bind(&body)

	var (
		action    string
		snoozeTil any
	)
	if body.Duration == nil || *body.Duration == "" {
		action = "unsnooze"
		snoozeTil = nil
	} else {
		d, err := parseDuration(*body.Duration)
		if err != nil {
			return JSONProblemf(c, http.StatusBadRequest, "invalid duration: %s", err)
		}
		action = "snooze"
		snoozeTil = time.Now().Add(d).Format("2006-01-02 15:04:05")
	}

	fields := map[string]any{
		"snooze_till": snoozeTil,
		"updated":     time.Now().Format("2006-01-02 15:04:05"),
	}
	if err := odb.UpdateNodeFields(ctx, node.NodeID, fields); err != nil {
		log.Error("cannot update node", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot update node")
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	logFmt := ""
	logDict := map[string]any{}
	if action == "snooze" {
		logFmt = "duration %(duration)s"
		logDict["duration"] = *body.Duration
	}
	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "node." + action,
		User:   userEmail,
		Fmt:    logFmt,
		Dict:   logDict,
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"info": action + "d"})
}
