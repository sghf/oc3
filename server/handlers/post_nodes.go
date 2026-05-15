package serverhandlers

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
	"github.com/opensvc/oc3/xauth"
)

// PostNodes handles POST /nodes (create or delegate to update if exists)
func (a *Api) PostNodes(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostNodes")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) && !IsAuthByNode(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "authentication required")
	}

	var body server.PostNodesJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	if body.Nodename == nil && body.NodeId == nil {
		return JSONProblemf(c, http.StatusBadRequest, "nodename or node_id is required")
	}

	// if node already exists delegate to update
	var lookupKey string
	if body.NodeId != nil {
		lookupKey = *body.NodeId
	} else {
		lookupKey = *body.Nodename
	}
	existing, err := odb.NodeByNodeIDOrNodename(ctx, lookupKey)
	if err != nil {
		log.Error("cannot lookup node", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup node")
	}
	if existing != nil {
		return postNodeUpdate(c, log, odb, ctx, existing.NodeID, nodeBodyFieldsFromPostNodes(body).toFields())
	}

	// Create path
	if !IsManager(c) {
		return JSONProblemf(c, http.StatusForbidden, "NodeManager privilege required")
	}
	if body.Nodename == nil || *body.Nodename == "" {
		return JSONProblemf(c, http.StatusBadRequest, "nodename is required to create a node")
	}

	teamResponsible := ""
	if body.TeamResponsible != nil {
		teamResponsible = *body.TeamResponsible
	}
	// Nodes cannot set team_responsible
	if IsAuthByNode(c) {
		teamResponsible = ""
	}
	if teamResponsible == "" {
		if IsAuthByUser(c) {
			user := UserInfoFromContext(c)
			if user != nil {
				userIDStr := user.GetExtensions().Get(xauth.XUserID)
				if userID, err2 := strconv.ParseInt(userIDStr, 10, 64); err2 == nil {
					if group, ok, err2 := odb.UserDefaultGroup(ctx, userID); err2 == nil && ok {
						teamResponsible = group
					}
				}
			}
		}
	}

	app := ""
	if body.App != nil {
		app = *body.App
	}
	if IsAuthByUser(c) {
		app = resolveApp(ctx, log, odb, c, app)
	}

	nodeID := uuid.New().String()
	if err := odb.InsertNode(ctx, *body.Nodename, teamResponsible, app, nodeID); err != nil {
		log.Error("cannot insert node", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot create node")
	}

	// Update optional fields if any were provided
	fields := nodeBodyFieldsFromPostNodes(body).toFields()
	delete(fields, "nodename")
	delete(fields, "team_responsible")
	delete(fields, "app")
	if len(fields) > 0 {
		if err := odb.UpdateNodeFields(ctx, nodeID, fields); err != nil {
			log.Error("cannot update node fields after insert", logkey.Error, err)
		}
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "node.add",
		User:   userEmail,
		Fmt:    "create properties %(data)s",
		Dict:   map[string]any{"data": body},
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		slog.Debug("PostNodes: cannot notify changes", logkey.Error, err)
	}

	return fetchAndReturnNode(c, odb, ctx, nodeID, "cannot fetch created node")
}

func postNodeUpdate(c echo.Context, log interface{ Error(string, ...any) }, odb *cdb.DB, ctx context.Context, nodeID string, fields map[string]any) error {
	if IsAuthByNode(c) {
		// A node can only update itself
		callerNodeID, _ := c.Get(XNodeID).(string)
		if callerNodeID != nodeID {
			return JSONProblemf(c, http.StatusForbidden, "a node can only update itself")
		}
		delete(fields, "team_responsible")
	} else {
		// Users need NodeManager privilege and must be responsible for the node
		if !IsManager(c) {
			return JSONProblemf(c, http.StatusForbidden, "NodeManager privilege required")
		}
		responsible, err := odb.NodeResponsible(ctx, nodeID, UserGroupsFromContext(c), false)
		if err != nil {
			log.Error("cannot check node responsibility", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot check node responsibility")
		}
		if !responsible {
			return JSONProblemf(c, http.StatusForbidden, "user is not responsible for node %s", nodeID)
		}
	}
	// Validate app: user must be responsible for the requested app; fall back to default otherwise.
	// Node auth bypasses this check.
	if app, ok := fields["app"].(string); ok && IsAuthByUser(c) {
		fields["app"] = resolveApp(ctx, log, odb, c, app)
	}

	fields["updated"] = time.Now().Format("2006-01-02 15:04:05")

	if err := odb.UpdateNodeFields(ctx, nodeID, fields); err != nil {
		log.Error("cannot update node", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot update node")
	}
	if nodename, ok := fields["nodename"].(string); ok && nodename != "" {
		if err := odb.UpdateAuthNodeNodename(ctx, nodeID, nodename); err != nil {
			log.Error("cannot sync nodename to auth_node", logkey.Error, err)
		}
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "node.change",
		User:   userEmail,
		Fmt:    "change properties %(data)s",
		Dict:   map[string]any{"data": fields},
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		slog.Debug("postNodeUpdate: cannot notify changes", logkey.Error, err)
	}

	return fetchAndReturnNode(c, odb, ctx, nodeID, "cannot fetch updated node")
}

func fetchAndReturnNode(c echo.Context, odb *cdb.DB, ctx context.Context, nodeID string, errMsg string) error {
	mapping := propsMapping["node"]
	props := defaultProps(mapping)
	selectExprs, err := buildSelectClause(props, mapping)
	if err != nil {
		return JSONProblemf(c, http.StatusInternalServerError, errMsg)
	}
	rows, err := odb.GetNode(ctx, nodeID, cdb.ListParams{
		IsManager:   true,
		Props:       props,
		SelectExprs: selectExprs,
	})
	if err != nil || len(rows) == 0 {
		return JSONProblemf(c, http.StatusInternalServerError, errMsg)
	}
	return c.JSON(http.StatusOK, rows[0])
}

// resolveApp returns the app to use for a node create/update.
func resolveApp(ctx context.Context, log interface{ Error(string, ...any) }, odb *cdb.DB, c echo.Context, app string) string {
	allowedApps, err := odb.AppsForGroups(ctx, UserGroupsFromContext(c))
	if err != nil {
		log.Error("cannot fetch allowed apps", logkey.Error, err)
		return app
	}
	for _, a := range allowedApps {
		if strings.EqualFold(a, app) {
			return app
		}
	}
	user := UserInfoFromContext(c)
	if user == nil {
		return app
	}
	userIDStr := user.GetExtensions().Get(xauth.XUserID)
	userID, err := strconv.ParseInt(userIDStr, 10, 64)
	if err != nil {
		return app
	}
	defaultApp, err := odb.UserDefaultApp(ctx, &userID)
	if err != nil {
		log.Error("cannot fetch default app", logkey.Error, err)
		return app
	}
	return defaultApp
}
