package serverhandlers

import (
	"context"
	"net/http"
	"slices"
	"strconv"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
	"github.com/opensvc/oc3/xauth"
)

func (a *Api) PostAuthNode(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostAuthNode")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	var body server.PostAuthNodeJSONBody

	if err := c.Bind(&body); err != nil {
		log.Error("PostAuthNode : invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	var app string
	var userID int64
	var teamResponsible string

	if body.App != nil {
		app = *body.App
	}

	switch {
	case IsAuthByUser(c):
		// User auth
		user := UserInfoFromContext(c)
		if user == nil {
			return JSONProblemf(c, http.StatusUnauthorized, "missing user context")
		}
		userIDStr := user.GetExtensions().Get(xauth.XUserID)
		var err error
		userID, err = strconv.ParseInt(userIDStr, 10, 64)
		if err != nil {
			return JSONProblemf(c, http.StatusBadRequest, "invalid user id")
		}

		// if the app is not provided, get the default app for the user
		if app == "" {
			defaultApp, err := odb.UserDefaultApp(ctx, &userID)
			if err != nil {
				log.Error("failed to find default app", logkey.Error, err)
				return JSONProblemf(c, http.StatusInternalServerError, "cannot find default app")
			}
			app = defaultApp
		} else {
			groups := UserGroupsFromContext(c)
			userApps, err := odb.AppsForGroups(ctx, groups)
			if err != nil {
				log.Error("failed to find user apps", logkey.Error, err)
				return JSONProblemf(c, http.StatusInternalServerError, "cannot find user apps")
			}
			if !slices.Contains(userApps, app) {
				return JSONProblemf(c, http.StatusForbidden, "you are not responsible for the '%s' app", app)
			}
		}

		teamResponsible, _, err = odb.UserDefaultGroup(ctx, userID)
		if err != nil {
			log.Error("failed to find default group", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot find default group")
		}
	case IsAuthByNode(c), IsAuthByAnonRegister(c):
		if !viper.GetBool("server.allow_anon_register") {
			return JSONProblemf(c, http.StatusForbidden, "anonymous node registration is disabled")
		}
		if app == "" && IsAuthByNode(c) {
			if s, ok := c.Get(XApp).(string); ok {
				app = s
			}
		}
		if app == "" {
			return JSONProblemf(c, http.StatusBadRequest, "missing app")
		}
		teamResponsible = "Everybody"
	default:
		return JSONProblemf(c, http.StatusUnauthorized, "missing authentication")
	}

	if ok, err := odb.AppExists(ctx, app); err != nil {
		log.Error("failed to verify app existence", "app", app, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot verify app")
	} else if !ok {
		return JSONProblemf(c, http.StatusBadRequest, "unknown app %s", app)
	}

	var (
		nodeID   string
		nodename = body.Nodename
	)

	node, err := odb.NodeByNodenameAndApp(ctx, nodename, app)
	if err != nil {
		log.Error("failed to find node", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "node lookup failed")
	}
	if node != nil {
		// Node already exists in node table
		nodeID = node.NodeID
	} else {
		// Node does not exist: create it with a new node_id
		nodeID = uuid.New().String()
		if err := odb.InsertNode(ctx, nodename, teamResponsible, app, nodeID); err != nil {
			log.Error("failed to insert node", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot create node")
		}
	}

	// check if this node_id is already registered
	authNodes, err := odb.AuthNodesByNodeID(ctx, nodeID)
	if err != nil {
		log.Error("failed to find auth_node", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "auth_node lookup failed")
	}

	switch len(authNodes) {
	case 1:
		// Already registered: resend uuid
		// TODO: add metrics
		log.Debug("node is already registered", logkey.NodeID, nodeID)
		return c.JSON(http.StatusOK, map[string]any{
			"uuid": authNodes[0].UUID,
			"info": "node is already registered",
		})
	case 0:
		// New registration: generate uuid, insert auth_node
		u := uuid.New().String()
		if err := odb.InsertAuthNode(ctx, nodename, u, nodeID); err != nil {
			log.Error("failed to insert auth_node", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot register node")
		}
		// TODO: add metrics
		log.Debug("node is registered", logkey.NodeID, nodeID)
		return c.JSON(http.StatusOK, map[string]any{
			"uuid": u,
			"info": "node is registered",
		})
	default:
		// Multiple registrations: bug?
		// TODO: add metrics
		log.Warn("node double registration attempt", logkey.NodeID, nodeID, "nodename", nodename)
		return JSONProblem(c, http.StatusConflict, "node double registration attempt")
	}
}
