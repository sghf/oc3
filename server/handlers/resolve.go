package serverhandlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/logkey"
	"github.com/opensvc/oc3/xauth"
)

// authUserID returns the authenticated user's auth_user.id, or nil when the
// request is not authenticated as a user (node credentials, public path) or the
// id does not parse.
func authUserID(c echo.Context) *int64 {
	user := UserInfoFromContext(c)
	if user == nil {
		return nil
	}
	id, err := strconv.ParseInt(user.GetExtensions().Get(xauth.XUserID), 10, 64)
	if err != nil {
		return nil
	}
	return &id
}

// resolveUserGroupIDs returns the group ids the authenticated user belongs to.
func (a *Api) resolveUserGroupIDs(c echo.Context, log *slog.Logger) ([]int64, error) {
	ctx := c.Request().Context()
	user := UserInfoFromContext(c)
	if user == nil {
		return nil, JSONProblemf(c, http.StatusUnauthorized, "missing user context")
	}
	userID, err := strconv.ParseInt(user.GetExtensions().Get(xauth.XUserID), 10, 64)
	if err != nil {
		return nil, JSONProblemf(c, http.StatusBadRequest, "invalid user id")
	}
	ids, err := a.ODB.UserGroupIDs(ctx, userID)
	if err != nil {
		log.Error("cannot list user groups", logkey.Error, err)
		return nil, JSONProblemf(c, http.StatusInternalServerError, "cannot list user groups")
	}
	return ids, nil
}

// resolveNode looks up a node by ID or name
func (a *Api) resolveNode(c echo.Context, log *slog.Logger, nodeId string) (*cdb.DBNode, error) {
	ctx := c.Request().Context()
	node, err := a.ODB.NodeByNodeIDOrNodename(ctx, nodeId)
	if err != nil {
		log.Error("cannot resolve node", logkey.NodeID, nodeId, logkey.Error, err)
		return nil, JSONProblemf(c, http.StatusInternalServerError, "cannot resolve node")
	}
	if node == nil {
		return nil, JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}
	return node, nil
}

// resolveServiceRow resolves a service by svc_id or name
func (a *Api) resolveServiceRow(c echo.Context, log *slog.Logger, ctx context.Context, svcId string) (*cdb.DBService, error) {
	if svcId == "" {
		return nil, JSONProblemf(c, http.StatusBadRequest, "invalid svc_id: ''")
	}
	svc, err := a.ODB.ServiceBySvcIDOrName(ctx, svcId)
	if err != nil {
		log.Error("cannot resolve service", "svc_id", svcId, logkey.Error, err)
		return nil, JSONProblemf(c, http.StatusInternalServerError, "cannot resolve service %s", svcId)
	}
	if svc == nil {
		return nil, JSONProblemf(c, http.StatusNotFound, "service %s not found", svcId)
	}
	return svc, nil
}

// resolveService verifies that a service exists and is accessible
func (a *Api) resolveService(c echo.Context, log *slog.Logger, svcId string) error {
	ctx := c.Request().Context()
	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)
	svcs, err := a.ODB.GetService(ctx, svcId, cdb.ListParams{Limit: 1, Groups: groups, IsManager: isManager})
	if err != nil {
		log.Error("cannot resolve service", "svc_id", svcId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve service")
	}
	if len(svcs) == 0 {
		return JSONProblemf(c, http.StatusNotFound, "service %s not found", svcId)
	}
	return nil
}

func decodeAppGroupKeys(c echo.Context) (string, string, error) {
	var body map[string]any
	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil {
		return "", "", JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	rawAppID, ok := body["app_id"]
	if !ok {
		return "", "", JSONProblemf(c, http.StatusBadRequest, "The 'app_id' key is mandatory")
	}
	rawGroupID, ok := body["group_id"]
	if !ok {
		return "", "", JSONProblemf(c, http.StatusBadRequest, "The 'group_id' key is mandatory")
	}
	return fmt.Sprintf("%v", rawAppID), fmt.Sprintf("%v", rawGroupID), nil
}
