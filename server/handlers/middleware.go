package serverhandlers

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/shaj13/go-guardian/v2/auth"
	"github.com/shaj13/go-guardian/v2/auth/strategies/union"

	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
	"github.com/opensvc/oc3/xauth"
)

const (
	XClusterID = "XClusterID"
	XNodeID    = "XNodeID"
	XNodename  = "XNodename"
	XApp       = "XApp"
	XUserID    = "XUserID"
	XUserEmail = "XUserEmail"
	XAuthMode  = "XAuthMode" // user or node
)

const (
	AuthModeUser         = "user"
	AuthModeNode         = "node"
	AuthModeAnonRegister = "anon_register"
)

// AuthMiddleware returns auth middleware that authenticate requests from strategies.
func AuthMiddleware(strategies union.Union) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			_, user, err := strategies.AuthenticateRequest(c.Request())
			if err != nil && xauth.IsUnavailable(err) {
				// A database failure is not a wrong password: say so, and log it.
				echolog.GetLog(c).Error("cannot authenticate", logkey.Error, err)
				return JSONProblem(c, http.StatusServiceUnavailable, "authentication is unavailable, the database cannot be reached; retry later")
			}

			if err != nil {
				// Credentials refused leave a trace in the server log, not in the
				// audit table: a flood of them must not fill the database. A
				// request without credentials, before a sign-in, is no attempt.
				// The name tried is that of Basic credentials; neither a password
				// nor a token is written.
				if authz := c.Request().Header.Get("Authorization"); authz != "" {
					attrs := []any{"remote", c.RealIP(), "path", c.Request().URL.Path, logkey.Error, err}
					if user, _, ok := c.Request().BasicAuth(); ok {
						attrs = append(attrs, "scheme", "basic", "user", user)
					} else if scheme, _, found := strings.Cut(authz, " "); found {
						attrs = append(attrs, "scheme", strings.ToLower(scheme))
					}
					echolog.GetLog(c).Warn("authentication failed", attrs...)
				}
				code := http.StatusUnauthorized
				return JSONProblem(c, code, err.Error())
			}

			ext := user.GetExtensions()
			if authMode := ext.Get(xauth.XAuthMode); authMode != "" {
				c.Set(XAuthMode, authMode)
			}
			if nodeID := ext.Get(xauth.XNodeID); nodeID != "" {
				c.Set(XNodeID, nodeID)
				c.Set(XAuthMode, AuthModeNode)
			}
			if nodename := ext.Get(xauth.XNodename); nodename != "" {
				c.Set(XNodename, nodename)
			}
			if clusterID := ext.Get(xauth.XClusterID); clusterID != "" {
				c.Set(XClusterID, clusterID)
			}
			if app := ext.Get(xauth.XApp); app != "" {
				c.Set(XApp, app)
			}
			if userEmail := ext.Get(xauth.XUserEmail); userEmail != "" {
				c.Set(XUserEmail, userEmail)
				c.Set(XAuthMode, AuthModeUser)
			}

			groups := user.GetGroups()
			c.Set("groups", groups)
			c.Set("user", user)

			return next(c)
		}
	}
}

func UserInfoFromContext(c echo.Context) auth.Info {
	user, ok := c.Get("user").(auth.Info)
	if ok {
		return user
	}
	return nil
}

func UserGroupsFromContext(c echo.Context) []string {
	groups, ok := c.Get("groups").([]string)
	if ok {
		return groups
	}
	return nil
}

// HasGroup reports whether the authenticated user belongs to the named group.
func HasGroup(c echo.Context, group string) bool {
	for _, g := range UserGroupsFromContext(c) {
		if g == group {
			return true
		}
	}
	return false
}

func IsManager(c echo.Context) bool {
	return HasGroup(c, "Manager")
}

func IsGroupManager(c echo.Context) bool {
	return IsManager(c) || HasGroup(c, "GroupManager")
}

func IsNodeManager(c echo.Context) bool {
	return IsManager(c) || HasGroup(c, "NodeManager")
}

func IsTagManager(c echo.Context) bool {
	return IsManager(c) || HasGroup(c, "TagManager")
}

func IsCompManager(c echo.Context) bool {
	return IsManager(c) || HasGroup(c, "CompManager")
}

func IsAppManager(c echo.Context) bool {
	return IsManager(c) || HasGroup(c, "AppManager")
}

func IsAlertsManager(c echo.Context) bool {
	return IsManager(c) || HasGroup(c, "AlertsManager")
}

func IsReportsManager(c echo.Context) bool {
	return IsManager(c) || HasGroup(c, "ReportsManager")
}

func IsObsManager(c echo.Context) bool {
	return IsManager(c) || HasGroup(c, "ObsManager")
}

// return true if the request is authenticated as a node
func IsAuthByNode(c echo.Context) bool {
	authMode, ok := c.Get(XAuthMode).(string)
	return ok && authMode == AuthModeNode
}

func IsAuthByUser(c echo.Context) bool {
	authMode, ok := c.Get(XAuthMode).(string)
	return ok && authMode == AuthModeUser
}

func IsAuthByAnonRegister(c echo.Context) bool {
	authMode, ok := c.Get(XAuthMode).(string)
	return ok && authMode == AuthModeAnonRegister
}
