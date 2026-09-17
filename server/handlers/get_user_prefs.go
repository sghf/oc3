package serverhandlers

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetUserPrefs handles GET /users/{user_id}/prefs
func (a *Api) GetUserPrefs(c echo.Context, userId string) error {
	log := echolog.GetLogHandler(c, "GetUserPrefs")
	ctx := c.Request().Context()

	log.Info("called", logkey.UserID, userId)

	raw, found, err := a.ODB.UserPrefs(ctx, userId, cdb.ListParams{
		Groups:    UserGroupsFromContext(c),
		IsManager: IsManager(c),
		UserID:    authUserID(c),
	})
	if err != nil {
		log.Error("cannot get user prefs", logkey.UserID, userId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get user prefs")
	}

	// An unknown user, a user the caller cannot see, a missing row and a stored
	// document that does not parse all answer with an empty object, as the python
	// handler does.
	prefs := map[string]any{}
	if found && raw != "" {
		if err := json.Unmarshal([]byte(raw), &prefs); err != nil {
			log.Warn("stored user prefs are not valid json", logkey.UserID, userId, logkey.Error, err)
			prefs = map[string]any{}
		}
	}

	return c.JSON(http.StatusOK, server.UserPrefsResponse{Data: prefs})
}
