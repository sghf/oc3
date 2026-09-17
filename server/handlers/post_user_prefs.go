package serverhandlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// PostUserPrefs handles POST /users/{user_id}/prefs
func (a *Api) PostUserPrefs(c echo.Context, userId string) error {
	log := echolog.GetLogHandler(c, "PostUserPrefs")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}

	var body server.PostUserPrefsJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if body.Data == nil {
		return JSONProblemf(c, http.StatusBadRequest, "The 'data' key is mandatory")
	}

	log.Info("called", logkey.UserID, userId)

	// The python handler answers 403 when the target user is unknown or not
	// visible, without distinguishing the two.
	targetID, found, err := odb.UserIDForPrefs(ctx, userId, cdb.ListParams{
		Groups:    UserGroupsFromContext(c),
		IsManager: IsManager(c),
		UserID:    authUserID(c),
	})
	if err != nil {
		log.Error("cannot resolve user", logkey.UserID, userId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve user")
	}
	if !found {
		return JSONProblemf(c, http.StatusForbidden, "Not authorized")
	}

	prefs, err := json.Marshal(body.Data)
	if err != nil {
		log.Error("cannot encode prefs", logkey.UserID, userId, logkey.Error, err)
		return JSONProblemf(c, http.StatusBadRequest, "cannot encode prefs")
	}

	tx, markSuccess, endTx, err := odb.BeginTxWithControl(ctx, log, &sql.TxOptions{})
	if err != nil {
		log.Error("cannot start transaction", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot save user prefs")
	}
	defer endTx()

	if err := tx.SaveUserPrefs(ctx, targetID, string(prefs)); err != nil {
		log.Error("cannot save user prefs", logkey.UserID, targetID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot save user prefs")
	}

	markSuccess()

	var uuid string
	if body.Uuid != nil {
		uuid = *body.Uuid
	}
	// Publishes the "user_prefs_change" event the python handler sends through
	// ws_send (init/models/rest/api_users.py:149).
	if err := odb.Session.NotifyTableChangeWithData(ctx, "user_prefs", map[string]any{
		"user_id": targetID,
		"uuid":    uuid,
	}); err != nil {
		log.Error("cannot notify user prefs change", logkey.Error, err)
	}

	return c.JSON(http.StatusOK, map[string]string{"info": "user prefs saved"})
}
