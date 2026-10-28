package serverhandlers

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/git"
	"github.com/opensvc/oc3/util/logkey"
)

// formTrack is the git history of the form definitions: one repository per
// form, holding its yaml in a file named "forms", as the historical collector
// lays them out.
func formTrack() git.Track {
	return git.Track{Dir: viper.GetString("server.directories.forms"), File: "forms"}
}

// formCommit records a new form definition in its history. As the historical
// collector, a failed commit does not fail the change of the form: it is logged.
func (a *Api) formCommit(log *slog.Logger, formID int64, content string, caller formCaller) {
	if err := formTrack().Commit(strconv.FormatInt(formID, 10), content, caller.author()); err != nil {
		log.Error("cannot record the form revision", "form_id", formID, logkey.Error, err)
	}
}

// formRevisionsAllowed tells whether the caller may read the history of a form:
// the historical handlers answer an empty list rather than an error otherwise.
func (a *Api) formRevisionsAllowed(ctx context.Context, c echo.Context, log *slog.Logger, formID int64) (bool, error) {
	ok, err := a.ODB.FormVisible(ctx, formID, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		return false, httpInternal(log, "cannot check form publication", err)
	}
	return ok, nil
}

// GetFormRevisions handles GET /forms/{form_id}/revisions.
func (a *Api) GetFormRevisions(c echo.Context, formId int) error {
	log := echolog.GetLogHandler(c, "GetFormRevisions")
	ctx := c.Request().Context()
	allowed, err := a.formRevisionsAllowed(ctx, c, log, int64(formId))
	if err != nil {
		return httpProblem(c, err)
	}
	if !allowed {
		return c.JSON(http.StatusOK, map[string]any{"data": []git.Revision{}})
	}
	revisions, err := formTrack().Timeline(strconv.Itoa(formId))
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the form history", err))
	}
	return c.JSON(http.StatusOK, map[string]any{"data": revisions})
}

// GetFormRevision handles GET /forms/{form_id}/revisions/{cid}.
func (a *Api) GetFormRevision(c echo.Context, formId int, cid string) error {
	log := echolog.GetLogHandler(c, "GetFormRevision")
	ctx := c.Request().Context()
	if !git.ValidRev(cid) {
		return JSONProblemf(c, http.StatusBadRequest, "invalid revision %q", cid)
	}
	allowed, err := a.formRevisionsAllowed(ctx, c, log, int64(formId))
	if err != nil {
		return httpProblem(c, err)
	}
	if !allowed {
		return c.JSON(http.StatusOK, map[string]any{"data": git.Blob{}})
	}
	blob, err := formTrack().At(strconv.Itoa(formId), cid)
	if err != nil {
		return httpProblem(c, httpErrorf(http.StatusNotFound, "revision %s not found", cid))
	}
	if blob == nil {
		return c.JSON(http.StatusOK, map[string]any{"data": git.Blob{}})
	}
	return c.JSON(http.StatusOK, map[string]any{"data": blob})
}

// GetFormDiff handles GET /forms/{form_id}/diff/{cid}: the change of a revision,
// or its differences with another.
func (a *Api) GetFormDiff(c echo.Context, formId int, cid string, params server.GetFormDiffParams) error {
	log := echolog.GetLogHandler(c, "GetFormDiff")
	ctx := c.Request().Context()
	if !git.ValidRev(cid) || (params.Other != nil && *params.Other != "" && !git.ValidRev(*params.Other)) {
		return JSONProblemf(c, http.StatusBadRequest, "invalid revision")
	}
	allowed, err := a.formRevisionsAllowed(ctx, c, log, int64(formId))
	if err != nil {
		return httpProblem(c, err)
	}
	if !allowed {
		return c.JSON(http.StatusOK, map[string]any{"data": ""})
	}
	var out string
	if params.Other != nil && *params.Other != "" {
		out, err = formTrack().Diff(strconv.Itoa(formId), cid, *params.Other)
	} else {
		out, err = formTrack().Show(strconv.Itoa(formId), cid)
	}
	if err != nil {
		return httpProblem(c, httpErrorf(http.StatusNotFound, "revision %s not found", cid))
	}
	return c.JSON(http.StatusOK, map[string]any{"data": out})
}

// PostFormRollback handles POST /forms/{form_id}/rollback/{cid}: restore the
// definition of a revision, recorded as a new revision, store it as the form
// yaml, and log it.
func (a *Api) PostFormRollback(c echo.Context, formId int, cid string) error {
	log := echolog.GetLogHandler(c, "PostFormRollback")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	if !git.ValidRev(cid) {
		return JSONProblemf(c, http.StatusBadRequest, "invalid revision %q", cid)
	}
	if err := requireFormsManager(c); err != nil {
		return httpProblem(c, err)
	}
	formID := int64(formId)
	if err := a.requireFormResponsible(c, log, ctx, formID); err != nil {
		return httpProblem(c, err)
	}
	caller, err := a.formCaller(ctx, c)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the caller", err))
	}
	track := formTrack()
	id := strconv.Itoa(formId)
	if err := track.Rollback(id, cid, caller.author()); err != nil {
		log.Error("cannot roll the form back", "form_id", formId, "cid", cid, logkey.Error, err)
		return httpProblem(c, httpErrorf(http.StatusNotFound, "cannot roll form %d back to %s", formId, cid))
	}
	content, err := track.Read(id)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the restored form", err))
	}
	if err := a.ODB.UpdateForm(ctx, formID, map[string]any{"form_yaml": content}); err != nil {
		return httpProblem(c, httpInternal(log, "cannot store the restored form", err))
	}
	// The historical handler logs nothing; a rollback changes the definition as
	// an edit does, which is logged.
	a.formLog(ctx, c, log, "form.rollback", "Form %(form_id)s rolled back to revision %(cid)s",
		map[string]any{"form_id": strconv.Itoa(formId), "cid": cid})
	a.formNotify(ctx, log)
	// The historical handler returns nothing: the info says what was done.
	return c.JSON(http.StatusOK, map[string]string{"info": fmt.Sprintf("form %d rolled back to %s", formId, cid)})
}
