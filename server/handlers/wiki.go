package serverhandlers

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
	"github.com/opensvc/oc3/xauth"
)

const (
	// wikiBodyMax keeps a revision within the TEXT column of wiki_pages.
	wikiBodyMax       = 60000
	wikiChangeNoteMax = 200
	wikiRevisionsMax  = 200
)

// wikiSubject is the node or the service a wiki page holds the notes of: the
// page is named after its node_id or svc_id, as in the legacy collector.
type wikiSubject struct {
	kind   string
	label  string
	nodeID string
	svcID  string
}

func (s wikiSubject) name() string {
	if s.kind == "node" {
		return s.nodeID
	}
	return s.svcID
}

// wikiNode resolves the node of a page: any user reads the notes of a node.
func (a *Api) wikiNode(c echo.Context, log *slog.Logger, nodeId string) (wikiSubject, error) {
	node, err := a.resolveNode(c, log, nodeId)
	if err != nil {
		return wikiSubject{}, err
	}
	return wikiSubject{kind: "node", label: node.Nodename, nodeID: node.NodeID}, nil
}

// wikiService resolves the service of a page, which the user must see.
func (a *Api) wikiService(c echo.Context, log *slog.Logger, svcId string) (wikiSubject, error) {
	if err := a.resolveService(c, log, svcId); err != nil {
		return wikiSubject{}, err
	}
	svc, err := a.resolveServiceRow(c, log, c.Request().Context(), svcId)
	if err != nil {
		return wikiSubject{}, err
	}
	return wikiSubject{kind: "service", label: svc.Svcname, svcID: svc.SvcID}, nil
}

// wikiWritable tells whether the user may save the notes of the subject: a
// Manager, or a user responsible for its application.
func (a *Api) wikiWritable(c echo.Context, ctx context.Context, s wikiSubject) (bool, error) {
	if s.kind == "node" {
		return a.ODB.NodeResponsible(ctx, s.nodeID, UserGroupsFromContext(c), IsManager(c))
	}
	return a.ODB.ServiceResponsible(ctx, s.svcID, UserGroupsFromContext(c), IsManager(c))
}

func toWikiRevision(r cdb.WikiRevision, withBody bool) server.WikiRevision {
	out := server.WikiRevision{Id: r.ID, SavedOn: r.SavedOn, Author: r.Author, ChangeNote: r.ChangeNote}
	if withBody {
		body := r.Body
		out.Body = &body
	}
	return out
}

func (a *Api) getWiki(c echo.Context, log *slog.Logger, s wikiSubject, limit *int) error {
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	n := 20
	if limit != nil {
		n = min(max(*limit, 1), wikiRevisionsMax)
	}
	latest, err := a.ODB.WikiRevisionByID(ctx, s.name(), 0)
	if err != nil {
		log.Error("cannot read the wiki page", "name", s.name(), logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the notes of %s %s", s.kind, s.label)
	}
	revisions, total, err := a.ODB.WikiRevisions(ctx, s.name(), n)
	if err != nil {
		log.Error("cannot read the wiki revisions", "name", s.name(), logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the notes of %s %s", s.kind, s.label)
	}
	writable, err := a.wikiWritable(c, ctx, s)
	if err != nil {
		log.Error("cannot check responsibility", "name", s.name(), logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check responsibility for %s %s", s.kind, s.label)
	}
	page := server.WikiPage{Revisions: make([]server.WikiRevision, 0, len(revisions)), Total: total, Writable: writable}
	if latest != nil {
		r := toWikiRevision(*latest, true)
		page.Revision = &r
	}
	for _, r := range revisions {
		page.Revisions = append(page.Revisions, toWikiRevision(r, false))
	}
	return c.JSON(http.StatusOK, page)
}

func (a *Api) getWikiRevision(c echo.Context, log *slog.Logger, s wikiSubject, id int64) error {
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	if id <= 0 {
		return JSONProblemf(c, http.StatusNotFound, "revision %d not found", id)
	}
	r, err := a.ODB.WikiRevisionByID(ctx, s.name(), id)
	if err != nil {
		log.Error("cannot read the wiki revision", "name", s.name(), "id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read revision %d", id)
	}
	if r == nil {
		return JSONProblemf(c, http.StatusNotFound, "revision %d of the notes of %s %s not found", id, s.kind, s.label)
	}
	return c.JSON(http.StatusOK, toWikiRevision(*r, true))
}

func (a *Api) postWiki(c echo.Context, log *slog.Logger, s wikiSubject) error {
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	var input server.WikiRevisionInput
	if err := json.NewDecoder(c.Request().Body).Decode(&input); err != nil {
		return JSONProblemf(c, http.StatusBadRequest, "invalid body: %s", err)
	}
	if len(input.Body) > wikiBodyMax {
		return JSONProblemf(c, http.StatusBadRequest, "the notes are %d bytes long, more than the %d a revision holds", len(input.Body), wikiBodyMax)
	}
	note := ""
	if input.ChangeNote != nil {
		note = strings.TrimSpace(*input.ChangeNote)
	}
	if utf8.RuneCountInString(note) > wikiChangeNoteMax {
		return JSONProblemf(c, http.StatusBadRequest, "the change note is longer than %d characters", wikiChangeNoteMax)
	}
	writable, err := a.wikiWritable(c, ctx, s)
	if err != nil {
		log.Error("cannot check responsibility", "name", s.name(), logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check responsibility for %s %s", s.kind, s.label)
	}
	if !writable {
		return JSONProblemf(c, http.StatusForbidden, "you are not responsible for %s %s", s.kind, s.label)
	}
	user := UserInfoFromContext(c)
	if user == nil {
		return JSONProblemf(c, http.StatusUnauthorized, "missing user context")
	}
	authorID, err := strconv.ParseInt(user.GetExtensions().Get(xauth.XUserID), 10, 64)
	if err != nil {
		return JSONProblemf(c, http.StatusBadRequest, "invalid user id")
	}
	id, err := a.ODB.InsertWikiRevision(ctx, s.name(), input.BaseId, input.Body, note, authorID)
	if errors.Is(err, cdb.ErrWikiConflict) {
		return JSONProblemf(c, http.StatusConflict, "the notes of %s %s were saved by someone else since the revision you edited: nothing was saved", s.kind, s.label)
	}
	if err != nil {
		log.Error("cannot save the wiki revision", "name", s.name(), logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot save the notes of %s %s", s.kind, s.label)
	}
	a.checksLog(c, ctx, log, "wiki.change", "changed the notes of %(kind)s %(name)s (revision %(id)s)",
		map[string]any{"kind": s.kind, "name": s.label, "id": id}, s.nodeID, s.svcID)
	if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
		log.Debug("cannot notify changes", logkey.Error, err)
	}
	r, err := a.ODB.WikiRevisionByID(ctx, s.name(), id)
	if err != nil || r == nil {
		log.Error("cannot read the saved wiki revision", "name", s.name(), "id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "notes saved, but cannot read them back")
	}
	return c.JSON(http.StatusCreated, toWikiRevision(*r, true))
}

// GetNodeWiki handles GET /nodes/{node_id}/wiki
func (a *Api) GetNodeWiki(c echo.Context, nodeId string, params server.GetNodeWikiParams) error {
	log := echolog.GetLogHandler(c, "GetNodeWiki")
	s, err := a.wikiNode(c, log, nodeId)
	if err != nil {
		return err
	}
	return a.getWiki(c, log, s, params.Limit)
}

// GetNodeWikiRevision handles GET /nodes/{node_id}/wiki/{revision_id}
func (a *Api) GetNodeWikiRevision(c echo.Context, nodeId string, revisionId int64) error {
	log := echolog.GetLogHandler(c, "GetNodeWikiRevision")
	s, err := a.wikiNode(c, log, nodeId)
	if err != nil {
		return err
	}
	return a.getWikiRevision(c, log, s, revisionId)
}

// PostNodeWiki handles POST /nodes/{node_id}/wiki
func (a *Api) PostNodeWiki(c echo.Context, nodeId string) error {
	log := echolog.GetLogHandler(c, "PostNodeWiki")
	s, err := a.wikiNode(c, log, nodeId)
	if err != nil {
		return err
	}
	return a.postWiki(c, log, s)
}

// GetServiceWiki handles GET /services/{svc_id}/wiki
func (a *Api) GetServiceWiki(c echo.Context, svcId string, params server.GetServiceWikiParams) error {
	log := echolog.GetLogHandler(c, "GetServiceWiki")
	s, err := a.wikiService(c, log, svcId)
	if err != nil {
		return err
	}
	return a.getWiki(c, log, s, params.Limit)
}

// GetServiceWikiRevision handles GET /services/{svc_id}/wiki/{revision_id}
func (a *Api) GetServiceWikiRevision(c echo.Context, svcId string, revisionId int64) error {
	log := echolog.GetLogHandler(c, "GetServiceWikiRevision")
	s, err := a.wikiService(c, log, svcId)
	if err != nil {
		return err
	}
	return a.getWikiRevision(c, log, s, revisionId)
}

// PostServiceWiki handles POST /services/{svc_id}/wiki
func (a *Api) PostServiceWiki(c echo.Context, svcId string) error {
	log := echolog.GetLogHandler(c, "PostServiceWiki")
	s, err := a.wikiService(c, log, svcId)
	if err != nil {
		return err
	}
	return a.postWiki(c, log, s)
}
