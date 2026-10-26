package serverhandlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/labstack/echo/v4"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/git"
	"github.com/opensvc/oc3/util/logkey"
)

// complianceHistoryRepo is the one repository of the compliance history, under
// server.directories.compliance, holding the export in compliance.json.
const complianceHistoryRepo = "export"

// complianceHistoryMu serializes the commits of the compliance history: two
// designers committing at once share the repository and its index.
var complianceHistoryMu sync.Mutex

func complianceTrack() git.Track {
	return git.Track{Dir: viper.GetString("server.directories.compliance"), File: "compliance.json"}
}

// GetComplianceHistory handles GET /compliance/history: the versions of the
// compliance export, newest first.
func (a *Api) GetComplianceHistory(c echo.Context, params server.GetComplianceHistoryParams) error {
	log := echolog.GetLogHandler(c, "GetComplianceHistory")
	if err := checkCompManager(c); err != nil {
		return httpProblem(c, err)
	}
	limit := 50
	if params.Limit != nil && *params.Limit > 0 {
		limit = min(*params.Limit, 300)
	}
	track := complianceTrack()
	if params.Object != nil && *params.Object != "" {
		kind, id, ok := parseComplianceObject(*params.Object)
		if !ok {
			return JSONProblemf(c, http.StatusBadRequest, "object is kind:id, kind one of ruleset, moduleset, filterset")
		}
		versions, err := complianceObjectVersions(track, kind, id, limit)
		if err != nil {
			log.Error("cannot read the compliance history", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot read the compliance history")
		}
		return c.JSON(http.StatusOK, map[string]any{"data": versions})
	}
	entries, err := track.Log(complianceHistoryRepo, limit)
	if err != nil {
		log.Error("cannot read the compliance history", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the compliance history")
	}
	return c.JSON(http.StatusOK, map[string]any{"data": complianceVersions(entries)})
}

// PostComplianceHistory handles POST /compliance/history: records the current
// compliance export as a version, authored by the caller, with their message.
func (a *Api) PostComplianceHistory(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostComplianceHistory")
	if err := checkCompManager(c); err != nil {
		return httpProblem(c, err)
	}
	var body server.PostComplianceHistoryJSONRequestBody
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	message := strings.TrimSpace(body.Message)
	if message == "" {
		return JSONProblemf(c, http.StatusBadRequest, "message is mandatory")
	}
	source := ""
	if body.Source != nil {
		source = string(*body.Source)
	}
	complianceHistoryMu.Lock()
	defer complianceHistoryMu.Unlock()
	commit, changed, err := a.recordComplianceVersion(c, message, source)
	if err != nil {
		log.Error("cannot record the compliance version", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot record the compliance version")
	}
	return c.JSON(http.StatusOK, map[string]any{"commit": commit, "changed": changed})
}

// recordComplianceVersion records the compliance export, as the collector holds
// it now, as a version of the compliance history, authored by the caller, with
// message and the source of the changes ("designer", "elsewhere", or none). It
// tells whether a version was recorded: none is when nothing changed. The caller
// holds complianceHistoryMu.
func (a *Api) recordComplianceVersion(c echo.Context, message, source string) (string, bool, error) {
	ctx := c.Request().Context()
	subject, _, _ := strings.Cut(message, "\n")
	// What the version records, kept as a trailer: the history tells the
	// designer's commits from the changes made elsewhere without reading prose.
	if source != "" {
		message += "\n\n" + complianceSourceTrailer + source
	}
	caller, err := a.formCaller(ctx, c)
	if err != nil {
		return "", false, err
	}
	export, err := a.ODB.ExportCompAll(ctx)
	if err != nil {
		return "", false, fmt.Errorf("export the compliance objects: %w", err)
	}
	// Indented, one property per line: a diff between versions reads line by line.
	// No HTML escaping: a filter reads ">=", not "\u003e=".
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(export); err != nil {
		return "", false, fmt.Errorf("encode the compliance export: %w", err)
	}
	commit, changed, err := complianceTrack().CommitMessage(complianceHistoryRepo, buf.String(), caller.author(), message)
	if err != nil {
		return "", false, err
	}
	if changed {
		a.compLog(c, "compliance.history.commit", "recorded the compliance version %(commit)s: %(subject)s",
			map[string]any{"commit": shortCommit(commit), "subject": subject}, "", "")
	}
	return commit, changed, nil
}

// shortCommit is the abbreviated commit id git prints.
func shortCommit(id string) string {
	if len(id) > 7 {
		return id[:7]
	}
	return id
}

// complianceSourceTrailer starts the trailer of a version saying what it records.
const complianceSourceTrailer = "Compliance-Source: "

// complianceVersion is a version of the compliance export, as the API returns it.
type complianceVersion struct {
	git.LogEntry
	Source string `json:"source"`
}

// complianceVersions turns the commits of the history into versions, their
// source trailer taken out of the body into its own field.
func complianceVersions(entries []git.LogEntry) []complianceVersion {
	out := make([]complianceVersion, 0, len(entries))
	for _, e := range entries {
		v := complianceVersion{LogEntry: e}
		lines := strings.Split(e.Body, "\n")
		kept := lines[:0]
		for _, line := range lines {
			if value, ok := strings.CutPrefix(line, complianceSourceTrailer); ok {
				v.Source = strings.TrimSpace(value)
				continue
			}
			kept = append(kept, line)
		}
		v.Body = strings.TrimSpace(strings.Join(kept, "\n"))
		out = append(out, v)
	}
	return out
}

// parseComplianceObject reads "ruleset:12": the kind of a compliance object and
// its id in the export.
func parseComplianceObject(s string) (kind string, id int64, ok bool) {
	kind, rest, found := strings.Cut(s, ":")
	if !found || (kind != "ruleset" && kind != "moduleset" && kind != "filterset") {
		return "", 0, false
	}
	id, err := strconv.ParseInt(rest, 10, 64)
	if err != nil {
		return "", 0, false
	}
	return kind, id, true
}

// complianceObjectOf returns the object of an export by kind and id, as its JSON,
// empty when the export does not hold it.
func complianceObjectOf(content, kind string, id int64) (string, error) {
	var export struct {
		Filtersets []json.RawMessage `json:"filtersets"`
		Rulesets   []json.RawMessage `json:"rulesets"`
		Modulesets []json.RawMessage `json:"modulesets"`
	}
	if err := json.Unmarshal([]byte(content), &export); err != nil {
		return "", err
	}
	objects := map[string][]json.RawMessage{
		"filterset": export.Filtersets, "ruleset": export.Rulesets, "moduleset": export.Modulesets,
	}[kind]
	for _, raw := range objects {
		var head struct {
			ID int64 `json:"id"`
		}
		if err := json.Unmarshal(raw, &head); err == nil && head.ID == id {
			return string(raw), nil
		}
	}
	return "", nil
}

// complianceObjectVersions returns, newest first and at most limit, the versions
// among the last 300 in which the object changed: each one is compared with the
// version before it.
func complianceObjectVersions(track git.Track, kind string, id int64, limit int) ([]complianceVersion, error) {
	entries, err := track.Log(complianceHistoryRepo, 300)
	if err != nil {
		return nil, err
	}
	matching := []git.LogEntry{}
	previous := ""
	// Oldest first, each version read once and compared with the one before.
	for i := len(entries) - 1; i >= 0; i-- {
		content, err := track.FileAt(complianceHistoryRepo, entries[i].ID)
		if err != nil {
			return nil, err
		}
		object, err := complianceObjectOf(content, kind, id)
		if err != nil {
			return nil, err
		}
		if object != previous {
			matching = append(matching, entries[i])
		}
		previous = object
	}
	slices.Reverse(matching)
	if len(matching) > limit {
		matching = matching[:limit]
	}
	return complianceVersions(matching), nil
}

// GetComplianceVersion handles GET /compliance/history/{commit}: a version, the
// export it recorded, the one before and the diff between them.
func (a *Api) GetComplianceVersion(c echo.Context, commit string) error {
	log := echolog.GetLogHandler(c, "GetComplianceVersion")
	if err := checkCompManager(c); err != nil {
		return httpProblem(c, err)
	}
	track := complianceTrack()
	id, ok := track.ResolveCommit(complianceHistoryRepo, commit)
	if !ok {
		return JSONProblemf(c, http.StatusNotFound, "compliance version %s not found", commit)
	}
	internal := func(err error) error {
		log.Error("cannot read the compliance version", "commit", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the compliance version")
	}
	entries, err := track.Log(complianceHistoryRepo, 1, id)
	if err != nil || len(entries) == 0 {
		return internal(err)
	}
	content, err := track.FileAt(complianceHistoryRepo, id)
	if err != nil {
		return internal(err)
	}
	out := map[string]any{
		"version": complianceVersions(entries)[0],
		"export":  json.RawMessage(content),
	}
	parent, hasParent := track.Parent(complianceHistoryRepo, id)
	if hasParent {
		previous, err := track.FileAt(complianceHistoryRepo, parent)
		if err != nil {
			return internal(err)
		}
		out["previous"] = json.RawMessage(previous)
		out["previous_id"] = parent
	}
	diff, err := track.DiffFile(complianceHistoryRepo, parent, id)
	if err != nil {
		return internal(err)
	}
	out["diff"] = diff
	return c.JSON(http.StatusOK, out)
}
