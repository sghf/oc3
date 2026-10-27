package serverhandlers

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/labstack/echo/v4"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/git"
	"github.com/opensvc/oc3/util/logkey"
)

// compModules is the git repository of the compliance modules.
func compModules() git.CompModules {
	return git.CompModules{Dir: viper.GetString("server.directories.compliance_modules")}
}

// requireUserOrNode refuses the callers neither a user nor a node.
func requireUserOrNode(c echo.Context) error {
	if !IsAuthByUser(c) && !IsAuthByNode(c) {
		return httpErrorf(http.StatusUnauthorized, "user or node authentication required")
	}
	return nil
}

func toComplianceModuleCommit(c *git.ModuleCommit) *server.ComplianceModuleCommit {
	if c == nil {
		return nil
	}
	return &server.ComplianceModuleCommit{Id: c.ID, Author: c.Author, Date: c.Date, Subject: c.Subject}
}

func toComplianceModule(m git.Module) server.ComplianceModule {
	return server.ComplianceModule{
		Name:     m.Name,
		Path:     m.Path,
		Order:    m.Order,
		Kind:     server.ComplianceModuleKind(m.Kind),
		Entry:    m.Entry,
		Runnable: m.Runnable,
		Issue:    server.ComplianceModuleIssue(m.Issue),
		Changed:  toComplianceModuleCommit(m.Changed),
	}
}

// moduleDetail answers a module with the modulesets naming it.
func (a *Api) moduleDetail(c echo.Context, log *slog.Logger, status int, d *git.ModuleDetail) error {
	modsets, err := a.ODB.ModulesetsNamingModule(c.Request().Context(), d.Name)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot list the modulesets of the module", err))
	}
	out := server.ComplianceModuleDetail{
		Name:       d.Name,
		Path:       d.Path,
		Order:      d.Order,
		Kind:       server.ComplianceModuleDetailKind(d.Kind),
		Entry:      d.Entry,
		Runnable:   d.Runnable,
		Issue:      server.ComplianceModuleIssue(d.Issue),
		Changed:    toComplianceModuleCommit(d.Changed),
		Files:      make([]server.ComplianceModuleFile, len(d.Files)),
		History:    make([]server.ComplianceModuleCommit, len(d.History)),
		Modulesets: make([]server.ComplianceModuleModuleset, len(modsets)),
	}
	for i, f := range d.Files {
		out.Files[i] = server.ComplianceModuleFile{Path: f.Path, Size: f.Size, Executable: f.Executable}
	}
	for i := range d.History {
		out.History[i] = *toComplianceModuleCommit(&d.History[i])
	}
	for i, m := range modsets {
		out.Modulesets[i] = server.ComplianceModuleModuleset{Id: int(m.ID), Name: m.Name}
	}
	return c.JSON(status, out)
}

// moduleWriteError answers an error of a change of the modules.
func moduleWriteError(c echo.Context, log *slog.Logger, name string, err error) error {
	var he *httpError
	switch {
	case errors.As(err, &he):
		return httpProblem(c, err)
	case errors.Is(err, git.ErrInvalidModule):
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "%s", strings.TrimPrefix(err.Error(), git.ErrInvalidModule.Error()+": ")))
	case errors.Is(err, git.ErrModuleNotFound):
		return httpProblem(c, httpErrorf(http.StatusNotFound, "no such compliance module or file: %s", name))
	case errors.Is(err, git.ErrModuleExists):
		return httpProblem(c, httpErrorf(http.StatusConflict, "a compliance module named %s exists", name))
	case errors.Is(err, git.ErrModulesChanged):
		return httpProblem(c, httpErrorf(http.StatusConflict, "the compliance modules changed meanwhile, try again"))
	case errors.Is(err, git.ErrModuleStale):
		return httpProblem(c, httpErrorf(http.StatusPreconditionFailed, "the module %s changed since it was read: reload it", name))
	default:
		return httpProblem(c, httpInternal(log, "cannot change the compliance module", err))
	}
}

// moduleAudit writes a change of a module in the audit log, as the changes of
// the modulesets and rulesets.
func (a *Api) moduleAudit(c echo.Context, log *slog.Logger, action, format string, dict map[string]any) {
	userEmail, _ := c.Get(XUserEmail).(string)
	if err := a.ODB.Log(c.Request().Context(), cdb.LogEntry{
		Action: action, User: userEmail, Fmt: format, Dict: dict, Level: "info",
	}); err != nil {
		log.Warn("cannot write the audit log", logkey.Error, err)
	}
}

// moduleAuthor returns the author of a commit made by the caller, a
// CompManager.
func (a *Api) moduleAuthor(c echo.Context) (string, error) {
	caller, err := a.formCaller(c.Request().Context(), c)
	if err != nil {
		return "", err
	}
	return caller.author(), nil
}

func messageOr(message *string, fallback string) string {
	if message != nil && strings.TrimSpace(*message) != "" {
		return strings.TrimSpace(*message)
	}
	return fallback
}

// decodeContent reads the content of a file as sent, text or base64.
func decodeContent(content string, encoding *string) ([]byte, error) {
	if encoding == nil || *encoding == "" || *encoding == "text" {
		return []byte(content), nil
	}
	if *encoding != "base64" {
		return nil, httpErrorf(http.StatusBadRequest, "unknown encoding %q: text or base64", *encoding)
	}
	b, err := base64.StdEncoding.DecodeString(content)
	if err != nil {
		return nil, httpErrorf(http.StatusBadRequest, "invalid base64 content: %s", err)
	}
	return b, nil
}

// readModuleArchive reads the "archive" part of a multipart request.
func readModuleArchive(c echo.Context) (map[string]git.ModuleBlob, error) {
	c.Request().Body = http.MaxBytesReader(c.Response(), c.Request().Body, git.MaxModuleArchiveSize+1024*1024)
	header, err := c.FormFile("archive")
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, httpErrorf(http.StatusRequestEntityTooLarge, "the archive is larger than the %d bytes allowed", git.MaxModuleArchiveSize)
		}
		return nil, httpErrorf(http.StatusBadRequest, "a multipart \"archive\" part is expected: %s", err)
	}
	part, err := header.Open()
	if err != nil {
		return nil, httpErrorf(http.StatusBadRequest, "cannot read the archive: %s", err)
	}
	defer func() { _ = part.Close() }()
	data, err := io.ReadAll(io.LimitReader(part, git.MaxModuleArchiveSize+1))
	if err != nil {
		return nil, httpErrorf(http.StatusBadRequest, "cannot read the archive: %s", err)
	}
	if len(data) > git.MaxModuleArchiveSize {
		return nil, httpErrorf(http.StatusRequestEntityTooLarge, "the archive is larger than the %d bytes allowed", git.MaxModuleArchiveSize)
	}
	return git.ParseModuleArchive(data)
}

// GetComplianceModules handles GET /compliance/modules.
func (a *Api) GetComplianceModules(c echo.Context) error {
	log := echolog.GetLogHandler(c, "GetComplianceModules")
	if err := requireUserOrNode(c); err != nil {
		return httpProblem(c, err)
	}
	mods, err := compModules().List()
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot list the compliance modules", err))
	}
	data := make([]server.ComplianceModule, len(mods))
	for i, m := range mods {
		data[i] = toComplianceModule(m)
	}
	return c.JSON(http.StatusOK, server.ComplianceModuleList{Data: data})
}

// GetComplianceModule handles GET /compliance/modules/{name}.
func (a *Api) GetComplianceModule(c echo.Context, name string) error {
	log := echolog.GetLogHandler(c, "GetComplianceModule")
	if err := requireUserOrNode(c); err != nil {
		return httpProblem(c, err)
	}
	d, err := compModules().Get(name)
	if errors.Is(err, git.ErrModuleNotFound) {
		return httpProblem(c, httpErrorf(http.StatusNotFound, "no compliance module named %s", name))
	} else if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the compliance module", err))
	}
	return a.moduleDetail(c, log, http.StatusOK, d)
}

// GetComplianceModuleFile handles GET /compliance/modules/{name}/file.
func (a *Api) GetComplianceModuleFile(c echo.Context, name string, params server.GetComplianceModuleFileParams) error {
	log := echolog.GetLogHandler(c, "GetComplianceModuleFile")
	if err := requireUserOrNode(c); err != nil {
		return httpProblem(c, err)
	}
	rel := ""
	if params.Path != nil {
		rel = *params.Path
	}
	content, truncated, err := compModules().ReadFile(name, rel)
	if errors.Is(err, git.ErrModuleNotFound) {
		return httpProblem(c, httpErrorf(http.StatusNotFound, "no file %q in a compliance module named %s", rel, name))
	} else if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the compliance module file", err))
	}
	out := server.ComplianceModuleFileContent{Path: rel, Truncated: truncated}
	// A binary file shows as such: its bytes would not survive the JSON.
	if bytes.IndexByte(content, 0) >= 0 || !utf8.Valid(content) {
		out.Binary = true
	} else {
		out.Content = string(content)
	}
	return c.JSON(http.StatusOK, out)
}

// PostComplianceModules handles POST /compliance/modules: a script module, or
// a directory module from its files.
func (a *Api) PostComplianceModules(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostComplianceModules")
	if err := checkCompManager(c); err != nil {
		return httpProblem(c, err)
	}
	var body server.ComplianceModuleCreate
	if err := c.Bind(&body); err != nil {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "invalid request body: %s", err))
	}
	author, err := a.moduleAuthor(c)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the user", err))
	}
	message := messageOr(body.Message, "add "+body.Name)
	var d *git.ModuleDetail
	if body.Kind != nil && *body.Kind == server.ComplianceModuleCreateKindDirectory {
		if body.Files == nil {
			return httpProblem(c, httpErrorf(http.StatusBadRequest, "a directory module needs its files"))
		}
		files := make(map[string]git.ModuleBlob, len(*body.Files))
		for _, f := range *body.Files {
			var encoding *string
			if f.Encoding != nil {
				e := string(*f.Encoding)
				encoding = &e
			}
			content, err := decodeContent(f.Content, encoding)
			if err != nil {
				return httpProblem(c, err)
			}
			if _, dup := files[f.Path]; dup {
				return httpProblem(c, httpErrorf(http.StatusBadRequest, "%s is given twice", f.Path))
			}
			files[f.Path] = git.ModuleBlob{Content: content, Executable: f.Executable != nil && *f.Executable}
		}
		d, err = compModules().CreateDirectory(body.Name, body.Order, files, author, message)
	} else {
		if body.Content == nil {
			return httpProblem(c, httpErrorf(http.StatusBadRequest, "a script module needs its content"))
		}
		d, err = compModules().CreateScript(body.Name, body.Order, []byte(*body.Content), author, message)
	}
	if err != nil {
		return moduleWriteError(c, log, body.Name, err)
	}
	a.moduleAudit(c, log, "compliance.module.add", "added module %(module)s", map[string]any{"module": d.Path})
	return a.moduleDetail(c, log, http.StatusCreated, d)
}

// PostComplianceModulesImport handles POST /compliance/modules_import: a
// directory module from an archive.
func (a *Api) PostComplianceModulesImport(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostComplianceModulesImport")
	if err := checkCompManager(c); err != nil {
		return httpProblem(c, err)
	}
	files, err := readModuleArchive(c)
	if err != nil {
		return moduleWriteError(c, log, "", err)
	}
	name := strings.TrimSpace(c.FormValue("name"))
	order, err := strconv.Atoi(strings.TrimSpace(c.FormValue("order")))
	if err != nil {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "invalid order %q", c.FormValue("order")))
	}
	author, err := a.moduleAuthor(c)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the user", err))
	}
	message := c.FormValue("message")
	d, err := compModules().CreateDirectory(name, order, files, author, messageOr(&message, "import "+name))
	if err != nil {
		return moduleWriteError(c, log, name, err)
	}
	a.moduleAudit(c, log, "compliance.module.add", "imported module %(module)s", map[string]any{"module": d.Path})
	return a.moduleDetail(c, log, http.StatusCreated, d)
}

// PutComplianceModuleFile handles PUT /compliance/modules/{name}/file.
func (a *Api) PutComplianceModuleFile(c echo.Context, name string) error {
	log := echolog.GetLogHandler(c, "PutComplianceModuleFile")
	if err := checkCompManager(c); err != nil {
		return httpProblem(c, err)
	}
	var body server.ComplianceModuleFileWrite
	if err := c.Bind(&body); err != nil {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "invalid request body: %s", err))
	}
	var encoding *string
	if body.Encoding != nil {
		e := string(*body.Encoding)
		encoding = &e
	}
	content, err := decodeContent(body.Content, encoding)
	if err != nil {
		return httpProblem(c, err)
	}
	author, err := a.moduleAuthor(c)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the user", err))
	}
	label := body.Path
	if label == "" {
		label = "the script"
	}
	f := git.ModuleBlob{Content: content, Executable: body.Executable != nil && *body.Executable}
	d, err := compModules().PutFile(name, body.Base, body.Path, f, author, messageOr(body.Message, "update "+label))
	if err != nil {
		return moduleWriteError(c, log, name, err)
	}
	a.moduleAudit(c, log, "compliance.module.file.update", "updated %(file)s of module %(module)s",
		map[string]any{"module": d.Path, "file": label})
	return a.moduleDetail(c, log, http.StatusOK, d)
}

// DeleteComplianceModuleFile handles DELETE /compliance/modules/{name}/file.
func (a *Api) DeleteComplianceModuleFile(c echo.Context, name string, params server.DeleteComplianceModuleFileParams) error {
	log := echolog.GetLogHandler(c, "DeleteComplianceModuleFile")
	if err := checkCompManager(c); err != nil {
		return httpProblem(c, err)
	}
	author, err := a.moduleAuthor(c)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the user", err))
	}
	d, err := compModules().DeleteFile(name, params.Base, params.Path, author, messageOr(params.Message, "remove "+params.Path))
	if err != nil {
		return moduleWriteError(c, log, name, err)
	}
	a.moduleAudit(c, log, "compliance.module.file.delete", "removed %(file)s of module %(module)s",
		map[string]any{"module": d.Path, "file": params.Path})
	return a.moduleDetail(c, log, http.StatusOK, d)
}

// PostComplianceModuleMove handles POST /compliance/modules/{name}/move.
func (a *Api) PostComplianceModuleMove(c echo.Context, name string) error {
	log := echolog.GetLogHandler(c, "PostComplianceModuleMove")
	if err := checkCompManager(c); err != nil {
		return httpProblem(c, err)
	}
	var body server.ComplianceModuleFileMove
	if err := c.Bind(&body); err != nil {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "invalid request body: %s", err))
	}
	author, err := a.moduleAuthor(c)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the user", err))
	}
	d, err := compModules().MoveFile(name, body.Base, body.From, body.To, author,
		messageOr(body.Message, "move "+body.From+" to "+body.To))
	if err != nil {
		return moduleWriteError(c, log, name, err)
	}
	a.moduleAudit(c, log, "compliance.module.file.move", "moved %(from)s to %(to)s in module %(module)s",
		map[string]any{"module": d.Path, "from": body.From, "to": body.To})
	return a.moduleDetail(c, log, http.StatusOK, d)
}

// PostComplianceModuleConvert handles POST /compliance/modules/{name}/convert.
func (a *Api) PostComplianceModuleConvert(c echo.Context, name string) error {
	log := echolog.GetLogHandler(c, "PostComplianceModuleConvert")
	if err := checkCompManager(c); err != nil {
		return httpProblem(c, err)
	}
	var body server.ComplianceModuleChange
	if err := c.Bind(&body); err != nil {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "invalid request body: %s", err))
	}
	author, err := a.moduleAuthor(c)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the user", err))
	}
	d, err := compModules().ConvertToDirectory(name, body.Base, author, messageOr(body.Message, "convert to a directory"))
	if err != nil {
		return moduleWriteError(c, log, name, err)
	}
	a.moduleAudit(c, log, "compliance.module.convert", "converted module %(module)s to a directory",
		map[string]any{"module": d.Path})
	return a.moduleDetail(c, log, http.StatusOK, d)
}

// GetComplianceModuleArchive handles GET /compliance/modules/{name}/archive.
func (a *Api) GetComplianceModuleArchive(c echo.Context, name string) error {
	log := echolog.GetLogHandler(c, "GetComplianceModuleArchive")
	if err := requireUserOrNode(c); err != nil {
		return httpProblem(c, err)
	}
	data, entry, err := compModules().Export(name)
	if errors.Is(err, git.ErrModuleNotFound) {
		return httpProblem(c, httpErrorf(http.StatusNotFound, "no compliance module named %s", name))
	} else if err != nil {
		return httpProblem(c, httpInternal(log, "cannot export the compliance module", err))
	}
	c.Response().Header().Set(echo.HeaderContentDisposition, `attachment; filename="`+entry+`.tar.gz"`)
	return c.Blob(http.StatusOK, "application/gzip", data)
}

// PutComplianceModuleArchive handles PUT /compliance/modules/{name}/archive.
func (a *Api) PutComplianceModuleArchive(c echo.Context, name string) error {
	log := echolog.GetLogHandler(c, "PutComplianceModuleArchive")
	if err := checkCompManager(c); err != nil {
		return httpProblem(c, err)
	}
	files, err := readModuleArchive(c)
	if err != nil {
		return moduleWriteError(c, log, name, err)
	}
	author, err := a.moduleAuthor(c)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the user", err))
	}
	message := c.FormValue("message")
	d, err := compModules().ReplaceFiles(name, c.FormValue("base"), files, author, messageOr(&message, "replace from an archive"))
	if err != nil {
		return moduleWriteError(c, log, name, err)
	}
	a.moduleAudit(c, log, "compliance.module.replace", "replaced the files of module %(module)s from an archive",
		map[string]any{"module": d.Path})
	return a.moduleDetail(c, log, http.StatusOK, d)
}

// GetComplianceModulesArchive handles GET /compliance/modules_repo/current:
// the archive om node updatecomp installs.
func (a *Api) GetComplianceModulesArchive(c echo.Context) error {
	log := echolog.GetLogHandler(c, "GetComplianceModulesArchive")
	if err := requireUserOrNode(c); err != nil {
		return httpProblem(c, err)
	}
	m := compModules()
	head, err := m.Head()
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the compliance modules", err))
	}
	if head == "" {
		return httpProblem(c, httpErrorf(http.StatusNotFound, "no compliance module yet"))
	}
	// Built in memory before answering: an error then is still an error, not a
	// truncated archive the agent would install.
	var buf bytes.Buffer
	if head, err = m.Archive(&buf); err != nil {
		return httpProblem(c, httpInternal(log, "cannot archive the compliance modules", err))
	}
	short := head
	if len(short) > 12 {
		short = short[:12]
	}
	h := c.Response().Header()
	h.Set("X-Compliance-Modules-Commit", head)
	h.Set(echo.HeaderContentDisposition, `attachment; filename="compliance-`+short+`.tar.gz"`)
	return c.Blob(http.StatusOK, "application/gzip", buf.Bytes())
}

// PostComplianceModuleRename handles POST /compliance/modules/{name}/rename:
// the module gets another name or order, and the modulesets naming it follow.
func (a *Api) PostComplianceModuleRename(c echo.Context, name string) error {
	log := echolog.GetLogHandler(c, "PostComplianceModuleRename")
	if err := checkCompManager(c); err != nil {
		return httpProblem(c, err)
	}
	var body server.ComplianceModuleRename
	if err := c.Bind(&body); err != nil {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "invalid request body: %s", err))
	}
	author, err := a.moduleAuthor(c)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the user", err))
	}
	before, err := compModules().Get(name)
	if err != nil {
		return moduleWriteError(c, log, name, err)
	}
	d, err := compModules().RenameModule(name, body.Base, body.Name, body.Order, author,
		messageOr(body.Message, fmt.Sprintf("rename %s to S%02d-%s", before.Path, body.Order, body.Name)))
	if err != nil {
		return moduleWriteError(c, log, body.Name, err)
	}
	if d.Path == before.Path {
		return a.moduleDetail(c, log, http.StatusOK, d)
	}
	// After the commit, which refuses most: the modulesets name the module
	// under its new name.
	if body.Name != name {
		if _, err := a.ODB.RenameModulesetModule(c.Request().Context(), name, body.Name); err != nil {
			return httpProblem(c, httpInternal(log,
				"the module is renamed, but the modulesets naming it still name "+name, err))
		}
	}
	a.moduleAudit(c, log, "compliance.module.rename", "renamed module %(from)s to %(to)s",
		map[string]any{"from": before.Path, "to": d.Path})
	return a.moduleDetail(c, log, http.StatusOK, d)
}

// PostComplianceModuleMode handles POST /compliance/modules/{name}/mode.
func (a *Api) PostComplianceModuleMode(c echo.Context, name string) error {
	log := echolog.GetLogHandler(c, "PostComplianceModuleMode")
	if err := checkCompManager(c); err != nil {
		return httpProblem(c, err)
	}
	var body server.ComplianceModuleFileMode
	if err := c.Bind(&body); err != nil {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "invalid request body: %s", err))
	}
	author, err := a.moduleAuthor(c)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the user", err))
	}
	fallback := "make " + body.Path + " not executable"
	if body.Executable {
		fallback = "make " + body.Path + " executable"
	}
	d, err := compModules().SetExecutable(name, body.Base, body.Path, body.Executable, author, messageOr(body.Message, fallback))
	if err != nil {
		return moduleWriteError(c, log, name, err)
	}
	a.moduleAudit(c, log, "compliance.module.file.mode", "changed the mode of %(file)s of module %(module)s",
		map[string]any{"module": d.Path, "file": body.Path, "executable": body.Executable})
	return a.moduleDetail(c, log, http.StatusOK, d)
}

// GetComplianceModuleVersionDiff handles GET
// /compliance/modules/{name}/versions/{rev}/diff: what changed in the module
// since one of its versions.
func (a *Api) GetComplianceModuleVersionDiff(c echo.Context, name, rev string) error {
	log := echolog.GetLogHandler(c, "GetComplianceModuleVersionDiff")
	if err := requireUserOrNode(c); err != nil {
		return httpProblem(c, err)
	}
	d, err := compModules().DiffVersion(name, rev)
	switch {
	case errors.Is(err, git.ErrInvalidModule):
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "invalid revision %q", rev))
	case errors.Is(err, git.ErrModuleNotFound):
		return httpProblem(c, httpErrorf(http.StatusNotFound, "no version %s of a compliance module named %s", rev, name))
	case err != nil:
		return httpProblem(c, httpInternal(log, "cannot compare the compliance module", err))
	}
	out := server.ComplianceModuleVersionDiff{
		Version: *toComplianceModuleCommit(&d.Version),
		Files:   make([]server.ComplianceModuleFileDiff, len(d.Files)),
	}
	for i, f := range d.Files {
		out.Files[i] = server.ComplianceModuleFileDiff{
			Path:          f.Path,
			Status:        server.ComplianceModuleFileDiffStatus(f.Status),
			Binary:        f.Binary,
			WasExecutable: f.WasExecutable,
			IsExecutable:  f.IsExecutable,
			Added:         f.Added,
			Deleted:       f.Deleted,
			Diff:          f.Diff,
			Truncated:     f.Truncated,
		}
	}
	return c.JSON(http.StatusOK, out)
}

// PostComplianceModuleRestore handles POST /compliance/modules/{name}/restore.
func (a *Api) PostComplianceModuleRestore(c echo.Context, name string) error {
	log := echolog.GetLogHandler(c, "PostComplianceModuleRestore")
	if err := checkCompManager(c); err != nil {
		return httpProblem(c, err)
	}
	var body server.ComplianceModuleRestore
	if err := c.Bind(&body); err != nil {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "invalid request body: %s", err))
	}
	author, err := a.moduleAuthor(c)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the user", err))
	}
	short := body.Rev
	if len(short) > 12 {
		short = short[:12]
	}
	d, err := compModules().RestoreVersion(name, body.Base, body.Rev, author, messageOr(body.Message, "restore the version "+short))
	if err != nil {
		return moduleWriteError(c, log, name, err)
	}
	a.moduleAudit(c, log, "compliance.module.restore", "restored module %(module)s as of %(rev)s",
		map[string]any{"module": d.Path, "rev": body.Rev})
	return a.moduleDetail(c, log, http.StatusOK, d)
}
