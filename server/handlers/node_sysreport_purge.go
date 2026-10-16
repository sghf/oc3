package serverhandlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/git"
	"github.com/opensvc/oc3/util/logkey"
)

const (
	sysreportPurgeMaxFiles = 50
	sysreportPurgeMaxPath  = 4096
)

// cleanPurgeFiles checks the files of a purge, trimmed and without duplicates.
func cleanPurgeFiles(raw []server.SysreportPurgeFile) ([]git.SysreportFile, error) {
	if len(raw) == 0 {
		return nil, errors.New("files: at least one file is expected")
	}
	if len(raw) > sysreportPurgeMaxFiles {
		return nil, errors.New("files: at most 50 files at once")
	}
	seen := map[git.SysreportFile]bool{}
	var files []git.SysreportFile
	for _, f := range raw {
		file := git.SysreportFile{Kind: string(f.Kind), Path: strings.TrimSpace(f.Path)}
		switch {
		case file.Kind != "file" && file.Kind != "command":
			return nil, errors.New("files: kind is file or command")
		case file.Path == "":
			return nil, errors.New("files: a path is empty")
		case utf8.RuneCountInString(file.Path) > sysreportPurgeMaxPath || strings.ContainsRune(file.Path, 0):
			return nil, errors.New("files: a path is too long or holds a NUL")
		case file.Kind == "file" && !strings.HasPrefix(file.Path, "/"):
			return nil, errors.New("files: the path of a file is absolute")
		}
		if !seen[file] {
			seen[file] = true
			files = append(files, file)
		}
	}
	return files, nil
}

// purgeStatusResponse turns the status the scheduler keeps into the API's.
func purgeStatusResponse(st git.PurgeStatus) server.SysreportPurgeResponse {
	files := make([]server.SysreportPurgeFile, len(st.Files))
	for i, f := range st.Files {
		files[i] = server.SysreportPurgeFile{Kind: server.SysreportPurgeFileKind(f.Kind), Path: f.Path}
	}
	nodes := make([]server.SysreportPurgeNode, len(st.Nodes))
	for i, n := range st.Nodes {
		paths := n.Paths
		if paths == nil {
			paths = []string{}
		}
		nodes[i] = server.SysreportPurgeNode{
			NodeId: n.NodeID, Paths: paths, Rewritten: n.Rewritten, Dropped: n.Dropped,
			Removed: n.Removed, Kept: n.Kept,
		}
		if n.Error != "" {
			nodes[i].Error = &n.Error
		}
	}
	nodeIDs := st.NodeIDs
	if nodeIDs == nil {
		nodeIDs = []string{}
	}
	out := server.SysreportPurge{
		Id: st.ID, State: server.SysreportPurgeState(st.State), NodeIds: nodeIDs, Files: files,
		Author: st.Author, Requested: st.Requested, Nodes: nodes,
	}
	if st.Started != "" {
		out.Started = &st.Started
	}
	if st.Finished != "" {
		out.Finished = &st.Finished
	}
	if st.Error != "" {
		out.Error = &st.Error
	}
	return server.SysreportPurgeResponse{Data: out}
}

// PostNodeSysreportPurge handles POST /nodes/{node_id}/sysreport/purge: the purge
// is queued for the scheduler, the only writer of the sysreport repositories.
func (a *Api) PostNodeSysreportPurge(c echo.Context, nodeId string) error {
	log := echolog.GetLogHandler(c, "PostNodeSysreportPurge")
	if err := requireManager(c); err != nil {
		return err
	}
	var body server.PostNodeSysreportPurgeJSONRequestBody
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	files, err := cleanPurgeFiles(body.Files)
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	ctx := c.Request().Context()
	if _, found, err := a.ODB.NodeTeamResponsible(ctx, nodeId); err != nil {
		log.Error("cannot read node", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot read node")
	} else if !found {
		return JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}
	if a.Redis == nil {
		return JSONProblem(c, http.StatusServiceUnavailable, "the purge queue is not available")
	}

	allNodes := body.AllNodes != nil && *body.AllNodes
	author, _ := c.Get(XUserEmail).(string)
	req := git.PurgeRequest{
		ID: uuid.NewString(), NodeIDs: []string{nodeId}, Files: files, Author: author,
		Requested: time.Now().Format(time.RFC3339),
	}
	if allNodes {
		req.NodeIDs = []string{}
	}
	status := git.PurgeStatus{PurgeRequest: req, State: "queued", Nodes: []git.PurgeNodeStatus{}}
	statusJSON, _ := json.Marshal(status)
	reqJSON, _ := json.Marshal(req)
	if err := a.Redis.Set(ctx, cachekeys.SysreportPurgePrefix+req.ID, statusJSON, git.PurgeStatusTTL).Err(); err != nil {
		log.Error("cannot save the purge status", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot queue the purge")
	}
	if err := a.Redis.RPush(ctx, cachekeys.SysreportPurgeQ, reqJSON).Err(); err != nil {
		log.Error("cannot queue the purge", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot queue the purge")
	}

	paths := make([]string, len(files))
	for i, f := range files {
		paths[i] = f.Path
	}
	entry := cdb.LogEntry{
		Action: "sysreport.purge.request", User: author, Level: "info",
		Fmt:  "asked to purge %(files)s from the sysreport history of %(scope)s",
		Dict: map[string]any{"files": strings.Join(paths, ", "), "scope": "this node", "purge_id": req.ID},
	}
	if allNodes {
		entry.Dict["scope"] = "every node"
	}
	if id, err := uuid.Parse(nodeId); err == nil {
		entry.NodeID = &id
	}
	if err := a.ODB.Log(ctx, entry); err != nil {
		log.Error("cannot write audit log", logkey.Error, err)
	}
	return c.JSON(http.StatusAccepted, purgeStatusResponse(status))
}

// GetSysreportPurge handles GET /sysreport/purges/{purge_id}.
func (a *Api) GetSysreportPurge(c echo.Context, purgeId string) error {
	log := echolog.GetLogHandler(c, "GetSysreportPurge")
	if err := requireManager(c); err != nil {
		return err
	}
	if _, err := uuid.Parse(purgeId); err != nil {
		return JSONProblemf(c, http.StatusNotFound, "purge %s not found", purgeId)
	}
	if a.Redis == nil {
		return JSONProblem(c, http.StatusServiceUnavailable, "the purge queue is not available")
	}
	raw, err := a.Redis.Get(c.Request().Context(), cachekeys.SysreportPurgePrefix+purgeId).Result()
	if errors.Is(err, redis.Nil) {
		return JSONProblemf(c, http.StatusNotFound, "purge %s not found", purgeId)
	}
	if err != nil {
		log.Error("cannot read the purge status", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot read the purge status")
	}
	var status git.PurgeStatus
	if err := json.Unmarshal([]byte(raw), &status); err != nil {
		log.Error("cannot read the purge status", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot read the purge status")
	}
	return c.JSON(http.StatusOK, purgeStatusResponse(status))
}
