package git

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// SysreportFile names a file of a sysreport as the API shows it: its kind, file
// or command, and its absolute path or command line (SysreportDisplay).
type SysreportFile struct {
	Kind string `json:"kind"`
	Path string `json:"path"`
}

// PurgeRequest is a purge queued for the scheduler: the files to remove from the
// sysreport history of the nodes, every node having a sysreport when NodeIDs is
// empty. Author is the email of the user who asked for it.
type PurgeRequest struct {
	ID        string          `json:"id"`
	NodeIDs   []string        `json:"node_ids"`
	Files     []SysreportFile `json:"files"`
	Author    string          `json:"author"`
	Requested string          `json:"requested"`
}

// PurgeStatusTTL is how long the status of a purge is kept once queued.
const PurgeStatusTTL = 7 * 24 * time.Hour

// PurgeStatus is the state of a purge: queued, running, done or failed, with
// what it did on each node.
type PurgeStatus struct {
	PurgeRequest
	State    string            `json:"state"`
	Started  string            `json:"started,omitempty"`
	Finished string            `json:"finished,omitempty"`
	Nodes    []PurgeNodeStatus `json:"nodes"`
	Error    string            `json:"error,omitempty"`
}

// PurgeNodeStatus is what a purge did on a node; Error when it failed there.
type PurgeNodeStatus struct {
	NodeID string `json:"node_id"`
	PurgeResult
	Error string `json:"error,omitempty"`
}

// PurgeResult tells what a purge removed from a node's sysreport.
type PurgeResult struct {
	// Paths are the paths of the repository the files resolved to.
	Paths []string `json:"paths"`
	// Rewritten is the number of revisions recreated without the files, Dropped
	// the number of revisions that held nothing else and are gone.
	Rewritten int `json:"rewritten"`
	Dropped   int `json:"dropped"`
	// Removed is the number of distinct contents of the files destroyed; Kept the
	// number still stored because another file of the node holds the same content.
	Removed int `json:"removed"`
	Kept    int `json:"kept"`
	// OldHead and NewHead are the last revision before and after the purge.
	OldHead string `json:"old_head,omitempty"`
	NewHead string `json:"new_head,omitempty"`
}

// ErrUnexpectedHistory is returned for a repository the purge does not rewrite:
// more than one branch, a tag, or a merge. The sysreport repositories hold a
// single straight branch, the one the scheduler commits to.
var ErrUnexpectedHistory = errors.New("unexpected history: a single branch without merge is expected")

// purgeGit runs git in the node's repository with literal pathspecs: a path is
// never read as a pattern or a pathspec magic. env is added to the environment.
func (s Sysreport) purgeGit(nodeID string, env []string, stdin string, args ...string) (string, error) {
	repo := filepath.Join(s.Dir, nodeID)
	cmd := command(append([]string{"--literal-pathspecs", "-c", "core.quotepath=off"}, args...)...)
	cmd.Dir = repo
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var out, errOut bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("git %s: %w: %s", args[0], err, strings.TrimSpace(errOut.String()))
	}
	return out.String(), nil
}

// Nodes returns the ids of the nodes having a sysreport.
func (s Sysreport) Nodes() ([]string, error) {
	entries, err := os.ReadDir(s.Dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var ids []string
	for _, e := range entries {
		if !e.IsDir() || !nodeIDPattern.MatchString(e.Name()) {
			continue
		}
		if _, err := os.Stat(filepath.Join(s.Dir, e.Name(), ".git")); err == nil {
			ids = append(ids, e.Name())
		}
	}
	return ids, nil
}

// resolve returns the paths of the repository, in its whole history, that the
// files designate: the encoding of a command line into a file name cannot be
// reversed reliably, so every path ever recorded is decoded and compared.
func (s Sysreport) resolve(nodeID string, files []SysreportFile) ([]string, error) {
	out, err := s.purgeGit(nodeID, nil, "", "log", "--all", "--format=", "--name-only", "--no-renames")
	if err != nil {
		return nil, err
	}
	want := make(map[SysreportFile]bool, len(files))
	for _, f := range files {
		want[f] = true
	}
	seen := map[string]bool{}
	var paths []string
	for _, line := range strings.Split(out, "\n") {
		if line == "" || seen[line] {
			continue
		}
		seen[line] = true
		display, kind := SysreportDisplay(line)
		if want[SysreportFile{Kind: kind, Path: display}] {
			paths = append(paths, line)
		}
	}
	sort.Strings(paths)
	return paths, nil
}

// commitInfo is what a recreated revision keeps of the original one.
type commitInfo struct {
	id, tree, parent string
	env              []string
	message          string
}

// history returns the revisions of the branch, oldest first, after checking it
// is a single straight branch.
func (s Sysreport) history(nodeID string) (branch string, commits []commitInfo, err error) {
	refs, err := s.purgeGit(nodeID, nil, "", "for-each-ref", "--format=%(refname)")
	if err != nil {
		return "", nil, err
	}
	refList := strings.Fields(refs)
	head, err := s.purgeGit(nodeID, nil, "", "symbolic-ref", "HEAD")
	if err != nil {
		return "", nil, err
	}
	branch = strings.TrimSpace(head)
	if len(refList) != 1 || refList[0] != branch {
		return "", nil, ErrUnexpectedHistory
	}
	merges, err := s.purgeGit(nodeID, nil, "", "rev-list", "--min-parents=2", branch)
	if err != nil {
		return "", nil, err
	}
	if strings.TrimSpace(merges) != "" {
		return "", nil, ErrUnexpectedHistory
	}
	// One record per revision, its fields NUL-separated, the records separated by
	// a byte no message holds (RS).
	out, err := s.purgeGit(nodeID, nil, "", "log", "--reverse", "--date=raw",
		"--format=%H%x00%T%x00%P%x00%an%x00%ae%x00%ad%x00%cn%x00%ce%x00%cd%x00%B%x1e", branch)
	if err != nil {
		return "", nil, err
	}
	for _, record := range strings.Split(out, "\x1e") {
		record = strings.TrimLeft(record, "\n")
		if record == "" {
			continue
		}
		f := strings.SplitN(record, "\x00", 10)
		if len(f) != 10 {
			return "", nil, fmt.Errorf("cannot read revision %q", record)
		}
		commits = append(commits, commitInfo{
			id: f[0], tree: f[1], parent: f[2],
			env: []string{
				"GIT_AUTHOR_NAME=" + f[3], "GIT_AUTHOR_EMAIL=" + f[4], "GIT_AUTHOR_DATE=" + f[5],
				"GIT_COMMITTER_NAME=" + f[6], "GIT_COMMITTER_EMAIL=" + f[7], "GIT_COMMITTER_DATE=" + f[8],
			},
			message: f[9],
		})
	}
	return branch, commits, nil
}

// blobs returns the ids of the contents the paths had in any revision.
func (s Sysreport) blobs(nodeID string, commits []commitInfo, paths []string) (map[string]bool, error) {
	ids := map[string]bool{}
	for _, c := range commits {
		out, err := s.purgeGit(nodeID, nil, "", append([]string{"ls-tree", "-r", c.id, "--"}, paths...)...)
		if err != nil {
			return nil, err
		}
		for _, line := range strings.Split(out, "\n") {
			if fields := strings.Fields(line); len(fields) >= 3 && fields[1] == "blob" {
				ids[fields[2]] = true
			}
		}
	}
	return ids, nil
}

// Purge removes the files from the whole history of the node's sysreport, for
// good: every revision is recreated without them, its dates, authors and message
// kept; a revision that held nothing else is dropped, the first one excepted;
// then the former revisions and every content no longer referenced are
// destroyed, and the files are removed from the working tree. The branch moves
// to the new history only once it is complete: a failure before leaves the
// repository as it was.
//
// The caller serializes the purge with the commits of the node's sysreport.
// Files the history does not hold purge nothing: the result has no paths.
func (s Sysreport) Purge(nodeID string, files []SysreportFile) (PurgeResult, error) {
	var result PurgeResult
	if !nodeIDPattern.MatchString(nodeID) {
		return result, ErrNoSysreport
	}
	repo := filepath.Join(s.Dir, nodeID)
	if _, err := os.Stat(filepath.Join(repo, ".git")); err != nil {
		return result, ErrNoSysreport
	}
	paths, err := s.resolve(nodeID, files)
	if err != nil || len(paths) == 0 {
		return result, err
	}
	result.Paths = paths

	branch, commits, err := s.history(nodeID)
	if err != nil {
		return result, err
	}
	result.OldHead = commits[len(commits)-1].id
	removed, err := s.blobs(nodeID, commits, paths)
	if err != nil {
		return result, err
	}

	// The trees are built in an index of their own: the one of the repository,
	// which the scheduler commits from, is left alone until the end.
	index, err := os.CreateTemp(filepath.Join(repo, ".git"), "purge-index-")
	if err != nil {
		return result, err
	}
	indexPath := index.Name()
	_ = index.Close()
	defer func() { _ = os.Remove(indexPath) }()
	indexEnv := []string{"GIT_INDEX_FILE=" + indexPath}

	// newParent is the revision the next one is recreated on, and its tree.
	newParent, newParentTree := "", ""
	touched := false
	for i, c := range commits {
		held, err := s.purgeGit(nodeID, nil, "", append([]string{"ls-tree", "-r", "--name-only", c.id, "--"}, paths...)...)
		if err != nil {
			return result, err
		}
		if !touched && strings.TrimSpace(held) == "" {
			// Before the first revision holding the files: kept as it is.
			newParent, newParentTree = c.id, c.tree
			continue
		}
		touched = true
		tree := c.tree
		if strings.TrimSpace(held) != "" {
			if _, err := s.purgeGit(nodeID, indexEnv, "", "read-tree", c.id); err != nil {
				return result, err
			}
			// --force: the index of an older revision differs from the working
			// tree and from HEAD, which git rm otherwise refuses; --cached keeps the
			// working tree out of it.
			if _, err := s.purgeGit(nodeID, indexEnv, "", append([]string{"rm", "-q", "-r", "--cached", "--force", "--ignore-unmatch", "--"}, paths...)...); err != nil {
				return result, err
			}
			out, err := s.purgeGit(nodeID, indexEnv, "", "write-tree")
			if err != nil {
				return result, err
			}
			tree = strings.TrimSpace(out)
		}
		// A revision whose files were all purged: nothing left to record.
		if i > 0 && tree == newParentTree {
			result.Dropped++
			continue
		}
		args := []string{"commit-tree", tree, "-F", "-"}
		if newParent != "" {
			args = append(args, "-p", newParent)
		}
		out, err := s.purgeGit(nodeID, c.env, c.message, args...)
		if err != nil {
			return result, err
		}
		newParent, newParentTree = strings.TrimSpace(out), tree
		result.Rewritten++
	}
	result.NewHead = newParent

	// The new history is complete: the branch moves to it, if nothing committed
	// meanwhile.
	if _, err := s.purgeGit(nodeID, nil, "", "update-ref", "-m", "sysreport purge", branch, result.NewHead, result.OldHead); err != nil {
		return result, err
	}
	// The files leave the working tree, and the index of the repository follows
	// the new last revision: the next commit does not record them again.
	for _, p := range paths {
		target := filepath.Join(repo, filepath.FromSlash(p))
		if rel, err := filepath.Rel(repo, target); err != nil || strings.HasPrefix(rel, "..") || strings.HasPrefix(rel, ".git") {
			continue
		}
		if err := os.RemoveAll(target); err != nil {
			return result, err
		}
		// The directories left empty go too, up to the root of the repository.
		for dir := filepath.Dir(target); dir != repo && strings.HasPrefix(dir, repo+string(filepath.Separator)); dir = filepath.Dir(dir) {
			if os.Remove(dir) != nil {
				break
			}
		}
	}
	if _, err := s.purgeGit(nodeID, nil, "", "read-tree", result.NewHead); err != nil {
		return result, err
	}
	// The former revisions are destroyed with every content they alone held: no
	// record of the former position of the branch is kept.
	if _, err := s.purgeGit(nodeID, nil, "", "reflog", "expire", "--expire=now", "--expire-unreachable=now", "--all"); err != nil {
		return result, err
	}
	if _, err := s.purgeGit(nodeID, nil, "", "gc", "--prune=now", "--quiet"); err != nil {
		return result, err
	}

	// Proof: the files are gone from the history, and a content still stored is
	// one another file of the node holds.
	left, err := s.purgeGit(nodeID, nil, "", append([]string{"log", "--all", "--format=%H", "--"}, paths...)...)
	if err != nil {
		return result, err
	}
	if strings.TrimSpace(left) != "" {
		return result, fmt.Errorf("purge of %s: the history still holds the files", nodeID)
	}
	for id := range removed {
		if _, err := s.purgeGit(nodeID, nil, "", "cat-file", "-e", id); err == nil {
			result.Kept++
		} else {
			result.Removed++
		}
	}
	return result, nil
}
