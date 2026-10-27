package git

import (
	"strings"
)

// ModuleFileDiff is the change of a file of a module between a version and
// the current one.
type ModuleFileDiff struct {
	// Path is relative to the module, "" for the script of a script module.
	Path string `json:"path"`
	// Status is "added" (in the current version only), "removed" (in the
	// earlier one only) or "modified".
	Status string `json:"status"`
	Binary bool   `json:"binary"`
	// Executable before and now, for a file whose mode changed; nil otherwise.
	WasExecutable *bool `json:"was_executable,omitempty"`
	IsExecutable  *bool `json:"is_executable,omitempty"`
	Added         int   `json:"added"`
	Deleted       int   `json:"deleted"`
	// Diff is the unified diff from the earlier version to the current one,
	// its hunks only.
	Diff      string `json:"diff"`
	Truncated bool   `json:"truncated"`
}

// ModuleVersionDiff is what changed in a module since one of its versions.
type ModuleVersionDiff struct {
	Version ModuleCommit     `json:"version"`
	Files   []ModuleFileDiff `json:"files"`
}

// moduleRevision checks rev names a commit of the branch, and returns its id.
func (m CompModules) moduleRevision(head, rev string) (string, error) {
	if !ValidRev(rev) {
		return "", invalidf("invalid revision %q", rev)
	}
	out, err := m.git(nil, nil, "rev-parse", "--verify", "--quiet", rev+"^{commit}")
	if err != nil {
		return "", ErrModuleNotFound
	}
	id := strings.TrimSpace(out)
	// A commit of the history of the modules, not any object of the store.
	if _, err := m.git(nil, nil, "merge-base", "--is-ancestor", id, head); err != nil {
		return "", ErrModuleNotFound
	}
	return id, nil
}

func (m CompModules) commitOf(id string) (ModuleCommit, error) {
	out, err := m.git(nil, nil, "log", "-1", "--format=%H%x00%an <%ae>%x00%aI%x00%s", id)
	if err != nil {
		return ModuleCommit{}, err
	}
	fields := strings.SplitN(strings.TrimSpace(out), "\x00", 4)
	if len(fields) != 4 {
		return ModuleCommit{}, ErrModuleNotFound
	}
	return ModuleCommit{ID: fields[0], Author: fields[1], Date: fields[2], Subject: fields[3]}, nil
}

// DiffVersion returns what changed in a module since its version rev, a
// commit of its history: file by file, from rev to the current version.
func (m CompModules) DiffVersion(name, rev string) (*ModuleVersionDiff, error) {
	head, err := m.head()
	if err != nil {
		return nil, err
	}
	if head == "" {
		return nil, ErrModuleNotFound
	}
	mods, err := m.find(head, name)
	if err != nil {
		return nil, err
	}
	if len(mods) == 0 {
		return nil, ErrModuleNotFound
	}
	mod := mods[0]
	id, err := m.moduleRevision(head, rev)
	if err != nil {
		return nil, err
	}
	version, err := m.commitOf(id)
	if err != nil {
		return nil, err
	}
	out, err := m.git(nil, nil, "diff", "--no-color", "--no-renames", "--no-ext-diff", id, head, "--", mod.Path)
	if err != nil {
		return nil, err
	}
	return &ModuleVersionDiff{Version: version, Files: parseModuleDiff(out, mod.Path)}, nil
}

// parseModuleDiff reads the patch of git diff, its paths made relative to the
// module entry.
func parseModuleDiff(patch, entry string) []ModuleFileDiff {
	files := []ModuleFileDiff{}
	for _, block := range strings.Split("\n"+patch, "\ndiff --git ")[1:] {
		d := ModuleFileDiff{Status: "modified"}
		for _, line := range strings.Split(block, "\n") {
			if strings.HasPrefix(line, "@@") {
				break
			}
			switch {
			case strings.HasPrefix(line, "new file mode "):
				d.Status = "added"
			case strings.HasPrefix(line, "deleted file mode "):
				d.Status = "removed"
			case strings.HasPrefix(line, "old mode "):
				was := strings.HasSuffix(line, "755")
				d.WasExecutable = &was
			case strings.HasPrefix(line, "new mode "):
				is := strings.HasSuffix(line, "755")
				d.IsExecutable = &is
			}
		}
		// The content: parsePatch reads the paths, the counts and the hunks.
		parsed := parsePatch("diff --git " + block)
		if len(parsed) == 0 {
			continue
		}
		p := parsed[0]
		d.Binary = p.Binary
		d.Added, d.Deleted = p.Added, p.Deleted
		d.Diff, d.Truncated = p.Diff, p.Truncated
		switch {
		case p.Path == entry:
			d.Path = ""
		case strings.HasPrefix(p.Path, entry+"/"):
			d.Path = strings.TrimPrefix(p.Path, entry+"/")
		default:
			d.Path = p.Path
		}
		files = append(files, d)
	}
	return files
}

// RestoreVersion brings a module back to its version rev, in one commit made
// from the revision base: the history keeps every version, the restored one
// included.
func (m CompModules) RestoreVersion(name, base, rev, author, message string) (*ModuleDetail, error) {
	head, err := m.head()
	if err != nil {
		return nil, err
	}
	if head == "" {
		return nil, ErrModuleNotFound
	}
	id, err := m.moduleRevision(head, rev)
	if err != nil {
		return nil, err
	}
	return m.rewrite(name, base, author, message, func(mod Module, _ map[string]ModuleBlob) (string, map[string]ModuleBlob, error) {
		files, _, err := m.moduleFiles(id, mod)
		if err != nil {
			return "", nil, err
		}
		if len(files) == 0 {
			return "", nil, invalidf("the module did not exist at %s", shortID(id))
		}
		return kindOf(files), files, nil
	})
}

func shortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}
