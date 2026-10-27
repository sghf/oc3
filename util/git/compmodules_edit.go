package git

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"
)

// The kinds of module.
const (
	ModuleKindScript    = "script"
	ModuleKindDirectory = "directory"
)

// Limits of a directory module.
const (
	// MaxModuleFiles is the most files a directory module holds.
	MaxModuleFiles = 500
	// MaxModuleSize is the most bytes the files of a module add up to.
	MaxModuleSize = 10 << 20
	// maxModuleDepth is the most directories a path of a module goes through.
	maxModuleDepth = 8
)

var (
	// ErrInvalidModule tells a module the rules refuse; the error says which.
	ErrInvalidModule = errors.New("invalid module")

	// ErrModuleStale tells the module changed since the revision a change was
	// made from: applied, it would undo the change made meanwhile.
	ErrModuleStale = errors.New("the module changed meanwhile")
)

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidModule, fmt.Sprintf(format, args...))
}

// modulePathSegment bounds the names of the files and directories of a
// module: no character a shell or a path would read otherwise.
var modulePathSegment = regexp.MustCompile(`^[A-Za-z0-9_.+@-]{1,100}$`)

// ValidModulePath reports whether p may name a file of a directory module:
// relative, at most 8 directories deep, no "." or ".." segment.
func ValidModulePath(p string) bool {
	if p == "" || len(p) > 255 || strings.HasPrefix(p, "/") {
		return false
	}
	segments := strings.Split(p, "/")
	if len(segments) > maxModuleDepth+1 {
		return false
	}
	for _, s := range segments {
		if s == "." || s == ".." || !modulePathSegment.MatchString(s) {
			return false
		}
	}
	return true
}

// ValidateModuleFiles checks the files of a module, by path relative to it:
// for a script, the one file "", starting with #!; for a directory, valid
// paths within the limits, no file where another has a directory, and one
// entry, main or scripts/main, executable, as the agents run it.
func ValidateModuleFiles(kind string, files map[string]ModuleBlob) error {
	switch kind {
	case ModuleKindScript:
		f, ok := files[""]
		if !ok || len(files) != 1 {
			return invalidf("a script module is one file")
		}
		if len(f.Content) > MaxModuleFileSize {
			return invalidf("the script is larger than %d bytes", MaxModuleFileSize)
		}
		if !bytes.HasPrefix(f.Content, []byte("#!")) {
			return invalidf("the script must start with #! naming its interpreter")
		}
		return nil
	case ModuleKindDirectory:
	default:
		return invalidf("unknown module kind %q", kind)
	}
	if len(files) == 0 {
		return invalidf("a directory module holds at least its entry, main or scripts/main")
	}
	if len(files) > MaxModuleFiles {
		return invalidf("a directory module holds %d files at most", MaxModuleFiles)
	}
	var total int64
	dirs := map[string]bool{}
	for p, f := range files {
		if !ValidModulePath(p) {
			return invalidf("invalid file path %q: relative, letters, digits and _.+@- in each name", p)
		}
		if len(f.Content) > MaxModuleFileSize {
			return invalidf("%s is larger than %d bytes", p, MaxModuleFileSize)
		}
		total += int64(len(f.Content))
		for d := path.Dir(p); d != "."; d = path.Dir(d) {
			dirs[d] = true
		}
	}
	if total > MaxModuleSize {
		return invalidf("the files add up to more than %d bytes", MaxModuleSize)
	}
	for p := range files {
		if dirs[p] {
			return invalidf("%s is both a file and a directory", p)
		}
	}
	main, hasMain := files["main"]
	scriptsMain, hasScriptsMain := files["scripts/main"]
	switch {
	case hasMain && hasScriptsMain:
		return invalidf("main and scripts/main both exist: the agents refuse to choose, keep one")
	case !hasMain && !hasScriptsMain:
		return invalidf("the agents run main or scripts/main: the module holds neither")
	case hasMain && !main.Executable, hasScriptsMain && !scriptsMain.Executable:
		return invalidf("the entry, main or scripts/main, must be executable")
	}
	return nil
}

// treeFiles places the files of a module under its entry in the tree.
func treeFiles(entry string, files map[string]ModuleBlob) map[string]ModuleBlob {
	out := make(map[string]ModuleBlob, len(files))
	for rel, f := range files {
		if rel == "" {
			out[entry] = f
		} else {
			out[entry+"/"+rel] = f
		}
	}
	return out
}

// moduleFiles reads the files of a module at the commit head, by path
// relative to it, and their paths in the tree.
func (m CompModules) moduleFiles(head string, mod Module) (map[string]ModuleBlob, []string, error) {
	entries, err := m.lsTree(head, true, mod.Path)
	if err != nil {
		return nil, nil, err
	}
	files := map[string]ModuleBlob{}
	var paths []string
	for _, e := range entries {
		if e.kind != "blob" {
			continue
		}
		out, err := m.git(nil, nil, "cat-file", "blob", e.oid)
		if err != nil {
			return nil, nil, err
		}
		rel := strings.TrimPrefix(strings.TrimPrefix(e.path, mod.Path), "/")
		files[rel] = ModuleBlob{Content: []byte(out), Executable: e.mode == "100755"}
		paths = append(paths, e.path)
	}
	return files, paths, nil
}

// revision returns the last commit that changed the module, at head.
func (m CompModules) revision(head, entry string) (string, error) {
	out, err := m.git(nil, nil, "log", "-1", "--format=%H", head, "--", entry)
	return strings.TrimSpace(out), err
}

// moduleChange turns the files of a module, by path relative to it, into new
// ones and the kind of the module they make.
type moduleChange func(mod Module, files map[string]ModuleBlob) (kind string, next map[string]ModuleBlob, err error)

// rewrite applies a change to a module in one commit, the module having last
// changed at base: a module changed since is refused with ErrModuleStale. It
// returns the module as changed.
func (m CompModules) rewrite(name, base, author, message string, change moduleChange) (*ModuleDetail, error) {
	if err := m.rewriteLocked(name, base, author, message, change); err != nil {
		return nil, err
	}
	return m.Get(name)
}

func (m CompModules) rewriteLocked(name, base, author, message string, change moduleChange) error {
	compModulesWrite.Lock()
	defer compModulesWrite.Unlock()
	head, err := m.head()
	if err != nil {
		return err
	}
	if head == "" {
		return ErrModuleNotFound
	}
	mods, err := m.find(head, name)
	if err != nil {
		return err
	}
	if len(mods) == 0 {
		return ErrModuleNotFound
	}
	if len(mods) > 1 {
		return invalidf("%d modules are named %s: the agents refuse to run either", len(mods), name)
	}
	mod := mods[0]
	rev, err := m.revision(head, mod.Path)
	if err != nil {
		return err
	}
	if base != rev {
		return ErrModuleStale
	}
	files, paths, err := m.moduleFiles(head, mod)
	if err != nil {
		return err
	}
	kind, next, err := change(mod, files)
	if err != nil {
		return err
	}
	if err := ValidateModuleFiles(kind, next); err != nil {
		return err
	}
	// Nothing changed: no empty commit.
	if sameFiles(files, next) {
		return nil
	}
	return m.commit(head, treeFiles(mod.Path, next), paths, author, message)
}

func sameFiles(a, b map[string]ModuleBlob) bool {
	if len(a) != len(b) {
		return false
	}
	for p, f := range a {
		g, ok := b[p]
		if !ok || f.Executable != g.Executable || !bytes.Equal(f.Content, g.Content) {
			return false
		}
	}
	return true
}

func kindOf(files map[string]ModuleBlob) string {
	if _, ok := files[""]; ok {
		return ModuleKindScript
	}
	return ModuleKindDirectory
}

func copyFiles(files map[string]ModuleBlob) map[string]ModuleBlob {
	out := make(map[string]ModuleBlob, len(files))
	for p, f := range files {
		out[p] = f
	}
	return out
}

// PutFile writes a file of a module: the script of a script module (rel ""),
// a file of a directory module, created or replaced.
func (m CompModules) PutFile(name, base, rel string, f ModuleBlob, author, message string) (*ModuleDetail, error) {
	return m.rewrite(name, base, author, message, func(_ Module, files map[string]ModuleBlob) (string, map[string]ModuleBlob, error) {
		kind := kindOf(files)
		if kind == ModuleKindScript {
			if rel != "" {
				return "", nil, invalidf("a script module is one file: convert it to a directory to add files")
			}
			f.Executable = true
		} else if rel == "" {
			return "", nil, invalidf("a file of a directory module needs a path")
		}
		next := copyFiles(files)
		next[rel] = f
		return kind, next, nil
	})
}

// DeleteFile removes a file of a directory module.
func (m CompModules) DeleteFile(name, base, rel, author, message string) (*ModuleDetail, error) {
	return m.rewrite(name, base, author, message, func(_ Module, files map[string]ModuleBlob) (string, map[string]ModuleBlob, error) {
		if kindOf(files) == ModuleKindScript {
			return "", nil, invalidf("the script of a script module cannot be removed")
		}
		if _, ok := files[rel]; !ok {
			return "", nil, ErrModuleNotFound
		}
		next := copyFiles(files)
		delete(next, rel)
		return ModuleKindDirectory, next, nil
	})
}

// MoveFile renames a file of a directory module.
func (m CompModules) MoveFile(name, base, from, to, author, message string) (*ModuleDetail, error) {
	return m.rewrite(name, base, author, message, func(_ Module, files map[string]ModuleBlob) (string, map[string]ModuleBlob, error) {
		if kindOf(files) == ModuleKindScript {
			return "", nil, invalidf("a script module is one file: convert it to a directory first")
		}
		f, ok := files[from]
		if !ok {
			return "", nil, ErrModuleNotFound
		}
		if _, taken := files[to]; taken {
			return "", nil, invalidf("%s exists", to)
		}
		next := copyFiles(files)
		delete(next, from)
		next[to] = f
		return ModuleKindDirectory, next, nil
	})
}

// ReplaceFiles replaces every file of a module with those of a directory: an
// archive uploaded, say. A script module becomes a directory.
func (m CompModules) ReplaceFiles(name, base string, files map[string]ModuleBlob, author, message string) (*ModuleDetail, error) {
	return m.rewrite(name, base, author, message, func(Module, map[string]ModuleBlob) (string, map[string]ModuleBlob, error) {
		return ModuleKindDirectory, files, nil
	})
}

// ConvertToDirectory turns a script module into a directory module whose
// entry, scripts/main, is the script.
func (m CompModules) ConvertToDirectory(name, base, author, message string) (*ModuleDetail, error) {
	return m.rewrite(name, base, author, message, func(_ Module, files map[string]ModuleBlob) (string, map[string]ModuleBlob, error) {
		script, ok := files[""]
		if !ok {
			return "", nil, invalidf("the module is a directory already")
		}
		script.Executable = true
		return ModuleKindDirectory, map[string]ModuleBlob{"scripts/main": script}, nil
	})
}

// Export writes the module as a gzipped tarball rooted at its entry,
// S<order>-<name>, its files owned by root: what an upload replacing it takes
// back.
func (m CompModules) Export(name string) (data []byte, entry string, err error) {
	head, err := m.head()
	if err != nil {
		return nil, "", err
	}
	if head == "" {
		return nil, "", ErrModuleNotFound
	}
	mods, err := m.find(head, name)
	if err != nil {
		return nil, "", err
	}
	if len(mods) == 0 {
		return nil, "", ErrModuleNotFound
	}
	mod := mods[0]
	files, _, err := m.moduleFiles(head, mod)
	if err != nil {
		return nil, "", err
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	now := time.Now()
	rels := make([]string, 0, len(files))
	for rel := range files {
		rels = append(rels, rel)
	}
	sort.Strings(rels)
	dirs := map[string]bool{}
	writeDir := func(d string) error {
		if dirs[d] {
			return nil
		}
		dirs[d] = true
		return tw.WriteHeader(&tar.Header{Typeflag: tar.TypeDir, Name: d + "/", Mode: 0o755, ModTime: now, Uname: "root", Gname: "root"})
	}
	for _, rel := range rels {
		f := files[rel]
		name := mod.Path
		if rel != "" {
			name = mod.Path + "/" + rel
			var parents []string
			for d := path.Dir(name); d != "."; d = path.Dir(d) {
				parents = append([]string{d}, parents...)
			}
			for _, d := range parents {
				if err := writeDir(d); err != nil {
					return nil, "", err
				}
			}
		}
		mode := int64(0o644)
		if f.Executable {
			mode = 0o755
		}
		if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: name, Mode: mode, Size: int64(len(f.Content)), ModTime: now, Uname: "root", Gname: "root"}); err != nil {
			return nil, "", err
		}
		if _, err := tw.Write(f.Content); err != nil {
			return nil, "", err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, "", err
	}
	if err := gz.Close(); err != nil {
		return nil, "", err
	}
	return buf.Bytes(), mod.Path, nil
}
