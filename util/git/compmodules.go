package git

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/spf13/viper"
)

// CompModules keeps the compliance modules in a bare git repository, Dir: the
// tree of its branch is the compliance directory of the agents, which they
// get as an archive (Archive), as the historical setup builds it from a git
// repository maintained on a dedicated machine.
//
// A module is an entry at the root of the tree named S<order>-<name>, or
// <order><name>, as the agents look them up (om3 util/compliance): an
// executable file, or a directory holding an executable main or scripts/main.
// The other entries, com.opensvc say, are not modules.
type CompModules struct {
	Dir string
}

const compModulesBranch = "refs/heads/main"

var (
	// ErrModuleExists tells a module of that name is there already, whatever
	// its order: an agent finding two refuses to run either.
	ErrModuleExists = errors.New("a module of that name exists")

	// ErrModuleNotFound tells no module has that name.
	ErrModuleNotFound = errors.New("module not found")

	// ErrModulesEmpty tells the repository has no module yet.
	ErrModulesEmpty = errors.New("no compliance module yet")

	// ErrModulesChanged tells the branch moved while a change was prepared.
	ErrModulesChanged = errors.New("the modules changed meanwhile")
)

// moduleEntry is the name of an entry of the root of the tree: its order and
// the name of the module, as the agents parse them.
var moduleEntry = regexp.MustCompile(`^S*([0-9]+)-*(.+)$`)

// moduleName bounds the names of the modules created: a letter first, the
// order before it being digits, and no character a shell or a path would
// read otherwise.
var moduleName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.+-]{0,63}$`)

// ValidModuleName reports whether s may name a new module.
func ValidModuleName(s string) bool { return moduleName.MatchString(s) }

// MaxModuleOrder is the largest order of a module.
const MaxModuleOrder = 99999

// MaxModuleFileSize is the largest file read or written, in bytes.
const MaxModuleFileSize = 1 << 20

// compModulesWrite serializes the changes made by this process: the branch
// update checks the others.
var compModulesWrite sync.Mutex

// Module is a compliance module of the tree.
type Module struct {
	Name string `json:"name"`
	// Path is the name of its entry at the root of the tree.
	Path  string `json:"path"`
	Order int    `json:"order"`
	// Kind is "script", an executable file, or "directory".
	Kind string `json:"kind"`
	// Entry is the file the agent runs, relative to Path, "" for a script:
	// main or scripts/main. Empty for a directory without either.
	Entry string `json:"entry"`
	// Runnable tells whether an agent can run it: an executable file.
	Runnable bool `json:"runnable"`
	// Issue tells why an agent cannot run it, "" when it can: see the
	// ModuleIssue constants.
	Issue   string        `json:"issue"`
	Changed *ModuleCommit `json:"changed,omitempty"`
}

// Why an agent cannot run a module.
const (
	// ModuleIssueNotExecutable: the file to run is not executable.
	ModuleIssueNotExecutable = "not_executable"
	// ModuleIssueNoEntry: a directory holding neither main nor scripts/main.
	ModuleIssueNoEntry = "no_entry"
	// ModuleIssueTwoEntries: a directory holding both main and scripts/main,
	// which the agents refuse to choose between.
	ModuleIssueTwoEntries = "two_entries"
)

// ModuleCommit is a commit that changed modules.
type ModuleCommit struct {
	ID      string `json:"id"`
	Author  string `json:"author"`
	Date    string `json:"date"`
	Subject string `json:"subject"`
}

// ModuleFile is a file of a module.
type ModuleFile struct {
	// Path is relative to the module entry, "" for a script.
	Path       string `json:"path"`
	Size       int64  `json:"size"`
	Executable bool   `json:"executable"`
}

// ModuleDetail is a module with its files and its history.
type ModuleDetail struct {
	Module
	Files   []ModuleFile   `json:"files"`
	History []ModuleCommit `json:"history"`
}

func (m CompModules) git(stdin io.Reader, env []string, args ...string) (string, error) {
	cmd := command(append([]string{"--git-dir=" + m.Dir}, args...)...)
	cmd.Stdin = stdin
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}
	var out, errOut bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("git %s: %w: %s", args[0], err, strings.TrimSpace(errOut.String()))
	}
	return out.String(), nil
}

// init creates the repository when missing.
func (m CompModules) init() error {
	if _, err := os.Stat(filepath.Join(m.Dir, "HEAD")); err == nil {
		return nil
	}
	if err := os.MkdirAll(m.Dir, 0o755); err != nil {
		return err
	}
	email := viper.GetString("git.user_email")
	if email == "" {
		email = "nobody@localhost.localdomain"
	}
	for _, args := range [][]string{
		{"init", "--bare", "--quiet"},
		{"symbolic-ref", "HEAD", compModulesBranch},
		{"config", "user.email", email},
		{"config", "user.name", "collector"},
	} {
		if _, err := m.git(nil, nil, args...); err != nil {
			return err
		}
	}
	return nil
}

// head returns the commit of the branch, "" before the first module.
func (m CompModules) head() (string, error) {
	if _, err := os.Stat(filepath.Join(m.Dir, "HEAD")); err != nil {
		return "", nil
	}
	out, err := m.git(nil, nil, "rev-parse", "--verify", "--quiet", compModulesBranch+"^{commit}")
	if err != nil {
		// --quiet: an unborn branch fails without a message.
		if strings.TrimSpace(out) == "" {
			return "", nil
		}
		return "", err
	}
	return strings.TrimSpace(out), nil
}

// treeEntry is a line of git ls-tree --long.
type treeEntry struct {
	mode, kind, oid, path string
	size                  int64
}

func (m CompModules) lsTree(rev string, recursive bool, paths ...string) ([]treeEntry, error) {
	args := []string{"ls-tree", "-z", "--long"}
	if recursive {
		args = append(args, "-r")
	}
	args = append(args, rev, "--")
	out, err := m.git(nil, nil, append(args, paths...)...)
	if err != nil {
		return nil, err
	}
	var l []treeEntry
	for _, line := range strings.Split(out, "\x00") {
		meta, name, ok := strings.Cut(line, "\t")
		if !ok {
			continue
		}
		fields := strings.Fields(meta)
		if len(fields) != 4 {
			continue
		}
		size, _ := strconv.ParseInt(fields[3], 10, 64)
		l = append(l, treeEntry{mode: fields[0], kind: fields[1], oid: fields[2], path: name, size: size})
	}
	return l, nil
}

// modulesOf reads the modules among the entries of the root of the tree.
func (m CompModules) modulesOf(rev string, root []treeEntry) ([]Module, error) {
	l := make([]Module, 0, len(root))
	for _, e := range root {
		match := moduleEntry.FindStringSubmatch(e.path)
		if match == nil {
			continue
		}
		order, _ := strconv.Atoi(match[1])
		mod := Module{Name: match[2], Path: e.path, Order: order}
		switch e.kind {
		case "blob":
			mod.Kind = "script"
			mod.Runnable = e.mode == "100755"
			if !mod.Runnable {
				mod.Issue = ModuleIssueNotExecutable
			}
		case "tree":
			mod.Kind = "directory"
			files, err := m.lsTree(rev, true, e.path+"/main", e.path+"/scripts/main")
			if err != nil {
				return nil, err
			}
			var entries []treeEntry
			for _, f := range files {
				if f.kind == "blob" {
					entries = append(entries, f)
				}
			}
			switch len(entries) {
			case 0:
				mod.Issue = ModuleIssueNoEntry
			case 1:
				mod.Entry = strings.TrimPrefix(entries[0].path, e.path+"/")
				mod.Runnable = entries[0].mode == "100755"
				if !mod.Runnable {
					mod.Issue = ModuleIssueNotExecutable
				}
			default:
				// The agents find both and refuse to run the module.
				mod.Issue = ModuleIssueTwoEntries
			}
		default:
			continue
		}
		l = append(l, mod)
	}
	return l, nil
}

// lastChanges returns the last commit that changed each entry of the root of
// the tree, read in one pass over the history.
func (m CompModules) lastChanges(head string, paths map[string]bool) (map[string]ModuleCommit, error) {
	cmd := command("--git-dir="+m.Dir, "log", "--format=%x01%H%x00%an <%ae>%x00%aI%x00%s", "--name-only", head)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	found := map[string]ModuleCommit{}
	var current ModuleCommit
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() && len(found) < len(paths) {
		line := scanner.Text()
		if strings.HasPrefix(line, "\x01") {
			fields := strings.SplitN(line[1:], "\x00", 4)
			if len(fields) == 4 {
				current = ModuleCommit{ID: fields[0], Author: fields[1], Date: fields[2], Subject: fields[3]}
			}
			continue
		}
		top, _, _ := strings.Cut(line, "/")
		if top == "" || !paths[top] {
			continue
		}
		if _, ok := found[top]; !ok {
			found[top] = current
		}
	}
	// Stopped early when everything was found: git may then fail on the
	// closed pipe, which is not an error here.
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	return found, scanner.Err()
}

// List returns the modules, by order then name.
func (m CompModules) List() ([]Module, error) {
	head, err := m.head()
	if err != nil || head == "" {
		return []Module{}, err
	}
	root, err := m.lsTree(head, false)
	if err != nil {
		return nil, err
	}
	l, err := m.modulesOf(head, root)
	if err != nil {
		return nil, err
	}
	paths := make(map[string]bool, len(l))
	for _, mod := range l {
		paths[mod.Path] = true
	}
	changes, err := m.lastChanges(head, paths)
	if err != nil {
		return nil, err
	}
	for i := range l {
		if c, ok := changes[l[i].Path]; ok {
			l[i].Changed = &c
		}
	}
	sort.SliceStable(l, func(i, j int) bool {
		if l[i].Order != l[j].Order {
			return l[i].Order < l[j].Order
		}
		return l[i].Name < l[j].Name
	})
	return l, nil
}

// find returns the modules of that name, at the commit head.
func (m CompModules) find(head, name string) ([]Module, error) {
	root, err := m.lsTree(head, false)
	if err != nil {
		return nil, err
	}
	all, err := m.modulesOf(head, root)
	if err != nil {
		return nil, err
	}
	var l []Module
	for _, mod := range all {
		if mod.Name == name {
			l = append(l, mod)
		}
	}
	return l, nil
}

// Get returns the module of that name with its files and its last 100 commits.
func (m CompModules) Get(name string) (*ModuleDetail, error) {
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
	// Two entries of that name: the agents refuse to run either, the first
	// shows.
	d := ModuleDetail{Module: mods[0], Files: []ModuleFile{}, History: []ModuleCommit{}}
	entries, err := m.lsTree(head, true, d.Path)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.kind != "blob" {
			continue
		}
		rel := strings.TrimPrefix(strings.TrimPrefix(e.path, d.Path), "/")
		d.Files = append(d.Files, ModuleFile{Path: rel, Size: e.size, Executable: e.mode == "100755"})
	}
	out, err := m.git(nil, nil, "log", "-n", "100", "--format=%H%x00%an <%ae>%x00%aI%x00%s", head, "--", d.Path)
	if err != nil {
		return nil, err
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		fields := strings.SplitN(line, "\x00", 4)
		if len(fields) == 4 {
			d.History = append(d.History, ModuleCommit{ID: fields[0], Author: fields[1], Date: fields[2], Subject: fields[3]})
		}
	}
	if len(d.History) > 0 {
		d.Changed = &d.History[0]
	}
	return &d, nil
}

// ReadFile returns the content of a file of the module, rel being one of the
// paths Get lists. truncated tells it was cut at MaxModuleFileSize.
func (m CompModules) ReadFile(name, rel string) (content []byte, truncated bool, err error) {
	d, err := m.Get(name)
	if err != nil {
		return nil, false, err
	}
	known := false
	for _, f := range d.Files {
		if f.Path == rel {
			known = true
			truncated = f.Size > MaxModuleFileSize
			break
		}
	}
	if !known {
		return nil, false, ErrModuleNotFound
	}
	p := d.Path
	if rel != "" {
		p = path.Join(d.Path, rel)
	}
	out, err := m.git(nil, nil, "cat-file", "blob", compModulesBranch+":"+p)
	if err != nil {
		return nil, false, err
	}
	b := []byte(out)
	if len(b) > MaxModuleFileSize {
		b = b[:MaxModuleFileSize]
	}
	return b, truncated, nil
}

// splitAuthor splits "Name <email>" for the git environment.
func splitAuthor(author string) (name, email string) {
	name, rest, ok := strings.Cut(author, "<")
	if !ok {
		return strings.TrimSpace(author), ""
	}
	return strings.TrimSpace(name), strings.TrimSuffix(strings.TrimSpace(rest), ">")
}

// CreateScript adds a module made of one executable script, S<order>-<name>,
// in a commit by author ("Name <email>") with that message, and returns it.
func (m CompModules) CreateScript(name string, order int, content []byte, author, message string) (*ModuleDetail, error) {
	return m.create(name, order, ModuleKindScript, map[string]ModuleBlob{"": {Content: content, Executable: true}}, author, message)
}

// CreateDirectory adds a module made of a directory, S<order>-<name>, holding
// files by path relative to it, in a commit by author with that message.
func (m CompModules) CreateDirectory(name string, order int, files map[string]ModuleBlob, author, message string) (*ModuleDetail, error) {
	return m.create(name, order, ModuleKindDirectory, files, author, message)
}

func (m CompModules) create(name string, order int, kind string, files map[string]ModuleBlob, author, message string) (*ModuleDetail, error) {
	if !ValidModuleName(name) {
		return nil, invalidf("invalid module name %q", name)
	}
	if order < 0 || order > MaxModuleOrder {
		return nil, invalidf("invalid module order %d", order)
	}
	if err := ValidateModuleFiles(kind, files); err != nil {
		return nil, err
	}
	compModulesWrite.Lock()
	defer compModulesWrite.Unlock()
	if err := m.init(); err != nil {
		return nil, err
	}
	head, err := m.head()
	if err != nil {
		return nil, err
	}
	if head != "" {
		mods, err := m.find(head, name)
		if err != nil {
			return nil, err
		}
		if len(mods) > 0 {
			return nil, ErrModuleExists
		}
	}
	entry := fmt.Sprintf("S%02d-%s", order, name)
	if err := m.commit(head, treeFiles(entry, files), nil, author, message); err != nil {
		return nil, err
	}
	return m.Get(name)
}

// ModuleBlob is the content of a file of a module.
type ModuleBlob struct {
	Content    []byte
	Executable bool
}

// commit records files, by path from the root of the tree, over the commit
// head ("" for the first one), the paths of remove taken out first, and moves
// the branch to it.
func (m CompModules) commit(head string, files map[string]ModuleBlob, remove []string, author, message string) error {
	index, err := os.CreateTemp("", "oc3-compmodules-index-")
	if err != nil {
		return err
	}
	indexPath := index.Name()
	_ = index.Close()
	// git reads an empty file as a corrupt index: it is to create it.
	_ = os.Remove(indexPath)
	defer func() { _ = os.Remove(indexPath) }()
	env := []string{"GIT_INDEX_FILE=" + indexPath}
	if head != "" {
		if _, err := m.git(nil, env, "read-tree", head); err != nil {
			return err
		}
	}
	if len(remove) > 0 {
		// A null entry takes a path out of the index: --force-remove would
		// want a work tree, which the bare repository has not.
		var lines strings.Builder
		for _, p := range remove {
			lines.WriteString("0 " + strings.Repeat("0", 40) + "\t" + p + "\n")
		}
		if _, err := m.git(strings.NewReader(lines.String()), env, "update-index", "--index-info"); err != nil {
			return err
		}
	}
	paths := make([]string, 0, len(files))
	for p := range files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		f := files[p]
		blob, err := m.git(bytes.NewReader(f.Content), nil, "hash-object", "-w", "--stdin")
		if err != nil {
			return err
		}
		mode := "100644"
		if f.Executable {
			mode = "100755"
		}
		if _, err := m.git(nil, env, "update-index", "--add", "--cacheinfo", mode+","+strings.TrimSpace(blob)+","+p); err != nil {
			return err
		}
	}
	tree, err := m.git(nil, env, "write-tree")
	if err != nil {
		return err
	}
	authorName, authorEmail := splitAuthor(author)
	if authorName == "" {
		authorName = "collector"
	}
	commitEnv := []string{"GIT_AUTHOR_NAME=" + authorName, "GIT_AUTHOR_EMAIL=" + authorEmail}
	args := []string{"commit-tree", strings.TrimSpace(tree), "-m", message}
	if head != "" {
		args = append(args, "-p", head)
	}
	commit, err := m.git(nil, commitEnv, args...)
	if err != nil {
		return err
	}
	// The branch moves only from the commit read: another writer having moved
	// it, the change is refused rather than lost.
	old := head
	if old == "" {
		old = strings.Repeat("0", 40)
	}
	if _, err := m.git(nil, nil, "update-ref", "-m", message, compModulesBranch, strings.TrimSpace(commit), old); err != nil {
		return fmt.Errorf("%w: %w", ErrModulesChanged, err)
	}
	return nil
}

// Archive writes the tree of the branch as a gzipped tarball whose root is
// compliance/, as the agents unpack it (om node updatecomp), and returns the
// commit archived.
func (m CompModules) Archive(w io.Writer) (string, error) {
	head, err := m.head()
	if err != nil {
		return "", err
	}
	if head == "" {
		return "", ErrModulesEmpty
	}
	// Writable by root only: the agents run the modules as root. git applies a
	// 002 umask by default.
	cmd := command("-c", "tar.umask=0022", "--git-dir="+m.Dir, "archive", "--format=tar.gz", "--prefix=compliance/", head)
	cmd.Stdout = w
	var errOut bytes.Buffer
	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("git archive: %w: %s", err, strings.TrimSpace(errOut.String()))
	}
	return head, nil
}

// Head returns the commit the archive is built from, "" before the first
// module.
func (m CompModules) Head() (string, error) { return m.head() }
