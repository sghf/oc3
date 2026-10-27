package git

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"testing"
)

var (
	shebang  = []byte("#!/bin/sh\n")
	execBlob = ModuleBlob{Content: shebang, Executable: true}
)

// TestCompModulesIssues checks why a module cannot run is told: no entry,
// two entries, an entry or a script not executable.
func TestCompModulesIssues(t *testing.T) {
	m := newCompModules(t)
	if err := m.init(); err != nil {
		t.Fatal(err)
	}
	err := m.commit("", map[string]ModuleBlob{
		"S10-both/main":         execBlob,
		"S10-both/scripts/main": execBlob,
		"S20-none/README":       {Content: []byte("x")},
		"S30-plain/main":        {Content: shebang},
		"S40-script":            {Content: shebang},
		"S50-ok/scripts/main":   execBlob,
	}, nil, "", "import")
	if err != nil {
		t.Fatal(err)
	}
	l, err := m.List()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"both": ModuleIssueTwoEntries, "none": ModuleIssueNoEntry, "plain": ModuleIssueNotExecutable,
		"script": ModuleIssueNotExecutable, "ok": "",
	}
	for _, mod := range l {
		if mod.Issue != want[mod.Name] || mod.Runnable != (want[mod.Name] == "") {
			t.Errorf("%s: issue %q runnable %v, want %q", mod.Name, mod.Issue, mod.Runnable, want[mod.Name])
		}
	}
}

// TestCompModulesCreateDirectoryRules checks the rules a directory module is
// created under.
func TestCompModulesCreateDirectoryRules(t *testing.T) {
	m := newCompModules(t)
	ok := map[string]ModuleBlob{"scripts/main": execBlob, "lib/a.sh": {Content: []byte("a=1\n")}}
	d, err := m.CreateDirectory("audit", 60, ok, "Jane <jane@example.com>", "add audit")
	if err != nil {
		t.Fatal(err)
	}
	if d.Kind != ModuleKindDirectory || d.Entry != "scripts/main" || !d.Runnable || len(d.Files) != 2 {
		t.Errorf("created %+v", d)
	}
	many := map[string]ModuleBlob{"main": execBlob}
	for i := 0; i < MaxModuleFiles; i++ {
		many[fmt.Sprintf("data/f%d", i)] = ModuleBlob{Content: []byte("x")}
	}
	for label, files := range map[string]map[string]ModuleBlob{
		"no entry":            {"lib/a.sh": {Content: []byte("x")}},
		"two entries":         {"main": execBlob, "scripts/main": execBlob},
		"entry not exec":      {"main": {Content: shebang}},
		"path out":            {"main": execBlob, "../x": {Content: []byte("x")}},
		"absolute":            {"main": execBlob, "/etc/x": {Content: []byte("x")}},
		"empty segment":       {"main": execBlob, "a//b": {Content: []byte("x")}},
		"bad character":       {"main": execBlob, "a b": {Content: []byte("x")}},
		"file and directory":  {"main": execBlob, "lib": {Content: []byte("x")}, "lib/a": {Content: []byte("x")}},
		"script key":          {"": execBlob, "main": execBlob},
		"too many files":      many,
		"file over the limit": {"main": execBlob, "big": {Content: make([]byte, MaxModuleFileSize+1)}},
	} {
		if _, err := m.CreateDirectory("x"+fmt.Sprint(len(label)), 1, files, "", label); !errors.Is(err, ErrInvalidModule) {
			t.Errorf("%s: error %v, want ErrInvalidModule", label, err)
		}
	}
}

// TestCompModulesEdits checks the changes of a module: from its revision
// only, each one commit, none for a change that changes nothing.
func TestCompModulesEdits(t *testing.T) {
	m := newCompModules(t)
	d, err := m.CreateScript("ssh", 40, shebang, "", "add ssh")
	if err != nil {
		t.Fatal(err)
	}
	base := d.Changed.ID

	if _, err := m.PutFile("ssh", base, "extra", ModuleBlob{Content: []byte("x")}, "", "add"); !errors.Is(err, ErrInvalidModule) {
		t.Errorf("a file added to a script module: %v", err)
	}
	d, err = m.PutFile("ssh", base, "", ModuleBlob{Content: []byte("#!/bin/sh\nexit 0\n")}, "Bob <bob@example.com>", "exit 0")
	if err != nil {
		t.Fatal(err)
	}
	if d.Changed.ID == base || d.Changed.Subject != "exit 0" || !d.Runnable {
		t.Errorf("script edited: %+v", d)
	}
	if _, err := m.PutFile("ssh", base, "", ModuleBlob{Content: shebang}, "", "stale"); !errors.Is(err, ErrModuleStale) {
		t.Errorf("an edit from an old revision: %v, want ErrModuleStale", err)
	}
	unchanged, err := m.PutFile("ssh", d.Changed.ID, "", ModuleBlob{Content: []byte("#!/bin/sh\nexit 0\n")}, "", "same")
	if err != nil || unchanged.Changed.ID != d.Changed.ID {
		t.Errorf("an edit changing nothing made a commit: %v %+v", err, unchanged.Changed)
	}

	d, err = m.ConvertToDirectory("ssh", d.Changed.ID, "", "to directory")
	if err != nil {
		t.Fatal(err)
	}
	if d.Kind != ModuleKindDirectory || d.Entry != "scripts/main" || !d.Runnable || d.Path != "S40-ssh" {
		t.Errorf("converted %+v", d)
	}
	if content, _, err := m.ReadFile("ssh", "scripts/main"); err != nil || string(content) != "#!/bin/sh\nexit 0\n" {
		t.Errorf("the script moved: %q, %v", content, err)
	}
	d, err = m.PutFile("ssh", d.Changed.ID, "lib/conf", ModuleBlob{Content: []byte("x=1\n")}, "", "add conf")
	if err != nil || len(d.Files) != 2 {
		t.Fatalf("file added: %+v, %v", d, err)
	}
	d, err = m.MoveFile("ssh", d.Changed.ID, "lib/conf", "etc/conf", "", "move conf")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.ReadFile("ssh", "etc/conf"); err != nil {
		t.Errorf("moved file: %v", err)
	}
	if _, err := m.MoveFile("ssh", d.Changed.ID, "etc/conf", "scripts/main", "", "clash"); !errors.Is(err, ErrInvalidModule) {
		t.Errorf("a move over a file: %v", err)
	}
	if _, err := m.DeleteFile("ssh", d.Changed.ID, "scripts/main", "", "rm entry"); !errors.Is(err, ErrInvalidModule) {
		t.Errorf("the entry removed: %v", err)
	}
	d, err = m.DeleteFile("ssh", d.Changed.ID, "etc/conf", "", "rm conf")
	if err != nil || len(d.Files) != 1 {
		t.Fatalf("file removed: %+v, %v", d, err)
	}
	if _, err := m.DeleteFile("ssh", d.Changed.ID, "nope", "", "rm nope"); !errors.Is(err, ErrModuleNotFound) {
		t.Errorf("a missing file removed: %v", err)
	}
	// The history of the module holds each change.
	if got := len(d.History); got != 6 {
		t.Errorf("history of %d commits, want 6", got)
	}
}

// TestCompModulesExportImport checks a module exported and read back gives
// the same files, the archive rooted at the module.
func TestCompModulesExportImport(t *testing.T) {
	m := newCompModules(t)
	files := map[string]ModuleBlob{
		"scripts/main": execBlob,
		"lib/a.sh":     {Content: []byte("a=1\n")},
		"README":       {Content: []byte("doc\n")},
	}
	if _, err := m.CreateDirectory("audit", 60, files, "", "add"); err != nil {
		t.Fatal(err)
	}
	data, entry, err := m.Export("audit")
	if err != nil || entry != "S60-audit" {
		t.Fatalf("Export() = %q, %v", entry, err)
	}
	back, err := ParseModuleArchive(data)
	if err != nil {
		t.Fatal(err)
	}
	if !sameFiles(files, back) {
		t.Errorf("read back %v", back)
	}
	d, err := m.Get("audit")
	if err != nil {
		t.Fatal(err)
	}
	back["lib/b.sh"] = ModuleBlob{Content: []byte("b=2\n")}
	d, err = m.ReplaceFiles("audit", d.Changed.ID, back, "", "replace")
	if err != nil || len(d.Files) != 4 {
		t.Errorf("replaced: %+v, %v", d, err)
	}
}

func tarGz(t *testing.T, entries ...*tar.Header) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, h := range entries {
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if h.Typeflag == tar.TypeReg {
			if _, err := tw.Write(make([]byte, h.Size)); err != nil {
				t.Fatal(err)
			}
		}
	}
	_ = tw.Close()
	_ = gz.Close()
	return buf.Bytes()
}

// TestParseModuleArchiveRefuses checks the entries out of a module are refused.
func TestParseModuleArchiveRefuses(t *testing.T) {
	for label, data := range map[string][]byte{
		"symlink": tarGz(t, &tar.Header{Typeflag: tar.TypeSymlink, Name: "main", Linkname: "/bin/sh"}),
		"dotdot":  tarGz(t, &tar.Header{Typeflag: tar.TypeReg, Name: "../evil", Mode: 0o755}),
		"abs":     tarGz(t, &tar.Header{Typeflag: tar.TypeReg, Name: "/etc/passwd", Mode: 0o644}),
		"device":  tarGz(t, &tar.Header{Typeflag: tar.TypeChar, Name: "dev"}),
		"text":    []byte("plain text"),
	} {
		if _, err := ParseModuleArchive(data); !errors.Is(err, ErrInvalidModule) {
			t.Errorf("%s: error %v, want ErrInvalidModule", label, err)
		}
	}
	// A ./ prefix and the directories are fine.
	files, err := ParseModuleArchive(tarGz(t,
		&tar.Header{Typeflag: tar.TypeDir, Name: "./", Mode: 0o755},
		&tar.Header{Typeflag: tar.TypeReg, Name: "./main", Mode: 0o755, Size: 3},
	))
	if err != nil || len(files) != 1 || !files["main"].Executable {
		t.Errorf("./main: %v, %v", files, err)
	}
}

// TestParseModuleArchiveZip checks a zip made without Unix modes marks its
// scripts executable by their #!, and its root directory is unwrapped.
func TestParseModuleArchiveZip(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, content := range map[string]string{
		"audit/scripts/main": "#!/bin/sh\n",
		"audit/lib/a.sh":     "a=1\n",
	} {
		h := &zip.FileHeader{Name: name, Method: zip.Deflate}
		// CreatorVersion 0: FAT, no Unix modes, as Windows makes them.
		h.CreatorVersion = 20
		w, err := zw.CreateHeader(h)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = w.Write([]byte(content))
	}
	_ = zw.Close()
	files, err := ParseModuleArchive(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if !files["scripts/main"].Executable || files["lib/a.sh"].Executable || len(files) != 2 {
		t.Errorf("zip read as %+v", files)
	}
}

// TestCompModulesRename checks a module renamed or reordered moves with its
// files and history, from its revision only, never onto another's name.
func TestCompModulesRename(t *testing.T) {
	m := newCompModules(t)
	files := map[string]ModuleBlob{"scripts/main": execBlob, "lib/a": {Content: []byte("a\n")}}
	d, err := m.CreateDirectory("audit", 60, files, "", "add")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.CreateScript("other", 10, shebang, "", "add other"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.RenameModule("audit", d.Changed.ID, "other", 70, "", "clash"); !errors.Is(err, ErrModuleExists) {
		t.Errorf("renamed onto another module: %v", err)
	}
	if _, err := m.RenameModule("audit", "stale", "audit2", 60, "", "stale"); !errors.Is(err, ErrModuleStale) {
		t.Errorf("renamed from an old revision: %v", err)
	}
	if _, err := m.RenameModule("audit", d.Changed.ID, "9bad", 60, "", "bad"); !errors.Is(err, ErrInvalidModule) {
		t.Errorf("an invalid name: %v", err)
	}
	r, err := m.RenameModule("audit", d.Changed.ID, "sec.audit", 65, "Ann <ann@example.com>", "rename")
	if err != nil {
		t.Fatal(err)
	}
	if r.Path != "S65-sec.audit" || r.Name != "sec.audit" || r.Order != 65 || len(r.Files) != 2 || !r.Runnable {
		t.Errorf("renamed %+v", r)
	}
	if _, err := m.Get("audit"); !errors.Is(err, ErrModuleNotFound) {
		t.Errorf("the old name is still there: %v", err)
	}
	// The order alone.
	r, err = m.RenameModule("sec.audit", r.Changed.ID, "sec.audit", 20, "", "reorder")
	if err != nil || r.Path != "S20-sec.audit" {
		t.Errorf("reordered %+v, %v", r, err)
	}
	// Nothing changed: no commit.
	same, err := m.RenameModule("sec.audit", r.Changed.ID, "sec.audit", 20, "", "same")
	if err != nil || same.Changed.ID != r.Changed.ID {
		t.Errorf("a rename changing nothing made a commit: %v", err)
	}
}

// TestCompModulesSetExecutable checks the executable bit of a file changes,
// the entry's staying.
func TestCompModulesSetExecutable(t *testing.T) {
	m := newCompModules(t)
	d, err := m.CreateDirectory("tools", 30, map[string]ModuleBlob{"main": execBlob, "bin/tool": {Content: []byte{0, 1}}}, "", "add")
	if err != nil {
		t.Fatal(err)
	}
	d, err = m.SetExecutable("tools", d.Changed.ID, "bin/tool", true, "", "chmod")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range d.Files {
		if f.Path == "bin/tool" && !f.Executable {
			t.Error("bin/tool not executable")
		}
	}
	if _, err := m.SetExecutable("tools", d.Changed.ID, "main", false, "", "entry"); !errors.Is(err, ErrInvalidModule) {
		t.Errorf("the entry made not executable: %v", err)
	}
}
