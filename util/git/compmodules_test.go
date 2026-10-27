package git

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"path/filepath"
	"testing"
)

func newCompModules(t *testing.T) CompModules {
	t.Helper()
	return CompModules{Dir: filepath.Join(t.TempDir(), "modules.git")}
}

// TestCompModulesEmpty checks a repository not created yet lists nothing and
// has no archive.
func TestCompModulesEmpty(t *testing.T) {
	m := newCompModules(t)
	if l, err := m.List(); err != nil || len(l) != 0 {
		t.Fatalf("List() = %v, %v", l, err)
	}
	if _, err := m.Get("x"); !errors.Is(err, ErrModuleNotFound) {
		t.Errorf("Get() error %v, want ErrModuleNotFound", err)
	}
	if _, err := m.Archive(io.Discard); !errors.Is(err, ErrModulesEmpty) {
		t.Errorf("Archive() error %v, want ErrModulesEmpty", err)
	}
}

// TestCompModulesCreateScript checks a script module is committed executable,
// by its author, listed, read and refused a second time under another order.
func TestCompModulesCreateScript(t *testing.T) {
	m := newCompModules(t)
	script := []byte("#!/bin/sh\necho ok\n")
	mod, err := m.CreateScript("sshd", 10, script, "Jane Doe <jane@example.com>", "add sshd")
	if err != nil {
		t.Fatal(err)
	}
	if mod.Path != "S10-sshd" {
		t.Errorf("path %q, want S10-sshd", mod.Path)
	}
	if _, err := m.CreateScript("sshd", 20, script, "", "again"); !errors.Is(err, ErrModuleExists) {
		t.Errorf("second creation error %v, want ErrModuleExists", err)
	}
	for _, bad := range []string{"", "9lives", "a/b", "../x", "a b"} {
		if _, err := m.CreateScript(bad, 1, script, "", "bad"); err == nil {
			t.Errorf("name %q accepted", bad)
		}
	}
	if _, err := m.CreateScript("ntp", MaxModuleOrder+1, script, "", "bad"); err == nil {
		t.Error("order over the maximum accepted")
	}

	l, err := m.List()
	if err != nil || len(l) != 1 {
		t.Fatalf("List() = %v, %v", l, err)
	}
	got := l[0]
	if got.Name != "sshd" || got.Order != 10 || got.Kind != "script" || !got.Runnable {
		t.Errorf("listed %+v", got)
	}
	if got.Changed == nil || got.Changed.Author != "Jane Doe <jane@example.com>" || got.Changed.Subject != "add sshd" {
		t.Errorf("last change %+v", got.Changed)
	}
	d, err := m.Get("sshd")
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Files) != 1 || d.Files[0].Path != "" || !d.Files[0].Executable || len(d.History) != 1 {
		t.Errorf("detail %+v", d)
	}
	content, truncated, err := m.ReadFile("sshd", "")
	if err != nil || truncated || !bytes.Equal(content, script) {
		t.Errorf("ReadFile() = %q, %v, %v", content, truncated, err)
	}
}

// TestCompModulesDirectory checks a directory module is found by its main,
// another entry is not a module, and the list follows the order.
func TestCompModulesDirectory(t *testing.T) {
	m := newCompModules(t)
	if err := m.init(); err != nil {
		t.Fatal(err)
	}
	err := m.commit("", map[string]ModuleBlob{
		"S20-files/main":        {Content: []byte("#!/bin/sh\n"), Executable: true},
		"S20-files/lib/conf.sh": {Content: []byte("x=1\n")},
		"S30-broken/README":     {Content: []byte("no main\n")},
		"com.opensvc/utilities": {Content: []byte("lib\n")},
	}, nil, "Bob <bob@example.com>", "import")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.CreateScript("aaa", 5, []byte("#!/bin/sh\n"), "", "add aaa"); err != nil {
		t.Fatal(err)
	}
	l, err := m.List()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, mod := range l {
		names = append(names, mod.Name)
	}
	if want := []string{"aaa", "files", "broken"}; len(names) != 3 || names[0] != want[0] || names[1] != want[1] || names[2] != want[2] {
		t.Fatalf("modules %v, want %v", names, want)
	}
	if l[1].Kind != "directory" || l[1].Entry != "main" || !l[1].Runnable || l[1].Changed == nil || l[1].Changed.Subject != "import" {
		t.Errorf("directory module %+v", l[1])
	}
	if l[2].Runnable || l[2].Entry != "" {
		t.Errorf("module without main %+v", l[2])
	}
	d, err := m.Get("files")
	if err != nil || len(d.Files) != 2 {
		t.Fatalf("Get() = %+v, %v", d, err)
	}
	if content, _, err := m.ReadFile("files", "lib/conf.sh"); err != nil || string(content) != "x=1\n" {
		t.Errorf("ReadFile() = %q, %v", content, err)
	}
	if _, _, err := m.ReadFile("files", "../S05-aaa"); !errors.Is(err, ErrModuleNotFound) {
		t.Errorf("ReadFile() outside the module: %v", err)
	}
}

// TestCompModulesArchive checks the archive holds the tree under compliance/,
// the scripts executable and owned by root, as the agents install them.
func TestCompModulesArchive(t *testing.T) {
	m := newCompModules(t)
	if _, err := m.CreateScript("sshd", 10, []byte("#!/bin/sh\n"), "", "add"); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	head, err := m.Archive(&buf)
	if err != nil || head == "" {
		t.Fatalf("Archive() = %q, %v", head, err)
	}
	gz, err := gzip.NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	r := tar.NewReader(gz)
	found := false
	for {
		h, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if h.Name == "compliance/S10-sshd" {
			found = true
			if h.Mode&0o111 == 0 || h.Mode&0o022 != 0 || h.Uid != 0 || h.Gid != 0 {
				t.Errorf("entry mode %o uid %d gid %d", h.Mode, h.Uid, h.Gid)
			}
		}
	}
	if !found {
		t.Error("compliance/S10-sshd not in the archive")
	}
}

// TestCompModulesStaleHead checks a change prepared over a commit the branch
// has moved from is refused: the change made meanwhile is not lost.
func TestCompModulesStaleHead(t *testing.T) {
	m := newCompModules(t)
	if _, err := m.CreateScript("one", 1, []byte("#!/bin/sh\n"), "", "one"); err != nil {
		t.Fatal(err)
	}
	stale, err := m.head()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.CreateScript("two", 2, []byte("#!/bin/sh\n"), "", "two"); err != nil {
		t.Fatal(err)
	}
	err = m.commit(stale, map[string]ModuleBlob{"S03-three": {Content: []byte("#!/bin/sh\n"), Executable: true}}, nil, "", "three")
	if !errors.Is(err, ErrModulesChanged) {
		t.Fatalf("commit over a stale head: %v, want ErrModulesChanged", err)
	}
	if _, err := m.Get("two"); err != nil {
		t.Errorf("the module created meanwhile is lost: %v", err)
	}
}
