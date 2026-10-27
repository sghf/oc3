package git

import (
	"errors"
	"strings"
	"testing"
)

// TestCompModulesDiffAndRestore checks what changed since a version is told
// file by file, and a version restored comes back as a new commit.
func TestCompModulesDiffAndRestore(t *testing.T) {
	m := newCompModules(t)
	v1, err := m.CreateDirectory("audit", 60, map[string]ModuleBlob{
		"scripts/main": {Content: []byte("#!/bin/sh\necho one\n"), Executable: true},
		"lib/old":      {Content: []byte("old\n")},
		"bin/tool":     {Content: []byte("tool\n")},
	}, "", "v1")
	if err != nil {
		t.Fatal(err)
	}
	first := v1.Changed.ID
	d, err := m.PutFile("audit", first, "scripts/main", ModuleBlob{Content: []byte("#!/bin/sh\necho two\n"), Executable: true}, "", "v2")
	if err != nil {
		t.Fatal(err)
	}
	d, err = m.PutFile("audit", d.Changed.ID, "lib/new", ModuleBlob{Content: []byte("new\n")}, "", "v3")
	if err != nil {
		t.Fatal(err)
	}
	d, err = m.DeleteFile("audit", d.Changed.ID, "lib/old", "", "v4")
	if err != nil {
		t.Fatal(err)
	}
	d, err = m.SetExecutable("audit", d.Changed.ID, "bin/tool", true, "", "v5")
	if err != nil {
		t.Fatal(err)
	}

	diff, err := m.DiffVersion("audit", first)
	if err != nil {
		t.Fatal(err)
	}
	if diff.Version.ID != first || diff.Version.Subject != "v1" {
		t.Errorf("version %+v", diff.Version)
	}
	got := map[string]ModuleFileDiff{}
	for _, f := range diff.Files {
		got[f.Path] = f
	}
	if f := got["scripts/main"]; f.Status != "modified" || f.Added != 1 || f.Deleted != 1 || !strings.Contains(f.Diff, "+echo two") {
		t.Errorf("scripts/main %+v", f)
	}
	if f := got["lib/new"]; f.Status != "added" {
		t.Errorf("lib/new %+v", f)
	}
	if f := got["lib/old"]; f.Status != "removed" {
		t.Errorf("lib/old %+v", f)
	}
	if f := got["bin/tool"]; f.WasExecutable == nil || *f.WasExecutable || f.IsExecutable == nil || !*f.IsExecutable {
		t.Errorf("bin/tool %+v", f)
	}
	if len(diff.Files) != 4 {
		t.Errorf("%d files, want 4: %+v", len(diff.Files), diff.Files)
	}

	if _, err := m.RestoreVersion("audit", first, first, "", "stale"); !errors.Is(err, ErrModuleStale) {
		t.Errorf("a restore from an old revision: %v", err)
	}
	if _, err := m.DiffVersion("audit", strings.Repeat("a", 40)); !errors.Is(err, ErrModuleNotFound) {
		t.Errorf("an unknown revision: %v", err)
	}
	if _, err := m.DiffVersion("audit", "--all"); !errors.Is(err, ErrInvalidModule) {
		t.Errorf("an option as revision: %v", err)
	}
	r, err := m.RestoreVersion("audit", d.Changed.ID, first, "Ann <ann@example.com>", "restore v1")
	if err != nil {
		t.Fatal(err)
	}
	if r.Changed.Subject != "restore v1" || len(r.History) != 6 {
		t.Errorf("restored %+v", r)
	}
	after, err := m.DiffVersion("audit", first)
	if err != nil || len(after.Files) != 0 {
		t.Errorf("the restored module differs from v1: %+v, %v", after, err)
	}
}

// TestCompModulesDiffScript checks the script of a script module is told by
// the empty path.
func TestCompModulesDiffScript(t *testing.T) {
	m := newCompModules(t)
	d, err := m.CreateScript("ssh", 40, []byte("#!/bin/sh\nexit 0\n"), "", "v1")
	if err != nil {
		t.Fatal(err)
	}
	first := d.Changed.ID
	if _, err := m.PutFile("ssh", first, "", ModuleBlob{Content: []byte("#!/bin/sh\nexit 1\n")}, "", "v2"); err != nil {
		t.Fatal(err)
	}
	diff, err := m.DiffVersion("ssh", first)
	if err != nil || len(diff.Files) != 1 || diff.Files[0].Path != "" || diff.Files[0].Status != "modified" {
		t.Errorf("DiffVersion() = %+v, %v", diff, err)
	}
}
