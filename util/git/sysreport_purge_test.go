package git

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// sysreportRepo builds the sysreport repository of a node, one revision per
// step, each step writing (or removing, for an empty content) files of the
// working tree, as the feeder and the scheduler do.
func sysreportRepo(t *testing.T, steps []map[string]string) (Sysreport, string) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	dir := t.TempDir()
	node := "node-1"
	repo := filepath.Join(dir, node)
	run := func(env []string, args ...string) string {
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		cmd.Env = append(os.Environ(), env...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
		return string(out)
	}
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	run(nil, "init", "-q", "-b", "master")
	run(nil, "config", "user.email", "collector@example.com")
	run(nil, "config", "user.name", "collector")
	for i, step := range steps {
		for path, content := range step {
			target := filepath.Join(repo, path)
			if content == "" {
				_ = os.Remove(target)
				continue
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(target, []byte(content), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		date := []string{
			"GIT_AUTHOR_DATE=170000000" + string(rune('0'+i)) + " +0200",
			"GIT_COMMITTER_DATE=170000000" + string(rune('0'+i)) + " +0200",
		}
		run(date, "add", "-A", ".")
		run(date, "commit", "-q", "-m", "sysreport")
	}
	return Sysreport{Dir: dir}, node
}

func gitOut(t *testing.T, s Sysreport, node string, args ...string) string {
	t.Helper()
	out, err := s.purgeGit(node, nil, "", args...)
	if err != nil {
		t.Fatalf("git %v: %v", args, err)
	}
	return out
}

func TestSysreportPurge(t *testing.T) {
	s, node := sysreportRepo(t, []map[string]string{
		{"file/etc/hosts": "127.0.0.1 localhost\n", "cmd/ip(space)route": "default via 10.0.0.1\n"},
		{"file/etc/app/db.conf": "password=hunter2\n"},
		{"file/etc/hosts": "127.0.0.1 localhost\n10.0.0.2 db\n", "file/etc/app/db.conf": "password=s3cret\n"},
		// The same content as a version of the secret, under another path.
		{"file/etc/copy": "password=hunter2\n"},
		{"file/etc/app/db.conf": ""},
		{"cmd/ip(space)route": "default via 10.0.0.254\n"},
	})
	repo := filepath.Join(s.Dir, node)
	before := strings.Fields(gitOut(t, s, node, "log", "--format=%ad", "--date=raw"))

	// The secret of revision 3, no other file holds.
	secret := strings.TrimSpace(gitOut(t, s, node, "rev-parse", "HEAD~3:file/etc/app/db.conf"))

	result, err := s.Purge(node, []SysreportFile{{Kind: "file", Path: "/etc/app/db.conf"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Paths) != 1 || result.Paths[0] != "file/etc/app/db.conf" {
		t.Errorf("paths = %q", result.Paths)
	}
	// Revisions 2 to 6 recreated; the 2nd and 5th held only the secret: dropped.
	if result.Rewritten != 3 || result.Dropped != 2 {
		t.Errorf("rewritten %d, dropped %d; want 3 and 2", result.Rewritten, result.Dropped)
	}
	// Two contents: one destroyed, the other kept, file/etc/copy holding it.
	if result.Removed != 1 || result.Kept != 1 {
		t.Errorf("removed %d, kept %d; want 1 and 1", result.Removed, result.Kept)
	}
	if out := gitOut(t, s, node, "log", "--all", "--format=%H", "--", "file/etc/app/db.conf"); strings.TrimSpace(out) != "" {
		t.Errorf("the history still holds the file: %s", out)
	}
	if _, err := s.purgeGit(node, nil, "", "cat-file", "-e", secret); err == nil {
		t.Error("the content of the secret is still stored")
	}
	if strings.TrimSpace(gitOut(t, s, node, "rev-parse", "HEAD")) != result.NewHead || result.NewHead == result.OldHead {
		t.Errorf("head %q, result %+v", gitOut(t, s, node, "rev-parse", "HEAD"), result)
	}
	// The other files and the dates of the revisions kept are unchanged.
	if got := gitOut(t, s, node, "show", "HEAD:file/etc/hosts"); got != "127.0.0.1 localhost\n10.0.0.2 db\n" {
		t.Errorf("hosts = %q", got)
	}
	if got := gitOut(t, s, node, "show", "HEAD:cmd/ip(space)route"); got != "default via 10.0.0.254\n" {
		t.Errorf("route = %q", got)
	}
	after := strings.Fields(gitOut(t, s, node, "log", "--format=%ad", "--date=raw"))
	if len(after) != 2*4 || after[0] != before[0] || after[len(after)-2] != before[len(before)-2] {
		t.Errorf("dates before %q, after %q", before, after)
	}
	// Nothing left behind: no former position of the branch, a clean index.
	if out := gitOut(t, s, node, "reflog", "--all"); strings.TrimSpace(out) != "" {
		t.Errorf("reflog = %q", out)
	}
	if out := gitOut(t, s, node, "status", "--porcelain"); strings.TrimSpace(out) != "" {
		t.Errorf("status = %q", out)
	}

	// Again: nothing to purge.
	again, err := s.Purge(node, []SysreportFile{{Kind: "file", Path: "/etc/app/db.conf"}})
	if err != nil || len(again.Paths) != 0 {
		t.Errorf("second purge = %+v, %v", again, err)
	}
	if _, err := os.Stat(filepath.Join(repo, "file", "etc", "copy")); err != nil {
		t.Error("another file was removed from the working tree")
	}
	if _, err := os.Stat(filepath.Join(repo, "file", "etc", "app")); !os.IsNotExist(err) {
		t.Errorf("the directory left empty is still there: %v", err)
	}
}

func TestSysreportPurgeCommandAndCurrentFile(t *testing.T) {
	// The command output changes and is still reported: the working tree holds
	// its last version while the former ones are rebuilt.
	s, node := sysreportRepo(t, []map[string]string{
		{"file/etc/hosts": "a\n", "cmd/cat(space)(slash)etc(slash)shadow": "root:$6$x\n"},
		{"file/etc/hosts": "b\n", "cmd/cat(space)(slash)etc(slash)shadow": "root:$6$y\n"},
		{"file/etc/hosts": "c\n"},
	})
	result, err := s.Purge(node, []SysreportFile{{Kind: "command", Path: "cat /etc/shadow"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Paths) != 1 || result.Paths[0] != "cmd/cat(space)(slash)etc(slash)shadow" {
		t.Errorf("paths = %q", result.Paths)
	}
	// Still in the working tree before: it leaves it, and the next commit does not
	// record it again.
	if _, err := os.Stat(filepath.Join(s.Dir, node, "cmd", "cat(space)(slash)etc(slash)shadow")); !os.IsNotExist(err) {
		t.Errorf("the file is still in the working tree: %v", err)
	}
	if err := Commit(filepath.Join(s.Dir, node)); err != nil {
		t.Fatal(err)
	}
	if out := gitOut(t, s, node, "log", "--all", "--format=%H", "--", "cmd/cat(space)(slash)etc(slash)shadow"); strings.TrimSpace(out) != "" {
		t.Errorf("recorded again: %s", out)
	}
}

func TestSysreportPurgeRefusesOtherHistories(t *testing.T) {
	s, node := sysreportRepo(t, []map[string]string{{"file/etc/hosts": "a\n"}, {"file/etc/hosts": "b\n"}})
	gitOut(t, s, node, "tag", "kept")
	if _, err := s.Purge(node, []SysreportFile{{Kind: "file", Path: "/etc/hosts"}}); err != ErrUnexpectedHistory {
		t.Errorf("purge with a tag = %v, want ErrUnexpectedHistory", err)
	}
	if out := gitOut(t, s, node, "log", "--format=%H", "--", "file/etc/hosts"); len(strings.Fields(out)) != 2 {
		t.Errorf("the history changed: %s", out)
	}
	if _, err := s.Purge("../escape", nil); err != ErrNoSysreport {
		t.Errorf("node id out of bounds = %v", err)
	}
}
