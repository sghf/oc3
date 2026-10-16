package serverhandlers

import (
	"strings"
	"testing"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/git"
)

func TestCleanPurgeFiles(t *testing.T) {
	files, err := cleanPurgeFiles([]server.SysreportPurgeFile{
		{Kind: "file", Path: " /etc/app/db.conf "},
		{Kind: "command", Path: "cat /etc/shadow"},
		{Kind: "file", Path: "/etc/app/db.conf"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []git.SysreportFile{{Kind: "file", Path: "/etc/app/db.conf"}, {Kind: "command", Path: "cat /etc/shadow"}}
	if len(files) != len(want) || files[0] != want[0] || files[1] != want[1] {
		t.Errorf("files = %+v, want %+v", files, want)
	}

	many := make([]server.SysreportPurgeFile, sysreportPurgeMaxFiles+1)
	for i := range many {
		many[i] = server.SysreportPurgeFile{Kind: "file", Path: "/f" + strings.Repeat("x", i)}
	}
	refused := map[string][]server.SysreportPurgeFile{
		"none":            nil,
		"too many":        many,
		"unknown kind":    {{Kind: "dir", Path: "/etc"}},
		"empty path":      {{Kind: "file", Path: "  "}},
		"relative file":   {{Kind: "file", Path: "etc/hosts"}},
		"path with a NUL": {{Kind: "command", Path: "cat\x00x"}},
		"path too long":   {{Kind: "file", Path: "/" + strings.Repeat("a", sysreportPurgeMaxPath)}},
	}
	for name, raw := range refused {
		if _, err := cleanPurgeFiles(raw); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestPurgeStatusResponse(t *testing.T) {
	st := git.PurgeStatus{
		PurgeRequest: git.PurgeRequest{ID: "p1", Files: []git.SysreportFile{{Kind: "file", Path: "/etc/x"}}, Author: "a@b"},
		State:        "failed",
		Nodes: []git.PurgeNodeStatus{
			{NodeID: "n1", PurgeResult: git.PurgeResult{Paths: []string{"file/etc/x"}, Rewritten: 2, Removed: 1}},
			{NodeID: "n2", Error: "boom"},
		},
	}
	got := purgeStatusResponse(st).Data
	if got.Id != "p1" || got.State != "failed" || len(got.NodeIds) != 0 || got.NodeIds == nil {
		t.Errorf("purge = %+v", got)
	}
	if len(got.Nodes) != 2 || got.Nodes[0].Rewritten != 2 || got.Nodes[0].Error != nil {
		t.Errorf("node 1 = %+v", got.Nodes[0])
	}
	if got.Nodes[1].Error == nil || *got.Nodes[1].Error != "boom" || got.Nodes[1].Paths == nil {
		t.Errorf("node 2 = %+v", got.Nodes[1])
	}
	if got.Started != nil || got.Finished != nil || got.Error != nil {
		t.Errorf("unset times or error are not absent: %+v", got)
	}
}
