package serverhandlers

import (
	"reflect"
	"testing"
)

func TestServiceActionCommand(t *testing.T) {
	cases := []struct {
		name                             string
		action, actionType, version, rid string
		local                            bool
		want                             string
	}{
		{"instance, python agent", "freeze", "pull", "2.1-1234", "", true, "freeze --local"},
		{"instance, om3 agent", "freeze", "feed", "v3.0.0-rc40-2-g2b577", "", true, "freeze --local"},
		{"instance, old agent", "freeze", "pull", "1.8-100", "", true, "freeze"},
		{"instance, unknown version", "freeze", "pull", "", "", true, "freeze"},
		{"instance, cluster-wide action", "switch", "pull", "2.1", "", true, "switch"},
		{"instance, resources", "freeze", "pull", "2.1", "fs#1,ip#0", true, "freeze --rid fs#1,ip#0"},
		{"service", "freeze", "pull", "2.1", "", false, "freeze"},
		{"instance, compliance", "compliance_check", "pull", "2.1", "", true, "compliance check"},
		{"instance, restart", "restart", "pull", "2.1", "", true, "restart --local"},
		{"push node", "thaw", "push", "2.1", "", true,
			"ssh -o StrictHostKeyChecking=no -o CheckHostIP=no -o ForwardX11=no -o ConnectTimeout=5 -o PasswordAuthentication=no opensvc@10.0.0.1 -- sudo svcmgr --service svc1 thaw --local"},
	}
	for _, c := range cases {
		got := serviceActionCommand(c.action, "svc1", c.actionType, "10.0.0.1", c.version, c.rid, c.local)
		if got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

func TestFactorizeActionEntries(t *testing.T) {
	entries := []actionEntry{
		{NodeID: "n1", SvcID: "s1", Action: "freeze", Rid: "fs#1"},
		{NodeID: "n1", Action: "pushasset"},
		{NodeID: "n1", SvcID: "s1", Action: "freeze", Rid: "ip#0"},
		{NodeID: "n2", SvcID: "s1", Action: "freeze", Rid: "fs#1"},
	}
	got := factorizeActionEntries(entries)
	if len(got) != 3 {
		t.Fatalf("got %d entries, want 3: %+v", len(got), got)
	}
	if got[0].Action != "pushasset" {
		t.Errorf("entries without rid come first, as factorize_actions() does: %+v", got)
	}
	if got[1].Rid != "fs#1,ip#0" || got[1].NodeID != "n1" {
		t.Errorf("rids of the same instance and action are merged: %+v", got[1])
	}
	if got[2].Rid != "fs#1" || got[2].NodeID != "n2" {
		t.Errorf("another instance keeps its own entry: %+v", got[2])
	}
	if !reflect.DeepEqual(got[0].indexes, []int{1}) || !reflect.DeepEqual(got[1].indexes, []int{0, 2}) || !reflect.DeepEqual(got[2].indexes, []int{3}) {
		t.Errorf("each entry knows the entries of the list it stands for: %v %v %v", got[0].indexes, got[1].indexes, got[2].indexes)
	}
}

func TestActionEntryUnsupportedKeys(t *testing.T) {
	var e actionEntry
	if err := e.UnmarshalJSON([]byte(`{"node_id":"n1","action":"compliance_check","module":"m1"}`)); err != nil {
		t.Fatal(err)
	}
	if e.NodeID != "n1" || e.Action != "compliance_check" {
		t.Errorf("known fields are read: %+v", e)
	}
	if len(e.unsupported) != 1 || e.unsupported[0] != "module" {
		t.Errorf("module is kept to be refused: %+v", e.unsupported)
	}
}

func TestServiceAndInstanceActions(t *testing.T) {
	for action := range serviceActions {
		if !instanceActions[action] {
			t.Errorf("%s is accepted on a service, not on an instance", action)
		}
	}
	for _, action := range []string{"restart", "syncall", "enable", "compliance_fix"} {
		if serviceActions[action] {
			t.Errorf("%s is accepted on a whole service", action)
		}
	}
	if serviceActions["rotate_root_pw"] || instanceActions["rotate_root_pw"] {
		t.Error("a node action is accepted on a service")
	}
}
