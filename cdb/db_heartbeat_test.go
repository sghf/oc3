package cdb

import "testing"

// TestHeartbeatAsLogKeepsTheKey checks the last state of a heartbeat is
// recorded under its node, peer and name: the feeder looks it up by them.
func TestHeartbeatAsLogKeepsTheKey(t *testing.T) {
	hb := &DBHeartbeat{ClusterID: "c1", NodeID: "n1", PeerNodeID: "n2", Name: "1.rx", State: "running", Beating: 1}
	l := hb.AsLog()
	if l.ClusterID != "c1" || l.NodeID != "n1" || l.PeerNodeID != "n2" || l.Name != "1.rx" {
		t.Errorf("AsLog() = %+v", l)
	}
	if !hb.SameAsLog(l) {
		t.Error("a heartbeat differs from its own log")
	}
}
