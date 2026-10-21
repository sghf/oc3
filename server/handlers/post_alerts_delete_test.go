package serverhandlers

import "testing"

func TestAuditAlertsDict(t *testing.T) {
	types := map[int64]string{}
	for i := int64(1); i <= auditIDs+5; i++ {
		typ := "node not updated"
		if i%2 == 0 {
			typ = "action errors"
		}
		types[i] = typ
	}
	dict := auditAlertsDict(types)
	if dict["count"] != auditIDs+5 {
		t.Errorf("count: %v", dict["count"])
	}
	if ids := dict["ids"].([]int64); len(ids) != auditIDs || ids[0] != 1 {
		t.Errorf("the first ids are listed, in order: %d ids, first %d", len(ids), ids[0])
	}
	if dict["more_ids"] != 5 {
		t.Errorf("more_ids: %v", dict["more_ids"])
	}
	if got, want := dict["types"], "node not updated (253), action errors (252)"; got != want {
		t.Errorf("types: got %q, want %q", got, want)
	}
}
