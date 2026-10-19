package serverhandlers

import (
	"strings"
	"testing"

	"github.com/opensvc/oc3/server"
)

func TestCheckCompPairs(t *testing.T) {
	str := func(s string) *string { return &s }
	yes := true
	node := server.ComplianceAttachmentPair{Object: server.ComplianceAttachmentPairObjectModuleset, ObjectId: "12", NodeId: str("n1")}
	svc := server.ComplianceAttachmentPair{Object: server.ComplianceAttachmentPairObjectRuleset, ObjectId: "7", SvcId: str("s1"), Slave: &yes}
	ok := server.ComplianceAttachmentsInput{Action: server.Attach, Pairs: []server.ComplianceAttachmentPair{node, svc}}
	if err := checkCompPairs(ok); err != nil {
		t.Errorf("valid batch refused: %v", err)
	}
	many := make([]server.ComplianceAttachmentPair, maxCompAttachmentPairs+1)
	for i := range many {
		many[i] = node
	}
	for name, c := range map[string]struct {
		body server.ComplianceAttachmentsInput
		want string
	}{
		"no action":     {server.ComplianceAttachmentsInput{Pairs: ok.Pairs}, "action"},
		"no pair":       {server.ComplianceAttachmentsInput{Action: server.Detach}, "pairs"},
		"too many":      {server.ComplianceAttachmentsInput{Action: server.Detach, Pairs: many}, "pairs"},
		"bad object":    {server.ComplianceAttachmentsInput{Action: server.Attach, Pairs: []server.ComplianceAttachmentPair{node, {Object: "tag", ObjectId: "1", NodeId: str("n1")}}}, "pairs[1].object"},
		"no object id":  {server.ComplianceAttachmentsInput{Action: server.Attach, Pairs: []server.ComplianceAttachmentPair{{Object: node.Object, NodeId: str("n1")}}}, "object_id"},
		"no target":     {server.ComplianceAttachmentsInput{Action: server.Attach, Pairs: []server.ComplianceAttachmentPair{{Object: node.Object, ObjectId: "1", NodeId: str("")}}}, "exactly one"},
		"both targets":  {server.ComplianceAttachmentsInput{Action: server.Attach, Pairs: []server.ComplianceAttachmentPair{{Object: node.Object, ObjectId: "1", NodeId: str("n1"), SvcId: str("s1")}}}, "exactly one"},
		"slave on node": {server.ComplianceAttachmentsInput{Action: server.Attach, Pairs: []server.ComplianceAttachmentPair{{Object: node.Object, ObjectId: "1", NodeId: str("n1"), Slave: &yes}}}, "slave"},
	} {
		err := checkCompPairs(c.body)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err %v, want one about %q", name, err, c.want)
		}
	}
}
