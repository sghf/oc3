package cdb

import (
	"testing"
)

func TestCompAttachmentTables(t *testing.T) {
	for _, k := range []CompKind{CompModulesetKind, CompRulesetKind} {
		nodes, services, object := compAttachmentTables(k)
		if nodes.Name != k.NodesTable || services.Name != k.ServicesTable || object.Name != k.Table {
			t.Errorf("%s: tables %s, %s, %s, want %s, %s, %s", k.Name,
				nodes.Name, services.Name, object.Name, k.NodesTable, k.ServicesTable, k.Table)
		}
	}
}
