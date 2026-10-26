package serverhandlers

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// TestCompDesignerApplyRefusals covers the refusals of a designer operation
// made before any query: the database of the writer is never reached.
func TestCompDesignerApplyRefusals(t *testing.T) {
	str := func(s string) *string { return &s }
	ruleset, moduleset := server.CompDesignerRuleset, server.CompDesignerModuleset
	for name, c := range map[string]struct {
		op   server.ComplianceDesignerOperation
		want string
	}{
		"bad kind":           {server.ComplianceDesignerOperation{Op: server.CompDesignerOpDelete, Kind: "tag", Ref: str("1")}, "invalid kind"},
		"create no name":     {server.ComplianceDesignerOperation{Op: server.CompDesignerOpCreate, Kind: ruleset, As: str("tmp:-1")}, "name is mandatory"},
		"create blank name":  {server.ComplianceDesignerOperation{Op: server.CompDesignerOpCreate, Kind: ruleset, Name: str("  "), As: str("tmp:-1")}, "name is mandatory"},
		"create no as":       {server.ComplianceDesignerOperation{Op: server.CompDesignerOpCreate, Kind: moduleset, Name: str("m")}, "as is mandatory"},
		"no ref":             {server.ComplianceDesignerOperation{Op: server.CompDesignerOpDelete, Kind: ruleset}, "ref is mandatory"},
		"unknown tmp":        {server.ComplianceDesignerOperation{Op: server.CompDesignerOpDelete, Kind: ruleset, Ref: str("tmp:-9")}, "not created by a previous operation"},
		"tmp of other kind":  {server.ComplianceDesignerOperation{Op: server.CompDesignerOpDelete, Kind: moduleset, Ref: str("tmp:-1")}, "not created by a previous operation"},
		"rename no name":     {server.ComplianceDesignerOperation{Op: server.CompDesignerOpRename, Kind: ruleset, Ref: str("tmp:-1")}, "name is mandatory"},
		"clone no as":        {server.ComplianceDesignerOperation{Op: server.CompDesignerOpClone, Kind: ruleset, Ref: str("tmp:-1")}, "as is mandatory"},
		"include no kind":    {server.ComplianceDesignerOperation{Op: server.CompDesignerOpInclude, Kind: ruleset, Ref: str("tmp:-1"), Child: str("tmp:-1")}, "child_kind is mandatory"},
		"include no child":   {server.ComplianceDesignerOperation{Op: server.CompDesignerOpInclude, Kind: ruleset, Ref: str("tmp:-1"), ChildKind: &ruleset}, "child is mandatory"},
		"moduleset in rset":  {server.ComplianceDesignerOperation{Op: server.CompDesignerOpInclude, Kind: ruleset, Ref: str("tmp:-1"), ChildKind: &moduleset, Child: str("tmp:-2")}, "holds no moduleset"},
		"team no team":       {server.ComplianceDesignerOperation{Op: server.CompDesignerOpAddTeam, Kind: ruleset, Ref: str("tmp:-1")}, "team is mandatory"},
		"team no role":       {server.ComplianceDesignerOperation{Op: server.CompDesignerOpAddTeam, Kind: ruleset, Ref: str("tmp:-1"), Team: str("ops")}, "role is mandatory"},
		"move no to_role":    {server.ComplianceDesignerOperation{Op: server.CompDesignerOpMoveTeam, Kind: ruleset, Ref: str("tmp:-1"), Team: str("ops"), Role: ptr(server.CompDesignerPublication)}, "to_role is mandatory"},
		"set type no type":   {server.ComplianceDesignerOperation{Op: server.CompDesignerOpSetType, Kind: ruleset, Ref: str("tmp:-1")}, "ruleset_type is mandatory"},
		"set public no flag": {server.ComplianceDesignerOperation{Op: server.CompDesignerOpSetPublic, Kind: ruleset, Ref: str("tmp:-1")}, "ruleset_public is mandatory"},
		"variable no var":    {server.ComplianceDesignerOperation{Op: server.CompDesignerOpDeleteVariable, Kind: ruleset, Ref: str("tmp:-1")}, "variable is mandatory"},
		"add var no name":    {server.ComplianceDesignerOperation{Op: server.CompDesignerOpAddVariable, Kind: ruleset, Ref: str("tmp:-1")}, "var_name is mandatory"},
		"module on ruleset":  {server.ComplianceDesignerOperation{Op: server.CompDesignerOpAddModule, Kind: ruleset, Ref: str("tmp:-1"), Name: str("m")}, "does not apply to a ruleset"},
		"variable on modset": {server.ComplianceDesignerOperation{Op: server.CompDesignerOpAddVariable, Kind: moduleset, Ref: str("tmp:-2"), VarName: str("v")}, "does not apply to a moduleset"},
		"module no module":   {server.ComplianceDesignerOperation{Op: server.CompDesignerOpDeleteModule, Kind: moduleset, Ref: str("tmp:-2")}, "module is mandatory"},
		"add module no name": {server.ComplianceDesignerOperation{Op: server.CompDesignerOpAddModule, Kind: moduleset, Ref: str("tmp:-2")}, "name is mandatory"},
	} {
		// tmp:-1 is a ruleset, tmp:-2 a moduleset, created by earlier operations.
		w := &compWriter{tmp: map[string]int64{}}
		w.created(cdb.CompRulesetKind, "tmp:-1", 10)
		w.created(cdb.CompModulesetKind, "tmp:-2", 20)
		err := w.apply(c.op)
		var he *httpError
		if !errors.As(err, &he) || he.status != http.StatusBadRequest || !strings.Contains(he.msg, c.want) {
			t.Errorf("%s: err %v, want a 400 about %q", name, err, c.want)
		}
	}
}

func TestCompWriterTmpRefs(t *testing.T) {
	w := &compWriter{tmp: map[string]int64{}}
	w.created(cdb.CompRulesetKind, "tmp:-3", 1057)
	// A real id or a name is no temporary reference: nothing recorded.
	w.created(cdb.CompRulesetKind, "12", 12)
	if len(w.tmp) != 1 {
		t.Fatalf("tmp %v, want one reference", w.tmp)
	}
	if id, err := w.resolve(cdb.CompRulesetKind, "tmp:-3"); err != nil || id != 1057 {
		t.Errorf("resolve tmp:-3: %d, %v; want 1057", id, err)
	}
	if _, err := w.resolve(cdb.CompModulesetKind, "tmp:-3"); err == nil {
		t.Errorf("a ruleset's reference resolved as a moduleset")
	}
}

func ptr[T any](v T) *T { return &v }
