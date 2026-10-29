package cdb

import (
	"testing"

	"github.com/opensvc/oc3/schema"
)

func TestActionsFirst(t *testing.T) {
	own := ColumnFilter{Col: schema.SvcactionsStatus, Expr: "svcactions.status = ?"}
	joined := ColumnFilter{Col: schema.ServicesSvcname, Expr: "services.svcname LIKE ?"}
	cases := []struct {
		name string
		p    ListParams
		want bool
	}{
		{"default sort", ListParams{}, true},
		{"own sort and filter", ListParams{OrderBy: []string{"svcactions.begin DESC", "svcactions.id DESC"},
			Filters: []ColumnFilter{own}}, true},
		{"joined filter", ListParams{Filters: []ColumnFilter{own, joined}}, false},
		{"raw filter", ListParams{Filters: []ColumnFilter{{Expr: "1=1"}}}, false},
		{"joined sort", ListParams{OrderBy: []string{"services.svcname", "svcactions.id"}}, false},
		{"count", ListParams{CountOnly: true}, false},
	}
	for _, tc := range cases {
		if got := actionsFirst(tc.p); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}
