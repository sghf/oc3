package scheduler

import (
	"testing"

	"github.com/opensvc/oc3/cdb"
)

func TestMetricFiltersetSQL(t *testing.T) {
	const template = "SELECT COUNT(*) FROM svcmon WHERE svc_id IN (%%fset_svc_ids%%) AND node_id IN (%%fset_node_ids%%)"
	cases := []struct {
		name string
		ids  cdb.FiltersetIDs
		want string
	}{
		{
			name: "resolved",
			ids:  cdb.FiltersetIDs{SvcIDs: `"s1","s2"`, NodeIDs: `"n1"`},
			want: `SELECT COUNT(*) FROM svcmon WHERE svc_id IN ("s1","s2") AND node_id IN ("n1")`,
		},
		{
			name: "empty",
			ids:  cdb.FiltersetIDs{},
			want: `SELECT COUNT(*) FROM svcmon WHERE svc_id IN ("magic1234567890") AND node_id IN ("magic1234567890")`,
		},
	}
	for _, c := range cases {
		if got := metricFiltersetSQL(template, c.ids); got != c.want {
			t.Errorf("%s: got %s, want %s", c.name, got, c.want)
		}
	}
}

// Each filterset is computed from the metric's template: the substitution of
// one must not leak into the next, whatever the order the filtersets come in.
func TestMetricFiltersetSQLKeepsTemplate(t *testing.T) {
	template := "SELECT COUNT(*) FROM nodes WHERE node_id IN (%%fset_node_ids%%)"
	metric := cdb.Metric{SQL: &template}
	first := metricFiltersetSQL(*metric.SQL, cdb.FiltersetIDs{NodeIDs: `"n1"`})
	second := metricFiltersetSQL(*metric.SQL, cdb.FiltersetIDs{NodeIDs: `"n2"`})
	if first == second {
		t.Fatalf("both filtersets got %s", first)
	}
	if want := `SELECT COUNT(*) FROM nodes WHERE node_id IN ("n2")`; second != want {
		t.Errorf("got %s, want %s", second, want)
	}
	if *metric.SQL != template {
		t.Errorf("the template changed to %s", *metric.SQL)
	}
}
