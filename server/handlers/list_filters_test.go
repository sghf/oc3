package serverhandlers

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/opensvc/oc3/server"
)

func TestFilterCondition(t *testing.T) {
	cases := []struct {
		expr string
		cond string
		args []any
	}{
		{"dev", "n.name LIKE ?", []any{"%dev%"}},
		{"50%", "n.name LIKE ?", []any{`%50\%%`}},
		{"~^dev", "n.name REGEXP ?", []any{"^dev"}},
		{"in:a,b", "n.name IN (?,?)", []any{"a", "b"}},
		{"gte:8", "n.name >= ?", []any{"8"}},
		// A date relative to now, as in the historical collector.
		{"lt:-15m", "n.name < DATE_SUB(NOW(), INTERVAL ? SECOND)", []any{int64(900)}},
		{"gte:-1w2d3h4m5s", "n.name >= DATE_SUB(NOW(), INTERVAL ? SECOND)", []any{int64(7*86400 + 2*86400 + 3*3600 + 4*60 + 5)}},
		{"!gte:-15m", "(n.name IS NULL OR NOT (n.name >= DATE_SUB(NOW(), INTERVAL ? SECOND)))", []any{int64(900)}},
		// A negative number, or a lone "-", stays a value.
		{"gt:-15", "n.name > ?", []any{"-15"}},
		{"lt:-", "n.name < ?", []any{"-"}},
		{"empty", "(n.name IS NULL OR n.name = '')", nil},
		{"!empty", "(n.name IS NOT NULL AND n.name <> '')", nil},
		{"!dev", "(n.name IS NULL OR NOT (n.name LIKE ?))", []any{"%dev%"}},
		{"!~^dev", "(n.name IS NULL OR NOT (n.name REGEXP ?))", []any{"^dev"}},
		{"!in:a,b", "(n.name IS NULL OR NOT (n.name IN (?,?)))", []any{"a", "b"}},
		{"!eq:a", "(n.name IS NULL OR NOT (n.name = ?))", []any{"a"}},
		// A lone "!" is text, and an inversion is inverted back.
		{"!", "n.name LIKE ?", []any{"%!%"}},
		{"!!dev", "(n.name IS NULL OR NOT ((n.name IS NULL OR NOT (n.name LIKE ?))))", []any{"%dev%"}},
	}
	for _, c := range cases {
		cond, args, err := filterCondition("n.name", "name", c.expr)
		if err != nil {
			t.Errorf("%q: unexpected error %v", c.expr, err)
			continue
		}
		if cond != c.cond {
			t.Errorf("%q: got condition %q, want %q", c.expr, cond, c.cond)
		}
		if !reflect.DeepEqual(args, c.args) {
			t.Errorf("%q: got args %v, want %v", c.expr, args, c.args)
		}
	}
	for _, expr := range []string{"~(", "!~("} {
		if _, _, err := filterCondition("n.name", "name", expr); err == nil {
			t.Errorf("%q: expected an invalid regular expression error", expr)
		}
	}
}

func TestAlertFilters(t *testing.T) {
	raw := server.InQueryFilter{"services.svcname:dev2n1", "alert:down", "dash_severity:gte:3"}
	filters, err := buildFilters(&raw, propsMapping["alert"])
	if err != nil {
		t.Fatal(err)
	}
	if len(filters) != 3 {
		t.Fatalf("filters: %v", filters)
	}
	for i, want := range []string{"COALESCE(NULLIF(services.svcname, ''), nodes.nodename)", "CONCAT(COALESCE(dashboard.dash_fmt, '')", "dashboard.dash_severity"} {
		if !strings.Contains(filters[i].Expr, want) {
			t.Errorf("filter %d: %q does not use %q", i, filters[i].Expr, want)
		}
	}
}

func TestClusterListFilters(t *testing.T) {
	raw := server.InQueryFilter{"node_count:>2", "frozen:1", "cluster_name:%leopard%"}
	filters, err := buildFilters(&raw, propsMapping["clusterList"])
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"clusters.node_count", "clusters.frozen", "clusters.cluster_name"} {
		if !strings.Contains(filters[i].Expr, want) {
			t.Errorf("filter %d: %q does not use %q", i, filters[i].Expr, want)
		}
	}
	orderby := "-svc_count,cluster_name"
	exprs, err := buildOrderBy(&orderby, propsMapping["clusterList"], false)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(exprs, ",") != "clusters.svc_count DESC,clusters.cluster_name,clusters.id" {
		t.Errorf("orderby: %v", exprs)
	}
}

func TestUserCredentialsAreNoProps(t *testing.T) {
	mapping := propsMapping["user"]
	for _, prop := range []string{"password", "registration_key", "reset_password_key"} {
		if slices.Contains(mapping.Available, prop) {
			t.Errorf("%s is available", prop)
		}
		if _, ok := mapping.Props[prop]; ok {
			t.Errorf("%s is a prop", prop)
		}
		// Not filterable either: a filter would let a caller guess the value.
		raw := server.InQueryFilter{prop + ":a%"}
		if _, err := buildFilters(&raw, mapping); err == nil {
			t.Errorf("%s can be filtered on", prop)
		}
		orderby := prop
		if _, err := buildOrderBy(&orderby, mapping, false); err == nil {
			t.Errorf("%s can be sorted on", prop)
		}
	}
}

func TestOrderByEndsWithPrimaryKey(t *testing.T) {
	cases := []struct {
		mapping, orderby string
		grouped          bool
		want             string
	}{
		{"instance", "mon_availstatus", false, "svcmon.mon_availstatus,svcmon.ID"},
		// The key in the direction of the last column: one direction, read by an index.
		{"node", "-nodename", false, "nodes.nodename DESC,nodes.id DESC"},
		{"alert", "-dash_severity,dash_type", false, "dashboard.dash_severity DESC,dashboard.dash_type,dashboard.id"},
		{"disk", "disk_size", false, "diskinfo.disk_size,diskinfo.id,svcdisks.id"},
		{"filterset_filter", "f_table", false, "gen_filters.f_table,gen_filtersets_filters.id"},
		// The key requested already, in either direction: not repeated.
		{"alert", "-id", false, "dashboard.id DESC"},
		{"alert", "dash_severity,id", false, "dashboard.dash_severity,dashboard.id"},
		// Grouped rows: the key names none of them.
		{"service", "svc_app", true, "services.svc_app"},
	}
	for _, tc := range cases {
		orderby := tc.orderby
		exprs, err := buildOrderBy(&orderby, propsMapping[tc.mapping], tc.grouped)
		if err != nil {
			t.Errorf("%s %s: %v", tc.mapping, tc.orderby, err)
			continue
		}
		if got := strings.Join(exprs, ","); got != tc.want {
			t.Errorf("%s %s: got %s, want %s", tc.mapping, tc.orderby, got, tc.want)
		}
	}
	// No sort requested: the query keeps its own default order.
	if exprs, _ := buildOrderBy(nil, propsMapping["instance"], false); exprs != nil {
		t.Errorf("no orderby: %v", exprs)
	}
}

func TestResourceListJoinedProps(t *testing.T) {
	mapping := propsMapping["resourceList"]
	orderby := "services.svcname,-nodes.nodename,rid"
	exprs, err := buildOrderBy(&orderby, mapping, false)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(exprs, ","); got != "services.svcname,nodes.nodename DESC,resmon.rid,resmon.id" {
		t.Errorf("orderby: %s", got)
	}
	raw := server.InQueryFilter{"services.svcname:dev%", "res_status:down"}
	filters, err := buildFilters(&raw, mapping)
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"services.svcname", "resmon.res_status"} {
		if !strings.Contains(filters[i].Expr, want) {
			t.Errorf("filter %d: %q does not use %q", i, filters[i].Expr, want)
		}
	}
}
