package cdb

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestFiltersetQuery(t *testing.T) {
	str := func(s string) *string { return &s }
	id := func(i int) *int { return &i }
	filter := func(logOp, table, field, op, value string) VFilterset {
		return VFilterset{FLogOp: logOp, FTable: str(table), FField: str(field), FOp: str(op), FValue: str(value)}
	}
	encap := func(logOp string, fsetID int) VFilterset {
		return VFilterset{FLogOp: logOp, EncapFsetID: id(fsetID)}
	}
	sets := map[int][]VFilterset{
		// demo: the nodes of Paris, but the big-memory ones.
		1: {filter("AND", "nodes", "loc_city", "=", "Paris"), encap("AND NOT", 2)},
		2: {filter("AND", "nodes", "mem_bytes", ">=", "131072")},
		// An empty filterset, and one encapsulating it.
		3: nil,
		4: {filter("AND", "nodes", "os_name", "=", "Linux"), encap("AND", 3)},
		5: {filter("AND", "nodes", "os_name", "=", "Linux"), encap("OR NOT", 3)},
		// A filterset encapsulating itself, through another.
		6: {encap("AND", 7)},
		7: {encap("AND", 6)},
		// A filter row without a filter, and an encapsulation met twice.
		8: {{FLogOp: "AND"}, encap("AND", 2), encap("OR", 2)},
	}
	entriesOf := func(_ context.Context, fsetID int) ([]VFilterset, error) { return sets[fsetID], nil }
	build := func(fsetID int) (string, []any, error) {
		return filtersetQuery(context.Background(), entriesOf, fsetID, "node_id", map[int]bool{})
	}

	t.Run("a negated encapsulation takes the complement of the filterset as a whole", func(t *testing.T) {
		query, args, err := build(1)
		if err != nil {
			t.Fatal(err)
		}
		want := `(SELECT node_id FROM nodes WHERE node_id != "" AND node_id  IN (SELECT node_id FROM nodes WHERE nodes.loc_city = ?))` +
			` INTERSECT ` +
			`(SELECT node_id FROM nodes WHERE node_id != "" AND node_id NOT IN (SELECT node_id FROM nodes WHERE node_id != "" AND node_id  IN (SELECT node_id FROM nodes WHERE nodes.mem_bytes >= ?)))`
		if query != want {
			t.Fatalf("query:\n%s\nwant:\n%s", query, want)
		}
		if !reflect.DeepEqual(args, []any{"Paris", "131072"}) {
			t.Fatalf("args %v", args)
		}
	})

	t.Run("an empty filterset selects nothing", func(t *testing.T) {
		query, _, err := build(3)
		if err != nil || query != "" {
			t.Fatalf("query %q, err %v", query, err)
		}
		query, _, err = build(4)
		if err != nil || !strings.HasSuffix(query, "INTERSECT (SELECT node_id FROM nodes WHERE 1=0)") {
			t.Fatalf("query %q, err %v", query, err)
		}
		query, _, err = build(5)
		if err != nil || !strings.HasSuffix(query, `UNION (SELECT node_id FROM nodes WHERE node_id != "" AND node_id NOT IN (SELECT node_id FROM nodes WHERE 1=0))`) {
			t.Fatalf("query %q, err %v", query, err)
		}
	})

	t.Run("a filterset encapsulating itself is an error", func(t *testing.T) {
		if _, _, err := build(6); err == nil || !strings.Contains(err.Error(), "encapsulates itself") {
			t.Fatalf("err %v", err)
		}
	})

	t.Run("a row without a filter is skipped, an encapsulation counted once", func(t *testing.T) {
		query, args, err := build(8)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(query, "UNION") || len(args) != 1 {
			t.Fatalf("query %q, args %v", query, args)
		}
	})
}
