package mariadb

import (
	"context"
	"database/sql"
	"slices"
	"strings"
	"testing"
)

type recordedExec struct {
	queries []string
	args    [][]any
}

type affected int64

func (a affected) LastInsertId() (int64, error) { return 0, nil }
func (a affected) RowsAffected() (int64, error) { return int64(a), nil }

func (r *recordedExec) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	r.queries = append(r.queries, query)
	r.args = append(r.args, args)
	return affected(len(r.queries)), nil
}

// TestInsertOrUpdateUpdatesThenInsertsMissing checks the rows are written by
// an UPDATE matching the key, then an INSERT of the rows the table has not,
// never an INSERT ... ON DUPLICATE KEY UPDATE of them all: that would consume
// an auto-increment value for each row, those only updated included.
func TestInsertOrUpdateUpdatesThenInsertsMissing(t *testing.T) {
	r := &recordedExec{}
	u := InsertOrUpdate{
		Table: "node_ip",
		Keys:  []string{"node_id", "addr"},
		Mappings: Mappings{
			{To: "node_id"},
			{To: "addr"},
			{To: "mask"},
			{To: "updated", Raw: "NOW()"},
		},
		Data: []any{
			map[string]any{"node_id": "n1", "addr": "10.0.0.1", "mask": "24"},
			map[string]any{"node_id": "n1", "addr": "10.0.0.2", "mask": "16"},
		},
	}
	changed, inserted, err := u.Exec(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.queries) != 2 {
		t.Fatalf("%d statements, want 2: %q", len(r.queries), r.queries)
	}
	source := "SELECT ? AS node_id, ? AS addr, ? AS mask, NOW() AS updated UNION ALL SELECT ?, ?, ?, NOW()"
	if want := "UPDATE node_ip t JOIN (" + source + ") s ON t.node_id <=> s.node_id AND t.addr <=> s.addr SET t.mask = s.mask, t.updated = s.updated"; r.queries[0] != want {
		t.Errorf("update:\n%s\nwant:\n%s", r.queries[0], want)
	}
	if want := "INSERT INTO node_ip (node_id, addr, mask, updated) SELECT s.node_id, s.addr, s.mask, s.updated FROM (" + source + ") s WHERE NOT EXISTS (SELECT 1 FROM node_ip t WHERE t.node_id <=> s.node_id AND t.addr <=> s.addr)"; !strings.HasPrefix(r.queries[1], want) {
		t.Errorf("insert:\n%s\nwant prefix:\n%s", r.queries[1], want)
	}
	args := []any{"n1", "10.0.0.1", "24", "n1", "10.0.0.2", "16"}
	for i := range r.args {
		if !slices.Equal(r.args[i], args) {
			t.Errorf("args of statement %d: %v, want %v", i, r.args[i], args)
		}
	}
	if changed != 1 || inserted != 2 {
		t.Errorf("changed %d inserted %d, want the counts of the update and the insert", changed, inserted)
	}
}

// TestInsertOrUpdateWithoutKeyFails checks a write without a unique key is
// refused: no row would ever be found again.
func TestInsertOrUpdateWithoutKeyFails(t *testing.T) {
	u := InsertOrUpdate{Table: "t", Mappings: Mappings{{To: "a"}}, Data: []any{map[string]any{"a": 1}}}
	if _, _, err := u.Exec(context.Background(), &recordedExec{}); err == nil {
		t.Error("no error without Keys")
	}
}
