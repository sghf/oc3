package cdb

import (
	"slices"
	"testing"
)

// TestGivenRowsNamesTheFirstRow checks the rows given as a source name their
// columns once, in the first SELECT, the values all passed as arguments in order.
func TestGivenRowsNamesTheFirstRow(t *testing.T) {
	source, args := givenRows([]string{"svc_id", "dash_type"}, [][]any{{"s1", "t1"}, {"s2", "t2"}})
	if want := "SELECT ? AS svc_id, ? AS dash_type UNION ALL SELECT ?, ?"; source != want {
		t.Errorf("source %q, want %q", source, want)
	}
	if want := []any{"s1", "t1", "s2", "t2"}; !slices.Equal(args, want) {
		t.Errorf("args %v, want %v", args, want)
	}
}
