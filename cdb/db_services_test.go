package cdb

import (
	"errors"
	"fmt"
	"testing"
)

// TestIsServiceID checks that a svc_id is told from a svcname, the two sharing
// the {svc_id} of the service routes.
func TestIsServiceID(t *testing.T) {
	for key, want := range map[string]bool{
		"d564e6df-b0fa-11f1-93de-b292af0c1f00":   true,
		"D564E6DF-B0FA-11F1-93DE-B292AF0C1F00":   true,
		"dev/svc/oc3":                            false,
		"system/sec/ca":                          false,
		"":                                       false,
		"{d564e6df-b0fa-11f1-93de-b292af0c1f00}": false,
		"urn:uuid:d564e6df-b0fa-11f1-93de-b292af0c1f00": false,
	} {
		if got := isServiceID(key); got != want {
			t.Errorf("isServiceID(%q) = %v, want %v", key, got, want)
		}
	}
}

// TestAmbiguousServiceError checks that the error naming the svcname is an
// ErrServiceAmbiguous, wrapped or not, which the handlers answer with 409.
func TestAmbiguousServiceError(t *testing.T) {
	err := fmt.Errorf("lookup: %w", AmbiguousServiceError{Name: "system/sec/ca"})
	if !errors.Is(err, ErrServiceAmbiguous) {
		t.Fatal("not an ErrServiceAmbiguous")
	}
	if want := "lookup: several services are named system/sec/ca, in different clusters: use the svc_id"; err.Error() != want {
		t.Errorf("got %q, want %q", err.Error(), want)
	}
}
