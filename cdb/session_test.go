package cdb

import (
	"context"
	"reflect"
	"testing"
)

type recordedEvent struct {
	name string
	data map[string]any
}

type fakePublisher struct{ events []recordedEvent }

func (f *fakePublisher) EventPublish(name string, data map[string]any) error {
	f.events = append(f.events, recordedEvent{name, data})
	return nil
}

func TestSessionNotifyChangesScope(t *testing.T) {
	ev := &fakePublisher{}
	s := NewSession(nil, ev)

	// Unscoped: the table alone, as before.
	s.SetChanges("nodes")
	if err := s.NotifyChanges(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(ev.events) != 1 || ev.events[0].name != "nodes_change" || ev.events[0].data != nil {
		t.Fatalf("unscoped events = %+v", ev.events)
	}

	// Scoped: every table carries the objects, sorted, empty ids left out.
	ev.events = nil
	s.SetChanges("svcmon", "resmon")
	s.Scope([]string{"svc-b", "", "svc-a"}, []string{"node-1"})
	s.Scope([]string{"svc-a"}, []string{"node-2"})
	if err := s.NotifyChanges(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(ev.events) != 2 {
		t.Fatalf("scoped events = %+v", ev.events)
	}
	want := map[string]any{"scope": map[string]any{
		"svc_ids": []string{"svc-a", "svc-b"}, "node_ids": []string{"node-1", "node-2"},
	}}
	for _, e := range ev.events {
		if !reflect.DeepEqual(e.data, want) {
			t.Errorf("%s data = %#v, want %#v", e.name, e.data, want)
		}
	}

	// The scope is forgotten with the tables: the next changes are unscoped.
	ev.events = nil
	s.SetChanges("services")
	if err := s.NotifyChanges(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(ev.events) != 1 || ev.events[0].data != nil {
		t.Errorf("after the scope, events = %+v", ev.events)
	}

	// A scope naming nothing still says that nothing else changed.
	ev.events = nil
	s.SetChanges("svcmon")
	s.Scope(nil, nil)
	if err := s.NotifyChanges(context.Background()); err != nil {
		t.Fatal(err)
	}
	empty := map[string]any{"scope": map[string]any{"svc_ids": []string{}, "node_ids": []string{}}}
	if len(ev.events) != 1 || !reflect.DeepEqual(ev.events[0].data, empty) {
		t.Errorf("empty scope events = %+v", ev.events)
	}
}
