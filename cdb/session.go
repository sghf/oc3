package cdb

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
)

type (
	Session struct {
		db     execContexter
		ev     eventPublisher
		tables map[string]struct{}
		mu     sync.RWMutex

		// scoped is set once Scope named the objects the changes concern; svcIDs
		// and nodeIDs are those objects.
		scoped  bool
		svcIDs  map[string]struct{}
		nodeIDs map[string]struct{}
	}

	eventPublisher interface {
		EventPublish(eventName string, data map[string]any) error
	}
)

func NewSession(db execContexter, ev eventPublisher) *Session {
	return &Session{db: db, ev: ev, tables: make(map[string]struct{})}
}

// Scope names the services and the nodes the changes of the session concern,
// for the next NotifyChanges to announce them with every table: a view showing
// another object needs not read it again. Only a session owned by a single job
// may be scoped, a worker job feeding the data of a node for instance: the
// changes of a shared session, as the api server's, concern any object. Empty
// ids are ignored; the ids add up until NotifyChanges.
func (t *Session) Scope(svcIDs, nodeIDs []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.svcIDs == nil {
		t.svcIDs = make(map[string]struct{})
		t.nodeIDs = make(map[string]struct{})
	}
	t.scoped = true
	for _, id := range svcIDs {
		if id != "" {
			t.svcIDs[id] = struct{}{}
		}
	}
	for _, id := range nodeIDs {
		if id != "" {
			t.nodeIDs[id] = struct{}{}
		}
	}
}

// NotifyChanges publishes a "<table>_change" event for each table changed since
// the previous call, and forgets them: a session shared by many requests, as the
// api server's, would otherwise announce again every table changed since it
// started. A scoped session (Scope) adds to each event the objects concerned,
// as {"scope": {"svc_ids": [...], "node_ids": [...]}}: only those changed.
func (t *Session) NotifyChanges(ctx context.Context) error {
	slog.Debug("NotifyChanges")
	if t.ev == nil {
		return fmt.Errorf("NotifyChanges: eventPublisher is not configured")
	}
	tables, scope := t.takeChanges()
	for _, tableName := range tables {
		var data map[string]any
		if scope != nil {
			data = map[string]any{"scope": scope}
		}
		if err := t.NotifyTableChangeWithData(ctx, tableName, data); err != nil {
			return err
		}
	}
	return nil
}

func (t *Session) NotifyTableChangeWithData(ctx context.Context, tableName string, data map[string]any) error {
	// Same guard as NotifyChanges: the server component creates its session with a
	// nil publisher, and dereferencing it here panicked the request after the write
	// had already been committed.
	if t.ev == nil {
		return fmt.Errorf("NotifyTableChangeWithData: eventPublisher is not configured")
	}
	if err := t.ev.EventPublish(tableName+"_change", data); err != nil {
		return fmt.Errorf("EventPublish send %s: %w", tableName, err)
	}
	slog.Debug(fmt.Sprintf("table %s change notified", tableName))
	return nil
}

func (t *Session) SetChanges(s ...string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, table := range s {
		t.tables[table] = struct{}{}
	}
}

// takeChanges returns the changed tables and their scope, nil when the session is
// not scoped, and empties both.
func (t *Session) takeChanges() ([]string, map[string]any) {
	t.mu.Lock()
	defer t.mu.Unlock()
	r := make([]string, 0, len(t.tables))
	for s := range t.tables {
		r = append(r, s)
	}
	t.tables = make(map[string]struct{})
	var scope map[string]any
	if t.scoped {
		scope = map[string]any{"svc_ids": sortedIDs(t.svcIDs), "node_ids": sortedIDs(t.nodeIDs)}
	}
	t.scoped, t.svcIDs, t.nodeIDs = false, nil, nil
	return r, scope
}

func sortedIDs(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
