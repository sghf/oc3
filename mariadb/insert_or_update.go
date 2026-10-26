package mariadb

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
)

type (
	// InsertOrUpdate writes rows in a table: those whose unique key matches a
	// row of the table update it in place, the others are inserted.
	//
	// An INSERT ... ON DUPLICATE KEY UPDATE would consume an auto-increment
	// value for each row given, a row only updated included: the feeds send the
	// same rows again and again. The rows already there are updated by an
	// UPDATE, which consumes none, then only the missing ones inserted.
	InsertOrUpdate struct {
		Table string

		// Keys are the columns of the unique key of the table the rows are
		// matched on, not updated.
		Keys []string

		Mappings Mappings
		Data     any

		// Log enable query logging
		Log bool

		names        []string
		placeholders [][]string
		updates      []string
		values       []any
	}
)

// Exec writes the rows and returns the count of rows changed and of rows
// inserted.
func (t *InsertOrUpdate) Exec(ctx context.Context, db ExecContexter) (changed, inserted int64, err error) {
	if len(t.Keys) == 0 {
		return 0, 0, fmt.Errorf("insert or update %s: no unique key", t.Table)
	}
	if err := t.load(); err != nil {
		return 0, 0, err
	}
	if len(t.values) == 0 {
		return 0, 0, nil
	}
	// The rows given, as a derived table s.
	rows := make([]string, len(t.placeholders))
	for i, placeholders := range t.placeholders {
		if i == 0 {
			named := make([]string, len(placeholders))
			for j, placeholder := range placeholders {
				named[j] = placeholder + " AS " + t.names[j]
			}
			placeholders = named
		}
		rows[i] = "SELECT " + strings.Join(placeholders, ", ")
	}
	source := strings.Join(rows, " UNION ALL ")
	// Null-safe: a key column may be NULL, which a unique key does not match.
	match := make([]string, len(t.Keys))
	for i, k := range t.Keys {
		match[i] = fmt.Sprintf("t.%s <=> s.%s", k, k)
	}
	matchClause := strings.Join(match, " AND ")

	if len(t.updates) > 0 {
		assignments := make([]string, len(t.updates))
		for i, c := range t.updates {
			assignments[i] = fmt.Sprintf("t.%s = s.%s", c, c)
		}
		query := fmt.Sprintf("UPDATE %s t JOIN (%s) s ON %s SET %s", t.Table, source, matchClause, strings.Join(assignments, ", "))
		if changed, err = t.exec(ctx, db, query); err != nil {
			return 0, 0, fmt.Errorf("insert or update %s: update: %w", t.Table, err)
		}
	}

	selected := make([]string, len(t.names))
	for i, c := range t.names {
		selected[i] = "s." + c
	}
	// A row inserted meanwhile by another writer takes the values given.
	onDuplicate := fmt.Sprintf("%s.%s = %s.%s", t.Table, t.Keys[0], t.Table, t.Keys[0])
	if len(t.updates) > 0 {
		assignments := make([]string, len(t.updates))
		for i, c := range t.updates {
			assignments[i] = fmt.Sprintf("%s = VALUES(%s)", c, c)
		}
		onDuplicate = strings.Join(assignments, ", ")
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM (%s) s WHERE NOT EXISTS (SELECT 1 FROM %s t WHERE %s) ON DUPLICATE KEY UPDATE %s",
		t.Table, strings.Join(t.names, ", "), strings.Join(selected, ", "), source, t.Table, matchClause, onDuplicate)
	if inserted, err = t.exec(ctx, db, query); err != nil {
		return 0, 0, fmt.Errorf("insert or update %s: insert: %w", t.Table, err)
	}
	return changed, inserted, nil
}

func (t *InsertOrUpdate) exec(ctx context.Context, db ExecContexter, query string) (int64, error) {
	if t.Log {
		slog.Info(fmt.Sprintf("InsertOrUpdate table: %s SQL: %s VALUES:%#v", t.Table, query, t.values))
	}
	result, err := db.ExecContext(ctx, query, t.values...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// ExecContextAndCountRowsAffected writes the rows and returns the count of
// rows changed or inserted.
func (t *InsertOrUpdate) ExecContextAndCountRowsAffected(ctx context.Context, db ExecContexter) (int64, error) {
	changed, inserted, err := t.Exec(ctx, db)
	return changed + inserted, err
}

func (t *InsertOrUpdate) load() error {
	switch v := t.Data.(type) {
	case map[string]any:
		return t.loadMap(v)
	case []any:
		return t.loadSlice(v)
	default:
		return fmt.Errorf("unsupported data format")
	}
}

func (t *InsertOrUpdate) loadMap(data map[string]any) error {
	var placeholders []string

	if len(data) == 0 {
		return nil
	}

	for _, mapping := range t.Mappings {
		if mapping.To == "" {
			return fmt.Errorf("invalid mapping definition (To is empty): %#v", mapping)
		}
		var key string
		var value any
		if mapping.From != "" {
			key = mapping.From
		} else if mapping.To != "" {
			key = mapping.To
		} else {
			return fmt.Errorf("unsupported mapping definition: %#v", mapping)
		}

		if v, ok := data[key]; !ok {
			if mapping.Optional {
				continue
			} else {
				return fmt.Errorf("load map: key '%s' not found", key)
			}
		} else {
			t.names = append(t.names, mapping.To)
			if !slices.Contains(t.Keys, mapping.To) {
				t.updates = append(t.updates, mapping.To)
			}
			value = v
		}

		if mapping.Get != nil {
			if v, err := mapping.Get(value); err != nil {
				return err
			} else {
				value = v
			}
		}
		if mapping.Modify != nil {
			if placeholder, values, err := mapping.Modify(value); err != nil {
				return err
			} else {
				placeholders = append(placeholders, placeholder)
				t.values = append(t.values, values...)
			}
		} else {
			placeholders = append(placeholders, "?")
			t.values = append(t.values, value)
		}
	}
	t.placeholders = append(t.placeholders, placeholders)
	return nil
}

func (t *InsertOrUpdate) loadSlice(data []any) error {
	if len(data) == 0 {
		return nil
	}

	for _, mapping := range t.Mappings {
		if mapping.To == "" {
			return fmt.Errorf("invalid mapping definition (To is empty): %#v", mapping)
		}
		t.names = append(t.names, mapping.To)
		if !slices.Contains(t.Keys, mapping.To) {
			t.updates = append(t.updates, mapping.To)
		}
	}

	for _, line := range data {
		var placeholders []string
		d, ok := line.(map[string]any)
		if !ok {
			return fmt.Errorf("unsupported data line format")
		}
		for _, mapping := range t.Mappings {
			if mapping.Raw != "" {
				placeholders = append(placeholders, mapping.Raw)
				continue
			}
			var value any
			var key string
			if mapping.From != "" {
				key = mapping.From
			} else {
				key = mapping.To
			}
			if v, ok := d[key]; !ok {
				return fmt.Errorf("load slice: key '%s' not found", key)
			} else {
				value = v
			}
			if mapping.Get != nil {
				if v, err := mapping.Get(value); err != nil {
					return err
				} else {
					value = v
				}
			}
			if mapping.Modify != nil {
				if placeholder, values, err := mapping.Modify(value); err != nil {
					return err
				} else {
					placeholders = append(placeholders, placeholder)
					t.values = append(t.values, values...)
				}
			} else {

				placeholders = append(placeholders, "?")
				t.values = append(t.values, value)
			}
		}
		t.placeholders = append(t.placeholders, placeholders)
	}
	return nil
}
