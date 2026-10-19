package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	// Table records the migrations applied to the database.
	Table = "oc3_schema_migrations"

	// lockName is the MariaDB named lock taken while migrating: two oc3 db
	// migrate started at once, by two nodes or two containers, apply each
	// migration once.
	lockName = "oc3_schema_migrate"
)

const createTable = `CREATE TABLE IF NOT EXISTS ` + Table + ` (
  version     INT NOT NULL PRIMARY KEY,
  name        VARCHAR(255) NOT NULL,
  checksum    CHAR(64) NOT NULL COMMENT 'sha256 of the statements of the migration',
  applied_at  DATETIME NOT NULL,
  oc3_version VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'oc3 that applied it',
  duration_ms INT NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

// Applied is a migration recorded in the database.
type Applied struct {
	Version    int
	Name       string
	Checksum   string
	AppliedAt  time.Time
	OC3Version string
	Duration   time.Duration
}

// Item is a migration of oc3 and its record, nil while it is not applied.
type Item struct {
	Migration
	Applied *Applied
}

// Modified tells whether the migration was applied with other statements than
// the ones oc3 holds now.
func (t Item) Modified() bool { return t.Applied != nil && t.Applied.Checksum != t.Checksum }

// Status is the state of the schema of a database for this oc3.
type Status struct {
	Items []Item
	// Unknown are the migrations recorded that this oc3 does not hold: the
	// database was migrated by a newer oc3. As the migrations only add, this oc3
	// still runs on it.
	Unknown []Applied
}

// Pending returns the migrations left to apply.
func (t Status) Pending() []Migration {
	var l []Migration
	for _, item := range t.Items {
		if item.Applied == nil {
			l = append(l, item.Migration)
		}
	}
	return l
}

// Modified returns the migrations applied with other statements than oc3 holds.
func (t Status) Modified() []Item {
	var l []Item
	for _, item := range t.Items {
		if item.Modified() {
			l = append(l, item)
		}
	}
	return l
}

// Err tells why oc3 cannot run on the database: a migration left to apply, or
// one applied that was changed since. Nil when the schema is up to date.
func (t Status) Err() error {
	if l := t.Modified(); len(l) > 0 {
		names := make([]string, len(l))
		for i, item := range l {
			names[i] = item.String()
		}
		return fmt.Errorf("%w: %s", ErrModified, strings.Join(names, ", "))
	}
	if l := t.Pending(); len(l) > 0 {
		names := make([]string, len(l))
		for i, m := range l {
			names[i] = m.String()
		}
		return fmt.Errorf("%w: %s", ErrPending, strings.Join(names, ", "))
	}
	return nil
}

var (
	// ErrPending tells that migrations are left to apply: run oc3 db migrate.
	ErrPending = errors.New("database schema migrations to apply with 'oc3 db migrate'")
	// ErrModified tells that applied migrations were changed since: a released
	// migration is never edited, so the binary and the database disagree on what
	// the schema is.
	ErrModified = errors.New("database schema migrations applied with other statements than this oc3 holds")
)

// Plan compares the migrations of oc3 with the ones the database records.
func Plan(all []Migration, applied []Applied) Status {
	byVersion := make(map[int]Applied, len(applied))
	for _, a := range applied {
		byVersion[a.Version] = a
	}
	var st Status
	for _, m := range all {
		item := Item{Migration: m}
		if a, ok := byVersion[m.Version]; ok {
			item.Applied = &a
			delete(byVersion, m.Version)
		}
		st.Items = append(st.Items, item)
	}
	for _, a := range applied {
		if _, ok := byVersion[a.Version]; ok {
			st.Unknown = append(st.Unknown, a)
		}
	}
	return st
}

type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// readApplied returns the migrations the database records, none when it has no
// record table yet.
func readApplied(ctx context.Context, db querier) ([]Applied, error) {
	rows, err := db.QueryContext(ctx, `SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name = ?`, Table)
	if err != nil {
		return nil, err
	}
	var n int
	for rows.Next() {
		if err := rows.Scan(&n); err != nil {
			_ = rows.Close()
			return nil, err
		}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	rows, err = db.QueryContext(ctx, `SELECT version, name, checksum, applied_at, oc3_version, duration_ms
		FROM `+Table+` ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var l []Applied
	for rows.Next() {
		var (
			a  Applied
			ms int64
		)
		if err := rows.Scan(&a.Version, &a.Name, &a.Checksum, &a.AppliedAt, &a.OC3Version, &ms); err != nil {
			return nil, err
		}
		a.Duration = time.Duration(ms) * time.Millisecond
		l = append(l, a)
	}
	return l, rows.Err()
}

// Check returns the state of the schema of the database. It writes nothing.
func Check(ctx context.Context, db *sql.DB) (Status, error) {
	all, err := All()
	if err != nil {
		return Status{}, err
	}
	applied, err := readApplied(ctx, db)
	if err != nil {
		return Status{}, fmt.Errorf("read the %s table: %w", Table, err)
	}
	return Plan(all, applied), nil
}

// Options tune Migrate.
type Options struct {
	// LockTimeout is how long to wait for another oc3 db migrate to end.
	LockTimeout time.Duration
	// OC3Version is recorded with each migration applied.
	OC3Version string
	// OnApply, if set, is told of each migration applied and how long it took.
	OnApply func(m Migration, d time.Duration)
}

// Migrate applies the migrations left to apply, in their order, under a named
// lock, and records each once applied. It refuses to apply any while an applied
// migration was changed since. On a failure, the migrations applied before stay
// recorded, and the one that failed can run again once the cause is fixed.
func Migrate(ctx context.Context, db *sql.DB, o Options) (applied []Migration, err error) {
	all, err := All()
	if err != nil {
		return nil, err
	}
	// The named lock belongs to a session: every statement runs on this one.
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()

	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, `SELECT GET_LOCK(?, ?)`, lockName, int(o.LockTimeout.Seconds())).Scan(&got); err != nil {
		return nil, fmt.Errorf("take the %s lock: %w", lockName, err)
	}
	if !got.Valid || got.Int64 != 1 {
		return nil, fmt.Errorf("take the %s lock: another oc3 db migrate still runs after %s", lockName, o.LockTimeout)
	}
	defer func() {
		// A context ended would leave the lock to the end of the session, which
		// the Close of the connection brings.
		_, _ = conn.ExecContext(context.Background(), `SELECT RELEASE_LOCK(?)`, lockName)
	}()

	if _, err := conn.ExecContext(ctx, createTable); err != nil {
		return nil, fmt.Errorf("create the %s table: %w", Table, err)
	}
	records, err := readApplied(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("read the %s table: %w", Table, err)
	}
	st := Plan(all, records)
	if l := st.Modified(); len(l) > 0 {
		return nil, st.Err()
	}
	for _, m := range st.Pending() {
		begin := time.Now()
		for i, stmt := range m.Statements {
			if _, err := conn.ExecContext(ctx, stmt); err != nil {
				return applied, fmt.Errorf("migration %s, statement %d of %d: %w", m, i+1, len(m.Statements), err)
			}
		}
		d := time.Since(begin)
		if _, err := conn.ExecContext(ctx, `INSERT INTO `+Table+`
			(version, name, checksum, applied_at, oc3_version, duration_ms) VALUES (?, ?, ?, ?, ?, ?)`,
			// The time of oc3, written and read in the location of the connection,
			// rather than NOW(), in the time zone of the server.
			m.Version, m.Name, m.Checksum, time.Now(), o.OC3Version, d.Milliseconds()); err != nil {
			return applied, fmt.Errorf("record migration %s: %w", m, err)
		}
		applied = append(applied, m)
		if o.OnApply != nil {
			o.OnApply(m, d)
		}
	}
	return applied, nil
}
