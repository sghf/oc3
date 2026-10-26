package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/opensvc/oc3/util/logkey"
)

type (
	execContexter interface {
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	}

	// DB exposes opensvc collector data management functions
	DB struct {
		DB DBOperater

		// DBLck is a pointer to DBLocker, used to manage concurrent access to
		// the database via locking mechanisms.
		DBLck *DBLocker

		Session *Session

		dbPool *sql.DB
		HasTx  bool

		Metrics *Metrics
	}

	// DBLocker combines a database connection and a sync.Locker
	// for managing concurrent access.
	DBLocker struct {
		DB *sql.DB
		sync.Locker
	}

	DBTxer interface {
		Commit() error
		Rollback() error
	}

	DBOperater interface {
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
		QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
		PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	}
)

var (
	dbLocker         *DBLocker
	dbLockerInitOnce sync.Once
)

func InitDbLocker(db *sql.DB) *DBLocker {
	dbLockerInitOnce.Do(func() {
		dbLocker = &DBLocker{DB: db, Locker: &sync.Mutex{}}
	})
	return dbLocker
}

func New(dbPool *sql.DB) *DB {
	return &DB{
		DB:      dbPool,
		DBLck:   InitDbLocker(dbPool),
		dbPool:  dbPool,
		Metrics: metrics,
	}
}

// CreateTx starts a transaction and installs it in place of the receiver's
// DBOperater, so that subsequent calls on oDb run inside that transaction.
//
// It MUTATES the receiver. Only call it on a *DB owned by a single goroutine
// (a worker job, a scheduler task). Calling it on a *DB shared between
// goroutines — an HTTP handler's, for instance — would divert the concurrent
// callers' statements into this transaction and let its Commit or Rollback
// decide their fate. Shared instances must use BeginTxWithControl, which
// returns a separate transaction-scoped *DB instead of mutating the receiver.
func (oDb *DB) CreateTx(ctx context.Context, opts *sql.TxOptions) error {
	if oDb.HasTx {
		return fmt.Errorf("already in a transaction")
	}
	if tx, err := oDb.dbPool.BeginTx(ctx, opts); err != nil {
		return err
	} else {
		oDb.DB = tx
		oDb.HasTx = true
		return nil
	}
}

// withTx returns a shallow copy of the receiver bound to tx. The copy shares the
// pool, the locker, the session and the metrics; only the DBOperater differs.
func (oDb *DB) withTx(tx *sql.Tx) *DB {
	return &DB{
		DB:      tx,
		DBLck:   oDb.DBLck,
		Session: oDb.Session,
		dbPool:  oDb.dbPool,
		HasTx:   true,
		Metrics: oDb.Metrics,
	}
}

// BeginTxWithControl starts a database transaction and returns a *DB bound to it,
// a function marking the transaction for commit, and a cleanup function that
// finalizes it.
//
// The returned *DB is a distinct instance: the receiver is left untouched and
// stays usable by concurrent goroutines. Statements that must run inside the
// transaction have to be issued on the returned *DB, not on the receiver — this
// is what makes the call safe on a *DB shared between HTTP requests.
//
//	tx, markSuccess, endTx, err := odb.BeginTxWithControl(ctx, log, &sql.TxOptions{})
//	if err != nil {
//		return err
//	}
//	defer endTx()                                    // rolls back unless marked
//	if err := tx.DeleteFoo(ctx, id); err != nil {    // note: tx, not odb
//		return err
//	}
//	markSuccess()                                    // endTx will commit
func (oDb *DB) BeginTxWithControl(ctx context.Context, log *slog.Logger, opts *sql.TxOptions) (txDB *DB, markSuccess func(), endTx func(), err error) {
	tx, err := oDb.dbPool.BeginTx(ctx, opts)
	if err != nil {
		return nil, nil, nil, err
	}
	txDB = oDb.withTx(tx)

	var needCommit bool
	markSuccess = func() { needCommit = true }
	endTx = func() {
		if needCommit {
			if err := txDB.Commit(); err != nil && log != nil {
				log.Error("commit failed", logkey.Error, err)
			}
			return
		}
		if err := txDB.Rollback(); err != nil && log != nil {
			log.Error("rollback failed", logkey.Error, err)
		}
	}
	return txDB, markSuccess, endTx, nil
}

func (oDb *DB) CreateSession(ev eventPublisher) {
	oDb.Session = &Session{
		db:     oDb.DB,
		ev:     ev,
		tables: make(map[string]struct{}),
	}
}

func (oDb *DB) Commit() error {
	if !oDb.HasTx {
		return nil
	}
	tx, ok := oDb.DB.(DBTxer)
	if !ok {
		return nil
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	oDb.HasTx = false
	return nil
}

func (oDb *DB) Rollback() error {
	if !oDb.HasTx {
		return nil
	}
	defer func() { oDb.HasTx = false }()
	tx, ok := oDb.DB.(DBTxer)
	if !ok {
		return nil
	}
	if r := recover(); r != nil {
		tx.Rollback()
		panic(r)
	}
	return tx.Rollback()
}

// databaseNow returns the time of the database, to pass as a parameter to the
// statements of a run that must share one instant. A user variable set by
// "SET @now = NOW()" would not do: without a transaction each statement may
// take another connection of the pool, where @now is unset or left by an
// earlier run. Read from the database rather than taken from the clock of the
// process, it is formatted back as it was read, whatever the time zone of the
// connection.
func (oDb *DB) databaseNow(ctx context.Context) (now time.Time, err error) {
	if err = oDb.DB.QueryRowContext(ctx, "SELECT NOW()").Scan(&now); err != nil {
		err = fmt.Errorf("databaseNow: %w", err)
	}
	return
}

func (oDb *DB) SetChange(s ...string) {
	oDb.Session.SetChanges(s...)
}

func (oDb *DB) DeleteBatched(ctx context.Context, table, dateCol, orderbyCol string, batchSize int64, retention int, where string) (totalDeleted int64, batchCount int64, err error) {
	// The base SQL query for the batched deletion.
	// ORDER BY is crucial for consistent performance and avoiding lock conflicts.
	query := fmt.Sprintf("DELETE FROM `%s` WHERE `%s` < DATE_SUB(NOW(), INTERVAL %d DAY) %s ORDER BY `%s` LIMIT %d",
		table, dateCol, retention, where, orderbyCol, batchSize)

	for {
		batchCount++

		ctx, cancel := context.WithTimeout(ctx, time.Minute)

		// Execute the DELETE statement
		count, err := oDb.execCountContext(ctx, query)
		cancel()
		if err != nil {
			return totalDeleted, batchCount, fmt.Errorf("%s: error executing batch %d: %w", table, batchCount, err)
		}

		// Check the number of affected rows
		totalDeleted += count
		if count > 0 {
			slog.Debug(fmt.Sprintf("DeleteBatched: %s: batch %d: deleted %d rows. total deleted: %d", table, batchCount, count, totalDeleted))
		}

		// If less than the batch size was deleted, we've reached the end of the matching rows.
		if count < batchSize {
			return totalDeleted, batchCount, nil
		}

		// Add a short sleep to yield CPU time, preventing resource monopolization
		time.Sleep(10 * time.Millisecond)
	}
}

// ExecContextAndCountRowsAffected executes the oDb.DB.ExecContext query with the provided context, returning the number of rows affected and an error.
func (oDb *DB) ExecContextAndCountRowsAffected(ctx context.Context, query string, args ...any) (int64, error) {
	return oDb.execCountContext(ctx, query, args...)
}

// execCountContext executes the oDb.DB.ExecContext query with the provided context and arguments, returning the number of affected rows and an error.
func (oDb *DB) execCountContext(ctx context.Context, query string, args ...any) (int64, error) {
	result, err := oDb.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	if result == nil {
		// len data may be 0, so no rows affected.
		return 0, nil
	}
	return result.RowsAffected()
}

func (oDb *DB) ExecContext(ctx context.Context, query string, args ...any) (res sql.Result, err error) {
	if oDb.HasTx {
		res, err := oDb.DB.ExecContext(ctx, query, args...)
		if err != nil {
			oDb.Metrics.ExecErr.Inc()
		} else {
			oDb.Metrics.ExecOk.Inc()
		}
		return res, err
	}
	const maxRetries = 9
	var tx *sql.Tx
	begin := time.Now()

	for i := 0; i < maxRetries; i++ {
		tx, err = oDb.dbPool.BeginTx(ctx, nil)
		if err != nil {
			oDb.Metrics.BeginTxErr.Inc()
			oDb.Metrics.ExecTxFailed.Inc()
			return res, fmt.Errorf("begin transaction: %w", err)
		}
		oDb.Metrics.BeginTxOk.Inc()

		res, err = tx.ExecContext(ctx, query, args...)
		if err == nil {
			oDb.Metrics.ExecTxOk.Inc()
			if err := tx.Commit(); err != nil {
				oDb.Metrics.CommitErr.Inc()
				oDb.Metrics.ExecTxFailed.Inc()
				return nil, fmt.Errorf("commit: %w", err)
			}
			oDb.Metrics.CommitOk.Inc()
			return res, nil
		}
		oDb.Metrics.ExecTxErr.Inc()
		if !isDeadlock(err) {
			oDb.Metrics.ExecTxFailed.Inc()
			// We must roll back any partial updates
			if err1 := tx.Rollback(); err1 != nil {
				oDb.Metrics.RollbackErr.Inc()
				return res, fmt.Errorf("exec and rollback failed: %w", errors.Join(err, err1))
			}
			oDb.Metrics.RollbackOk.Inc()
			return nil, err
		}
		oDb.Metrics.ExecTxDeadlock.Inc()

		if err1 := tx.Rollback(); err1 != nil {
			oDb.Metrics.RollbackErr.Inc()
			oDb.Metrics.ExecTxFailed.Inc()
			return res, fmt.Errorf("exec and rollback failed: %w", errors.Join(err, err1))
		}
		oDb.Metrics.RollbackOk.Inc()
		oDb.Metrics.ExecTxRetry.Inc()
		time.Sleep(Backoff(100*time.Millisecond, i, time.Second))
		continue
	}
	oDb.Metrics.ExecTxFailed.Inc()
	return res, fmt.Errorf("exec failed after %d retries (duration %s): %w", maxRetries, time.Since(begin), err)
}

func isDeadlock(err error) bool {
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		return me.Number == 1213
	}
	return false
}

// Backoff calculates an exponential backoff duration with optional jitter,
// capped by a maximum duration.
func Backoff(base time.Duration, attempt int, max time.Duration) time.Duration {
	d := base * (1 << attempt)
	if d > max {
		d = max
	}

	jitter := time.Duration(rand.Int63n(int64(d / 2)))
	return d/2 + jitter
}
