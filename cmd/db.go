package cmd

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"text/tabwriter"
	"time"
	_ "time/tzdata"

	"github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/migrate"
	"github.com/opensvc/oc3/util/logkey"
	"github.com/opensvc/oc3/util/version"
)

const sectionDB = "db"

// openDatabase setup database handler. vars are session variables of its
// connections, max_statement_time for instance.
func openDatabase(vars map[string]string) (*sql.DB, error) {
	cdb.InitMetrics()

	cfg := mysql.Config{
		User:                 viper.GetString("db.username"),
		Passwd:               viper.GetString("db.password"),
		Net:                  "tcp",
		Addr:                 viper.GetString("db.host") + ":" + viper.GetString("db.port"),
		DBName:               "opensvc",
		AllowNativePasswords: true,
		ParseTime:            true,
		Loc:                  time.Local,
		Params:               vars,
	}
	slog.Info(fmt.Sprintf("db addr=%s", cfg.Addr))
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	// expose the connection pool usage as go_sql_*{db_name="opensvc"}
	if err := prometheus.Register(collectors.NewDBStatsCollector(db, cfg.DBName)); err != nil {
		slog.Warn(fmt.Sprintf("db stats metrics: %s", err))
	}
	return db, nil
}

// newDatabase opens the database once its schema is the one this oc3 needs,
// see checkSchema.
func newDatabase() (*sql.DB, error) {
	return newDatabaseWith(nil)
}

// newDatabaseWith is newDatabase, its connections with the session variables vars.
func newDatabaseWith(vars map[string]string) (*sql.DB, error) {
	db, err := openDatabase(vars)
	if err != nil {
		return nil, err
	}
	if err := checkSchema(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// schemaRetryInterval is how often checkSchema reads the schema again while it
// waits for it.
const schemaRetryInterval = 10 * time.Second

// checkSchema compares the migrations the database records with the ones of this
// oc3. With db.schema.check "enforce", the default, it waits up to db.schema.wait
// for the database to answer and for oc3 db migrate, started at the same time,
// to apply the migrations left, then fails rather than run on a schema it does
// not know. An applied migration changed since fails at once: waiting cannot fix
// it. With "warn", it logs what is wrong and goes on.
func checkSchema(db *sql.DB) error {
	mode := viper.GetString("db.schema.check")
	if mode != "enforce" && mode != "warn" {
		return fmt.Errorf("db.schema.check: %q is neither enforce nor warn", mode)
	}
	deadline := time.Now().Add(viper.GetDuration("db.schema.wait"))
	for {
		ctx, cancel := context.WithTimeout(context.Background(), schemaRetryInterval)
		st, err := migrate.Check(ctx, db)
		cancel()
		if err == nil {
			for _, a := range st.Unknown {
				slog.Warn(fmt.Sprintf("database schema: migration %04d_%s was applied by a newer oc3 (%s)", a.Version, a.Name, a.OC3Version))
			}
			err = st.Err()
		}
		switch {
		case err == nil:
			slog.Info(fmt.Sprintf("database schema: %d migrations applied", len(st.Items)))
			return nil
		case mode == "warn":
			slog.Warn("database schema: going on, as db.schema.check is warn", logkey.Error, err)
			return nil
		case errors.Is(err, migrate.ErrModified) || !transient(err) || time.Now().After(deadline):
			return fmt.Errorf("database schema: %w", err)
		}
		slog.Info(fmt.Sprintf("database schema: waiting up to %s", time.Until(deadline).Round(time.Second)), logkey.Error, err)
		time.Sleep(schemaRetryInterval)
	}
}

// waitDatabase waits up to wait for the database to answer: oc3 db migrate may
// start along with it.
func waitDatabase(db *sql.DB, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), schemaRetryInterval)
		err := db.PingContext(ctx)
		cancel()
		if err == nil {
			return nil
		} else if !transient(err) || time.Now().After(deadline) {
			return err
		}
		slog.Info(fmt.Sprintf("database: waiting up to %s", time.Until(deadline).Round(time.Second)), logkey.Error, err)
		time.Sleep(schemaRetryInterval)
	}
}

// transient tells whether waiting may fix err: not an error the server answered,
// a wrong password or a missing grant, which only the operator can fix.
func transient(err error) bool {
	var serverErr *mysql.MySQLError
	return !errors.As(err, &serverErr)
}

func cmdDB() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "db",
		Short: "manage the schema of the collector database",
	}
	cmd.AddCommand(cmdDBStatus(), cmdDBMigrate())
	return cmd
}

func cmdDBStatus() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "list the schema migrations, applied or left to apply; fails unless the schema is up to date",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := setup(sectionDB); err != nil {
				return err
			}
			db, err := openDatabase(nil)
			if err != nil {
				return err
			}
			defer func() { _ = db.Close() }()
			st, err := migrate.Check(context.Background(), db)
			if err != nil {
				return err
			}
			printSchemaStatus(st)
			return st.Err()
		},
	}
}

func printSchemaStatus(st migrate.Status) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "VERSION\tNAME\tSTATE\tAPPLIED AT\tBY\tDURATION")
	for _, item := range st.Items {
		state, at, by, took := "pending", "", "", ""
		if a := item.Applied; a != nil {
			state, at, by, took = "applied", a.AppliedAt.Format(time.DateTime), a.OC3Version, a.Duration.String()
			if item.Modified() {
				state = "modified"
			}
		}
		_, _ = fmt.Fprintf(w, "%04d\t%s\t%s\t%s\t%s\t%s\n", item.Version, item.Name, state, at, by, took)
	}
	for _, a := range st.Unknown {
		_, _ = fmt.Fprintf(w, "%04d\t%s\t%s\t%s\t%s\t%s\n", a.Version, a.Name, "newer", a.AppliedAt.Format(time.DateTime), a.OC3Version, a.Duration)
	}
	_ = w.Flush()
}

func cmdDBMigrate() *cobra.Command {
	var (
		dryRun      bool
		lockTimeout time.Duration
		wait        time.Duration
	)
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "apply the schema migrations left to apply",
		Long: `Apply the schema migrations left to apply, in their order, and record each
in the oc3_schema_migrations table. Run it at each installation and upgrade, before
the oc3 subsystems, which wait for it then refuse to start on an older schema.

Every migration can run again on a database that already has its effect: a
database kept up to date by the historical collector, or by hand, is migrated the
same way as a new one. Back the database up before.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := setup(sectionDB); err != nil {
				return err
			}
			db, err := openDatabase(nil)
			if err != nil {
				return err
			}
			defer func() { _ = db.Close() }()
			if err := waitDatabase(db, wait); err != nil {
				return err
			}
			ctx := context.Background()
			if dryRun {
				st, err := migrate.Check(ctx, db)
				if err != nil {
					return err
				}
				if l := st.Modified(); len(l) > 0 {
					return st.Err()
				}
				for _, m := range st.Pending() {
					fmt.Printf("-- %s\n%s;\n\n", m, strings.Join(m.Statements, ";\n"))
				}
				slog.Info(fmt.Sprintf("database schema: %d migrations to apply", len(st.Pending())))
				return nil
			}
			applied, err := migrate.Migrate(ctx, db, migrate.Options{
				LockTimeout: lockTimeout,
				OC3Version:  version.Version(),
				OnApply: func(m migrate.Migration, d time.Duration) {
					slog.Info(fmt.Sprintf("database schema: migration %s applied in %s", m, d.Round(time.Millisecond)))
				},
			})
			if err != nil {
				return err
			}
			slog.Info(fmt.Sprintf("database schema: up to date, %d migrations applied now", len(applied)))
			return nil
		},
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print the statements of the migrations left to apply, apply none")
	cmd.Flags().DurationVar(&lockTimeout, "lock-timeout", 5*time.Minute, "how long to wait for another oc3 db migrate to end")
	cmd.Flags().DurationVar(&wait, "wait", 2*time.Minute, "how long to wait for the database to answer")
	return cmd
}
