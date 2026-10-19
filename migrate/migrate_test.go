package migrate

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"
)

func TestSplit(t *testing.T) {
	got, err := Split(`-- a comment; with a semicolon
ALTER TABLE t
  ADD COLUMN IF NOT EXISTS c VARCHAR(8) DEFAULT 'a;b -- c' COMMENT "it's", # trailing
  ADD COLUMN d INT; /* a block;
comment */ INSERT INTO t VALUES ('it''s', 'back\'slash', ` + "`a;b`" + `);
--
`)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		`ALTER TABLE t ADD COLUMN IF NOT EXISTS c VARCHAR(8) DEFAULT 'a;b -- c' COMMENT "it's", ADD COLUMN d INT`,
		`INSERT INTO t VALUES ('it''s', 'back\'slash', ` + "`a;b`" + `)`,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got\n%q\nwant\n%q", got, want)
	}
	if got, _ := Split("SELECT 1--2"); len(got) != 1 || got[0] != "SELECT 1--2" {
		t.Errorf("a -- without a space is not a comment: %q", got)
	}
	for _, s := range []string{"SELECT 'a", "SELECT 1 /* a"} {
		if _, err := Split(s); err == nil {
			t.Errorf("%q: no error", s)
		}
	}
}

func TestAll(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) == 0 {
		t.Fatal("no migration")
	}
	for _, m := range all {
		for _, s := range m.Statements {
			// A statement that cannot run again would fail on a database
			// migrated by hand, or by the historical collector.
			upper := strings.ToUpper(s)
			switch {
			case strings.HasPrefix(upper, "CREATE TABLE ") && !strings.HasPrefix(upper, "CREATE TABLE IF NOT EXISTS "):
				t.Errorf("%s: CREATE TABLE without IF NOT EXISTS", m)
			case strings.Contains(upper, "ADD COLUMN ") && strings.Count(upper, "ADD COLUMN ") != strings.Count(upper, "ADD COLUMN IF NOT EXISTS "):
				t.Errorf("%s: ADD COLUMN without IF NOT EXISTS", m)
			case strings.Contains(upper, "DROP INDEX ") && !strings.Contains(upper, "DROP INDEX IF EXISTS "):
				t.Errorf("%s: DROP INDEX without IF EXISTS", m)
			}
		}
	}
}

func TestLoad(t *testing.T) {
	fsys := fstest.MapFS{
		"sql/0002_b.sql": {Data: []byte("-- b\nSELECT 2;")},
		"sql/0001_a.sql": {Data: []byte("SELECT 1;")},
	}
	l, err := load(fsys, "sql")
	if err != nil {
		t.Fatal(err)
	}
	if len(l) != 2 || l[0].String() != "0001_a" || l[1].String() != "0002_b" {
		t.Fatalf("got %v", l)
	}
	// The checksum is the one of the statements, whatever the comments.
	fsys["sql/0001_a.sql"] = &fstest.MapFile{Data: []byte("-- a comment fixed\nSELECT   1 ;\n")}
	if l2, _ := load(fsys, "sql"); l2[0].Checksum != l[0].Checksum {
		t.Error("a comment changes the checksum")
	}
	fsys["sql/0001_a.sql"] = &fstest.MapFile{Data: []byte("SELECT 3;")}
	if l2, _ := load(fsys, "sql"); l2[0].Checksum == l[0].Checksum {
		t.Error("a statement does not change the checksum")
	}
	for name, f := range map[string]fstest.MapFS{
		"a gap":       {"sql/0001_a.sql": {Data: []byte("SELECT 1;")}, "sql/0003_c.sql": {Data: []byte("SELECT 3;")}},
		"a duplicate": {"sql/0001_a.sql": {Data: []byte("SELECT 1;")}, "sql/0001_b.sql": {Data: []byte("SELECT 1;")}},
		"a bad name":  {"sql/1_a.sql": {Data: []byte("SELECT 1;")}},
		"no SQL":      {"sql/0001_a.sql": {Data: []byte("-- nothing\n")}},
	} {
		if _, err := load(f, "sql"); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestPlan(t *testing.T) {
	all := []Migration{
		{Version: 1, Name: "a", Checksum: "x1"},
		{Version: 2, Name: "b", Checksum: "x2"},
		{Version: 3, Name: "c", Checksum: "x3"},
	}
	st := Plan(all, []Applied{{Version: 1, Checksum: "x1"}, {Version: 4, Name: "d"}})
	if p := st.Pending(); len(p) != 2 || p[0].Version != 2 || p[1].Version != 3 {
		t.Errorf("pending %v", p)
	}
	if len(st.Unknown) != 1 || st.Unknown[0].Version != 4 {
		t.Errorf("unknown %v", st.Unknown)
	}
	if err := st.Err(); !errors.Is(err, ErrPending) || !strings.Contains(err.Error(), "0002_b, 0003_c") {
		t.Errorf("err %v", err)
	}

	st = Plan(all, []Applied{{Version: 1, Checksum: "x1"}, {Version: 2, Checksum: "changed"}, {Version: 3, Checksum: "x3"}})
	if err := st.Err(); !errors.Is(err, ErrModified) {
		t.Errorf("err %v, want ErrModified", err)
	}

	// A newer oc3 migrated the database: this one still runs on it.
	st = Plan(all[:2], []Applied{{Version: 1, Checksum: "x1"}, {Version: 2, Checksum: "x2"}, {Version: 3, Checksum: "x3"}})
	if err := st.Err(); err != nil || len(st.Unknown) != 1 {
		t.Errorf("err %v, unknown %v", err, st.Unknown)
	}
}
