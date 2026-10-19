// Package migrate applies the schema changes oc3 needs to a collector database.
//
// The changes are the files of sql/, named NNNN_name.sql, applied in the order
// of their number and recorded in the table oc3_schema_migrations. A released
// migration is never edited: a mistake is fixed by a new one. Every statement can
// run again on a database that already has its effect (IF NOT EXISTS, INSERT
// IGNORE...), so that a database the historical collector keeps up to date, or
// one migrated by hand from the former migration.sql, is migrated the same way as
// a new one. A file holds one DDL statement, or one followed by the data
// statements that depend on it: MariaDB commits each DDL at once, so a failure
// leaves the database at a known step, the migration to run again.
package migrate

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

//go:embed sql/*.sql
var files embed.FS

// Migration is one file of sql/.
type Migration struct {
	Version int
	Name    string
	// Statements are the statements of the file, comments removed and spaces
	// collapsed, in their order.
	Statements []string
	// Checksum is the sha256 of the statements: a comment or the layout of a
	// file can change, not what it runs.
	Checksum string
}

func (m Migration) String() string { return fmt.Sprintf("%04d_%s", m.Version, m.Name) }

var fileName = regexp.MustCompile(`^(\d{4})_([a-z0-9_]+)\.sql$`)

// All returns the migrations built into oc3, in their order.
func All() ([]Migration, error) {
	return load(files, "sql")
}

func load(fsys fs.FS, dir string) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, err
	}
	var l []Migration
	for _, e := range entries {
		match := fileName.FindStringSubmatch(e.Name())
		if match == nil {
			return nil, fmt.Errorf("migration %s: not named NNNN_name.sql", e.Name())
		}
		b, err := fs.ReadFile(fsys, dir+"/"+e.Name())
		if err != nil {
			return nil, err
		}
		version, _ := strconv.Atoi(match[1])
		statements, err := Split(string(b))
		if err != nil {
			return nil, fmt.Errorf("migration %s: %w", e.Name(), err)
		}
		if len(statements) == 0 {
			return nil, fmt.Errorf("migration %s: no statement", e.Name())
		}
		sum := sha256.Sum256([]byte(strings.Join(statements, ";\n")))
		l = append(l, Migration{Version: version, Name: match[2], Statements: statements, Checksum: hex.EncodeToString(sum[:])})
	}
	sort.Slice(l, func(i, j int) bool { return l[i].Version < l[j].Version })
	// The versions follow each other from 1: a gap or a duplicate is a merge
	// mistake, which would otherwise apply the migrations out of their order.
	for i, m := range l {
		if m.Version != i+1 {
			return nil, fmt.Errorf("migration %s: version %d expected", m, i+1)
		}
	}
	return l, nil
}

// Split returns the statements of a SQL text separated by ';', without their
// comments ('-- ', '#' and '/* */'), their runs of spaces collapsed to one. The
// quoted strings and names are kept as written, a ';' or a '--' in them included.
func Split(text string) ([]string, error) {
	var (
		statements []string
		cur        strings.Builder
		space      bool
	)
	flush := func() {
		if s := strings.TrimSpace(cur.String()); s != "" {
			statements = append(statements, s)
		}
		cur.Reset()
		space = false
	}
	r := []rune(text)
	for i := 0; i < len(r); i++ {
		c := r[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			end := i + 1
			for ; end < len(r); end++ {
				if r[end] == '\\' && c != '`' {
					end++
					continue
				}
				if r[end] == c {
					// A doubled quote is the quote itself.
					if end+1 < len(r) && r[end+1] == c {
						end++
						continue
					}
					break
				}
			}
			if end >= len(r) {
				return nil, fmt.Errorf("unterminated %c quote", c)
			}
			if space {
				cur.WriteRune(' ')
				space = false
			}
			cur.WriteString(string(r[i : end+1]))
			i = end
		case c == '#' || isDashComment(r, i):
			for i < len(r) && r[i] != '\n' {
				i++
			}
			space = cur.Len() > 0
		case c == '/' && i+1 < len(r) && r[i+1] == '*':
			end := i + 2
			for end+1 < len(r) && (r[end] != '*' || r[end+1] != '/') {
				end++
			}
			if end+1 >= len(r) {
				return nil, fmt.Errorf("unterminated /* comment")
			}
			i = end + 1
			space = cur.Len() > 0
		case c == ';':
			flush()
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			space = cur.Len() > 0
		default:
			if space {
				cur.WriteRune(' ')
				space = false
			}
			cur.WriteRune(c)
		}
	}
	flush()
	return statements, nil
}

// isDashComment tells whether a '--' comment begins at i: MariaDB wants a space,
// or the end of the line, after the dashes.
func isDashComment(r []rune, i int) bool {
	if r[i] != '-' || i+1 >= len(r) || r[i+1] != '-' {
		return false
	}
	return i+2 == len(r) || r[i+2] == ' ' || r[i+2] == '\t' || r[i+2] == '\n' || r[i+2] == '\r'
}
