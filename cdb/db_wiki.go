package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type (
	// WikiRevision is a row of wiki_pages: one saved version of the notes of a
	// node or a service, the page named after its node_id or svc_id. Body is
	// empty in a list of revisions.
	WikiRevision struct {
		ID         int64
		Name       string
		Body       string
		ChangeNote string
		SavedOn    time.Time
		// Author is the email of the user who saved it, empty when unknown.
		Author string
	}
)

// ErrWikiConflict is returned when a revision is saved over another one than the
// latest of its page: someone saved meanwhile.
var ErrWikiConflict = errors.New("the page changed since the revision edited")

const wikiRevisionColumns = `w.id, COALESCE(w.name, ''), COALESCE(w.change_note, ''),
	w.saved_on, COALESCE(u.email, '')`

func scanWikiRevision(scan func(...any) error, body *string) (WikiRevision, error) {
	var r WikiRevision
	var savedOn sql.NullTime
	dest := []any{&r.ID, &r.Name, &r.ChangeNote, &savedOn, &r.Author}
	if body != nil {
		dest = append(dest, body)
	}
	err := scan(dest...)
	r.SavedOn = savedOn.Time
	return r, err
}

// WikiRevisions returns the revisions of a page, the latest first, without their
// body, at most limit of them, and how many it has in all.
func (oDb *DB) WikiRevisions(ctx context.Context, name string, limit int) ([]WikiRevision, int, error) {
	defer logDuration("WikiRevisions", time.Now())
	var total int
	if err := oDb.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM wiki_pages WHERE name = ?`, name).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("wikiRevisions: %w", err)
	}
	rows, err := oDb.DB.QueryContext(ctx, `SELECT `+wikiRevisionColumns+`
		FROM wiki_pages w LEFT JOIN auth_user u ON u.id = w.author
		WHERE w.name = ? ORDER BY w.id DESC LIMIT ?`, name, limit)
	if err != nil {
		return nil, 0, fmt.Errorf("wikiRevisions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	revisions := make([]WikiRevision, 0)
	for rows.Next() {
		r, err := scanWikiRevision(rows.Scan, nil)
		if err != nil {
			return nil, 0, fmt.Errorf("wikiRevisions: %w", err)
		}
		revisions = append(revisions, r)
	}
	return revisions, total, rows.Err()
}

// WikiRevisionByID returns a revision of a page with its body, the latest when
// id is 0, nil when there is none.
func (oDb *DB) WikiRevisionByID(ctx context.Context, name string, id int64) (*WikiRevision, error) {
	defer logDuration("WikiRevisionByID", time.Now())
	query := `SELECT ` + wikiRevisionColumns + `, COALESCE(w.body, '')
		FROM wiki_pages w LEFT JOIN auth_user u ON u.id = w.author WHERE w.name = ?`
	args := []any{name}
	if id > 0 {
		query += ` AND w.id = ?`
		args = append(args, id)
	}
	query += ` ORDER BY w.id DESC LIMIT 1`
	var body string
	r, err := scanWikiRevision(oDb.DB.QueryRowContext(ctx, query, args...).Scan, &body)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("wikiRevisionByID: %w", err)
	}
	r.Body = body
	return &r, nil
}

// InsertWikiRevision saves a new revision of a page over the revision baseID,
// 0 for a page without any, and returns its id. ErrWikiConflict when baseID is
// no longer the latest revision: the check and the insert are one statement.
func (oDb *DB) InsertWikiRevision(ctx context.Context, name string, baseID int64, body, changeNote string, authorID int64) (int64, error) {
	defer logDuration("InsertWikiRevision", time.Now())
	res, err := oDb.DB.ExecContext(ctx, `INSERT INTO wiki_pages (name, title, author, saved_on, body, change_note)
		SELECT ?, '', ?, NOW(), ?, ? FROM DUAL
		WHERE COALESCE((SELECT MAX(id) FROM wiki_pages WHERE name = ?), 0) = ?`,
		name, authorID, body, changeNote, name, baseID)
	if err != nil {
		return 0, fmt.Errorf("insertWikiRevision: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return 0, fmt.Errorf("insertWikiRevision: %w", err)
	} else if n == 0 {
		return 0, ErrWikiConflict
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("insertWikiRevision: %w", err)
	}
	oDb.SetChange("wiki_pages")
	return id, nil
}
