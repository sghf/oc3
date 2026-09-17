package cdb

import "strings"

// ListParams bundles the standard query parameters shared by all list endpoints.
// Groups and IsManager encode the caller's access control context.
type ListParams struct {
	Groups    []string
	IsManager bool

	// UserID is the authenticated user's auth_user.id, set only by the endpoints
	// whose access control references the caller's identity rather than just its
	// groups. Nil when the caller is not a user (node credentials) or when the
	// endpoint does not need it.
	UserID *int64

	Limit       int
	Offset      int
	Props       []string
	SelectExprs []string
	TypeHints   map[string]string // Used by scanRowsToMaps to convert []byte driver values to the correct type
	OrderBy     []string
	GroupBy     []string
}

// HasGroup reports whether the caller belongs to the named group. Use it for the
// privileges that are not covered by IsManager, which only tracks "Manager".
func (p ListParams) HasGroup(role string) bool {
	for _, g := range p.Groups {
		if g == role {
			return true
		}
	}
	return false
}

func (p ListParams) OrderByClause(defaultClause string) string {
	if len(p.OrderBy) == 0 {
		return "ORDER BY " + defaultClause
	}
	return "ORDER BY " + strings.Join(p.OrderBy, ", ")
}

func (p ListParams) GroupByClause(defaultClause string) string {
	if len(p.GroupBy) > 0 {
		return "GROUP BY " + strings.Join(p.GroupBy, ", ")
	}
	if defaultClause != "" {
		return "GROUP BY " + defaultClause
	}
	return ""
}
