package serverhandlers

import (
	"context"
	"log/slog"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/logkey"
)

// auditLog writes an entry of the audit log on behalf of the caller: the user
// by their email, "agent" for a node, as POST /logs names them; info unless
// the entry says otherwise. A failure to write is logged, not returned: the
// change it records is made.
func (a *Api) auditLog(c echo.Context, ctx context.Context, log *slog.Logger, entry cdb.LogEntry) {
	if entry.User == "" {
		if IsAuthByUser(c) {
			entry.User, _ = c.Get(XUserEmail).(string)
		} else {
			entry.User = "agent"
		}
	}
	if entry.Level == "" {
		entry.Level = "info"
	}
	if err := a.ODB.Log(ctx, entry); err != nil {
		log.Error("cannot write audit log", "log_action", entry.Action, logkey.Error, err)
	}
}

// uuidRef is the id of a node or a service on an audit entry, nil when id is
// not one.
func uuidRef(id string) *uuid.UUID {
	parsed, err := uuid.Parse(id)
	if err != nil {
		return nil
	}
	return &parsed
}
