package serverhandlers

import (
	"context"
	"log/slog"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
)

// nodeRegisterLog writes the audit entry of a node registration or enrollment,
// node.register as the historical collector writes it (api_register.py): by
// the user who registered the node, or "agent" for a node registering itself
// or another one of its cluster.
func (a *Api) nodeRegisterLog(c echo.Context, ctx context.Context, log *slog.Logger, level, format string, nodeID string, dict map[string]any) {
	a.auditLog(c, ctx, log, cdb.LogEntry{
		Action: "node.register", Level: level, Fmt: format, Dict: dict, NodeID: uuidRef(nodeID),
	})
}
