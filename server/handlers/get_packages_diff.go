package serverhandlers

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// splitIDs reads a comma-separated list of ids, blanks dropped.
func splitIDs(s *string) []string {
	if s == nil {
		return nil
	}
	var ids []string
	for _, id := range strings.Split(*s, ",") {
		if id = strings.TrimSpace(id); id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}

// GetPackagesDiff handles GET /packages/diff, as the historical
// rest_get_packages_diff: the package versions installed on some of the compared
// nodes but not on all of them.
func (a *Api) GetPackagesDiff(c echo.Context, params server.GetPackagesDiffParams) error {
	log := echolog.GetLogHandler(c, "GetPackagesDiff")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	encap := params.Encap != nil && *params.Encap
	nodes, err := a.ODB.PackagesDiffNodes(ctx, splitIDs(params.NodeIds), splitIDs(params.SvcIds), encap,
		UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		log.Error("cannot resolve the nodes to compare", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve the nodes to compare")
	}
	// With encap, a service without encapsulated nodes has nothing to compare: an
	// empty answer rather than an error, as the historical API.
	if len(nodes) < 2 && !encap {
		return JSONProblemf(c, http.StatusBadRequest, "At least two nodes should be selected")
	}

	nodeIDs := make([]string, len(nodes))
	resultNodes := make([]server.PackageDiffNode, len(nodes))
	for i, n := range nodes {
		nodeIDs[i] = n.NodeID
		resultNodes[i] = server.PackageDiffNode{NodeId: n.NodeID, Nodename: n.Nodename}
	}
	// The diff may take longer than the other requests: its own time, and the
	// computation goes on for the next request when this one stops waiting.
	diffCtx, diffCancel := context.WithTimeout(c.Request().Context(), pkgDiffTimeout())
	defer diffCancel()
	rows, err := a.packagesDiff(diffCtx, log, nodeIDs)
	if errors.Is(err, errPkgDiffBusy) {
		return JSONProblemf(c, http.StatusServiceUnavailable, "the packages diff is being computed, try again")
	}
	if err != nil {
		log.Error("cannot compare packages", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot compare packages")
	}
	data := make([]server.PackageDiffRow, len(rows))
	for i, r := range rows {
		data[i] = server.PackageDiffRow{
			NodeId: r.NodeID, PkgName: r.PkgName, PkgVersion: r.PkgVersion,
			PkgArch: r.PkgArch, PkgType: r.PkgType,
		}
	}
	resp := server.PackagesDiffResponse{Data: data}
	resp.Meta.NodeIds = nodeIDs
	resp.Meta.Nodes = resultNodes
	return c.JSON(http.StatusOK, resp)
}
