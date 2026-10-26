package feederhandlers

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/feeder"
	"github.com/opensvc/oc3/util/logkey"
	"github.com/opensvc/oc3/util/pendingchanges"
)

func (a *Api) PostDaemonStatus(c echo.Context) error {
	nodeID, clusterID, log := getNodeIDClusterIDAndLogger(c, "PostDaemonStatus")
	if nodeID == "" {
		return JSONNodeAuthProblem(c)
	}

	body := c.Request().Body
	b, err := io.ReadAll(body)
	defer func() {
		if err := body.Close(); err != nil {
			log.Warn("request body Close", logkey.Error, err)
		}
	}()
	if err != nil {
		log.Warn("request ReadAll", logkey.Error, err)
		return JSONProblemf(c, http.StatusBadRequest, "ReadAll: %s", err)
	}
	postData := &feeder.PostDaemonStatus{}
	if err := json.Unmarshal(b, postData); err != nil {
		log.Debug("request Unmarshal", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if !strings.HasPrefix(postData.Version, "2.") && !strings.HasPrefix(postData.Version, "3.") {
		msg := fmt.Sprintf("unexpected version %s", postData.Version)
		log.Debug(msg)
		return JSONProblem(c, http.StatusBadRequest, msg)
	}
	ctx := c.Request().Context()
	log.Debug("HSet FeedDaemonStatusH")
	if err := a.Redis.HSet(ctx, cachekeys.FeedDaemonStatusH, nodeID, string(b)).Err(); err != nil {
		log.Error("HSet FeedDaemonStatusH", logkey.Error, err)
		return JSONError(c)
	}
	// The changes, merged with those not applied yet: the worker only writes in
	// full the objects and instances listed.
	if err := pendingchanges.Merge(ctx, a.Redis, cachekeys.FeedDaemonStatusChangesH, nodeID, postData.Changes); err != nil {
		log.Error("merge FeedDaemonStatusChangesH", logkey.Changes, postData.Changes, logkey.Error, err)
		return JSONError(c)
	}
	if err := a.pushNotPending(ctx, log, cachekeys.FeedDaemonStatusPendingH, cachekeys.FeedDaemonStatusQ, nodeID); err != nil {
		log.Error("pushNotPending", logkey.Error, err)
		return JSONError(c)
	}

	responseData := feeder.DaemonStatusAccepted{}
	if clusterID != "" {
		if nodes, err := a.getNodeWithActionQueued(ctx, clusterID); err != nil {
			log.Warn("nodesWithActionQueued", logkey.Error, err)
		} else {
			responseData.NodeWithActionQueued = &nodes
			// TODO: add metric
			log.Debug(fmt.Sprintf("found action queued for nodes: %s", nodes))
		}
		if objects, err := a.getObjectConfigToFeed(ctx, clusterID); err != nil {
			log.Warn("getObjectConfigToFeed", logkey.Error, err)
		} else if len(objects) > 0 {
			if err := a.removeObjectConfigToFeed(ctx, clusterID); err != nil {
				log.Warn("removeObjectConfigToFeed", logkey.Error, err)
			}
			responseData.ObjectWithoutConfig = &objects
			// TODO: add metric
			log.Debug("detect missing object configs", logkey.Objects, objects)
		}
	}
	log.Debug("accepted")
	return c.JSON(http.StatusAccepted, responseData)
}
