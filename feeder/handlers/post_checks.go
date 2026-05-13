package feederhandlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/feeder"
	"github.com/opensvc/oc3/util/logkey"
)

func (a *Api) PostChecks(c echo.Context) error {
	nodeID, log := getNodeIDAndLogger(c, "PostChecks")
	if nodeID == "" {
		return JSONNodeAuthProblem(c)
	}

	var payload feeder.PostChecks
	if err := c.Bind(&payload); err != nil {
		return JSONProblemf(c, http.StatusBadRequest, "Unable to parse body: %s", err)
	}

	val := []any{payload.Vars, payload.Vals}
	valBytes, err := json.Marshal(val)
	if err != nil {
		log.Error("json encode body", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "Unable to marshal val: %s", err)
	}

	ctx := c.Request().Context()
	if err := a.Redis.HSet(ctx, cachekeys.FeedChecksH, nodeID, string(valBytes)).Err(); err != nil {
		log.Error("HSet FeedChecksH", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "Unable to HSet check: %s", err)
	}

	if err := a.Redis.LRem(ctx, cachekeys.FeedChecksQ, 0, nodeID).Err(); err != nil {
		log.Error("LRem FeedChecksQ", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "Unable to LRem check: %s", err)
	}

	if err := a.Redis.LPush(ctx, cachekeys.FeedChecksQ, nodeID).Err(); err != nil {
		log.Error("LPush FeedChecksQ", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "Unable to LPush check: %s", err)
	}

	msg := fmt.Sprintf("Checks Vars: %v Vals: %v", payload.Vars, payload.Vals)
	log.Info(msg)

	return c.NoContent(http.StatusOK)
}
