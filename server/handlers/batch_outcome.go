package serverhandlers

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
)

// maxBatchItems is the most objects a batch route takes in one request: each
// one costs its checks and its change, the batch a single request.
const maxBatchItems = 1000

// batchOutcome is what became of one object of a write: an http status and a
// message. The single route answers with it, a batch route lists it.
type batchOutcome struct {
	status int
	text   string
}

func batchOK(format string, args ...any) batchOutcome {
	return batchOutcome{status: http.StatusOK, text: fmt.Sprintf(format, args...)}
}

func batchFail(code int, format string, args ...any) batchOutcome {
	return batchOutcome{status: code, text: fmt.Sprintf(format, args...)}
}

func (o batchOutcome) ok() bool { return o.status < http.StatusBadRequest }

// writeOutcome answers a single route with the outcome of its object: a problem,
// or the message as info.
func writeOutcome(c echo.Context, o batchOutcome) error {
	if !o.ok() {
		return JSONProblem(c, o.status, o.text)
	}
	return c.JSON(http.StatusOK, infoAnswer("%s", o.text))
}

// batchItem is the outcome of one object of a batch, named by its id.
func batchItem(id string, o batchOutcome) server.BatchOutcome {
	return server.BatchOutcome{Id: id, Status: o.status, Info: o.text}
}

// checkBatchSize refuses an empty batch, or one above maxBatchItems.
func checkBatchSize(n int) error {
	if n == 0 || n > maxBatchItems {
		return fmt.Errorf("1 to %d objects expected, got %d", maxBatchItems, n)
	}
	return nil
}
