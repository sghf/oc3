package serverhandlers

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
)

// maxCompAttachmentPairs is the most pairs POST /compliance/attachments takes:
// each one costs a few queries, the batch a request.
const maxCompAttachmentPairs = 1000

// PostComplianceAttachments handles POST /compliance/attachments: each pair goes
// through the checks and the change of its single route, a refusal not stopping
// the others.
func (a *Api) PostComplianceAttachments(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostComplianceAttachments")
	var body server.PostComplianceAttachmentsJSONRequestBody
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if err := checkCompPairs(body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	attach := body.Action == server.Attach
	outcomes := make([]server.ComplianceAttachmentOutcome, 0, len(body.Pairs))
	done := 0
	for _, pair := range body.Pairs {
		// A caller gone: the pairs left are not applied.
		if err := c.Request().Context().Err(); err != nil {
			log.Info("caller gone", "applied", len(outcomes), "pairs", len(body.Pairs))
			return err
		}
		o := a.applyCompPair(c, attach, pair)
		if o.status < http.StatusBadRequest {
			done++
		}
		outcomes = append(outcomes, server.ComplianceAttachmentOutcome{
			Object:   server.ComplianceAttachmentOutcomeObject(pair.Object),
			ObjectId: pair.ObjectId,
			NodeId:   pair.NodeId,
			SvcId:    pair.SvcId,
			Slave:    pair.Slave,
			Status:   o.status,
			Info:     o.text,
		})
	}
	log.Info("applied", "action", body.Action, "pairs", len(body.Pairs), "done", done)
	return c.JSON(http.StatusOK, server.ComplianceAttachmentsResponse{Data: outcomes})
}

// applyCompPair attaches or detaches one pair through the function of its single
// route.
func (a *Api) applyCompPair(c echo.Context, attach bool, p server.ComplianceAttachmentPair) compOutcome {
	moduleset := p.Object == server.ComplianceAttachmentPairObjectModuleset
	if p.NodeId != nil {
		switch {
		case moduleset && attach:
			return a.attachNodeModuleset(c, *p.NodeId, p.ObjectId)
		case moduleset:
			return a.detachNodeModuleset(c, *p.NodeId, p.ObjectId)
		case attach:
			return a.attachNodeRuleset(c, *p.NodeId, p.ObjectId)
		default:
			return a.detachNodeRuleset(c, *p.NodeId, p.ObjectId)
		}
	}
	slave := p.Slave != nil && *p.Slave
	switch {
	case moduleset && attach:
		return a.attachServiceModuleset(c, *p.SvcId, p.ObjectId, slave)
	case moduleset:
		return a.detachServiceModuleset(c, *p.SvcId, p.ObjectId, slave)
	case attach:
		return a.attachServiceRuleset(c, *p.SvcId, p.ObjectId, slave)
	default:
		return a.detachServiceRuleset(c, *p.SvcId, p.ObjectId, slave)
	}
}

// checkCompPairs refuses a batch with a pair it cannot apply, before applying
// any: a known action, 1 to maxCompAttachmentPairs pairs, each naming a moduleset
// or a ruleset and exactly one node or service, slave only with a service.
func checkCompPairs(body server.ComplianceAttachmentsInput) error {
	if body.Action != server.Attach && body.Action != server.Detach {
		return fmt.Errorf("action: %q is neither attach nor detach", body.Action)
	}
	if len(body.Pairs) == 0 || len(body.Pairs) > maxCompAttachmentPairs {
		return fmt.Errorf("pairs: 1 to %d expected, got %d", maxCompAttachmentPairs, len(body.Pairs))
	}
	for i, p := range body.Pairs {
		switch {
		case p.Object != server.ComplianceAttachmentPairObjectModuleset && p.Object != server.ComplianceAttachmentPairObjectRuleset:
			return fmt.Errorf("pairs[%d].object: %q is neither moduleset nor ruleset", i, p.Object)
		case p.ObjectId == "":
			return fmt.Errorf("pairs[%d].object_id is empty", i)
		case (p.NodeId == nil || *p.NodeId == "") == (p.SvcId == nil || *p.SvcId == ""):
			return fmt.Errorf("pairs[%d]: exactly one of node_id and svc_id expected", i)
		case p.Slave != nil && p.NodeId != nil && *p.NodeId != "":
			return fmt.Errorf("pairs[%d].slave: only for a service", i)
		}
	}
	return nil
}
