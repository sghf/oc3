package serverhandlers

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// maxCompDesignerOperations is the most operations a designer commit takes,
// as the schema says.
const maxCompDesignerOperations = 5000

// compDesignerBaselineMessage is the message of the version recording the
// changes made elsewhere, when the commit gives none.
const compDesignerBaselineMessage = "Changes made outside the designer"

// PostComplianceDesignerCommits handles POST /compliance/designer/commits: the
// changes of the compliance designer, applied all or nothing on one transaction
// (compWriter), between two versions of the compliance history, the changes made
// elsewhere and those of the commit. The commits are serialized by
// complianceHistoryMu, for each version to record its own changes only.
func (a *Api) PostComplianceDesignerCommits(c echo.Context) error {
	const name = "PostComplianceDesignerCommits"
	log := echolog.GetLogHandler(c, name)
	if err := checkCompManager(c); err != nil {
		return httpProblem(c, err)
	}
	var body server.ComplianceDesignerCommitInput
	if err := c.Bind(&body); err != nil {
		return c.JSON(http.StatusBadRequest, server.ComplianceDesignerRefusal{Text: err.Error()})
	}
	message := strings.TrimSpace(body.Message)
	if message == "" {
		return c.JSON(http.StatusBadRequest, server.ComplianceDesignerRefusal{Text: "message is mandatory"})
	}
	if n := len(body.Operations); n == 0 || n > maxCompDesignerOperations {
		return c.JSON(http.StatusBadRequest, server.ComplianceDesignerRefusal{
			Text: fmt.Sprintf("operations: 1 to %d expected, got %d", maxCompDesignerOperations, n),
		})
	}
	baseline := compDesignerBaselineMessage
	if body.BaselineMessage != nil && strings.TrimSpace(*body.BaselineMessage) != "" {
		baseline = strings.TrimSpace(*body.BaselineMessage)
	}

	complianceHistoryMu.Lock()
	defer complianceHistoryMu.Unlock()
	// What changed since the last version came from elsewhere: a version of its
	// own, not to be counted as this commit's. Without it, the commit goes on all
	// the same, as the designer did before.
	if _, _, err := a.recordComplianceVersion(c, baseline, "elsewhere"); err != nil {
		log.Error("cannot record the compliance version of the changes made elsewhere", logkey.Error, err)
	}

	w, err := a.compBegin(c, name)
	if err != nil {
		return httpProblem(c, err)
	}
	// A panic: rolled back, then raised again (DB.Rollback).
	defer func() { _ = w.db.Rollback() }()
	for i, op := range body.Operations {
		// A caller gone: nothing is applied.
		if err := c.Request().Context().Err(); err != nil {
			log.Info("caller gone", "applied", i, "operations", len(body.Operations))
			return err
		}
		if err := w.apply(op); err != nil && !errors.Is(err, errNothing) {
			_ = w.db.Rollback()
			refusal := server.ComplianceDesignerRefusal{Text: err.Error(), Index: &i}
			status := http.StatusInternalServerError
			var he *httpError
			if errors.As(w.err(err), &he) {
				refusal.Text, status = he.msg, he.status
			}
			log.Info("refused", "index", i, "op", op.Op, "status", status, "text", refusal.Text)
			return c.JSON(status, refusal)
		}
	}
	if err := w.end(nil); err != nil {
		return httpProblem(c, err)
	}

	result := server.ComplianceDesignerCommitResult{Applied: len(body.Operations), Ids: map[string]int64{}}
	for key, id := range w.tmp {
		_, ref, _ := strings.Cut(key, ":")
		result.Ids[ref] = id
	}
	commit, changed, err := a.recordComplianceVersion(c, message, "designer")
	if err != nil {
		log.Error("cannot record the compliance version", logkey.Error, err)
		text := "cannot record the compliance version"
		result.VersionError = &text
	} else {
		result.Version = &struct {
			Changed bool   `json:"changed"`
			Commit  string `json:"commit"`
		}{Changed: changed, Commit: commit}
	}
	log.Info("committed", "operations", len(body.Operations), "version", commit)
	return c.JSON(http.StatusOK, result)
}

// apply makes one operation of a designer commit.
func (w *compWriter) apply(op server.ComplianceDesignerOperation) error {
	k, err := compDesignerKind(op.Kind)
	if err != nil {
		return err
	}
	switch op.Op {
	case server.CompDesignerOpCreate:
		name, err := need("name", op.Name)
		if err != nil {
			return err
		}
		as, err := need("as", op.As)
		if err != nil {
			return err
		}
		var id int64
		if k == cdb.CompRulesetKind {
			id, err = w.createRuleset(name, "explicit", "T")
		} else {
			id, err = w.createModuleset(name)
		}
		if err != nil {
			return err
		}
		w.created(k, as, id)
		return nil
	}

	// The other operations apply to an object.
	ref, err := need("ref", op.Ref)
	if err != nil {
		return err
	}
	id, err := w.resolve(k, ref)
	if err != nil {
		return err
	}
	switch op.Op {
	case server.CompDesignerOpRename:
		name, err := need("name", op.Name)
		if err != nil {
			return err
		}
		if k == cdb.CompRulesetKind {
			return w.updateRuleset(id, map[string]any{"ruleset_name": name})
		}
		return w.renameModuleset(id, name)
	case server.CompDesignerOpClone:
		as, err := need("as", op.As)
		if err != nil {
			return err
		}
		newID, _, err := w.cloneObject(k, id, strings.TrimSpace(value(op.Name)))
		if err != nil {
			return err
		}
		w.created(k, as, newID)
		return nil
	case server.CompDesignerOpDelete:
		return w.deleteObject(k, id)
	case server.CompDesignerOpInclude, server.CompDesignerOpDetach:
		return w.applyInclusion(op, k, id)
	case server.CompDesignerOpAddTeam, server.CompDesignerOpRemoveTeam:
		team, err := need("team", op.Team)
		if err != nil {
			return err
		}
		role, err := compDesignerRole("role", op.Role)
		if err != nil {
			return err
		}
		return w.teamChange(k, id, role, team, op.Op == server.CompDesignerOpAddTeam)
	case server.CompDesignerOpMoveTeam:
		team, err := need("team", op.Team)
		if err != nil {
			return err
		}
		from, err := compDesignerRole("role", op.Role)
		if err != nil {
			return err
		}
		to, err := compDesignerRole("to_role", op.ToRole)
		if err != nil {
			return err
		}
		// One change in the transaction: the group never loses its role alone.
		if err := w.teamChange(k, id, from, team, false); err != nil {
			return err
		}
		return w.teamChange(k, id, to, team, true)
	}
	if k == cdb.CompRulesetKind {
		return w.applyRuleset(op, id)
	}
	return w.applyModuleset(op, id)
}

// applyRuleset makes an operation on the properties or the variables of a ruleset.
func (w *compWriter) applyRuleset(op server.ComplianceDesignerOperation, rid int64) error {
	switch op.Op {
	case server.CompDesignerOpSetType:
		if op.RulesetType == nil {
			return httpErrorf(http.StatusBadRequest, "ruleset_type is mandatory")
		}
		return w.updateRuleset(rid, map[string]any{"ruleset_type": string(*op.RulesetType)})
	case server.CompDesignerOpSetPublic:
		if op.RulesetPublic == nil {
			return httpErrorf(http.StatusBadRequest, "ruleset_public is mandatory")
		}
		return w.updateRuleset(rid, map[string]any{"ruleset_public": compFlag(*op.RulesetPublic)})
	case server.CompDesignerOpSetFilterset:
		fset := strings.TrimSpace(value(op.Filterset))
		if fset == "" {
			return w.detachRulesetFilterset(rid, errNothing)
		}
		// A filterset goes to a contextual ruleset: made one in the same change.
		contextual, err := w.db.RulesetIsContextual(w.ctx, rid)
		if err != nil {
			return err
		}
		if !contextual {
			if err := w.updateRuleset(rid, map[string]any{"ruleset_type": "contextual"}); err != nil {
				return err
			}
		}
		return w.setRulesetFilterset(rid, fset)
	case server.CompDesignerOpAddVariable:
		name, err := need("var_name", op.VarName)
		if err != nil {
			return err
		}
		// An addition, not the update the single route makes of a variable of the
		// same name: one made meanwhile is not overwritten.
		if _, found, err := w.db.CompRulesetVariableID(w.ctx, rid, name); err != nil {
			return err
		} else if found {
			return httpErrorf(http.StatusConflict, "the ruleset already has a variable named %s", name)
		}
		_, err = w.createOrUpdateVariable(rid, map[string]any{
			"var_name": name, "var_class": value(op.VarClass), "var_value": value(op.VarValue),
		})
		return err
	case server.CompDesignerOpUpdateVariable, server.CompDesignerOpDeleteVariable, server.CompDesignerOpCopyVariable:
		ref, err := need("variable", op.Variable)
		if err != nil {
			return err
		}
		vid, err := w.resolveVariable(rid, ref)
		if err != nil {
			return err
		}
		switch op.Op {
		case server.CompDesignerOpUpdateVariable:
			fields := map[string]any{}
			if op.VarName != nil {
				name, err := need("var_name", op.VarName)
				if err != nil {
					return err
				}
				fields["var_name"] = name
			}
			if op.VarClass != nil {
				fields["var_class"] = strings.TrimSpace(*op.VarClass)
			}
			if op.VarValue != nil {
				fields["var_value"] = *op.VarValue
			}
			if len(fields) == 0 {
				return errNothing
			}
			return w.updateVariable(rid, vid, fields)
		case server.CompDesignerOpDeleteVariable:
			return w.deleteVariable(rid, vid)
		default:
			dstRef, err := need("dst", op.Dst)
			if err != nil {
				return err
			}
			dst, err := w.resolveRuleset(dstRef)
			if err != nil {
				return err
			}
			return w.copyVariable(rid, vid, dst, op.Move != nil && *op.Move)
		}
	}
	return httpErrorf(http.StatusBadRequest, "operation %s does not apply to a ruleset", op.Op)
}

// applyModuleset makes an operation on the modules of a moduleset.
func (w *compWriter) applyModuleset(op server.ComplianceDesignerOperation, mid int64) error {
	if op.Op == server.CompDesignerOpAddModule {
		name, err := need("name", op.Name)
		if err != nil {
			return err
		}
		_, err = w.createModule(mid, map[string]any{"modset_mod_name": name, "autofix": "F"})
		return err
	}
	switch op.Op {
	case server.CompDesignerOpRenameModule, server.CompDesignerOpSetAutofix, server.CompDesignerOpDeleteModule:
	default:
		return httpErrorf(http.StatusBadRequest, "operation %s does not apply to a moduleset", op.Op)
	}
	ref, err := need("module", op.Module)
	if err != nil {
		return err
	}
	id, err := w.resolveModule(mid, ref)
	if err != nil {
		return err
	}
	switch op.Op {
	case server.CompDesignerOpRenameModule:
		name, err := need("name", op.Name)
		if err != nil {
			return err
		}
		return w.updateModule(mid, id, map[string]any{"modset_mod_name": name})
	case server.CompDesignerOpSetAutofix:
		if op.Autofix == nil {
			return httpErrorf(http.StatusBadRequest, "autofix is mandatory")
		}
		return w.updateModule(mid, id, map[string]any{"autofix": compFlag(*op.Autofix)})
	default:
		return w.deleteModule(mid, id)
	}
}

// applyInclusion includes a child in a parent or detaches it: a ruleset in a
// ruleset, a ruleset or a moduleset in a moduleset.
func (w *compWriter) applyInclusion(op server.ComplianceDesignerOperation, k cdb.CompKind, parent int64) error {
	if op.ChildKind == nil {
		return httpErrorf(http.StatusBadRequest, "child_kind is mandatory")
	}
	childKind, err := compDesignerKind(*op.ChildKind)
	if err != nil {
		return err
	}
	ref, err := need("child", op.Child)
	if err != nil {
		return err
	}
	child, err := w.resolve(childKind, ref)
	if err != nil {
		return err
	}
	include := op.Op == server.CompDesignerOpInclude
	switch {
	case k == cdb.CompRulesetKind && childKind == cdb.CompRulesetKind && include:
		return w.attachRulesetChild(parent, child)
	case k == cdb.CompRulesetKind && childKind == cdb.CompRulesetKind:
		return w.detachRulesetChild(parent, child)
	case k == cdb.CompRulesetKind:
		return httpErrorf(http.StatusBadRequest, "a ruleset holds no moduleset")
	case childKind == cdb.CompRulesetKind && include:
		return w.attachModulesetRuleset(parent, child)
	case childKind == cdb.CompRulesetKind:
		return w.detachModulesetRuleset(parent, child)
	case include:
		return w.attachModulesetChild(parent, child)
	default:
		return w.detachModulesetChild(parent, child)
	}
}

func compDesignerKind(kind server.ComplianceDesignerKind) (cdb.CompKind, error) {
	switch kind {
	case server.CompDesignerRuleset:
		return cdb.CompRulesetKind, nil
	case server.CompDesignerModuleset:
		return cdb.CompModulesetKind, nil
	}
	return cdb.CompKind{}, httpErrorf(http.StatusBadRequest, "invalid kind %q: expected ruleset or moduleset", kind)
}

func compDesignerRole(key string, role *server.ComplianceDesignerTeamRole) (string, error) {
	if role == nil {
		return "", httpErrorf(http.StatusBadRequest, "%s is mandatory", key)
	}
	switch *role {
	case server.CompDesignerPublication, server.CompDesignerResponsible:
		return string(*role), nil
	}
	return "", httpErrorf(http.StatusBadRequest, "invalid %s %q: expected publication or responsible", key, *role)
}

// need returns a mandatory string property, trimmed.
func need(key string, v *string) (string, error) {
	if v == nil || strings.TrimSpace(*v) == "" {
		return "", httpErrorf(http.StatusBadRequest, "%s is mandatory", key)
	}
	return strings.TrimSpace(*v), nil
}

func value(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

// compFlag is a boolean as the compliance tables store it.
func compFlag(b bool) string {
	if b {
		return "T"
	}
	return "F"
}
