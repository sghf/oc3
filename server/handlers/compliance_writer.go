package serverhandlers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
)

// compWriter makes the changes of the compliance designer, to the rulesets, the
// modulesets and their relations, on one transaction: a single route applies its
// change all or nothing, POST /compliance/designer/commits a whole list of them.
// The checks read the transaction, so that a change sees those made before it in
// the same list, an object created by one of them say. The audit lines are
// written with the changes; the ruleset chains are rebuilt once, at the end.
type compWriter struct {
	a    *Api
	c    echo.Context
	ctx  context.Context
	db   *cdb.DB
	name string
	// chains tells the ruleset hierarchy or names changed: comp_rulesets_chains
	// is rebuilt before the commit.
	chains bool
	logs   []cdb.LogEntry
	// tmp maps the temporary references of a designer commit ("tmp:-3"), by
	// kind, to the ids of the objects its operations created.
	tmp    map[string]int64
	caller *formCaller
	group  *int64
}

// compTmpPrefix starts the reference of an object created earlier in the same
// designer commit, its temporary id in the designer's draft.
const compTmpPrefix = "tmp:"

// compWrite runs fn on a compWriter bound to a new transaction, committed when
// fn succeeds, rolled back otherwise. The changes are announced once committed.
// The error returned is an httpError, or errNothing as fn returned it.
func (a *Api) compWrite(c echo.Context, name string, fn func(w *compWriter) error) error {
	w, err := a.compBegin(c, name)
	if err != nil {
		return err
	}
	// A panic in fn: rolled back, then raised again (DB.Rollback).
	defer func() { _ = w.db.Rollback() }()
	return w.end(fn(w))
}

// compBegin starts the transaction of a compWriter; end finishes it.
func (a *Api) compBegin(c echo.Context, name string) (*compWriter, error) {
	ctx := c.Request().Context()
	tx, _, _, err := a.ODB.BeginTxWithControl(ctx, echolog.GetLogHandler(c, name), &sql.TxOptions{})
	if err != nil {
		return nil, httpInternal(echolog.GetLogHandler(c, name), "cannot start transaction", err)
	}
	return &compWriter{a: a, c: c, ctx: ctx, db: tx, name: name, tmp: map[string]int64{}}, nil
}

// end commits the changes when err is nil, after the chains and the audit lines,
// and rolls them back otherwise. It returns the error to answer, as an httpError,
// or errNothing as is: a change with nothing to do, which is no refusal.
func (w *compWriter) end(err error) error {
	log := echolog.GetLogHandler(w.c, w.name)
	if errors.Is(err, errNothing) {
		_ = w.db.Rollback()
		return errNothing
	}
	if err == nil && w.chains {
		if err = w.db.CompRulesetsChains(w.ctx); err != nil {
			err = fmt.Errorf("rebuild the ruleset chains: %w", err)
		}
	}
	if err == nil && len(w.logs) > 0 {
		// The audit is no reason to refuse the change, as for the other routes.
		if logErr := w.db.Log(w.ctx, w.logs...); logErr != nil {
			log.Error("cannot write audit log", "error", logErr)
		}
	}
	if err == nil {
		if err = w.db.Commit(); err != nil {
			err = fmt.Errorf("commit: %w", err)
		}
	}
	if err != nil {
		if rbErr := w.db.Rollback(); rbErr != nil {
			log.Error("rollback failed", "error", rbErr)
		}
		return w.err(err)
	}
	w.a.notifyChanges(log, w.ctx)
	return nil
}

// err turns an error into the httpError to answer.
func (w *compWriter) err(err error) error {
	if _, ok := err.(*httpError); ok {
		return err
	}
	return compDesignerError(w.c, w.name, err)
}

// log adds an audit line, written with the changes.
func (w *compWriter) log(action, format string, dict map[string]any) {
	userEmail, _ := w.c.Get(XUserEmail).(string)
	w.logs = append(w.logs, cdb.LogEntry{Action: action, User: userEmail, Fmt: format, Dict: dict, Level: "info"})
}

// callerName is the name of the user, as the objects record their author.
func (w *compWriter) callerName() (string, error) {
	if w.caller == nil {
		caller, err := w.a.formCaller(w.ctx, w.c)
		if err != nil {
			return "", err
		}
		w.caller = &caller
	}
	return w.caller.name, nil
}

// defaultGroup is the group a new object is published to and under the
// responsibility of: the user's default group, or Manager.
func (w *compWriter) defaultGroup() (int64, error) {
	if w.group == nil {
		id, err := w.db.DefaultGroupOrManager(w.ctx, authUserID(w.c))
		if err != nil {
			return 0, err
		}
		w.group = &id
	}
	return *w.group, nil
}

// created records the id of an object created under a temporary reference.
func (w *compWriter) created(k cdb.CompKind, ref string, id int64) {
	if strings.HasPrefix(ref, compTmpPrefix) {
		w.tmp[k.Name+":"+ref] = id
	}
}

// resolve returns the id of a ruleset or a moduleset given by id, by name, or by
// the temporary reference of an object created earlier in the same commit.
func (w *compWriter) resolve(k cdb.CompKind, ref string) (int64, error) {
	if strings.HasPrefix(ref, compTmpPrefix) {
		id, ok := w.tmp[k.Name+":"+ref]
		if !ok {
			return 0, httpErrorf(http.StatusBadRequest, "%s %s is not created by a previous operation", k.Name, ref)
		}
		return id, nil
	}
	var id int64
	var found bool
	var err error
	if k == cdb.CompRulesetKind {
		id, found, err = w.db.CompRulesetID(w.ctx, ref)
	} else {
		id, found, err = w.db.CompModulesetID(w.ctx, ref)
	}
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, httpErrorf(http.StatusNotFound, "%s %s not found", k.Name, ref)
	}
	return id, nil
}

func (w *compWriter) resolveRuleset(ref string) (int64, error) {
	return w.resolve(cdb.CompRulesetKind, ref)
}

func (w *compWriter) resolveModuleset(ref string) (int64, error) {
	return w.resolve(cdb.CompModulesetKind, ref)
}

// resolveVariable returns the id of a variable of a ruleset, given by id or name.
func (w *compWriter) resolveVariable(rid int64, ref string) (int64, error) {
	vid, found, err := w.db.CompRulesetVariableID(w.ctx, rid, ref)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, httpErrorf(http.StatusNotFound, "variable %s not found", ref)
	}
	return vid, nil
}

// resolveModule returns the id of a module of a moduleset, given by id or name.
func (w *compWriter) resolveModule(mid int64, ref string) (int64, error) {
	id, found, err := w.db.CompModulesetModuleID(w.ctx, mid, ref)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, httpErrorf(http.StatusNotFound, "module %s not found", ref)
	}
	return id, nil
}

// requireResponsible refuses a user none of whose groups is responsible for the
// object.
func (w *compWriter) requireResponsible(k cdb.CompKind, id int64) error {
	ok, err := w.db.CompObjectResponsible(w.ctx, k, id, UserGroupsFromContext(w.c), IsManager(w.c))
	if err != nil {
		return err
	}
	if !ok {
		return httpErrorf(http.StatusForbidden, "you are not responsible for this %s", k.Name)
	}
	return nil
}

// requirePublished refuses a user the object is not published to.
func (w *compWriter) requirePublished(k cdb.CompKind, id int64) error {
	ok, err := w.db.CompObjectPublished(w.ctx, k, id, UserGroupsFromContext(w.c), IsManager(w.c))
	if err != nil {
		return err
	}
	if !ok {
		return httpErrorf(http.StatusForbidden, "you are not member of one of the %s publication groups", k.Name)
	}
	return nil
}

func (w *compWriter) objectName(k cdb.CompKind, id int64) (string, error) {
	return w.db.CompObjectName(w.ctx, k, id)
}

// createRuleset creates a ruleset, published to and under the responsibility of
// the user's default group.
func (w *compWriter) createRuleset(name, rtype, public string) (int64, error) {
	if err := checkCompManager(w.c); err != nil {
		return 0, err
	}
	if _, found, err := w.db.CompRulesetID(w.ctx, name); err != nil {
		return 0, err
	} else if found {
		return 0, httpErrorf(http.StatusConflict, "a ruleset named '%s' already exists", name)
	}
	groupID, err := w.defaultGroup()
	if err != nil {
		return 0, err
	}
	id, err := w.db.CreateCompRuleset(w.ctx, name, rtype, public, groupID)
	if err != nil {
		return 0, err
	}
	w.chains = true
	w.log("compliance.ruleset.add", "added ruleset %(rset_name)s", map[string]any{"rset_name": name})
	return id, nil
}

// rulesetFields checks the settable properties of a ruleset.
func rulesetFields(entry map[string]any) (map[string]any, error) {
	fields := map[string]any{}
	for k, v := range entry {
		switch k {
		case "ruleset_name":
			name, err := compString(k, v, true)
			if err != nil {
				return nil, err
			}
			fields[k] = name
		case "ruleset_type":
			rtype, err := compString(k, v, true)
			if err != nil {
				return nil, err
			}
			if err := compOneOf(k, rtype, "explicit", "contextual"); err != nil {
				return nil, err
			}
			fields[k] = rtype
		case "ruleset_public":
			public, err := compBool(k, v)
			if err != nil {
				return nil, err
			}
			fields[k] = public
		case "id":
		default:
			return nil, httpErrorf(http.StatusBadRequest, "unknown ruleset property %s", k)
		}
	}
	return fields, nil
}

// updateRuleset changes properties of a ruleset, checked by rulesetFields.
func (w *compWriter) updateRuleset(id int64, fields map[string]any) error {
	if err := checkCompManager(w.c); err != nil {
		return err
	}
	if err := w.requireResponsible(cdb.CompRulesetKind, id); err != nil {
		return err
	}
	if len(fields) == 0 {
		return nil
	}
	if name, ok := fields["ruleset_name"].(string); ok {
		if other, found, err := w.db.CompRulesetID(w.ctx, name); err != nil {
			return err
		} else if found && other != id {
			return httpErrorf(http.StatusConflict, "a ruleset named '%s' already exists", name)
		}
		// The chains name their rulesets: a renaming rewrites them.
		w.chains = true
	}
	if err := w.db.UpdateCompRuleset(w.ctx, id, fields); err != nil {
		return err
	}
	w.log("compliance.ruleset.change", "update properties %(data)s", map[string]any{"data": compJSON(fields)})
	return nil
}

// cloneObject copies a ruleset or a moduleset as cloneName, "<name>_clone" when
// empty, under the responsibility of the user's default group.
func (w *compWriter) cloneObject(k cdb.CompKind, id int64, cloneName string) (int64, string, error) {
	if err := checkCompManager(w.c); err != nil {
		return 0, "", err
	}
	author, err := w.callerName()
	if err != nil {
		return 0, "", err
	}
	groupID, err := w.defaultGroup()
	if err != nil {
		return 0, "", err
	}
	name, err := w.objectName(k, id)
	if err != nil {
		return 0, "", err
	}
	var newID int64
	if k == cdb.CompRulesetKind {
		newID, cloneName, err = w.db.CloneCompRuleset(w.ctx, id, author, groupID, cloneName)
		w.chains = true
	} else {
		newID, cloneName, err = w.db.CloneCompModuleset(w.ctx, id, author, groupID, cloneName)
	}
	if err != nil {
		return 0, "", err
	}
	w.log("compliance."+k.Name+".clone", "cloned "+k.Name+" %(o)s from %(n)s", map[string]any{"n": name, "o": cloneName})
	return newID, cloneName, nil
}

// deleteObject deletes a ruleset or a moduleset and all its relations, by a
// CompManager responsible for it.
func (w *compWriter) deleteObject(k cdb.CompKind, id int64) error {
	if err := checkCompManager(w.c); err != nil {
		return err
	}
	if err := w.requireResponsible(k, id); err != nil {
		return err
	}
	name, err := w.objectName(k, id)
	if err != nil {
		return err
	}
	if k == cdb.CompRulesetKind {
		err = w.db.DeleteCompRuleset(w.ctx, id)
		w.chains = true
		if err == nil {
			w.log("compliance.ruleset.delete", "deleted ruleset %(rset_name)s", map[string]any{"rset_name": name})
		}
	} else {
		err = w.db.DeleteCompModuleset(w.ctx, id)
		if err == nil {
			w.log("compliance.moduleset.delete", "deleted moduleset %(modset_name)s", map[string]any{"modset_name": name})
		}
	}
	return err
}

// setRulesetFilterset gives a ruleset the filterset selecting the nodes and
// services it applies to, replacing the previous one.
func (w *compWriter) setRulesetFilterset(rid int64, fsetRef string) error {
	fid, fname, err := w.db.FiltersetByIDOrName(w.ctx, fsetRef)
	if err != nil {
		return err
	}
	if fid == 0 {
		return httpErrorf(http.StatusNotFound, "filterset %s not found", fsetRef)
	}
	if err := checkCompManager(w.c); err != nil {
		return err
	}
	if err := w.requireResponsible(cdb.CompRulesetKind, rid); err != nil {
		return err
	}
	if current, _, found, err := w.db.CompRulesetFilterset(w.ctx, rid); err != nil {
		return err
	} else if found && current == int64(fid) {
		return nil
	}
	name, err := w.objectName(cdb.CompRulesetKind, rid)
	if err != nil {
		return err
	}
	if err := w.db.SetCompRulesetFilterset(w.ctx, rid, int64(fid)); err != nil {
		return err
	}
	w.log("compliance.ruleset.change", "attach filterset %(fset_name)s to ruleset %(rset_name)s",
		map[string]any{"rset_name": name, "fset_name": fname})
	return nil
}

// detachRulesetFilterset removes the filterset of a ruleset; when it has none,
// errNone, or a 404 when nil.
func (w *compWriter) detachRulesetFilterset(rid int64, errNone error) error {
	if err := checkCompManager(w.c); err != nil {
		return err
	}
	if err := w.requireResponsible(cdb.CompRulesetKind, rid); err != nil {
		return err
	}
	_, fname, found, err := w.db.CompRulesetFilterset(w.ctx, rid)
	if err != nil {
		return err
	}
	if !found {
		if errNone == nil {
			return httpErrorf(http.StatusNotFound, "filterset not found")
		}
		return errNone
	}
	name, err := w.objectName(cdb.CompRulesetKind, rid)
	if err != nil {
		return err
	}
	if err := w.db.DeleteCompRulesetFilterset(w.ctx, rid); err != nil {
		return err
	}
	w.log("compliance.filterset.detach", "detach filterset %(fset_name)s from ruleset %(rset_name)s",
		map[string]any{"rset_name": name, "fset_name": fname})
	return nil
}

// errNothing is the errNone of a change with nothing to do, which is no refusal.
var errNothing = errors.New("nothing to do")

// attachRulesetChild makes a ruleset a child of another, unless that would close
// a loop, by a CompManager responsible for the parent.
func (w *compWriter) attachRulesetChild(parent, child int64) error {
	if err := checkCompManager(w.c); err != nil {
		return err
	}
	if parent == child {
		return httpErrorf(http.StatusBadRequest, "abort action to avoid introducing a recursion loop")
	}
	if err := w.requireResponsible(cdb.CompRulesetKind, parent); err != nil {
		return err
	}
	if attached, err := w.db.CompRulesetChildAttached(w.ctx, parent, child); err != nil {
		return err
	} else if attached {
		return httpErrorf(http.StatusConflict, "ruleset already attached")
	}
	if loop, err := w.db.CompRulesetLoop(w.ctx, child, parent); err != nil {
		return err
	} else if loop {
		return httpErrorf(http.StatusConflict, "the parent ruleset is already a child of the encapsulated ruleset. abort encapsulation not to cause infinite recursion")
	}
	parentName, childName, err := w.names(cdb.CompRulesetKind, parent, child)
	if err != nil {
		return err
	}
	if err := w.db.AttachCompRulesetChild(w.ctx, parent, child); err != nil {
		return err
	}
	w.chains = true
	w.log("compliance.ruleset.attach", "attach ruleset %(rset_name)s to %(dst_rset_name)s",
		map[string]any{"rset_name": childName, "dst_rset_name": parentName})
	return nil
}

// detachRulesetChild detaches a child ruleset, by a CompManager responsible for
// the parent, the child being published to them.
func (w *compWriter) detachRulesetChild(parent, child int64) error {
	if err := checkCompManager(w.c); err != nil {
		return err
	}
	if err := w.requireResponsible(cdb.CompRulesetKind, parent); err != nil {
		return err
	}
	if err := w.requirePublished(cdb.CompRulesetKind, child); err != nil {
		return err
	}
	parentName, childName, err := w.names(cdb.CompRulesetKind, parent, child)
	if err != nil {
		return err
	}
	if err := w.db.DetachCompRulesetChild(w.ctx, parent, child); err != nil {
		return err
	}
	w.chains = true
	w.log("compliance.ruleset.detach", "detach ruleset %(rset_name)s from %(parent_rset_name)s",
		map[string]any{"rset_name": childName, "parent_rset_name": parentName})
	return nil
}

// names returns the names of two objects of a kind, for the logs.
func (w *compWriter) names(k cdb.CompKind, a, b int64) (string, string, error) {
	aName, err := w.objectName(k, a)
	if err != nil {
		return "", "", err
	}
	bName, err := w.objectName(k, b)
	if err != nil {
		return "", "", err
	}
	return aName, bName, nil
}

// createModuleset creates a moduleset authored by the user, published to and
// under the responsibility of their default group.
func (w *compWriter) createModuleset(name string) (int64, error) {
	if err := checkCompManager(w.c); err != nil {
		return 0, err
	}
	if _, found, err := w.db.CompModulesetID(w.ctx, name); err != nil {
		return 0, err
	} else if found {
		return 0, httpErrorf(http.StatusConflict, "a moduleset named '%s' already exists", name)
	}
	author, err := w.callerName()
	if err != nil {
		return 0, err
	}
	groupID, err := w.defaultGroup()
	if err != nil {
		return 0, err
	}
	id, err := w.db.CreateCompModuleset(w.ctx, name, author, groupID)
	if err != nil {
		return 0, err
	}
	w.log("compliance.moduleset.add", "added moduleset %(modset_name)s", map[string]any{"modset_name": name})
	return id, nil
}

// requireModulesetChange checks the user may change the composition or the name
// of a moduleset: a CompManager responsible for it.
func (w *compWriter) requireModulesetChange(id int64) error {
	if err := checkCompManager(w.c); err != nil {
		return err
	}
	return w.requireResponsible(cdb.CompModulesetKind, id)
}

// renameModuleset renames a moduleset, recording the update date.
func (w *compWriter) renameModuleset(id int64, name string) error {
	if err := w.requireModulesetChange(id); err != nil {
		return err
	}
	if other, found, err := w.db.CompModulesetID(w.ctx, name); err != nil {
		return err
	} else if found && other != id {
		return httpErrorf(http.StatusConflict, "a moduleset named '%s' already exists", name)
	}
	if err := w.db.RenameCompModuleset(w.ctx, id, name); err != nil {
		return err
	}
	w.log("compliance.moduleset.change", "update properties %(data)s",
		map[string]any{"data": compJSON(map[string]any{"modset_name": name})})
	return nil
}

// attachModulesetChild makes a moduleset published to the user a child of
// another, unless that would close a loop; errNothing when it already is.
func (w *compWriter) attachModulesetChild(parent, child int64) error {
	if err := w.requireModulesetChange(parent); err != nil {
		return err
	}
	if err := w.requirePublished(cdb.CompModulesetKind, child); err != nil {
		return err
	}
	if parent == child {
		return httpErrorf(http.StatusBadRequest, "abort action to avoid introducing a recursion loop")
	}
	if attached, err := w.db.CompModulesetChildAttached(w.ctx, parent, child); err != nil {
		return err
	} else if attached {
		return errNothing
	}
	if loop, err := w.db.CompModulesetLoop(w.ctx, child, parent); err != nil {
		return err
	} else if loop {
		return httpErrorf(http.StatusConflict, "the parent moduleset is already a child of the encapsulated moduleset. abort encapsulation not to cause infinite recursion")
	}
	parentName, childName, err := w.names(cdb.CompModulesetKind, parent, child)
	if err != nil {
		return err
	}
	if err := w.db.AttachCompModulesetChild(w.ctx, parent, child); err != nil {
		return err
	}
	w.log("compliance.moduleset.moduleset.attach", "attached moduleset %(child_modset_name)s to moduleset %(parent_modset_name)s",
		map[string]any{"child_modset_name": childName, "parent_modset_name": parentName})
	return nil
}

// detachModulesetChild detaches a child moduleset.
func (w *compWriter) detachModulesetChild(parent, child int64) error {
	if err := w.requireModulesetChange(parent); err != nil {
		return err
	}
	parentName, childName, err := w.names(cdb.CompModulesetKind, parent, child)
	if err != nil {
		return err
	}
	if err := w.db.DetachCompModulesetChild(w.ctx, parent, child); err != nil {
		return err
	}
	w.log("compliance.moduleset.detach", "detach moduleset %(child_modset_name)s from moduleset %(parent_modset_name)s",
		map[string]any{"child_modset_name": childName, "parent_modset_name": parentName})
	return nil
}

// attachModulesetRuleset adds a ruleset published to the user to a moduleset;
// errNothing when it already is.
func (w *compWriter) attachModulesetRuleset(modset, rset int64) error {
	if err := w.requireModulesetChange(modset); err != nil {
		return err
	}
	if err := w.requirePublished(cdb.CompRulesetKind, rset); err != nil {
		return err
	}
	if attached, err := w.db.CompModulesetRulesetAttached(w.ctx, modset, rset); err != nil {
		return err
	} else if attached {
		return errNothing
	}
	modsetName, err := w.objectName(cdb.CompModulesetKind, modset)
	if err != nil {
		return err
	}
	rsetName, err := w.objectName(cdb.CompRulesetKind, rset)
	if err != nil {
		return err
	}
	if err := w.db.AttachCompModulesetRuleset(w.ctx, modset, rset); err != nil {
		return err
	}
	w.log("compliance.moduleset.ruleset.attach", "attach ruleset %(rset_name)s to moduleset %(modset_name)s",
		map[string]any{"rset_name": rsetName, "modset_name": modsetName})
	return nil
}

// detachModulesetRuleset removes a ruleset published to the user from a moduleset.
func (w *compWriter) detachModulesetRuleset(modset, rset int64) error {
	if err := w.requireModulesetChange(modset); err != nil {
		return err
	}
	if err := w.requirePublished(cdb.CompRulesetKind, rset); err != nil {
		return err
	}
	modsetName, err := w.objectName(cdb.CompModulesetKind, modset)
	if err != nil {
		return err
	}
	rsetName, err := w.objectName(cdb.CompRulesetKind, rset)
	if err != nil {
		return err
	}
	if err := w.db.DetachCompModulesetRuleset(w.ctx, modset, rset); err != nil {
		return err
	}
	w.log("compliance.ruleset.detach", "detach ruleset %(rset_name)s from moduleset %(modset_name)s",
		map[string]any{"rset_name": rsetName, "modset_name": modsetName})
	return nil
}

// requireRulesetChange checks the user may change the variables of a ruleset: a
// CompManager responsible for it.
func (w *compWriter) requireRulesetChange(rid int64) error {
	if err := checkCompManager(w.c); err != nil {
		return err
	}
	return w.requireResponsible(cdb.CompRulesetKind, rid)
}

// createOrUpdateVariable creates the variable fields name, checked by
// variableFields, or updates the ruleset's variable of that name.
func (w *compWriter) createOrUpdateVariable(rid int64, fields map[string]any) (int64, error) {
	if err := w.requireRulesetChange(rid); err != nil {
		return 0, err
	}
	name, ok := fields["var_name"].(string)
	if !ok {
		return 0, httpErrorf(http.StatusBadRequest, "var_name is mandatory in the posted data")
	}
	if vid, found, err := w.db.CompRulesetVariableID(w.ctx, rid, name); err != nil {
		return 0, err
	} else if found {
		return vid, w.updateVariable(rid, vid, fields)
	}
	author, err := w.callerName()
	if err != nil {
		return 0, err
	}
	vid, err := w.db.CreateCompRulesetVariable(w.ctx, rid, fields, author)
	if err != nil {
		return 0, err
	}
	logged := loggedVariableFields(fields)
	logged["ruleset_id"] = rid
	w.log("compliance.ruleset.variable.create", "properties %(data)s", map[string]any{"data": compJSON(logged)})
	return vid, nil
}

// updateVariable changes properties of a variable, checked by variableFields.
func (w *compWriter) updateVariable(rid, vid int64, fields map[string]any) error {
	if err := w.requireRulesetChange(rid); err != nil {
		return err
	}
	if name, ok := fields["var_name"].(string); ok {
		if other, found, err := w.db.CompRulesetVariableID(w.ctx, rid, name); err != nil {
			return err
		} else if found && other != vid {
			return httpErrorf(http.StatusConflict, "'var_name' already exist in the same ruleset")
		}
	}
	before, err := w.db.CompRulesetVariableByID(w.ctx, vid)
	if err != nil {
		return err
	}
	author, err := w.callerName()
	if err != nil {
		return err
	}
	if err := w.db.UpdateCompRulesetVariable(w.ctx, vid, fields, author); err != nil {
		return err
	}
	w.log("compliance.ruleset.variable.change", "changed properties %(data)s", map[string]any{
		"data": compJSON(map[string]any{"ruleset": before.RulesetName, "variable": before.Name, "changes": loggedVariableFields(fields)}),
	})
	return nil
}

// loggedVariableFields are the properties of a variable as the audit log
// records them: its value, a password or a key as often as not, by its length
// only. The historical collector logged it whole; the log is readable more
// widely than the ruleset. Not even a digest: a weak secret could be found
// from it by trying candidates.
func loggedVariableFields(fields map[string]any) map[string]any {
	logged := make(map[string]any, len(fields))
	for k, v := range fields {
		logged[k] = v
	}
	if v, ok := fields["var_value"]; ok {
		logged["var_value"] = fmt.Sprintf("(hidden, %d characters)", utf8.RuneCountInString(fmt.Sprint(v)))
	}
	return logged
}

// deleteVariable deletes a variable of a ruleset.
func (w *compWriter) deleteVariable(rid, vid int64) error {
	if err := w.requireRulesetChange(rid); err != nil {
		return err
	}
	v, err := w.db.CompRulesetVariableByID(w.ctx, vid)
	if err != nil {
		return err
	}
	if err := w.db.DeleteCompRulesetVariable(w.ctx, vid); err != nil {
		return err
	}
	w.log("compliance.ruleset.variable.delete", "deleted variable %(var_name)s from ruleset %(rset_name)s",
		map[string]any{"var_name": v.Name, "rset_name": v.RulesetName})
	return nil
}

// copyVariable copies or moves a variable to another ruleset. A copy requires
// the source ruleset to be published to the user, a move its responsibility;
// both the responsibility of the destination.
func (w *compWriter) copyVariable(rid, vid, dstID int64, move bool) error {
	if err := checkCompManager(w.c); err != nil {
		return err
	}
	var err error
	if move {
		err = w.requireResponsible(cdb.CompRulesetKind, rid)
	} else {
		err = w.requirePublished(cdb.CompRulesetKind, rid)
	}
	if err == nil {
		err = w.requireResponsible(cdb.CompRulesetKind, dstID)
	}
	if err != nil {
		return err
	}
	v, err := w.db.CompRulesetVariableByID(w.ctx, vid)
	if err != nil {
		return err
	}
	// Names are unique in a ruleset: the historical handlers refuse a duplicate on
	// creation and change, not on copy or move; refused here as well.
	if _, found, err := w.db.CompRulesetVariableID(w.ctx, dstID, v.Name); err != nil {
		return err
	} else if found {
		return httpErrorf(http.StatusConflict, "the destination ruleset already has a variable named %s", v.Name)
	}
	dstName, err := w.objectName(cdb.CompRulesetKind, dstID)
	if err != nil {
		return err
	}
	dict := map[string]any{"rset_name": v.RulesetName, "dst_rset_name": dstName, "var_name": v.Name}
	if move {
		if err := w.db.MoveCompRulesetVariable(w.ctx, vid, dstID); err != nil {
			return err
		}
		w.log("compliance.variable.change", "move variable %(var_name)s from ruleset %(rset_name)s to %(dst_rset_name)s", dict)
		return nil
	}
	author, err := w.callerName()
	if err != nil {
		return err
	}
	if _, err := w.db.CopyCompRulesetVariable(w.ctx, vid, dstID, author); err != nil {
		return err
	}
	w.log("compliance.variable.copy", "copy variable %(var_name)s from ruleset %(rset_name)s to %(dst_rset_name)s", dict)
	return nil
}

// createModule adds the module fields name to a moduleset, checked by moduleFields.
func (w *compWriter) createModule(mid int64, fields map[string]any) (int64, error) {
	if err := w.requireModulesetChange(mid); err != nil {
		return 0, err
	}
	name, ok := fields["modset_mod_name"].(string)
	if !ok {
		return 0, httpErrorf(http.StatusBadRequest, "modset_mod_name is mandatory in the posted data")
	}
	if _, found, err := w.db.CompModulesetModuleID(w.ctx, mid, name); err != nil {
		return 0, err
	} else if found {
		return 0, httpErrorf(http.StatusConflict, "this module name already exists")
	}
	author, err := w.callerName()
	if err != nil {
		return 0, err
	}
	id, err := w.db.CreateCompModulesetModule(w.ctx, mid, fields, author)
	if err != nil {
		return 0, err
	}
	logged := map[string]any{"modset_id": mid}
	for k, v := range fields {
		logged[k] = v
	}
	w.log("compliance.moduleset.module.create", "properties %(data)s", map[string]any{"data": compJSON(logged)})
	return id, nil
}

// updateModule changes the name or the autofix flag of a module, checked by
// moduleFields.
func (w *compWriter) updateModule(mid, id int64, fields map[string]any) error {
	if err := w.requireModulesetChange(mid); err != nil {
		return err
	}
	if name, ok := fields["modset_mod_name"].(string); ok {
		if other, found, err := w.db.CompModulesetModuleID(w.ctx, mid, name); err != nil {
			return err
		} else if found && other != id {
			return httpErrorf(http.StatusConflict, "this module name already exists")
		}
	}
	author, err := w.callerName()
	if err != nil {
		return err
	}
	if err := w.db.UpdateCompModulesetModule(w.ctx, id, fields, author); err != nil {
		return err
	}
	logged := map[string]any{"id": id}
	for k, v := range fields {
		logged[k] = v
	}
	w.log("compliance.moduleset.module.change", "changed properties %(data)s", map[string]any{"data": compJSON(logged)})
	return nil
}

// deleteModule removes a module from a moduleset.
func (w *compWriter) deleteModule(mid, id int64) error {
	if err := w.requireModulesetChange(mid); err != nil {
		return err
	}
	name, err := w.db.CompModulesetModuleName(w.ctx, id)
	if err != nil {
		return err
	}
	modset, err := w.objectName(cdb.CompModulesetKind, mid)
	if err != nil {
		return err
	}
	if err := w.db.DeleteCompModulesetModule(w.ctx, id); err != nil {
		return err
	}
	w.log("compliance.moduleset.module.delete", "deleted module %(mod_name)s from moduleset %(modset_name)s",
		map[string]any{"mod_name": name, "modset_name": modset})
	return nil
}

// teamChange attaches or detaches a publication or responsible group of an
// object, as attach_group_to_ruleset() and detach_group_from_ruleset() and their
// moduleset counterparts: by a CompManager responsible for the object. A
// non-manager may attach only one of their groups; Everybody may not be made
// responsible, nor be given the publication of a contextual ruleset. errNothing
// when the group is already attached.
func (w *compWriter) teamChange(k cdb.CompKind, objID int64, gtype, groupRef string, attach bool) error {
	if err := checkCompManager(w.c); err != nil {
		return err
	}
	if err := w.requireResponsible(k, objID); err != nil {
		if he, ok := err.(*httpError); ok && he.status == http.StatusForbidden {
			return httpErrorf(http.StatusForbidden, "%s not found or not owned by you", k.Name)
		}
		return err
	}
	group, found, err := w.db.AuthGroupByIDOrRole(w.ctx, groupRef)
	if err != nil {
		return err
	}
	if !found {
		return httpErrorf(http.StatusNotFound, "group %s not found", groupRef)
	}
	objName, err := w.objectName(k, objID)
	if err != nil {
		return err
	}
	nameKey := "rset_name"
	if k == cdb.CompModulesetKind {
		nameKey = "modset_name"
	}
	dict := map[string]any{"gtype": gtype, "role": group.Role, nameKey: objName}
	if !attach {
		if err := w.db.DetachCompTeam(w.ctx, k, objID, gtype, group.ID); err != nil {
			return err
		}
		w.log("compliance."+k.Name+".detach", "detach %(gtype)s group %(role)s from "+k.Name+" %("+nameKey+")s", dict)
		return nil
	}
	if group.Role == "Everybody" {
		if gtype == "responsible" {
			return httpErrorf(http.StatusBadRequest, "Giving responsibility of a %s to Everybody is not allowed", k.Name)
		}
		if k == cdb.CompRulesetKind {
			if contextual, err := w.db.RulesetIsContextual(w.ctx, objID); err != nil {
				return err
			} else if contextual {
				return httpErrorf(http.StatusBadRequest, "Publishing a contextual ruleset to Everybody is not allowed")
			}
		}
	}
	if !IsManager(w.c) && !HasGroup(w.c, group.Role) {
		return httpErrorf(http.StatusForbidden, "you can't attach a group you are not a member of")
	}
	if attached, err := w.db.CompTeamAttached(w.ctx, k, objID, gtype, group.ID); err != nil {
		return err
	} else if attached {
		return errNothing
	}
	if err := w.db.AttachCompTeam(w.ctx, k, objID, gtype, group.ID); err != nil {
		return err
	}
	w.log("compliance."+k.Name+".change", "attach %(gtype)s group %(role)s to "+k.Name+" %("+nameKey+")s", dict)
	return nil
}
