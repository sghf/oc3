package serverhandlers

import (
	"encoding/json"
	"fmt"
	neturl "net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// The functions of this file port the form data helpers of the historical
// collector (init/models/rest/lib_forms.py), keeping their semantics: form data
// is the object a form submission carries, keyed by input id.

// formValueString returns the text of a submitted value when it is one; the
// historical conditions compare strings, a number never equals "1".
func formValueString(v any) (string, bool) {
	s, ok := v.(string)
	return s, ok
}

// formValueText renders a submitted value as unicode(val) does, for the
// dereferencing of "#key" references.
func formValueText(v any) string {
	switch t := v.(type) {
	case nil:
		return "None"
	case string:
		return t
	case bool:
		if t {
			return "True"
		}
		return "False"
	case json.Number:
		return t.String()
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}

// formGetVal returns the value of a dotted key "a.b.c" in nested objects, as
// form_get_val() does.
func formGetVal(d any, key string) (any, error) {
	// A flat key, as oc3 names the columns of a joined table, "stor_array.id".
	if m, ok := d.(map[string]any); ok {
		if v, ok := m[key]; ok {
			return v, nil
		}
	}
	parts := strings.Split(key, ".")
	cur := d
	for _, part := range parts {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s", part)
		}
		v, ok := m[part]
		if !ok {
			return nil, fmt.Errorf("%s", part)
		}
		cur = v
	}
	return cur, nil
}

// formRestArgs splits a rest url into path elements, "#key" references
// replaced by their value, as form_rest_args() does:
// "/arrays/#id/diskgroups" gives [arrays 554 diskgroups]. Each element is
// escaped as one path segment (url.PathEscape): a "/", "?", "#" or "%" coming
// from a value cannot change the endpoint called, add a query or cut the path;
// a value leaving an element empty or a dot segment is refused.
func formRestArgs(url string, d any) ([]string, error) {
	var args []string
	for _, s := range strings.Split(strings.TrimRight(url, "/"), "/") {
		if s == "" {
			continue
		}
		if !strings.Contains(s, "#") {
			args = append(args, s)
			continue
		}
		var text string
		if formRefOnly.MatchString(s) {
			v, err := formGetVal(d, strings.TrimLeft(s, "#"))
			if err != nil {
				return nil, err
			}
			text = formValueText(v)
		} else {
			m, _ := d.(map[string]any)
			text = formDereference(s, m, "")
			if formRef.MatchString(text) {
				return nil, fmt.Errorf("%s", s)
			}
		}
		switch text {
		case "", ".", "..":
			return nil, fmt.Errorf("%s: the value %q makes no path element", s, text)
		}
		args = append(args, neturl.PathEscape(text))
	}
	return args, nil
}

// formRef is a "#key" or "#a.b" reference; formRefOnly a whole element of one.
var (
	formRef     = regexp.MustCompile(`#[\w.]+`)
	formRefOnly = regexp.MustCompile(`^#[\w.]+$`)
)

// formDereference replaces the "#key" references of s by the values of the
// data, nested keys as "#a.b", as form_dereference() does: keys are replaced
// longest name first, so that "#name" does not eat the start of "#names".
func formDereference(s string, data map[string]any, prefix string) string {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	for _, k := range keys {
		if sub, ok := data[k].(map[string]any); ok {
			s = formDereference(s, sub, prefix+k+".")
			continue
		}
		s = strings.ReplaceAll(s, "#"+prefix+k, formValueText(data[k]))
	}
	return s
}

// formEmpty tells whether a submitted value counts as empty in a condition.
func formEmpty(d map[string]any, key string) bool {
	v, ok := d[key]
	if !ok || v == nil {
		return true
	}
	s, isString := v.(string)
	return isString && (s == "" || s == "undefined")
}

// checkFormConditions evaluates an input condition: one condition, or a list of
// them that must all hold.
func checkFormConditions(cond any, d map[string]any) (bool, error) {
	if list, ok := cond.([]any); ok {
		for _, c := range list {
			ok, err := checkFormCondition(c, d)
			if err != nil || !ok {
				return false, err
			}
		}
		return true, nil
	}
	return checkFormCondition(cond, d)
}

// checkFormCondition evaluates one "#var op value" condition, as
// check_condition() does: ==, !=, IN, NOT IN, > and <, "empty" standing for a
// missing or empty value.
func checkFormCondition(condAny any, d map[string]any) (bool, error) {
	if condAny == nil {
		return false, fmt.Errorf("malformed condition: None")
	}
	cond, ok := condAny.(string)
	if !ok {
		return false, fmt.Errorf("malformed condition: %v", condAny)
	}
	if cond == "" || cond == "none" {
		return true, nil
	}
	getVarVal := func(op string) (string, string, error) {
		parts := strings.SplitN(cond, op, 2)
		if len(parts) != 2 {
			return "", "", fmt.Errorf("malformed output condition: %s", cond)
		}
		v, val := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		if !strings.HasPrefix(v, "#") || len(v) < 2 {
			return "", "", fmt.Errorf("malformed output condition: %s", cond)
		}
		return v[1:], val, nil
	}
	if d == nil {
		return false, fmt.Errorf("no form data")
	}
	switch {
	case strings.Contains(cond, "=="):
		v, val, err := getVarVal("==")
		if err != nil {
			return false, err
		}
		if val == "empty" {
			return formEmpty(d, v), nil
		}
		s, isString := formValueString(d[v])
		return isString && s == val, nil
	case strings.Contains(cond, "!="):
		v, val, err := getVarVal("!=")
		if err != nil {
			return false, err
		}
		if val == "empty" {
			return !formEmpty(d, v), nil
		}
		if _, ok := d[v]; !ok {
			return true, nil
		}
		s, isString := formValueString(d[v])
		return !isString || s != val, nil
	case strings.Contains(cond, " NOT IN "):
		v, val, err := getVarVal("NOT IN")
		if err != nil {
			return false, err
		}
		if _, ok := d[v]; !ok {
			return true, nil
		}
		s, isString := formValueString(d[v])
		return !isString || !containsString(strings.Split(val, ","), s), nil
	case strings.Contains(cond, " IN "):
		v, val, err := getVarVal("IN")
		if err != nil {
			return false, err
		}
		if _, ok := d[v]; !ok {
			return false, nil
		}
		s, isString := formValueString(d[v])
		return isString && containsString(strings.Split(val, ","), s), nil
	case strings.Contains(cond, " > "), strings.Contains(cond, " < "):
		op := ">"
		if !strings.Contains(cond, " > ") {
			op = "<"
		}
		v, val, err := getVarVal(op)
		if err != nil {
			return false, err
		}
		ref, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return false, nil
		}
		raw, ok := d[v]
		if !ok {
			return false, nil
		}
		f, err := strconv.ParseFloat(formValueText(raw), 64)
		if err != nil {
			return false, nil
		}
		if op == ">" {
			return f > ref, nil
		}
		return f < ref, nil
	}
	return false, fmt.Errorf("operator is not supported in condition %s", cond)
}

func containsString(list []string, s string) bool {
	for _, item := range list {
		if item == s {
			return true
		}
	}
	return false
}

// checkInputCondition tells whether an input applies to the data.
func checkInputCondition(input map[string]any, d map[string]any) (bool, error) {
	cond, ok := input["Condition"]
	if !ok {
		return true, nil
	}
	return checkFormConditions(cond, d)
}

// checkOutputCondition tells whether an output runs for the data. A condition
// is only allowed on a dict-format output.
func checkOutputCondition(output map[string]any, d any) (bool, error) {
	cond, ok := output["Condition"]
	if !ok {
		return true, nil
	}
	if output["Format"] != "dict" {
		return false, fmt.Errorf("Output condition can only be set on dict-format output")
	}
	m, _ := d.(map[string]any)
	return checkFormCondition(cond, m)
}

// defString reads a string property of a form definition element.
func defString(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

// defBool reads a boolean property of a form definition element, as the
// historical collector read the forms with PyYAML (YAML 1.1): yes, true and on,
// in any case. yaml.v3 follows YAML 1.2 and leaves "Yes" or "On" as strings,
// which would silently drop `Mandatory: Yes`.
func defBool(m map[string]any, key string) bool {
	switch t := m[key].(type) {
	case bool:
		return t
	case string:
		switch strings.ToLower(t) {
		case "yes", "true", "on":
			return true
		}
	}
	return false
}

// defList reads a list property of a form definition element.
func defList(m map[string]any, key string) []any {
	l, _ := m[key].([]any)
	return l
}

// defMaps reads a list of objects of a form definition element.
func defMaps(m map[string]any, key string) []map[string]any {
	var out []map[string]any
	for _, item := range defList(m, key) {
		if sub, ok := item.(map[string]any); ok {
			out = append(out, sub)
		}
	}
	return out
}

// legacyRowShape gives a candidate row the shape the historical API returned,
// which the forms are written against: oc3 names the columns of a joined table
// with flat keys, "stor_array.id", where the historical API returned pydal's
// as_list(), plain columns when they all came from one table, `{id}`, and rows
// nested by table otherwise, `{stor_array: {id}, diskinfo: {...}}`. Anything but
// a row with such keys is returned as is.
func legacyRowShape(row any) any {
	m, ok := row.(map[string]any)
	if !ok {
		return row
	}
	tables := map[string]map[string]any{}
	plain := map[string]any{}
	for k, v := range m {
		table, column, found := strings.Cut(k, ".")
		if !found || table == "" {
			plain[k] = v
			continue
		}
		if tables[table] == nil {
			tables[table] = map[string]any{}
		}
		tables[table][column] = v
	}
	if len(tables) == 0 {
		return row
	}
	if len(tables) == 1 && len(plain) == 0 {
		for _, only := range tables {
			return only
		}
	}
	out := make(map[string]any, len(plain)+len(tables))
	for k, v := range plain {
		out[k] = v
	}
	for t, cols := range tables {
		out[t] = cols
	}
	return out
}
