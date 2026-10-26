package serverhandlers

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

// validate checks the submitted data against the inputs of the form, as
// validate_data() does: one object, or a list of objects each checked.
func (s *formSubmission) validate(ctx context.Context) error {
	switch t := s.data.(type) {
	case map[string]any:
		return s.validateData(ctx, t)
	case []any:
		for _, item := range t {
			m, ok := item.(map[string]any)
			if !ok {
				return httpErrorf(http.StatusBadRequest, "form data list entries must be objects")
			}
			if err := s.validateData(ctx, m); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *formSubmission) validateData(ctx context.Context, data map[string]any) error {
	for _, input := range defMaps(s.definition, "Inputs") {
		if err := s.validateInput(ctx, data, input); err != nil {
			return err
		}
	}
	return nil
}

// validateInput checks one input, as validate_input_data() does: mandatory
// values, forced keys, strict static candidates and strict dynamic candidates,
// the last ones fetched from the API as the submitter.
func (s *formSubmission) validateInput(ctx context.Context, data map[string]any, input map[string]any) error {
	inputID := defString(input, "Id")
	keyID := defString(input, "Key")
	lookup := inputID
	if keyID != "" {
		lookup = keyID
	}
	val := data[lookup]

	applies, err := checkInputCondition(input, data)
	if err != nil {
		return httpErrorf(http.StatusBadRequest, "%s", err)
	}
	if !applies {
		return nil
	}
	if val == nil {
		if defBool(input, "Mandatory") {
			return httpErrorf(http.StatusBadRequest, "Missing value for mandatory input '%s'", inputID)
		}
		return nil
	}

	vals, isList := val.([]any)
	if !isList {
		vals = []any{val}
	}

	// Forced keys: "key=value" entries the submitted data must carry as defined.
	var keyDefs []string
	for _, k := range defList(input, "Keys") {
		if ks, ok := k.(string); ok {
			keyDefs = append(keyDefs, ks)
		}
	}
	refData := []map[string]any{data}
	if isList {
		refData = nil
		for _, item := range vals {
			if m, ok := item.(map[string]any); ok {
				refData = append(refData, m)
			}
		}
	}
	for _, ref := range refData {
		for _, keyDef := range keyDefs {
			key, forced, found := strings.Cut(keyDef, "=")
			if !found {
				continue
			}
			key = strings.TrimSpace(key)
			forced = formDereference(strings.TrimSpace(forced), ref, "")
			refVal, ok := ref[key]
			if !ok {
				return httpErrorf(http.StatusBadRequest, "missing key '%s', from input %s", key, inputID)
			}
			if !strings.Contains(forced, "#") && forced != formValueText(refVal) {
				return httpErrorf(http.StatusBadRequest, "unallowed key value '%s=%s', expecting '%s', from input %s",
					key, formValueText(refVal), forced, inputID)
			}
		}
	}

	strict := defBool(input, "StrictCandidates")
	mandatory := defBool(input, "Mandatory")

	// Strict static candidates.
	if candidates := defList(input, "Candidates"); len(candidates) > 0 && strict {
		var allowed []string
		for _, candidate := range candidates {
			if m, ok := candidate.(map[string]any); ok {
				if v, ok := m["Value"]; ok {
					allowed = append(allowed, formValueText(v))
					continue
				}
			}
			allowed = append(allowed, formValueText(candidate))
		}
		for _, v := range vals {
			if isFalsy(v) && !mandatory {
				continue
			}
			if !containsString(allowed, formValueText(v)) {
				return httpErrorf(http.StatusBadRequest, "Input '%s' value '%s' not in allowed candidates", inputID, formValueText(v))
			}
		}
	}

	// Strict dynamic candidates: the key to compare is a forced key naming the
	// input, else the input Value property.
	var key string
	for _, keyDef := range keyDefs {
		k, v, found := strings.Cut(keyDef, "=")
		if found && strings.TrimSpace(k) == inputID {
			key = strings.TrimSpace(v)
		}
	}
	if key == "" {
		key = defString(input, "Value")
	}
	fn := defString(input, "Function")
	if fn == "" || key == "" || !strict {
		return nil
	}
	if defString(input, "Connector") != "" {
		return s.validateConnectorCandidates(ctx, input, data, inputID, key, vals, mandatory)
	}
	// The references are replaced by formRestArgs, each value escaped as a path
	// segment of its own: not here, where a "/" in a value would add segments.
	if !strings.HasPrefix(fn, "/") {
		return nil
	}
	key = strings.TrimLeft(key, "#")
	for _, v := range vals {
		args, err := formRestArgs(fn, data)
		if err != nil {
			return httpErrorf(http.StatusBadRequest, "cannot build the candidates url %s: missing %s", fn, err)
		}
		kwargs := map[string]any{}
		for _, entry := range defList(input, "Args") {
			es, ok := entry.(string)
			if !ok {
				continue
			}
			es = formDereference(es, data, "")
			k, kv, found := strings.Cut(es, "=")
			if !found {
				continue
			}
			kwargs[strings.TrimSpace(k)] = formDereference(strings.TrimSpace(kv), data, "")
		}
		keyVal := v
		if m, ok := v.(map[string]any); ok {
			keyVal = m[key]
		}
		kwargs["limit"] = 0
		kwargs["search"] = formValueText(keyVal)
		kwargs["search_props"] = key
		path := "/" + strings.Join(args, "/")
		resp, err := s.callAPI(ctx, http.MethodGet, path, kwargs)
		if err != nil {
			return httpErrorf(http.StatusBadRequest, "cannot verify the submitted value is a valid candidate: %s", err)
		}
		if resp.status == http.StatusNotFound {
			return httpErrorf(http.StatusBadRequest, "Unknown handler '%s': can not verify the submitted value is a valid candidates", fn)
		}
		if resp.status >= 300 {
			return httpErrorf(http.StatusBadRequest, "cannot verify the submitted value is a valid candidate: %s", problemTextOf(resp))
		}
		var candidates []string
		if m, ok := resp.body.(map[string]any); ok {
			list, _ := m["data"].([]any)
			for _, candidate := range list {
				cv, err := formGetVal(legacyRowShape(candidate), key)
				if err != nil {
					return httpErrorf(http.StatusBadRequest, "Key '%s' not in candidates", key)
				}
				candidates = append(candidates, formValueText(cv))
			}
		}
		if isFalsy(keyVal) && !mandatory {
			continue
		}
		if !containsString(candidates, formValueText(keyVal)) {
			return httpErrorf(http.StatusBadRequest, "Input '%s' value '%s' not in allowed candidates %s obtained from %s",
				inputID, formValueText(keyVal), fmt.Sprint(candidates), path)
		}
	}
	return nil
}

// isFalsy tells whether a value is false for python: empty or zero.
func isFalsy(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case string:
		return t == ""
	case bool:
		return !t
	case []any:
		return len(t) == 0
	case map[string]any:
		return len(t) == 0
	}
	return formValueText(v) == "0"
}
