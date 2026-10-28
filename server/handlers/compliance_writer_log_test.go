package serverhandlers

import "testing"

func TestLoggedVariableFields(t *testing.T) {
	fields := map[string]any{"var_name": "app_conf", "var_class": "raw", "var_value": `{"db_password":"s3cret"}`}
	logged := loggedVariableFields(fields)
	if logged["var_value"] != "(hidden, 24 characters)" {
		t.Errorf("var_value logged as %v", logged["var_value"])
	}
	if logged["var_name"] != "app_conf" || logged["var_class"] != "raw" {
		t.Errorf("other properties lost: %v", logged)
	}
	if fields["var_value"] != `{"db_password":"s3cret"}` {
		t.Error("the fields written were changed")
	}
	if _, ok := loggedVariableFields(map[string]any{"var_name": "x"})["var_value"]; ok {
		t.Error("a change without a value logs one")
	}
}
