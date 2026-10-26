package serverhandlers

import (
	"encoding/json"
	"net/url"
	"reflect"
	"testing"

	"github.com/spf13/viper"
)

func TestCheckFormCondition(t *testing.T) {
	d := map[string]any{
		"os":    "linux",
		"size":  "12",
		"count": json.Number("3"),
		"blank": "",
		"undef": "undefined",
	}
	cases := []struct {
		cond string
		want bool
	}{
		{"", true},
		{"none", true},
		{"#os == linux", true},
		{"#os == aix", false},
		{"#missing == linux", false},
		{"#count == 3", false}, // a number never equals a string, as in python
		{"#blank == empty", true},
		{"#undef == empty", true},
		{"#missing == empty", true},
		{"#os == empty", false},
		{"#os != aix", true},
		{"#os != linux", false},
		{"#missing != linux", true},
		{"#count != 3", true},
		{"#os != empty", true},
		{"#blank != empty", false},
		{"#os IN aix,linux", true},
		{"#os IN aix,sunos", false},
		{"#missing IN linux", false},
		{"#os NOT IN aix,sunos", true},
		{"#os NOT IN aix,linux", false},
		{"#missing NOT IN linux", true},
		{"#size > 10", true},
		{"#size > 12", false},
		{"#size < 13", true},
		{"#count > 2", true},
		{"#os > 2", false},
	}
	for _, c := range cases {
		got, err := checkFormCondition(c.cond, d)
		if err != nil {
			t.Errorf("%q: unexpected error %s", c.cond, err)
			continue
		}
		if got != c.want {
			t.Errorf("%q: got %v, want %v", c.cond, got, c.want)
		}
	}
	for _, bad := range []any{nil, "os == linux", "#os ~ linux"} {
		if _, err := checkFormCondition(bad, d); err == nil {
			t.Errorf("%v: expected an error", bad)
		}
	}
}

func TestCheckFormConditionsList(t *testing.T) {
	d := map[string]any{"os": "linux", "arch": "x86_64"}
	ok, err := checkFormConditions([]any{"#os == linux", "#arch == x86_64"}, d)
	if err != nil || !ok {
		t.Errorf("all conditions hold: got %v %v", ok, err)
	}
	ok, err = checkFormConditions([]any{"#os == linux", "#arch == ppc"}, d)
	if err != nil || ok {
		t.Errorf("one condition fails: got %v %v", ok, err)
	}
}

func TestFormDereference(t *testing.T) {
	data := map[string]any{
		"name":  "db1",
		"names": "db1,db2",
		"array": map[string]any{"id": json.Number("554")},
	}
	got := formDereference("#names for #name on #array.id", data, "")
	if want := "db1,db2 for db1 on 554"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestFormRestArgs(t *testing.T) {
	data := map[string]any{"id": json.Number("554"), "dg": map[string]any{"id": "2525"}}
	got, err := formRestArgs("/arrays/#id/diskgroups/#dg.id/quotas/", data)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"arrays", "554", "diskgroups", "2525", "quotas"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if _, err := formRestArgs("/arrays/#missing", data); err == nil {
		t.Error("a missing reference must fail")
	}
}

// TestFormRestArgsEscapesValues checks a value stays one path segment: a "/",
// "?", "#" or "%" in it is escaped, a dot segment or an empty value refused.
func TestFormRestArgsEscapesValues(t *testing.T) {
	for _, c := range []struct {
		url  string
		data map[string]any
		want []string
	}{
		{"/arrays/#id/disks", map[string]any{"id": "../../users"}, []string{"arrays", "..%2F..%2Fusers", "disks"}},
		{"/services/#svc/disks", map[string]any{"svc": "dev/svc/oc3"}, []string{"services", "dev%2Fsvc%2Foc3", "disks"}},
		{"/apps/#id/quotas", map[string]any{"id": "1?props=all#x"}, []string{"apps", "1%3Fprops=all%23x", "quotas"}},
		{"/apps/#id/quotas", map[string]any{"id": "%2e%2e"}, []string{"apps", "%252e%252e", "quotas"}},
		{"/nodes/node-#id/targets", map[string]any{"id": "a/b"}, []string{"nodes", "node-a%2Fb", "targets"}},
		{"https://cmdb.example/api/#id", map[string]any{"id": "x/../y"}, []string{"https:", "cmdb.example", "api", "x%2F..%2Fy"}},
	} {
		got, err := formRestArgs(c.url, c.data)
		if err != nil || !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s %v: got %v %v, want %v", c.url, c.data, got, err, c.want)
		}
	}
	for _, c := range []struct {
		url  string
		data map[string]any
	}{
		{"/apps/#id/quotas", map[string]any{"id": ".."}},
		{"/apps/#id/quotas", map[string]any{"id": "."}},
		{"/apps/#id/quotas", map[string]any{"id": ""}},
		{"/nodes/node-#id/targets", map[string]any{}},
	} {
		if got, err := formRestArgs(c.url, c.data); err == nil {
			t.Errorf("%s %v: got %v, want a refusal", c.url, c.data, got)
		}
	}
}

func TestFormsExternalHostAllowed(t *testing.T) {
	viper.Set("server.forms.external_hosts", []string{"cmdb.example", "itsm.example:8443"})
	defer viper.Set("server.forms.external_hosts", []string{})
	for raw, want := range map[string]bool{
		"https://cmdb.example/api":      true,
		"https://CMDB.example:443/api":  true,
		"https://itsm.example:8443/x":   true,
		"https://itsm.example/x":        false,
		"http://169.254.169.254/latest": false,
		"https://cmdb.example.evil/api": false,
	} {
		u, _ := url.Parse(raw)
		if got := formsExternalHostAllowed(u); got != want {
			t.Errorf("%s: %v, want %v", raw, got, want)
		}
	}
}

func TestCheckOutputCondition(t *testing.T) {
	d := map[string]any{"os": "linux"}
	if _, err := checkOutputCondition(map[string]any{"Condition": "#os == linux"}, d); err == nil {
		t.Error("a condition on a non-dict output must fail")
	}
	ok, err := checkOutputCondition(map[string]any{"Condition": "#os == linux", "Format": "dict"}, d)
	if err != nil || !ok {
		t.Errorf("got %v %v", ok, err)
	}
	ok, err = checkOutputCondition(map[string]any{}, d)
	if err != nil || !ok {
		t.Errorf("no condition: got %v %v", ok, err)
	}
}

func TestInternalForms(t *testing.T) {
	for id := int64(-1); id >= -10; id-- {
		f, ok := internalFormByID(id)
		if !ok {
			t.Errorf("internal form %d missing", id)
			continue
		}
		if f.Name == "" || len(defMaps(f.Definition, "Outputs")) == 0 && id > -9 {
			t.Errorf("internal form %d: incomplete %+v", id, f)
		}
	}
	if _, ok := internalFormByID(-11); ok {
		t.Error("internal form -11 must not exist")
	}
}

// The forms were read with PyYAML: Yes, True and On are booleans, in any case.
func TestDefBool(t *testing.T) {
	for v, want := range map[any]bool{
		true: true, false: false, "Yes": true, "yes": true, "YES": true, "True": true,
		"true": true, "On": true, "No": false, "False": false, "": false, "1": false,
	} {
		if got := defBool(map[string]any{"Mandatory": v}, "Mandatory"); got != want {
			t.Errorf("defBool(%#v) = %v, want %v", v, got, want)
		}
	}
	if defBool(map[string]any{}, "Mandatory") {
		t.Error("a missing key is false")
	}
}

// A candidate row from oc3, flat keys of joined tables, reads as the historical
// API gave it: one table as plain columns, several nested by table.
func TestLegacyRowShape(t *testing.T) {
	one := legacyRowShape(map[string]any{"stor_array.id": 5, "stor_array.array_name": "a"})
	if v, err := formGetVal(one, "id"); err != nil || v != 5 {
		t.Errorf("single table: got %v %v", v, err)
	}
	if v, err := formGetVal(one, "array_name"); err != nil || v != "a" {
		t.Errorf("single table: got %v %v", v, err)
	}
	several := legacyRowShape(map[string]any{"diskinfo.disk_id": "d1", "stor_array.array_name": "a", "x": 1})
	if v, err := formGetVal(several, "diskinfo.disk_id"); err != nil || v != "d1" {
		t.Errorf("several tables: got %v %v", v, err)
	}
	if v, err := formGetVal(several, "x"); err != nil || v != 1 {
		t.Errorf("plain column: got %v %v", v, err)
	}
	if v, err := formGetVal(map[string]any{"a.b": 2}, "a.b"); err != nil || v != 2 {
		t.Errorf("flat key: got %v %v", v, err)
	}
	if got := legacyRowShape("text"); got != "text" {
		t.Errorf("a non row: got %v", got)
	}
}
