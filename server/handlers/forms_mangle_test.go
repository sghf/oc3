package serverhandlers

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestRunMangler(t *testing.T) {
	data := []byte(`{"name":"web","size":12345678901234567,"tags":["a","b"]}`)
	outputs := []byte(`{"o1":{"returncode":0}}`)
	run := func(mangler string) (any, error) {
		return runMangler(context.Background(), mangler, data, outputs, 42, time.Second)
	}

	t.Run("the result replaces the data, as with vm2", func(t *testing.T) {
		out, err := run(`function(data, outputs, id) {
			return {svcname: data.name.toUpperCase(), n: data.tags.length, ret: outputs.o1.returncode, id: id};
		}`)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := json.Marshal(out)
		if string(got) != `{"id":42,"n":2,"ret":0,"svcname":"WEB"}` {
			t.Fatalf("got %s", got)
		}
	})

	t.Run("numbers come back as JSON numbers", func(t *testing.T) {
		out, err := run(`function(d) { return [d.name] }`)
		if err != nil {
			t.Fatal(err)
		}
		if got, _ := json.Marshal(out); string(got) != `["web"]` {
			t.Fatalf("got %s", got)
		}
		out, err = run(`function(d) { return {v: 1.5, i: 7} }`)
		if err != nil {
			t.Fatal(err)
		}
		m := out.(map[string]any)
		if m["v"] != json.Number("1.5") || m["i"] != json.Number("7") {
			t.Fatalf("got %#v", m)
		}
	})

	t.Run("a log does not mix with the result", func(t *testing.T) {
		out, err := run(`function(d) { console.log("seen", d.name); return {ok: true} }`)
		if err != nil {
			t.Fatal(err)
		}
		if out.(map[string]any)["ok"] != true {
			t.Fatalf("got %#v", out)
		}
	})

	t.Run("no result is an error, with what was logged", func(t *testing.T) {
		_, err := run(`function(d) { console.log("nothing to send") }`)
		if err == nil || !strings.Contains(err.Error(), "produced no data") || !strings.Contains(err.Error(), "nothing to send") {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("an exception is an error", func(t *testing.T) {
		_, err := run(`function(d) { throw new Error("bad input") }`)
		if err == nil || !strings.Contains(err.Error(), "bad input") {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("a syntax error is an error", func(t *testing.T) {
		if _, err := run(`function(d) { return {`); err == nil {
			t.Fatal("no error")
		}
	})

	t.Run("an endless loop is interrupted", func(t *testing.T) {
		begin := time.Now()
		_, err := runMangler(context.Background(), `function(d) { for (;;) {} }`, data, outputs, 1, 100*time.Millisecond)
		if err == nil || !strings.Contains(err.Error(), "interrupted") {
			t.Fatalf("got %v", err)
		}
		if elapsed := time.Since(begin); elapsed > 2*time.Second {
			t.Fatalf("interrupted after %s", elapsed)
		}
	})

	t.Run("an endless recursion is stopped", func(t *testing.T) {
		if _, err := run(`function f(d) { return f(d) }`); err == nil {
			t.Fatal("no error")
		}
	})

	t.Run("the end of the submission interrupts it", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(50*time.Millisecond, cancel)
		_, err := runMangler(ctx, `function(d) { for (;;) {} }`, data, outputs, 1, time.Minute)
		if err == nil || !strings.Contains(err.Error(), "interrupted") {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("nothing of the host is reachable", func(t *testing.T) {
		for _, name := range []string{"require", "process", "global", "module", "setTimeout", "fetch", "XMLHttpRequest", "Deno", "Bun"} {
			out, err := run(`function(d) { return {t: typeof ` + name + `} }`)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if got := out.(map[string]any)["t"]; got != "undefined" {
				t.Errorf("%s is %v", name, got)
			}
		}
		// The way out of vm2: reaching the host through a constructor.
		out, err := run(`function(d) {
			var f = this.constructor.constructor("return typeof process")();
			return {t: f};
		}`)
		if err != nil {
			t.Fatal(err)
		}
		if got := out.(map[string]any)["t"]; got != "undefined" {
			t.Errorf("process through the constructor is %v", got)
		}
	})

	t.Run("an oversized mangler is refused", func(t *testing.T) {
		big := "function(d) { return {} }" + strings.Repeat(" ", mangleMaxSource)
		if _, err := run(big); err == nil {
			t.Fatal("no error")
		}
	})
}
