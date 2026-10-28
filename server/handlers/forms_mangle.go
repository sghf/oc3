package serverhandlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dop251/goja"
)

const (
	// mangleMaxSource bounds the javascript of a mangler, as written in a form.
	mangleMaxSource = 256 << 10
	// mangleMaxResult bounds the JSON a mangler returns, the data then sent.
	mangleMaxResult = 10 << 20
	// mangleMaxLog bounds what a mangler prints, kept for the error message.
	mangleMaxLog = 64 << 10
)

// runMangler runs the mangler of a rest output: a javascript function of the
// form definition, called as mangle(data, outputs, results_id) as by the
// historical collector, whose result, as JSON, replaces the data sent.
//
// It runs in goja, an interpreter inside oc3, rather than in the vm2 sandbox
// of nodejs, abandoned and known to be escapable: the script sees the
// ECMAScript built-ins and a console whose log is kept for the error message,
// and nothing of the host, no require, process, file, network or timer. It is
// interrupted after timeout, or when ctx ends. Its memory is not bounded
// otherwise: the timeout limits what it can allocate.
func runMangler(ctx context.Context, mangler string, data, outputs []byte, resultsID int64, timeout time.Duration) (any, error) {
	if len(mangler) > mangleMaxSource {
		return nil, fmt.Errorf("the mangler exceeds %d bytes", mangleMaxSource)
	}
	vm := goja.New()
	vm.SetMaxCallStackSize(1024)

	var logs strings.Builder
	console := vm.NewObject()
	if err := console.Set("log", func(call goja.FunctionCall) goja.Value {
		parts := make([]string, len(call.Arguments))
		for i, arg := range call.Arguments {
			parts[i] = arg.String()
		}
		if logs.Len() < mangleMaxLog {
			logs.WriteString(strings.Join(parts, " "))
			logs.WriteString("\n")
		}
		return goja.Undefined()
	}); err != nil {
		return nil, err
	}
	for name, value := range map[string]any{
		"console":          console,
		"__oc3_data":       string(data),
		"__oc3_outputs":    string(outputs),
		"__oc3_results_id": resultsID,
	} {
		if err := vm.Set(name, value); err != nil {
			return nil, err
		}
	}

	timer := time.AfterFunc(timeout, func() { vm.Interrupt("the mangler ran longer than " + timeout.String()) })
	defer timer.Stop()
	stop := context.AfterFunc(ctx, func() { vm.Interrupt("the form submission ended") })
	defer stop()

	// The data reach the script as JSON parsed inside it, plain javascript
	// values as nodejs gave them, rather than Go values wrapped by goja.
	script := "var mangle = " + mangler + ";\n" +
		"JSON.stringify(mangle(JSON.parse(__oc3_data), JSON.parse(__oc3_outputs), __oc3_results_id));"
	value, err := vm.RunString(script)
	if err != nil {
		var interrupted *goja.InterruptedError
		if errors.As(err, &interrupted) {
			return nil, fmt.Errorf("mangling interrupted: %v", interrupted.Value())
		}
		return nil, fmt.Errorf("mangling failed: %s%s", err, printed(logs.String()))
	}
	text, ok := value.Export().(string)
	if !ok || text == "" {
		return nil, fmt.Errorf("mangling produced no data:\nmangler: %s%s", mangler, printed(logs.String()))
	}
	if len(text) > mangleMaxResult {
		return nil, fmt.Errorf("the mangled data exceed %d bytes", mangleMaxResult)
	}
	var out any
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	if err := decoder.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// printed is what a mangler logged, as a line of an error message.
func printed(logs string) string {
	if logs == "" {
		return ""
	}
	return "\noutput: " + logs
}
