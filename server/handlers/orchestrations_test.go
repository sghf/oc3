package serverhandlers

import (
	"strings"
	"testing"

	"github.com/opensvc/oc3/server"
)

func TestValidateOrchestration(t *testing.T) {
	ids := func(l ...string) *[]string { return &l }
	n := func(v int) *int { return &v }
	s := func(v string) *string { return &v }
	freeze := server.OrchestrationStep{
		Name: "freeze", Action: "freeze", Kind: server.OrchestrationTargetKindService, Pace: server.All,
		Targets: server.OrchestrationTargets{Mode: server.Pick, Ids: ids("svc1")},
	}
	cases := []struct {
		name  string
		steps []server.OrchestrationStep
		err   string
	}{
		{"valid", []server.OrchestrationStep{freeze}, ""},
		{"no step", nil, "at least one step"},
		{"action of another kind", []server.OrchestrationStep{{
			Name: "x", Action: "reboot", Kind: server.OrchestrationTargetKindService, Pace: server.All,
			Targets: server.OrchestrationTargets{Mode: server.Pick, Ids: ids("svc1")},
		}}, `step 1: action "reboot" is not accepted on a service`},
		{"rid on a service", []server.OrchestrationStep{{
			Name: "x", Action: "start", Kind: server.OrchestrationTargetKindService, Pace: server.All, Rid: s("fs#1"),
			Targets: server.OrchestrationTargets{Mode: server.Pick, Ids: ids("svc1")},
		}}, "rid applies to instances only"},
		{"instance id", []server.OrchestrationStep{{
			Name: "x", Action: "restart", Kind: server.OrchestrationTargetKindInstance, Pace: server.One,
			Targets: server.OrchestrationTargets{Mode: server.Pick, Ids: ids("svc1")},
		}}, "is not svc_id@node_id"},
		{"batch without size", []server.OrchestrationStep{{
			Name: "x", Action: "checks", Kind: server.OrchestrationTargetKindNode, Pace: server.Batch,
			Targets: server.OrchestrationTargets{Mode: server.Pick, Ids: ids("n1")},
		}}, "batch_size"},
		{"same_as of a later step", []server.OrchestrationStep{{
			Name: "x", Action: "thaw", Kind: server.OrchestrationTargetKindService, Pace: server.All,
			Targets: server.OrchestrationTargets{Mode: server.SameAs, Step: n(0)},
		}}, "names an earlier step"},
		{"same_as of another kind", []server.OrchestrationStep{freeze, {
			Name: "x", Action: "checks", Kind: server.OrchestrationTargetKindNode, Pace: server.All,
			Targets: server.OrchestrationTargets{Mode: server.SameAs, Step: n(0)},
		}}, "whose targets are not nodes"},
		{"same_as", []server.OrchestrationStep{freeze, {
			Name: "thaw", Action: "thaw", Kind: server.OrchestrationTargetKindService, Pace: server.All,
			Targets: server.OrchestrationTargets{Mode: server.SameAs, Step: n(0)},
		}}, ""},
		{"filterset without id", []server.OrchestrationStep{{
			Name: "x", Action: "freeze", Kind: server.OrchestrationTargetKindCluster, Pace: server.All,
			Targets: server.OrchestrationTargets{Mode: server.Filterset},
		}}, "no filterset chosen"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateOrchestration(server.OrchestrationInput{Name: "plan", Steps: tc.steps})
			switch {
			case tc.err == "" && err != nil:
				t.Fatalf("unexpected error: %s", err)
			case tc.err != "" && (err == nil || !strings.Contains(err.Error(), tc.err)):
				t.Fatalf("want an error with %q, got %v", tc.err, err)
			}
		})
	}
}
