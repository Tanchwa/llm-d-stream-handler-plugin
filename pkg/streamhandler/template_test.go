/*
Copyright 2026 The llm-d-stream-handler-plugin Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package streamhandler

import (
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
)

// envOf flattens the rendered container env into a map for easy assertions.
func envOf(t *testing.T, job *batchv1.Job) map[string]string {
	t.Helper()
	containers := job.Spec.Template.Spec.Containers
	if len(containers) != 1 {
		t.Fatalf("got %d containers, want 1", len(containers))
	}
	out := map[string]string{}
	for _, e := range containers[0].Env {
		out[e.Name] = e.Value
	}
	return out
}

func fullAssignment() Assignment {
	return Assignment{
		SessionID:     "cellphone-camera-abc123",
		StreamURL:     "http://192.168.1.42:4747/video",
		PoolEndpoint:  "http://10.1.2.3:8000",
		Prompt:        "What is happening?",
		FrameInterval: "1.5",
		ResultSinkURL: "http://frontend/ingest/cellphone-camera-abc123",
	}
}

func TestRenderJobFillsRequiredFields(t *testing.T) {
	job, err := renderJob([]byte(jobTemplate), fullAssignment(), "cellphone-camera")
	if err != nil {
		t.Fatalf("renderJob: %v", err)
	}

	if got, want := job.Name, "handler-cellphone-camera-abc123"; got != want {
		t.Errorf("job name = %q, want %q", got, want)
	}
	if got, want := job.Labels["cellphone-camera.io/session-id"], "cellphone-camera-abc123"; got != want {
		t.Errorf("job session label = %q, want %q", got, want)
	}
	// The pod template label is what a human greps for when debugging a session.
	if got, want := job.Spec.Template.Labels["cellphone-camera.io/session-id"], "cellphone-camera-abc123"; got != want {
		t.Errorf("pod session label = %q, want %q", got, want)
	}

	env := envOf(t, job)
	for name, want := range map[string]string{
		"SESSION_ID":      "cellphone-camera-abc123",
		"STREAM_URL":      "http://192.168.1.42:4747/video",
		"POOL_ENDPOINT":   "http://10.1.2.3:8000",
		"PROMPT":          "What is happening?",
		"FRAME_INTERVAL":  "1.5",
		"RESULT_SINK_URL": "http://frontend/ingest/cellphone-camera-abc123",
	} {
		if env[name] != want {
			t.Errorf("env %s = %q, want %q", name, env[name], want)
		}
	}
}

// The configured namespace has to win: the plugin's RBAC is namespace-scoped, so
// honouring a template that names somewhere else would just produce a forbidden
// error at create time.
func TestRenderJobOverridesTemplateNamespace(t *testing.T) {
	job, err := renderJob([]byte(jobTemplate), fullAssignment(), "somewhere-else")
	if err != nil {
		t.Fatalf("renderJob: %v", err)
	}
	if got, want := job.Namespace, "somewhere-else"; got != want {
		t.Errorf("namespace = %q, want %q", got, want)
	}
}

// Unset optional values must remove the env entry rather than set it empty. The
// entries are overrides on top of an envFrom ConfigMap of defaults, so a blank
// PROMPT would override the default with nothing, and a blank FRAME_INTERVAL
// would crash the handler on float("").
func TestRenderJobDropsUnsetOptionalEnv(t *testing.T) {
	a := fullAssignment()
	a.Prompt = ""
	a.FrameInterval = ""
	a.ResultSinkURL = ""

	job, err := renderJob([]byte(jobTemplate), a, "cellphone-camera")
	if err != nil {
		t.Fatalf("renderJob: %v", err)
	}

	env := envOf(t, job)
	for _, name := range []string{"PROMPT", "FRAME_INTERVAL", "RESULT_SINK_URL"} {
		if v, present := env[name]; present {
			t.Errorf("env %s is present with value %q, want the entry dropped entirely", name, v)
		}
	}
	// The required entries must survive the pruning untouched.
	if env["SESSION_ID"] == "" || env["STREAM_URL"] == "" || env["POOL_ENDPOINT"] == "" {
		t.Errorf("pruning removed a required env entry: %#v", env)
	}
}

// A camera URL is arbitrary text from a browser field. Substituting it into the
// YAML source before parsing would let quotes and newlines rewrite the pod spec;
// substituting into the decoded object cannot, no matter what the value contains.
func TestRenderJobIsNotTemplateInjectable(t *testing.T) {
	a := fullAssignment()
	a.StreamURL = "http://evil\"\n              - name: INJECTED\n                value: \"pwned"

	job, err := renderJob([]byte(jobTemplate), a, "cellphone-camera")
	if err != nil {
		t.Fatalf("renderJob: %v", err)
	}

	env := envOf(t, job)
	if _, injected := env["INJECTED"]; injected {
		t.Fatal("a crafted stream URL introduced a new env var; substitution is not injection-safe")
	}
	if env["STREAM_URL"] != a.StreamURL {
		t.Errorf("STREAM_URL = %q, want the value carried through verbatim as a single leaf string", env["STREAM_URL"])
	}
	if got := len(job.Spec.Template.Spec.Containers); got != 1 {
		t.Errorf("container count = %d, want the spec structurally unchanged at 1", got)
	}
}

// A session label that cannot be resolved would produce a Job that Release can
// never select, stranding a running handler until its backstop fires. Better to
// refuse to create it.
func TestRenderJobRejectsUnresolvableLabel(t *testing.T) {
	a := fullAssignment()
	a.SessionID = ""

	_, err := renderJob([]byte(jobTemplate), a, "cellphone-camera")
	if err == nil {
		t.Fatal("renderJob succeeded with an empty session id, want an error")
	}
	if !strings.Contains(err.Error(), "placeholder") {
		t.Errorf("error = %v, want it to name the unset placeholder", err)
	}
}

func TestRenderJobRejectsMalformedTemplate(t *testing.T) {
	for name, raw := range map[string]string{
		"not yaml":      "{{ this is not yaml",
		"wrong kind":    "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: nope\n",
		"unknown field": "apiVersion: batch/v1\nkind: Job\nmetadata:\n  name: x\nspec:\n  notAField: true\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := renderJob([]byte(raw), fullAssignment(), "cellphone-camera"); err == nil {
				t.Fatalf("renderJob succeeded on a %s template, want an error", name)
			}
		})
	}
}

func TestSubstitute(t *testing.T) {
	values := map[string]string{"A": "1", "B": "2", "EMPTY": ""}
	tests := []struct {
		in       string
		want     string
		resolved bool
	}{
		{"no placeholders", "no placeholders", true},
		{"${A}", "1", true},
		{"${A}-${B}", "1-2", true},
		{"prefix-${A}-suffix", "prefix-1-suffix", true},
		{"${MISSING}", "", false},
		{"${EMPTY}", "", false},
		{"${A}-${MISSING}", "", false},
		// A lone dollar or an unclosed brace is literal text, not a placeholder.
		{"$A", "$A", true},
		{"${unclosed", "${unclosed", true},
	}
	for _, tc := range tests {
		got, resolved := substitute(tc.in, values)
		if resolved != tc.resolved {
			t.Errorf("substitute(%q) resolved = %v, want %v", tc.in, resolved, tc.resolved)
			continue
		}
		if resolved && got != tc.want {
			t.Errorf("substitute(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
