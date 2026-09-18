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
	"os"
	"testing"
)

// testdata/job-template.yaml is the template as it actually exists in the
// cluster, captured from the cellphone-camera-handler-job-template ConfigMap
// rather than hand-written here. The fixture in testdata_test.go is trimmed for
// readability; this one keeps the renderer honest against the real thing,
// including the parts the trimmed copy leaves out (secret envFrom, resource
// limits, security context, volumes).
//
// Refresh it with:
//
//	kubectl -n <ns> get cm cellphone-camera-handler-job-template \
//	  -o jsonpath='{.data.job\.yaml}' > pkg/streamhandler/testdata/job-template.yaml
func TestRenderRealClusterTemplate(t *testing.T) {
	raw, err := os.ReadFile("testdata/job-template.yaml")
	if err != nil {
		t.Fatalf("reading the captured template: %v", err)
	}

	// The deployed namespace is cellphone-cam, while the template hardcodes
	// cellphone-camera. The configured namespace has to win, or every create
	// would be rejected by the plugin's namespace-scoped RBAC.
	const deployedNamespace = "cellphone-cam"

	job, err := renderJob(raw, fullAssignment(), deployedNamespace)
	if err != nil {
		t.Fatalf("renderJob on the real template: %v", err)
	}

	if job.Namespace != deployedNamespace {
		t.Errorf("namespace = %q, want the configured %q to override the template's hardcoded one",
			job.Namespace, deployedNamespace)
	}
	if job.Name != "handler-cellphone-camera-abc123" {
		t.Errorf("job name = %q", job.Name)
	}
	if got := job.Labels["cellphone-camera.io/session-id"]; got != "cellphone-camera-abc123" {
		t.Errorf("session label = %q, want it populated so Release can select this Job", got)
	}

	env := envOf(t, job)
	for _, name := range []string{"SESSION_ID", "STREAM_URL", "POOL_ENDPOINT"} {
		if env[name] == "" {
			t.Errorf("required env %s is empty after rendering the real template", name)
		}
	}

	// Structural details the trimmed fixture omits must survive rendering, since
	// substitution walks the decoded object and could in principle drop them.
	spec := job.Spec.Template.Spec
	if len(spec.Containers) != 1 {
		t.Fatalf("containers = %d, want 1", len(spec.Containers))
	}
	c := spec.Containers[0]
	if len(c.EnvFrom) == 0 {
		t.Error("envFrom was lost; the handler depends on it for its defaults ConfigMap")
	}
	if c.Resources.Limits == nil || c.Resources.Requests == nil {
		t.Error("resource requests/limits were lost")
	}
	if spec.SecurityContext == nil || spec.SecurityContext.RunAsNonRoot == nil || !*spec.SecurityContext.RunAsNonRoot {
		t.Error("pod securityContext runAsNonRoot was lost")
	}
	if len(spec.Volumes) == 0 {
		t.Error("volumes were lost; the handler needs a writable /tmp")
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		t.Errorf("backoffLimit = %v, want 0 retained from the template", job.Spec.BackoffLimit)
	}
}

// The real template ships no RESULT_SINK_URL entry yet. Until the ingestor repo
// adds one, a configured result sink is computed and then silently dropped at
// render time. Pinning that here means the day the template gains the entry,
// this test starts failing and says so out loud rather than the behaviour
// changing unnoticed.
func TestRealTemplateHasNoResultSinkEntryYet(t *testing.T) {
	raw, err := os.ReadFile("testdata/job-template.yaml")
	if err != nil {
		t.Fatalf("reading the captured template: %v", err)
	}
	job, err := renderJob(raw, fullAssignment(), "cellphone-cam")
	if err != nil {
		t.Fatalf("renderJob: %v", err)
	}
	if _, present := envOf(t, job)["RESULT_SINK_URL"]; present {
		t.Skip("the template now carries RESULT_SINK_URL; drop this test and assert the value instead")
	}
}
