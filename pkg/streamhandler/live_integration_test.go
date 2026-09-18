//go:build integration

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

// This file is behind the "integration" build tag because it talks to a real
// cluster and creates real objects. `go test ./...` never runs it.
//
//	KUBECONFIG=~/.kube/test-single.config \
//	  go test -tags integration ./pkg/streamhandler/ -run TestLive -v
//
// The client impersonates the EPP's ServiceAccount rather than using the
// kubeconfig's own identity, so the test exercises the RBAC the plugin will
// actually run under instead of an admin's ambient power.
package streamhandler

import (
	"context"
	"os"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	liveNamespace = "cellphone-cam"
	liveEPPUser   = "system:serviceaccount:llm-d-quickstart:llm-d-quickstart-epp"
	// Deliberately recognisable, and short enough to stay inside the pod-name
	// budget so a failure here is never about length.
	liveSessionID = "streamhandler-livetest"
)

func liveClient(t *testing.T) kubernetes.Interface {
	t.Helper()
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		t.Skip("KUBECONFIG is not set; skipping live cluster test")
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatalf("loading kubeconfig: %v", err)
	}
	// Run as the EPP would, so RBAC is part of what is under test.
	cfg.Impersonate = rest.ImpersonationConfig{UserName: liveEPPUser}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("building client: %v", err)
	}
	return client
}

// TestLiveAcquireAndRelease drives the real provisioner against the real API
// server: read the real job template ConfigMap, render it, create the Job, then
// delete it again through Release.
func TestLiveAcquireAndRelease(t *testing.T) {
	client := liveClient(t)

	params := testParams(t)
	params.Namespace = liveNamespace
	p := NewJobProvisioner(client, params, defaultTemplateCacheTTL)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	assignment := Assignment{
		SessionID: liveSessionID,
		// Unroutable on purpose: if this Job ever starts a pod, the handler must
		// fail fast on its stream rather than sit consuming a real camera.
		StreamURL:     "http://192.0.2.1:4747/video",
		PoolEndpoint:  "http://10.0.0.1:8000",
		FrameInterval: "60",
	}

	// Belt and braces: reap any leftover from an interrupted earlier run, and
	// guarantee cleanup even if an assertion below fails.
	_ = p.Release(ctx, liveSessionID)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := p.Release(cleanupCtx, liveSessionID); err != nil {
			t.Errorf("cleanup Release failed, a Job may be left behind: %v", err)
		}
	})

	if err := p.Acquire(ctx, assignment); err != nil {
		t.Fatalf("Acquire against the live cluster: %v", err)
	}

	job, err := client.BatchV1().Jobs(liveNamespace).Get(ctx, "handler-"+liveSessionID, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting the created Job: %v", err)
	}
	t.Logf("created Job %s/%s", job.Namespace, job.Name)

	if got := job.Labels[params.SessionLabel]; got != liveSessionID {
		t.Errorf("session label = %q, want %q", got, liveSessionID)
	}
	env := map[string]string{}
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		env[e.Name] = e.Value
	}
	if env["POOL_ENDPOINT"] != assignment.PoolEndpoint {
		t.Errorf("POOL_ENDPOINT = %q, want %q", env["POOL_ENDPOINT"], assignment.PoolEndpoint)
	}
	if env["STREAM_URL"] != assignment.StreamURL {
		t.Errorf("STREAM_URL = %q, want %q", env["STREAM_URL"], assignment.StreamURL)
	}
	// PROMPT was not set on the assignment, so the entry must be absent rather
	// than blank, letting the handler's defaults ConfigMap supply it.
	if v, present := env["PROMPT"]; present {
		t.Errorf("PROMPT is present as %q, want the entry dropped so envFrom defaults apply", v)
	}

	// A second Acquire for the same session must be a no-op, not a conflict.
	if err := p.Acquire(ctx, assignment); err != nil {
		t.Errorf("second Acquire should be idempotent, got: %v", err)
	}

	if err := p.Release(ctx, liveSessionID); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, err := client.BatchV1().Jobs(liveNamespace).Get(ctx, "handler-"+liveSessionID, metav1.GetOptions{}); err == nil {
		t.Error("Job still exists after Release")
	}
	t.Log("Job released")

	// Releasing a session that is already gone must stay quiet.
	if err := p.Release(ctx, liveSessionID); err != nil {
		t.Errorf("Release of an already-released session = %v, want nil", err)
	}
}
