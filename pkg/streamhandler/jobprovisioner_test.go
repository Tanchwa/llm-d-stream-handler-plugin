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
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func templateConfigMap(params Parameters, data string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      params.JobTemplateConfigMap,
			Namespace: params.Namespace,
		},
		Data: map[string]string{params.JobTemplateKey: data},
	}
}

func newProvisioner(t *testing.T, objects ...any) (*JobProvisioner, *fake.Clientset, Parameters) {
	t.Helper()
	params := testParams(t)

	runtimeObjs := []any{}
	runtimeObjs = append(runtimeObjs, objects...)

	client := fake.NewSimpleClientset()
	for _, o := range runtimeObjs {
		switch v := o.(type) {
		case *corev1.ConfigMap:
			if _, err := client.CoreV1().ConfigMaps(v.Namespace).Create(context.Background(), v, metav1.CreateOptions{}); err != nil {
				t.Fatalf("seeding ConfigMap: %v", err)
			}
		default:
			t.Fatalf("unsupported seed object %T", o)
		}
	}
	return NewJobProvisioner(client, params, defaultTemplateCacheTTL), client, params
}

func TestAcquireCreatesJob(t *testing.T) {
	params := testParams(t)
	p, client, _ := newProvisioner(t, templateConfigMap(params, jobTemplate))

	if err := p.Acquire(context.Background(), fullAssignment()); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	jobs, err := client.BatchV1().Jobs(params.Namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing jobs: %v", err)
	}
	if len(jobs.Items) != 1 {
		t.Fatalf("got %d jobs, want 1", len(jobs.Items))
	}
	job := jobs.Items[0]
	if job.Name != "handler-cellphone-camera-abc123" {
		t.Errorf("job name = %q", job.Name)
	}
	if job.Labels[params.SessionLabel] != "cellphone-camera-abc123" {
		t.Errorf("session label = %q, want it set so Release can find this Job", job.Labels[params.SessionLabel])
	}
}

// A retried trigger, or a user who hits Start twice, must not turn into an error
// for a session that already has exactly the handler it needs.
func TestAcquireIsIdempotent(t *testing.T) {
	params := testParams(t)
	p, client, _ := newProvisioner(t, templateConfigMap(params, jobTemplate))

	for i := range 3 {
		if err := p.Acquire(context.Background(), fullAssignment()); err != nil {
			t.Fatalf("Acquire call %d: %v", i+1, err)
		}
	}

	jobs, _ := client.BatchV1().Jobs(params.Namespace).List(context.Background(), metav1.ListOptions{})
	if len(jobs.Items) != 1 {
		t.Errorf("got %d jobs after three Acquires, want 1", len(jobs.Items))
	}
}

func TestAcquireReportsTemplateProblems(t *testing.T) {
	params := testParams(t)

	t.Run("configmap missing", func(t *testing.T) {
		p, _, _ := newProvisioner(t)
		err := p.Acquire(context.Background(), fullAssignment())
		if err == nil || !strings.Contains(err.Error(), "job template ConfigMap") {
			t.Fatalf("err = %v, want it to name the missing ConfigMap", err)
		}
	})

	t.Run("key missing", func(t *testing.T) {
		cm := templateConfigMap(params, jobTemplate)
		cm.Data = map[string]string{"some-other-key": jobTemplate}
		p, _, _ := newProvisioner(t, cm)
		err := p.Acquire(context.Background(), fullAssignment())
		if err == nil || !strings.Contains(err.Error(), "has no key") {
			t.Fatalf("err = %v, want it to name the missing key", err)
		}
	})
}

func TestReleaseDeletesBySessionLabel(t *testing.T) {
	params := testParams(t)
	p, client, _ := newProvisioner(t, templateConfigMap(params, jobTemplate))

	// Two sessions running side by side; only one is being stopped.
	first := fullAssignment()
	second := fullAssignment()
	second.SessionID = "cellphone-camera-other1"
	for _, a := range []Assignment{first, second} {
		if err := p.Acquire(context.Background(), a); err != nil {
			t.Fatalf("Acquire %s: %v", a.SessionID, err)
		}
	}

	if err := p.Release(context.Background(), first.SessionID); err != nil {
		t.Fatalf("Release: %v", err)
	}

	jobs, _ := client.BatchV1().Jobs(params.Namespace).List(context.Background(), metav1.ListOptions{})
	if len(jobs.Items) != 1 {
		t.Fatalf("got %d jobs after releasing one of two sessions, want 1", len(jobs.Items))
	}
	if got := jobs.Items[0].Labels[params.SessionLabel]; got != second.SessionID {
		t.Errorf("surviving job belongs to session %q, want %q -- the wrong session was reaped", got, second.SessionID)
	}
}

// Orphaning the pod is worse than not reaping at all, so the delete must cascade.
func TestReleaseDeletesInBackground(t *testing.T) {
	params := testParams(t)
	p, client, _ := newProvisioner(t, templateConfigMap(params, jobTemplate))
	if err := p.Acquire(context.Background(), fullAssignment()); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	client.ClearActions()
	if err := p.Release(context.Background(), fullAssignment().SessionID); err != nil {
		t.Fatalf("Release: %v", err)
	}

	var deletes int
	for _, action := range client.Actions() {
		d, ok := action.(k8stesting.DeleteActionImpl)
		if !ok {
			continue
		}
		deletes++
		if d.DeleteOptions.PropagationPolicy == nil || *d.DeleteOptions.PropagationPolicy != metav1.DeletePropagationBackground {
			t.Errorf("delete propagation = %v, want Background so the handler pod goes with the Job", d.DeleteOptions.PropagationPolicy)
		}
	}
	if deletes != 1 {
		t.Errorf("got %d deletes, want 1", deletes)
	}
}

// A stop for a session that already ended is the outcome the caller wanted.
func TestReleaseIsQuietWhenNothingMatches(t *testing.T) {
	params := testParams(t)
	p, _, _ := newProvisioner(t, templateConfigMap(params, jobTemplate))
	if err := p.Release(context.Background(), "cellphone-camera-ghost1"); err != nil {
		t.Errorf("Release of an unknown session = %v, want nil", err)
	}
}

// The cache keeps a burst of sessions from becoming a burst of API reads, while
// the TTL preserves the documented behaviour that editing the template ConfigMap
// takes effect for the next session without a restart.
func TestTemplateIsCachedUntilTTLExpires(t *testing.T) {
	params := testParams(t)
	p, client, _ := newProvisioner(t, templateConfigMap(params, jobTemplate))

	now := time.Now()
	p.now = func() time.Time { return now }
	p.ttl = 30 * time.Second

	countGets := func() int {
		n := 0
		for _, a := range client.Actions() {
			if a.Matches("get", "configmaps") {
				n++
			}
		}
		return n
	}

	a := fullAssignment()
	for i := range 3 {
		a.SessionID = "cellphone-camera-sess" + string(rune('a'+i))
		if err := p.Acquire(context.Background(), a); err != nil {
			t.Fatalf("Acquire %d: %v", i, err)
		}
	}
	if got := countGets(); got != 1 {
		t.Errorf("three Acquires within the TTL made %d ConfigMap reads, want 1", got)
	}

	now = now.Add(31 * time.Second)
	a.SessionID = "cellphone-camera-sessz"
	if err := p.Acquire(context.Background(), a); err != nil {
		t.Fatalf("Acquire after TTL: %v", err)
	}
	if got := countGets(); got != 2 {
		t.Errorf("made %d ConfigMap reads after the TTL expired, want 2", got)
	}
}

func TestTemplateCacheCanBeDisabled(t *testing.T) {
	params := testParams(t)
	p, client, _ := newProvisioner(t, templateConfigMap(params, jobTemplate))
	p.ttl = 0

	a := fullAssignment()
	for i := range 2 {
		a.SessionID = "cellphone-camera-nocache" + string(rune('a'+i))
		if err := p.Acquire(context.Background(), a); err != nil {
			t.Fatalf("Acquire %d: %v", i, err)
		}
	}

	gets := 0
	for _, action := range client.Actions() {
		if action.Matches("get", "configmaps") {
			gets++
		}
	}
	if gets != 2 {
		t.Errorf("got %d ConfigMap reads with caching disabled, want one per Acquire", gets)
	}
}
