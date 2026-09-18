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
	"errors"
	"fmt"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// JobProvisioner creates one Kubernetes Job per session from a template held in a
// ConfigMap, and deletes it again on release.
type JobProvisioner struct {
	client       kubernetes.Interface
	namespace    string
	cmName       string
	cmKey        string
	sessionLabel string

	ttl time.Duration
	// now is injectable so tests can drive cache expiry without sleeping.
	now func() time.Time

	mu       sync.Mutex
	cached   []byte
	cachedAt time.Time
}

var _ Provisioner = (*JobProvisioner)(nil)

// NewJobProvisioner returns a Provisioner backed by batch/v1 Jobs.
func NewJobProvisioner(client kubernetes.Interface, params Parameters, ttl time.Duration) *JobProvisioner {
	return &JobProvisioner{
		client:       client,
		namespace:    params.Namespace,
		cmName:       params.JobTemplateConfigMap,
		cmKey:        params.JobTemplateKey,
		sessionLabel: params.SessionLabel,
		ttl:          ttl,
		now:          time.Now,
	}
}

// Acquire renders the job template for this assignment and creates the Job.
//
// An existing Job for the session is success, not failure. The trigger request
// can legitimately arrive more than once -- a client retry, or a user who hits
// Start twice -- and a session already has exactly the handler it needs.
func (p *JobProvisioner) Acquire(ctx context.Context, a Assignment) error {
	raw, err := p.template(ctx)
	if err != nil {
		return err
	}
	job, err := renderJob(raw, a, p.namespace)
	if err != nil {
		return err
	}
	if _, err := p.client.BatchV1().Jobs(p.namespace).Create(ctx, job, metav1.CreateOptions{}); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("creating handler Job %s/%s: %w", p.namespace, job.Name, err)
	}
	return nil
}

// Release deletes every Job labelled with this session id.
//
// Deletion goes through the session label rather than a reconstructed Job name so
// that the naming scheme stays owned by the template. Propagation is Background so
// the handler pod goes away with its Job; without it the Job object is removed and
// the pod is orphaned, which is the one outcome worse than not reaping at all.
func (p *JobProvisioner) Release(ctx context.Context, sessionID string) error {
	jobs := p.client.BatchV1().Jobs(p.namespace)
	list, err := jobs.List(ctx, metav1.ListOptions{
		LabelSelector: p.sessionLabel + "=" + sessionID,
	})
	if err != nil {
		return fmt.Errorf("listing handler Jobs for session %s: %w", sessionID, err)
	}

	policy := metav1.DeletePropagationBackground
	var errs []error
	for i := range list.Items {
		name := list.Items[i].Name
		err := jobs.Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &policy})
		// Something else reaping the Job first is the outcome we wanted anyway.
		if err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("deleting handler Job %s/%s: %w", p.namespace, name, err))
		}
	}
	return errors.Join(errs...)
}

// template returns the raw job template, re-reading it when the cached copy has
// aged out. Caching keeps a burst of sessions from turning into a burst of API
// reads, while the TTL preserves the documented behaviour that editing the
// ConfigMap takes effect for the next session without restarting anything.
func (p *JobProvisioner) template(ctx context.Context) ([]byte, error) {
	p.mu.Lock()
	if p.cached != nil && p.ttl > 0 && p.now().Sub(p.cachedAt) < p.ttl {
		raw := p.cached
		p.mu.Unlock()
		return raw, nil
	}
	p.mu.Unlock()

	cm, err := p.client.CoreV1().ConfigMaps(p.namespace).Get(ctx, p.cmName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("reading job template ConfigMap %s/%s: %w", p.namespace, p.cmName, err)
	}
	data, ok := cm.Data[p.cmKey]
	if !ok {
		return nil, fmt.Errorf("job template ConfigMap %s/%s has no key %q", p.namespace, p.cmName, p.cmKey)
	}
	raw := []byte(data)

	p.mu.Lock()
	p.cached, p.cachedAt = raw, p.now()
	p.mu.Unlock()

	return raw, nil
}
