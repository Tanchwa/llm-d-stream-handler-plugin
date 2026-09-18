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
	"fmt"
	"regexp"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

// placeholderPattern matches the ${NAME} tokens the handler's job template uses.
var placeholderPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// renderJob turns the raw job template into a Job for one session.
//
// The substitution deliberately happens on the *decoded* object, never on the
// YAML source. StreamURL is arbitrary text from a browser field, and splicing it
// into YAML before parsing would let a crafted camera URL -- a quote, a newline
// and some indentation -- rewrite the surrounding pod spec. Parsing first makes
// every value a leaf string that cannot restructure the document no matter what
// it contains.
func renderJob(raw []byte, a Assignment, namespace string) (*batchv1.Job, error) {
	job := &batchv1.Job{}
	if err := yaml.UnmarshalStrict(raw, job); err != nil {
		return nil, fmt.Errorf("parsing job template: %w", err)
	}
	if job.Kind != "" && job.Kind != "Job" {
		return nil, fmt.Errorf("job template declares kind %q, want Job", job.Kind)
	}

	values := a.placeholders()

	name, ok := substitute(job.Name, values)
	if !ok {
		return nil, fmt.Errorf("job template name %q references an unset placeholder", job.Name)
	}
	job.Name = name
	// The configured namespace wins over whatever the template hardcodes, so the
	// plugin creates Jobs only where its RBAC actually grants it.
	job.Namespace = namespace

	if err := substituteLabels(job.Labels, values, "metadata.labels"); err != nil {
		return nil, err
	}
	if err := substituteLabels(job.Spec.Template.Labels, values, "spec.template.metadata.labels"); err != nil {
		return nil, err
	}

	podSpec := &job.Spec.Template.Spec
	substituteEnv(podSpec.Containers, values)
	substituteEnv(podSpec.InitContainers, values)

	return job, nil
}

// substituteLabels rewrites label values in place. A label that references an
// unset placeholder is an error rather than a silent drop: the session-id label
// is how Release finds the Job, so a Job carrying a blank one could never be
// reaped and would run to its backstop.
func substituteLabels(labels map[string]string, values map[string]string, where string) error {
	for k, v := range labels {
		out, ok := substitute(v, values)
		if !ok {
			return fmt.Errorf("%s[%q] references an unset placeholder in %q", where, k, v)
		}
		labels[k] = out
	}
	return nil
}

// substituteEnv rewrites container env values in place, dropping any entry whose
// value references a placeholder we have no value for.
//
// Dropping rather than blanking is what the template's own header comment asks
// for, and it matters: the entries are overrides layered on top of an envFrom
// ConfigMap of defaults. A dropped PROMPT falls through to the default prompt,
// while a PROMPT set to "" would override the default with nothing, and a blank
// FRAME_INTERVAL would crash the handler outright on float("").
func substituteEnv(containers []corev1.Container, values map[string]string) {
	for i := range containers {
		env := containers[i].Env
		kept := env[:0]
		for _, e := range env {
			// ValueFrom entries carry no literal to substitute; pass them through.
			if e.Value == "" && e.ValueFrom != nil {
				kept = append(kept, e)
				continue
			}
			out, ok := substitute(e.Value, values)
			if !ok {
				continue
			}
			e.Value = out
			kept = append(kept, e)
		}
		// Clear the tail so the dropped entries are not retained by the backing array.
		for j := len(kept); j < len(env); j++ {
			env[j] = corev1.EnvVar{}
		}
		containers[i].Env = kept
	}
}

// substitute replaces every ${NAME} token in s. It reports false if any token
// names a placeholder that is absent or empty, leaving the caller to decide
// whether that is a dropped env entry or a hard error.
func substitute(s string, values map[string]string) (string, bool) {
	if !strings.Contains(s, "${") {
		return s, true
	}
	resolved := true
	out := placeholderPattern.ReplaceAllStringFunc(s, func(token string) string {
		name := placeholderPattern.FindStringSubmatch(token)[1]
		v, found := values[name]
		if !found || v == "" {
			resolved = false
			return ""
		}
		return v
	})
	if !resolved {
		return "", false
	}
	return out, true
}
