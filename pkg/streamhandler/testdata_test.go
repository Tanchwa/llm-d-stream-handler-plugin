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
	"testing"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

// jobTemplate mirrors the template the ingestor repo ships in
// k8s/handler/configmap-job-template.yaml, kept faithful to the parts this
// plugin actually manipulates: the ${...} placeholders, the session-id labels
// Release selects on, and the two optional env entries that must be dropped
// rather than blanked when unset.
const jobTemplate = `
apiVersion: batch/v1
kind: Job
metadata:
  name: handler-${SESSION_ID}
  namespace: cellphone-camera
  labels:
    app.kubernetes.io/name: cellphone-camera-handler
    cellphone-camera.io/session-id: "${SESSION_ID}"
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 360
  ttlSecondsAfterFinished: 300
  template:
    metadata:
      labels:
        app.kubernetes.io/name: cellphone-camera-handler
        cellphone-camera.io/session-id: "${SESSION_ID}"
    spec:
      restartPolicy: Never
      serviceAccountName: cellphone-camera-handler
      automountServiceAccountToken: false
      containers:
        - name: handler
          image: docker.io/tanchwa/cellphone-camera-handler:latest
          envFrom:
            - configMapRef:
                name: cellphone-camera-handler-defaults
          env:
            - name: SESSION_ID
              value: "${SESSION_ID}"
            - name: STREAM_URL
              value: "${STREAM_URL}"
            - name: POOL_ENDPOINT
              value: "${POOL_ENDPOINT}"
            - name: RESULT_SINK_URL
              value: "${RESULT_SINK_URL}"
            - name: PROMPT
              value: "${PROMPT}"
            - name: FRAME_INTERVAL
              value: "${FRAME_INTERVAL}"
`

// testParams returns parameters with defaults applied, as the factory would.
func testParams(t *testing.T) Parameters {
	t.Helper()
	p := Parameters{
		Namespace:            "cellphone-camera",
		JobTemplateConfigMap: "cellphone-camera-handler-job-template",
		ResultSinkBaseURL:    "http://frontend.cellphone-camera.svc.cluster.local/ingest",
	}
	p.applyDefaults()
	if err := p.validate(); err != nil {
		t.Fatalf("test parameters are invalid: %v", err)
	}
	return p
}

// fakeProvisioner records calls so tests can assert on what the plugin decided,
// independently of how a real provisioner would carry it out.
type fakeProvisioner struct {
	acquired   []Assignment
	released   []string
	acquireErr error
	releaseErr error
}

func (f *fakeProvisioner) Acquire(_ context.Context, a Assignment) error {
	f.acquired = append(f.acquired, a)
	return f.acquireErr
}

func (f *fakeProvisioner) Release(_ context.Context, sessionID string) error {
	f.released = append(f.released, sessionID)
	return f.releaseErr
}

// schedulingResult builds a result whose named profile picked one endpoint.
func schedulingResult(profile, primary, address, port string) *fwksched.SchedulingResult {
	endpoint := fwksched.NewEndpoint(
		&fwkdl.EndpointMetadata{PodName: "decode-pod", Address: address, Port: port},
		&fwkdl.Metrics{},
		nil,
	)
	return &fwksched.SchedulingResult{
		PrimaryProfileName: primary,
		ProfileResults: map[string]*fwksched.ProfileRunResult{
			profile: {TargetEndpoints: []fwksched.Endpoint{endpoint}},
		},
	}
}
