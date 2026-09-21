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
	"strings"
	"testing"

	errcommon "github.com/llm-d/llm-d-router/pkg/common/error"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

func newPlugin(t *testing.T) (*Plugin, *fakeProvisioner) {
	t.Helper()
	prov := &fakeProvisioner{}
	return New("stream-handler", testParams(t), prov), prov
}

func triggerRequest() *fwksched.InferenceRequest {
	r := &fwksched.InferenceRequest{
		RequestID: "req-1",
		Headers: map[string]string{
			"x-llmd-frame-source":           "frontend-trigger",
			"x-llmd-session-id":             "cellphone-camera-abc123",
			"x-cellphone-camera-stream-url": "http://192.168.1.42:4747/video",
		},
	}
	return r
}

// The happy path: validate on admission, then provision against whatever decode
// endpoint scheduling settled on.
func TestTriggerProvisionsAgainstDecodePick(t *testing.T) {
	p, prov := newPlugin(t)
	req := triggerRequest()

	if err := p.PreAdmit(context.Background(), req); err != nil {
		t.Fatalf("PreAdmit: %v", err)
	}
	p.PreRequest(context.Background(), req, schedulingResult("decode", "decode", "10.1.2.3", "8000"))

	if len(prov.acquired) != 1 {
		t.Fatalf("got %d Acquire calls, want 1", len(prov.acquired))
	}
	got := prov.acquired[0]
	if got.SessionID != "cellphone-camera-abc123" {
		t.Errorf("SessionID = %q", got.SessionID)
	}
	if got.PoolEndpoint != "http://10.1.2.3:8000" {
		t.Errorf("PoolEndpoint = %q, want the decode profile's pick", got.PoolEndpoint)
	}
	if got.StreamURL != "http://192.168.1.42:4747/video" {
		t.Errorf("StreamURL = %q", got.StreamURL)
	}
	want := "http://frontend.cellphone-camera.svc.cluster.local/ingest/cellphone-camera-abc123"
	if got.ResultsCallbackURL != want {
		t.Errorf("ResultsCallbackURL = %q, want %q", got.ResultsCallbackURL, want)
	}
}

// This plugin sits on every request the EPP handles. Anything that is not a
// trigger or a stop must cost nothing and provision nothing -- most importantly
// the handler's own frame submissions, which would otherwise provision a second
// handler for a session that already has one.
func TestNonSessionTrafficIsUntouched(t *testing.T) {
	for name, origin := range map[string]string{
		"handler frame traffic": "cellphone-camera-handler",
		"unrelated origin":      "something-else",
		"absent origin":         "",
	} {
		t.Run(name, func(t *testing.T) {
			p, prov := newPlugin(t)
			req := &fwksched.InferenceRequest{Headers: map[string]string{}}
			if origin != "" {
				req.Headers["x-llmd-frame-source"] = origin
			}
			// A session header alone must not be enough to trigger provisioning.
			req.Headers["x-llmd-session-id"] = "cellphone-camera-abc123"

			if err := p.PreAdmit(context.Background(), req); err != nil {
				t.Fatalf("PreAdmit: %v", err)
			}
			p.PreRequest(context.Background(), req, schedulingResult("decode", "decode", "10.1.2.3", "8000"))

			if len(prov.acquired) != 0 || len(prov.released) != 0 {
				t.Errorf("provisioner was called (%d acquire, %d release), want none",
					len(prov.acquired), len(prov.released))
			}
		})
	}
}

func TestStopReleasesSession(t *testing.T) {
	p, prov := newPlugin(t)
	req := &fwksched.InferenceRequest{Headers: map[string]string{
		"x-llmd-frame-source": "frontend-stop",
		"x-llmd-session-id":   "cellphone-camera-abc123",
	}}

	if err := p.PreAdmit(context.Background(), req); err != nil {
		t.Fatalf("PreAdmit: %v", err)
	}
	if len(prov.released) != 1 || prov.released[0] != "cellphone-camera-abc123" {
		t.Fatalf("released = %v, want one release of cellphone-camera-abc123", prov.released)
	}
	if len(prov.acquired) != 0 {
		t.Errorf("a stop request provisioned %d handlers, want 0", len(prov.acquired))
	}
}

// A failed delete is not something a browser closing a tab can act on, and the
// handler's own backstops still bound the damage. Surfacing it as a request
// error would turn best-effort cleanup into a user-visible failure.
func TestStopDoesNotFailOnReleaseError(t *testing.T) {
	prov := &fakeProvisioner{releaseErr: errors.New("api server is down")}
	p := New("stream-handler", testParams(t), prov)
	req := &fwksched.InferenceRequest{Headers: map[string]string{
		"x-llmd-frame-source": "frontend-stop",
		"x-llmd-session-id":   "cellphone-camera-abc123",
	}}

	if err := p.PreAdmit(context.Background(), req); err != nil {
		t.Fatalf("PreAdmit returned %v, want nil so the stop call still succeeds", err)
	}
}

// Bad input is caller error and must reach the frontend as a 400, which is why
// validation lives in PreAdmit: it is the last hook whose error becomes a status.
func TestTriggerRejectsBadInputAsBadRequest(t *testing.T) {
	p, prov := newPlugin(t)
	req := &fwksched.InferenceRequest{Headers: map[string]string{
		"x-llmd-frame-source": "frontend-trigger",
		// No session id and no stream URL.
	}}

	err := p.PreAdmit(context.Background(), req)
	if err == nil {
		t.Fatal("PreAdmit accepted a trigger with no session id, want an error")
	}
	var apiErr errcommon.Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("error is %T, want errcommon.Error so the director maps it to a status", err)
	}
	if apiErr.Code != errcommon.BadRequest {
		t.Errorf("code = %q, want %q", apiErr.Code, errcommon.BadRequest)
	}

	// A rejected trigger must not go on to provision anything.
	p.PreRequest(context.Background(), req, schedulingResult("decode", "decode", "10.1.2.3", "8000"))
	if len(prov.acquired) != 0 {
		t.Errorf("a rejected trigger provisioned %d handlers, want 0", len(prov.acquired))
	}
}

func TestStopRejectsBadSessionID(t *testing.T) {
	p, prov := newPlugin(t)
	req := &fwksched.InferenceRequest{Headers: map[string]string{
		"x-llmd-frame-source": "frontend-stop",
		"x-llmd-session-id":   "Not_A_Label",
	}}

	if err := p.PreAdmit(context.Background(), req); err == nil {
		t.Fatal("PreAdmit accepted a stop with a malformed session id, want an error")
	}
	if len(prov.released) != 0 {
		t.Errorf("released %v, want no release attempt for a malformed id", prov.released)
	}
}

// The trigger body is the fallback path, and it has to survive being the only
// source of truth.
func TestTriggerReadsBodyWhenHeadersAreAbsent(t *testing.T) {
	p, prov := newPlugin(t)
	req := &fwksched.InferenceRequest{
		Headers: map[string]string{"x-llmd-frame-source": "frontend-trigger"},
		Body: &fwkrh.InferenceRequestBody{
			Payload: fwkrh.PayloadMap{
				"session_id": "cellphone-camera-body01",
				"cellphone-camera": map[string]any{
					"stream_url": "rtsp://192.168.1.42:4747/",
					"prompt":     "What do you see?",
					"interval":   4.0,
				},
			},
		},
	}

	if err := p.PreAdmit(context.Background(), req); err != nil {
		t.Fatalf("PreAdmit: %v", err)
	}
	p.PreRequest(context.Background(), req, schedulingResult("decode", "decode", "10.1.2.3", "8000"))

	if len(prov.acquired) != 1 {
		t.Fatalf("got %d Acquire calls, want 1", len(prov.acquired))
	}
	got := prov.acquired[0]
	if got.SessionID != "cellphone-camera-body01" || got.StreamURL != "rtsp://192.168.1.42:4747/" {
		t.Errorf("assignment = %+v, want the values taken from the body", got)
	}
	if got.Prompt != "What do you see?" || got.FrameInterval != "4" {
		t.Errorf("optional fields = %q / %q, want them carried from the body", got.Prompt, got.FrameInterval)
	}
}

// The configured profile name is the primary source, but a config that names its
// decode profile something else still works via the result's primary profile.
func TestDecodeEndpointFallsBackToPrimaryProfile(t *testing.T) {
	p, prov := newPlugin(t)
	req := triggerRequest()
	if err := p.PreAdmit(context.Background(), req); err != nil {
		t.Fatalf("PreAdmit: %v", err)
	}
	// Nothing under "decode"; the result's primary profile is named differently.
	p.PreRequest(context.Background(), req, schedulingResult("vlm-decode", "vlm-decode", "10.9.9.9", "8001"))

	if len(prov.acquired) != 1 {
		t.Fatalf("got %d Acquire calls, want 1", len(prov.acquired))
	}
	if got := prov.acquired[0].PoolEndpoint; got != "http://10.9.9.9:8001" {
		t.Errorf("PoolEndpoint = %q, want the primary profile's pick", got)
	}
}

func TestNoProvisionWithoutAnEndpoint(t *testing.T) {
	tests := map[string]*fwksched.SchedulingResult{
		"nil result":        nil,
		"no profile result": {PrimaryProfileName: "decode", ProfileResults: map[string]*fwksched.ProfileRunResult{}},
		"no endpoints": {
			PrimaryProfileName: "decode",
			ProfileResults:     map[string]*fwksched.ProfileRunResult{"decode": {}},
		},
	}
	for name, result := range tests {
		t.Run(name, func(t *testing.T) {
			p, prov := newPlugin(t)
			req := triggerRequest()
			if err := p.PreAdmit(context.Background(), req); err != nil {
				t.Fatalf("PreAdmit: %v", err)
			}
			p.PreRequest(context.Background(), req, result)
			if len(prov.acquired) != 0 {
				t.Errorf("provisioned %d handlers with no endpoint available, want 0", len(prov.acquired))
			}
		})
	}
}

// At v0.9.0 PreRequest has no error return, so a provisioning failure can only be
// logged. This pins that it is survivable rather than a panic, and documents the
// behaviour the frontend has to cope with: a trigger that succeeds but yields no
// frames.
func TestProvisionFailureIsSurvivable(t *testing.T) {
	prov := &fakeProvisioner{acquireErr: errors.New("jobs.batch is forbidden")}
	p := New("stream-handler", testParams(t), prov)
	req := triggerRequest()

	if err := p.PreAdmit(context.Background(), req); err != nil {
		t.Fatalf("PreAdmit: %v", err)
	}
	p.PreRequest(context.Background(), req, schedulingResult("decode", "decode", "10.1.2.3", "8000"))

	if len(prov.acquired) != 1 {
		t.Errorf("got %d Acquire calls, want 1 attempt", len(prov.acquired))
	}
}

func TestNilRequestIsSafe(t *testing.T) {
	p, _ := newPlugin(t)
	if err := p.PreAdmit(context.Background(), nil); err != nil {
		t.Errorf("PreAdmit(nil) = %v, want nil", err)
	}
	p.PreRequest(context.Background(), nil, nil)
}

func TestResultsCallbackURLOmittedWhenUnconfigured(t *testing.T) {
	params := testParams(t)
	params.ResultsCallbackBaseURL = ""
	prov := &fakeProvisioner{}
	p := New("stream-handler", params, prov)

	req := triggerRequest()
	if err := p.PreAdmit(context.Background(), req); err != nil {
		t.Fatalf("PreAdmit: %v", err)
	}
	p.PreRequest(context.Background(), req, schedulingResult("decode", "decode", "10.1.2.3", "8000"))

	if len(prov.acquired) != 1 {
		t.Fatalf("got %d Acquire calls, want 1", len(prov.acquired))
	}
	if got := prov.acquired[0].ResultsCallbackURL; got != "" {
		t.Errorf("ResultsCallbackURL = %q, want empty so the renderer drops the env entry", got)
	}
}

// The precedence that makes multi-replica frontends work: whatever the caller
// named beats the configured fallback.
func TestCallerCallbackBeatsConfiguredBase(t *testing.T) {
	prov := &fakeProvisioner{}
	p := New("stream-handler", testParams(t), prov)

	req := triggerRequest()
	req.Headers["x-cellphone-camera-results-callback"] = "http://10.42.1.7:8080/ingest"
	if err := p.PreAdmit(context.Background(), req); err != nil {
		t.Fatalf("PreAdmit: %v", err)
	}
	p.PreRequest(context.Background(), req, schedulingResult("decode", "decode", "10.1.2.3", "8000"))

	if len(prov.acquired) != 1 {
		t.Fatalf("got %d Acquire calls, want 1", len(prov.acquired))
	}
	want := "http://10.42.1.7:8080/ingest/" + prov.acquired[0].SessionID
	if got := prov.acquired[0].ResultsCallbackURL; got != want {
		t.Errorf("ResultsCallbackURL = %q, want %q -- the caller's pod, not the configured Service", got, want)
	}
}

// A caller that names nothing still gets results, via the configured base.
func TestConfiguredBaseUsedWhenCallerNamesNoCallback(t *testing.T) {
	prov := &fakeProvisioner{}
	p := New("stream-handler", testParams(t), prov)

	req := triggerRequest()
	if err := p.PreAdmit(context.Background(), req); err != nil {
		t.Fatalf("PreAdmit: %v", err)
	}
	p.PreRequest(context.Background(), req, schedulingResult("decode", "decode", "10.1.2.3", "8000"))

	if len(prov.acquired) != 1 {
		t.Fatalf("got %d Acquire calls, want 1", len(prov.acquired))
	}
	want := "http://frontend.cellphone-camera.svc.cluster.local/ingest/" + prov.acquired[0].SessionID
	if got := prov.acquired[0].ResultsCallbackURL; got != want {
		t.Errorf("ResultsCallbackURL = %q, want the configured fallback %q", got, want)
	}
}

func TestParametersValidation(t *testing.T) {
	tests := map[string]struct {
		params   Parameters
		wantText string
	}{
		"missing namespace": {
			params:   Parameters{JobTemplateConfigMap: "tmpl"},
			wantText: "namespace is required",
		},
		"missing template configmap": {
			params:   Parameters{Namespace: "ns"},
			wantText: "jobTemplateConfigMap is required",
		},
		"origins collide": {
			params:   Parameters{Namespace: "ns", JobTemplateConfigMap: "tmpl", TriggerOrigin: "same", StopOrigin: "same"},
			wantText: "must differ",
		},
		"bad cache ttl": {
			params:   Parameters{Namespace: "ns", JobTemplateConfigMap: "tmpl", TemplateCacheTTL: "soon"},
			wantText: "not a valid duration",
		},
		"bad callback cidr": {
			params:   Parameters{Namespace: "ns", JobTemplateConfigMap: "tmpl", AllowedResultsCallbackCIDRs: []string{"10.0.0.0/8", "not-a-cidr"}},
			wantText: "not a valid CIDR",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			p := tc.params
			p.applyDefaults()
			err := p.validate()
			if err == nil {
				t.Fatalf("validate succeeded, want an error mentioning %q", tc.wantText)
			}
			if !strings.Contains(err.Error(), tc.wantText) {
				t.Errorf("error = %v, want it to mention %q", err, tc.wantText)
			}
		})
	}
}
