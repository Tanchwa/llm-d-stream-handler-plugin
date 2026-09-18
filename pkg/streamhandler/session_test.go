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

	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
)

func TestParseTriggerFromHeaders(t *testing.T) {
	p := testParams(t)
	got, err := p.parseTrigger(map[string]string{
		"x-llmd-session-id":                 "cellphone-camera-abc123",
		"x-cellphone-camera-stream-url":     "http://192.168.1.42:4747/video",
		"x-cellphone-camera-frame-interval": "3.5",
	}, nil)
	if err != nil {
		t.Fatalf("parseTrigger: %v", err)
	}
	want := sessionRequest{
		SessionID:     "cellphone-camera-abc123",
		StreamURL:     "http://192.168.1.42:4747/video",
		FrameInterval: "3.5",
	}
	if got != want {
		t.Errorf("parseTrigger = %+v, want %+v", got, want)
	}
}

// The body is the fallback for gateways that strip unknown request headers.
func TestParseTriggerFallsBackToBody(t *testing.T) {
	p := testParams(t)
	payload := fwkrh.PayloadMap{
		"session_id": "cellphone-camera-fromdbody",
		"cellphone-camera": map[string]any{
			"stream_url": "rtsp://192.168.1.42:4747/",
			"prompt":     "Describe the scene.",
			"interval":   2.0,
		},
	}
	got, err := p.parseTrigger(map[string]string{}, payload)
	if err != nil {
		t.Fatalf("parseTrigger: %v", err)
	}
	want := sessionRequest{
		SessionID:     "cellphone-camera-fromdbody",
		StreamURL:     "rtsp://192.168.1.42:4747/",
		Prompt:        "Describe the scene.",
		FrameInterval: "2",
	}
	if got != want {
		t.Errorf("parseTrigger = %+v, want %+v", got, want)
	}
}

func TestParseTriggerHeadersBeatBody(t *testing.T) {
	p := testParams(t)
	payload := fwkrh.PayloadMap{
		"session_id": "from-body",
		"cellphone-camera": map[string]any{
			"stream_url": "http://body-wins.invalid/video",
			"interval":   9.0,
		},
	}
	got, err := p.parseTrigger(map[string]string{
		"x-llmd-session-id":                 "from-header",
		"x-cellphone-camera-stream-url":     "http://header-wins.invalid/video",
		"x-cellphone-camera-frame-interval": "1",
	}, payload)
	if err != nil {
		t.Fatalf("parseTrigger: %v", err)
	}
	if got.SessionID != "from-header" || got.StreamURL != "http://header-wins.invalid/video" || got.FrameInterval != "1" {
		t.Errorf("parseTrigger = %+v, want every field taken from the headers", got)
	}
}

func TestParseTriggerRejectsBadInput(t *testing.T) {
	p := testParams(t)
	tests := map[string]struct {
		headers  map[string]string
		wantText string
	}{
		"no session id": {
			headers:  map[string]string{"x-cellphone-camera-stream-url": "http://cam/video"},
			wantText: "session id is required",
		},
		"session id is not a DNS label": {
			headers: map[string]string{
				"x-llmd-session-id":             "Not_A_Label",
				"x-cellphone-camera-stream-url": "http://cam/video",
			},
			wantText: "DNS-1123",
		},
		// Long ids would render a pod name past Kubernetes' 63-character limit, so
		// the Job would be created and then never schedule a pod.
		"session id too long": {
			headers: map[string]string{
				"x-llmd-session-id":             strings.Repeat("a", maxSessionIDLen+1),
				"x-cellphone-camera-stream-url": "http://cam/video",
			},
			wantText: "limit is",
		},
		"no stream url": {
			headers:  map[string]string{"x-llmd-session-id": "session-one"},
			wantText: "stream URL is required",
		},
		"stream url scheme not allowed": {
			headers: map[string]string{
				"x-llmd-session-id":             "session-one",
				"x-cellphone-camera-stream-url": "file:///etc/passwd",
			},
			wantText: "not allowed",
		},
		// Parses cleanly as a relative reference, so it reaches the scheme check
		// rather than failing in url.Parse.
		"stream url has no scheme": {
			headers: map[string]string{
				"x-llmd-session-id":             "session-one",
				"x-cellphone-camera-stream-url": "cam.local/video",
			},
			wantText: "has no scheme",
		},
		"stream url is unparseable": {
			headers: map[string]string{
				"x-llmd-session-id":             "session-one",
				"x-cellphone-camera-stream-url": "http://[::1",
			},
			wantText: "not a valid URL",
		},
		"stream url has no host": {
			headers: map[string]string{
				"x-llmd-session-id":             "session-one",
				"x-cellphone-camera-stream-url": "http:///video",
			},
			wantText: "no host",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := p.parseTrigger(tc.headers, nil)
			if err == nil {
				t.Fatalf("parseTrigger succeeded, want an error mentioning %q", tc.wantText)
			}
			if !strings.Contains(err.Error(), tc.wantText) {
				t.Errorf("error = %v, want it to mention %q", err, tc.wantText)
			}
		})
	}
}

// The interval arrives as whatever type the body decoder produced, and lands in
// an env var either way.
func TestNumberField(t *testing.T) {
	tests := map[string]struct {
		value any
		want  string
	}{
		"float":       {2.5, "2.5"},
		"whole float": {2.0, "2"},
		"string":      {"1.25", "1.25"},
		"absent":      {nil, ""},
		"wrong type":  {[]any{1}, ""},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			m := map[string]any{}
			if tc.value != nil {
				m["interval"] = tc.value
			}
			if got := numberField(m, "interval"); got != tc.want {
				t.Errorf("numberField = %q, want %q", got, tc.want)
			}
		})
	}
	if got := numberField(nil, "interval"); got != "" {
		t.Errorf("numberField(nil) = %q, want empty", got)
	}
}
