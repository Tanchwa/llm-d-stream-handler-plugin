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

// Assignment is everything a single RTSP handler needs to run one session. It is
// the whole contract between this plugin and the handler pod: the handler reads
// each field as an environment variable and talks to no Kubernetes API of its own.
//
// SessionID, StreamURL and PoolEndpoint are mandatory. Prompt, FrameInterval and
// ResultsCallbackURL are optional, and an empty value means "unset" rather than "empty
// string" -- the renderer drops the env entry entirely so the handler falls
// through to the cellphone-camera-handler-defaults ConfigMap via envFrom. Passing
// an empty FrameInterval through instead of dropping it would crash the handler on
// float("").
type Assignment struct {
	// SessionID correlates this handler with the caller's session. It comes from
	// the trigger request's session header and becomes both the Job name suffix
	// and the value of the session-id label used to find and reap the Job.
	SessionID string
	// StreamURL is the camera address the user typed into the frontend. It is
	// arbitrary user input and is validated against an allowlist of schemes
	// before it ever reaches this struct.
	StreamURL string
	// PoolEndpoint is the decode endpoint llm-d picked for this session, as a
	// scheme-qualified base URL. Pinning every frame to this one endpoint is what
	// keeps the constant text prefix warm in the pod's prefix cache.
	PoolEndpoint string
	// Prompt optionally overrides the handler's default per-frame prompt.
	Prompt string
	// FrameInterval optionally overrides the handler's sampling interval, in
	// seconds, carried as a string because it is written straight into an env var.
	FrameInterval string
	// ResultsCallbackURL is where the handler forwards the inference server's response
	// bytes so they reach the frontend. Empty when no callback is configured, in which
	// case the handler falls back to its own default behaviour.
	ResultsCallbackURL string
}

// placeholders maps the template's ${...} tokens to this assignment's values.
// Keys absent from this map, or present with an empty value, are treated as
// unresolved by the renderer.
func (a Assignment) placeholders() map[string]string {
	return map[string]string{
		"SESSION_ID":           a.SessionID,
		"STREAM_URL":           a.StreamURL,
		"POOL_ENDPOINT":        a.PoolEndpoint,
		"PROMPT":               a.Prompt,
		"FRAME_INTERVAL":       a.FrameInterval,
		"RESULTS_CALLBACK_URL": a.ResultsCallbackURL,
	}
}
