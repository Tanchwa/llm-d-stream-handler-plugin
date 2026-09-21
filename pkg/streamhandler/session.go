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
	"encoding/json"
	"fmt"
	"net/netip"
	"net/url"
	"slices"
	"strconv"
	"strings"

	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	"k8s.io/apimachinery/pkg/util/validation"
)

// maxSessionIDLen leaves room inside Kubernetes' 63-character DNS label limit for
// everything the session id gets wrapped in before it becomes a pod name: the
// template's "handler-" prefix (8) and the suffix the Job controller appends to
// name each pod (6). 63 - 8 - 6 = 49. The ingestor's job template calls out this
// same budget; if either the prefix or the session id format changes, both sides
// need rechecking.
const maxSessionIDLen = 49

// cameraExtensionKey is the top-level body object the frontend hangs handler
// configuration off. Keeping it out of the completion proper means the trigger
// request stays a valid, routable completion even where this plugin is absent.
const cameraExtensionKey = "cellphone-camera"

// sessionRequest is what a trigger request asks us to provision, after headers
// and body have been read and validated but before scheduling has picked an
// endpoint.
type sessionRequest struct {
	SessionID     string
	StreamURL     string
	Prompt        string
	FrameInterval string
	// ResultsCallback is where this caller wants the session's inference output
	// delivered, as a base URL with no session id. Empty means the caller named
	// no callback and the configured resultsCallbackBaseURL applies instead.
	ResultsCallback string
}

// parseTrigger extracts and validates a provisioning request. Headers win over
// the body: the body is a fallback for deployments whose gateway strips custom
// headers.
//
// Every error here is caller error, reported so the frontend gets a 400 rather
// than a silently missing handler.
func (p *Parameters) parseTrigger(headers map[string]string, payload fwkrh.RequestPayload) (sessionRequest, error) {
	// A body that never parsed, or one shaped differently than we expect, is not
	// fatal on its own: the headers may carry everything required. Missing fields
	// are reported below, once we know they are actually missing.
	var body fwkrh.PayloadMap
	if payload != nil {
		// A nil RequestPayload is a nil interface, not an empty map; calling
		// through it would panic rather than yield nothing.
		body, _ = payload.AsMap()
	}
	camera, _ := body[cameraExtensionKey].(map[string]any)

	sessionID := headers[p.SessionHeader]
	if sessionID == "" {
		sessionID = stringField(body, "session_id")
	}
	sessionID = strings.TrimSpace(sessionID)
	if err := validateSessionID(sessionID); err != nil {
		return sessionRequest{}, err
	}

	streamURL := strings.TrimSpace(headers[p.StreamURLHeader])
	if streamURL == "" {
		streamURL = strings.TrimSpace(stringField(camera, "stream_url"))
	}
	if err := p.validateStreamURL(streamURL); err != nil {
		return sessionRequest{}, err
	}

	req := sessionRequest{SessionID: sessionID, StreamURL: streamURL}

	// The caller names its own callback because it is the only party that knows
	// which of its replicas holds the browser socket for this session.
	resultsCallback := strings.TrimSpace(headers[p.ResultsCallbackHeader])
	if resultsCallback == "" {
		resultsCallback = strings.TrimSpace(stringField(camera, "results_callback"))
	}
	if resultsCallback != "" {
		resultsCallback = strings.TrimRight(resultsCallback, "/")
		if err := p.validateResultsCallbackURL(resultsCallback); err != nil {
			return sessionRequest{}, err
		}
		req.ResultsCallback = resultsCallback
	}

	req.Prompt = stringField(camera, "prompt")
	req.FrameInterval = numberField(camera, "interval")
	// The header overrides the body for the interval, matching the precedence
	// used for the two required fields.
	if v := strings.TrimSpace(headers[p.FrameIntervalHeader]); v != "" {
		req.FrameInterval = v
	}

	return req, nil
}

// validateSessionID rejects anything that cannot safely become a Job name, a pod
// name and a label value. The session id is attacker-influenced -- it arrives in
// a header -- and it is also the key Release uses to find the Job again, so a
// value that survives Job creation but is mangled in the label would strand a
// running handler.
func validateSessionID(id string) error {
	if id == "" {
		return fmt.Errorf("session id is required")
	}
	if len(id) > maxSessionIDLen {
		return fmt.Errorf("session id is %d characters, limit is %d so the handler pod name stays within Kubernetes' 63-character limit", len(id), maxSessionIDLen)
	}
	if errs := validation.IsDNS1123Label(id); len(errs) > 0 {
		return fmt.Errorf("session id %q is not a valid DNS-1123 label: %s", id, strings.Join(errs, "; "))
	}
	return nil
}

// validateStreamURL confirms the camera address is a URL we are willing to hand
// to a handler pod.
//
// This value is typed into a browser field and becomes the target of an outbound
// connection from inside the cluster, so the scheme allowlist is the control that
// keeps it from naming something like file:// or gopher://. It is not a full SSRF
// defence: an operator who needs one should pair this with a NetworkPolicy, since
// the legitimate destinations here are arbitrary user LAN addresses and cannot be
// enumerated in advance.
func (p *Parameters) validateStreamURL(raw string) error {
	if raw == "" {
		return fmt.Errorf("stream URL is required (header %q or body cellphone-camera.stream_url)", p.StreamURLHeader)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("stream URL is not a valid URL: %w", err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme == "" {
		return fmt.Errorf("stream URL %q has no scheme, want one of %s", raw, strings.Join(p.AllowedStreamSchemes, ", "))
	}
	if !slices.Contains(p.AllowedStreamSchemes, scheme) {
		return fmt.Errorf("stream URL scheme %q is not allowed, want one of %s", scheme, strings.Join(p.AllowedStreamSchemes, ", "))
	}
	if u.Host == "" {
		return fmt.Errorf("stream URL %q has no host", raw)
	}
	return nil
}

// validateResultsCallbackURL confirms a caller-supplied results callback is somewhere we
// are willing to point a handler pod at.
//
// This is a stricter check than the one above, and deliberately so. A camera
// address is arbitrary user LAN and cannot be enumerated in advance, so all a
// scheme allowlist can do there is rule out the absurd. A results callback is the
// opposite: the only legitimate destinations are frontend pods inside this
// cluster, which means an address allowlist is both writable and worth writing.
// Left unchecked, this header would let anything that can reach the gateway
// choose where a handler sends inference output.
//
// The session id is appended to whatever survives this, so a base URL carrying
// a query or a fragment is refused rather than silently mangled into
// "http://host/ingest?x=1/session-id".
func (p *Parameters) validateResultsCallbackURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("results callback is not a valid URL: %w", err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return fmt.Errorf("results callback scheme %q is not allowed, want http or https", u.Scheme)
	}
	if u.User != nil {
		return fmt.Errorf("results callback %q must not carry credentials", raw)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("results callback %q must be a plain base URL: the session id is appended to it, so a query or fragment cannot be honoured", raw)
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("results callback %q has no host", raw)
	}

	if addr, err := netip.ParseAddr(host); err == nil {
		addr = addr.Unmap()
		// Refused ahead of the allowlist so a deployment that widens the CIDRs
		// cannot accidentally expose the instance metadata service on
		// 169.254.169.254, or reach back into the handler pod itself.
		if addr.IsLoopback() || addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() || addr.IsUnspecified() {
			return fmt.Errorf("results callback address %s is never allowed (loopback, link-local or unspecified)", addr)
		}
		for _, n := range p.resultsCallbackNets {
			if n.Contains(addr) {
				return nil
			}
		}
		return fmt.Errorf("results callback address %s is outside allowedResultsCallbackCIDRs (%s)", addr, strings.Join(p.AllowedResultsCallbackCIDRs, ", "))
	}

	name := strings.ToLower(strings.TrimSuffix(host, "."))
	for _, d := range p.AllowedResultsCallbackDomains {
		if name == d || strings.HasSuffix(name, "."+d) {
			return nil
		}
	}
	return fmt.Errorf("results callback host %q is outside allowedResultsCallbackDomains (%s)", host, strings.Join(p.AllowedResultsCallbackDomains, ", "))
}

// stringField reads a string out of a decoded JSON object, tolerating a missing
// key, a nil map, or a value of some other type.
func stringField(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	s, _ := m[key].(string)
	return s
}

// numberField reads a JSON number out of a decoded object and renders it the way
// an environment variable needs it.
//
// The concrete type depends on how the body was decoded -- encoding/json yields
// float64 by default and json.Number under UseNumber -- and a client may well
// have sent the value as a string. All three are accepted; anything else reads as
// absent, which makes the handler fall back to its configured default rather than
// crash on an unparseable interval.
func numberField(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	switch v := m[key].(type) {
	case json.Number:
		return v.String()
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case string:
		return strings.TrimSpace(v)
	default:
		return ""
	}
}
