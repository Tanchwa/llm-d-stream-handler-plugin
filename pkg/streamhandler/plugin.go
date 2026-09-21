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

// Package streamhandler provisions an RTSP stream handler for a session once
// llm-d has chosen the inference endpoint that session will use.
//
// The plugin sits on two request-control extension points and does nothing at
// all on ordinary traffic:
//
//   - PreAdmit validates a session trigger early, so bad input becomes a 400
//     instead of a handler that was never created, and services a stop request.
//   - PreRequest runs once after the whole scheduling cycle, where the chosen
//     decode endpoint is finally known, and provisions the handler against it.
//
// Pinning the handler to that one endpoint is what keeps a session's constant
// text prompt resident in the pod's prefix cache: the frames change every
// request, the prompt does not, and reuse only survives if the frames keep
// landing on the same pod.
package streamhandler

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	errcommon "github.com/llm-d/llm-d-router/pkg/common/error"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// triggerAttributeKey carries the validated trigger from PreAdmit to PreRequest.
// Parsing once and passing the result forward keeps the two hooks from
// disagreeing about what the request asked for, and keeps PreRequest -- which
// cannot report an error -- out of the validation business entirely.
const triggerAttributeKey = PluginType + "/trigger"

// compile-time assertions that the framework will wire this plugin where we expect.
var (
	_ fwkrc.PreAdmitter = (*Plugin)(nil)
	_ fwkrc.PreRequest  = (*Plugin)(nil)
)

// Plugin is the stream handler provisioner.
type Plugin struct {
	typedName   fwkplugin.TypedName
	params      Parameters
	provisioner Provisioner
}

// New builds a plugin around an already-configured Provisioner. Factory is the
// entry point the framework uses; this exists so tests can supply a fake.
func New(name string, params Parameters, provisioner Provisioner) *Plugin {
	return &Plugin{
		typedName:   fwkplugin.TypedName{Type: PluginType, Name: name},
		params:      params,
		provisioner: provisioner,
	}
}

// TypedName returns the type and name tuple of this plugin instance.
func (p *Plugin) TypedName() fwkplugin.TypedName { return p.typedName }

// PreAdmit classifies the request and handles everything that can be decided
// before scheduling runs.
//
// This hook sees every request the EPP handles, so the origin header check is
// the first thing it does and an unrecognised origin costs one map lookup. In
// particular the handler's own frame submissions and all ordinary inference
// traffic fall through untouched.
func (p *Plugin) PreAdmit(ctx context.Context, request *fwksched.InferenceRequest) error {
	if request == nil {
		return nil
	}
	switch strings.ToLower(strings.TrimSpace(request.Headers[p.params.OriginHeader])) {
	case p.params.TriggerOrigin:
		return p.admitTrigger(request)
	case p.params.StopOrigin:
		return p.releaseSession(ctx, request)
	default:
		return nil
	}
}

// admitTrigger validates a provisioning request and stashes the result for
// PreRequest. Validation lives here because PreAdmit is the last hook in the
// pipeline whose error still reaches the caller as a status code.
func (p *Plugin) admitTrigger(request *fwksched.InferenceRequest) error {
	var payload fwkrh.RequestPayload
	if request.Body != nil {
		payload = request.Body.Payload
	}
	trigger, err := p.params.parseTrigger(request.Headers, payload)
	if err != nil {
		return errcommon.Error{
			Code: errcommon.BadRequest,
			Msg:  fmt.Sprintf("%s: %v", PluginType, err),
		}
	}
	request.PutAttribute(triggerAttributeKey, trigger)
	return nil
}

// releaseSession ends the handler named by a stop request.
//
// A malformed session id is caller error and gets a 400, but a failure to
// actually delete the Job only logs: the caller is a browser closing a tab, it
// cannot act on the failure, and the handler's own backstops still bound the
// damage. Failing the request here would turn a best-effort cleanup into a
// user-visible error.
func (p *Plugin) releaseSession(ctx context.Context, request *fwksched.InferenceRequest) error {
	sessionID := strings.TrimSpace(request.Headers[p.params.SessionHeader])
	if err := validateSessionID(sessionID); err != nil {
		return errcommon.Error{
			Code: errcommon.BadRequest,
			Msg:  fmt.Sprintf("%s: stop request: %v", PluginType, err),
		}
	}

	logger := log.FromContext(ctx).WithValues("plugin", p.typedName.String(), "sessionID", sessionID)
	if err := p.provisioner.Release(ctx, sessionID); err != nil {
		logger.Error(err, "Failed to release stream handler; it will exit on its own backstops")
		return nil
	}
	logger.Info("Released stream handler")
	return nil
}

// PreRequest provisions the handler now that scheduling has picked an endpoint.
//
// Note the signature: at llm-d-router v0.9.0 PreRequest cannot return an error,
// so a provisioning failure is logged and the request proceeds. The trigger POST
// itself still succeeds; what the user sees is a session that produces no frames.
// The upstream interface grew an error return after v0.9.0, and this should
// return one as soon as the pinned version does.
func (p *Plugin) PreRequest(ctx context.Context, request *fwksched.InferenceRequest, schedulingResult *fwksched.SchedulingResult) {
	if request == nil {
		return
	}
	trigger, ok := fwksched.ReadRequestAttribute[sessionRequest](request, triggerAttributeKey)
	if !ok {
		// Not a trigger request, or PreAdmit rejected it.
		return
	}

	logger := log.FromContext(ctx).WithValues("plugin", p.typedName.String(), "sessionID", trigger.SessionID)

	poolEndpoint, err := p.decodeEndpoint(schedulingResult)
	if err != nil {
		logger.Error(err, "Cannot provision stream handler without a decode endpoint")
		return
	}

	assignment := Assignment{
		SessionID:     trigger.SessionID,
		StreamURL:     trigger.StreamURL,
		PoolEndpoint:  poolEndpoint,
		Prompt:        trigger.Prompt,
		FrameInterval: trigger.FrameInterval,
	}

	assignment.ResultsCallbackURL = p.resultsCallbackURL(trigger)

	if err := p.provisioner.Acquire(ctx, assignment); err != nil {
		logger.Error(err, "Failed to provision stream handler; this session will receive no frames")
		return
	}
	logger.Info("Provisioned stream handler",
		"poolEndpoint", poolEndpoint, "streamURL", assignment.StreamURL,
		"resultsCallback", assignment.ResultsCallbackURL)
}

// resultsCallbackURL is where this session's inference output should be delivered.
//
// The caller's own callback wins over the configured one. The frontend runs more
// than one replica and each session's browser socket lives in exactly one of
// them, so the configured Service URL would load-balance results to a replica
// that knows nothing about the session. Only the caller can name the replica
// that is actually holding the socket, which is why it gets to.
//
// The configured base remains the fallback for a caller that names no callback --
// a curl-driven test, or a frontend behind a single replica. Empty means no
// callback at all, and the rendered Job simply drops the env entry.
func (p *Plugin) resultsCallbackURL(trigger sessionRequest) string {
	base := trigger.ResultsCallback
	if base == "" {
		base = p.params.ResultsCallbackBaseURL
	}
	if base == "" {
		return ""
	}
	return base + "/" + trigger.SessionID
}

// decodeEndpoint turns the scheduling result into the base URL the handler will
// post frames to.
//
// It prefers the configured decode profile and falls back to the result's primary
// profile, which the disagg profile handler sets to its own decode profile. The
// fallback covers a configuration whose profile is named something other than
// "decode" without requiring the two configs to be kept in sync by hand.
func (p *Plugin) decodeEndpoint(result *fwksched.SchedulingResult) (string, error) {
	if result == nil {
		return "", errors.New("scheduling returned no result")
	}

	run := result.ProfileResults[p.params.DecodeProfile]
	if run == nil || len(run.TargetEndpoints) == 0 {
		if result.PrimaryProfileName != "" && result.PrimaryProfileName != p.params.DecodeProfile {
			run = result.ProfileResults[result.PrimaryProfileName]
		}
	}
	if run == nil || len(run.TargetEndpoints) == 0 {
		return "", fmt.Errorf("no target endpoint for profile %q (primary profile %q)",
			p.params.DecodeProfile, result.PrimaryProfileName)
	}

	meta := run.TargetEndpoints[0].GetMetadata()
	if meta == nil || meta.Address == "" {
		return "", fmt.Errorf("profile %q picked an endpoint with no address", p.params.DecodeProfile)
	}
	if meta.Port == "" {
		return "", fmt.Errorf("profile %q picked endpoint %s with no port", p.params.DecodeProfile, meta.Address)
	}
	// Plain HTTP: the handler talks straight to a pod IP inside the cluster,
	// bypassing the gateway, and the model servers serve h2c/HTTP there.
	return "http://" + net.JoinHostPort(meta.Address, meta.Port), nil
}
