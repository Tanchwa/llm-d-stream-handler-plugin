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
	"strings"
	"time"

	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	ctrl "sigs.k8s.io/controller-runtime"

	"k8s.io/client-go/kubernetes"
)

// PluginType is the type name used to select this plugin in an EndpointPickerConfig.
const PluginType = "stream-handler-provisioner"

// Defaults mirror the contract the cellphone-camera ingestor already ships, so a
// stock deployment needs to set only namespace, jobTemplateConfigMap and
// resultsCallbackBaseURL.
const (
	defaultJobTemplateKey        = "job.yaml"
	defaultDecodeProfile         = "decode"
	defaultSessionHeader         = "x-llmd-session-id"
	defaultOriginHeader          = "x-llmd-frame-source"
	defaultTriggerOrigin         = "frontend-trigger"
	defaultStopOrigin            = "frontend-stop"
	defaultStreamURLHeader       = "x-cellphone-camera-stream-url"
	defaultResultsCallbackHeader = "x-cellphone-camera-results-callback"
	defaultFrameIntervalHeader   = "x-cellphone-camera-frame-interval"
	defaultSessionLabel          = "cellphone-camera.io/session-id"
	defaultTemplateCacheTTL      = 30 * time.Second
)

// Parameters is the plugin's YAML configuration.
type Parameters struct {
	// Namespace is where handler Jobs are created and where the job template
	// ConfigMap is read from. The plugin's RBAC must grant it access there, and
	// it overrides whatever namespace the template hardcodes.
	Namespace string `json:"namespace"`
	// JobTemplateConfigMap names the ConfigMap holding the handler Job template.
	// Keeping the template in a ConfigMap rather than in this binary means the
	// shape of a handler pod is owned and reviewed by the ingestor repo.
	JobTemplateConfigMap string `json:"jobTemplateConfigMap"`
	// JobTemplateKey is the key within that ConfigMap. Defaults to job.yaml.
	JobTemplateKey string `json:"jobTemplateKey,omitempty"`
	// DecodeProfile is the scheduling profile whose pick becomes POOL_ENDPOINT.
	// It must match the disagg profile handler's profiles.decode. Defaults to
	// "decode"; if the named profile is absent from a result, the plugin falls
	// back to the result's primary profile.
	DecodeProfile string `json:"decodeProfile,omitempty"`
	// ResultsCallbackBaseURL is the fallback endpoint the handler forwards inference
	// output to, used only when the trigger names no callback of its own. The session
	// id is appended to form RESULTS_CALLBACK_URL. Leave empty to inject nothing and
	// let the handler use its own default behaviour.
	//
	// A caller-supplied callback wins over this because the caller knows something
	// this config cannot: which frontend replica is holding the browser's socket.
	// Sending results to the frontend Service instead would load-balance them
	// across replicas, and a replica that does not own the session has nowhere to
	// put them. Keep this set anyway -- it is what a caller that sends no callback
	// header falls back to, and it is the only callback a frontend behind a single
	// replica needs.
	ResultsCallbackBaseURL string `json:"resultsCallbackBaseURL,omitempty"`
	// ResultsCallbackHeader is the trigger header naming where that caller wants its
	// results delivered, as a base URL with no session id. It takes precedence
	// over ResultsCallbackBaseURL.
	ResultsCallbackHeader string `json:"resultsCallbackHeader,omitempty"`
	// AllowedResultsCallbackCIDRs and AllowedResultsCallbackDomains bound where a
	// caller-supplied callback may point. Unlike a camera address -- arbitrary user
	// LAN, impossible to enumerate -- the legitimate destinations here are known: they
	// are frontend pods inside this cluster, so an allowlist is both possible and
	// worth having. Without one, any client that can reach the gateway could aim
	// a handler's inference output at an address of its choosing.
	//
	// They default to the private ranges a pod IP comes from plus cluster Service
	// DNS. Loopback, link-local (which is where cloud instance metadata lives)
	// and the unspecified address are always refused, whatever these say.
	AllowedResultsCallbackCIDRs   []string `json:"allowedResultsCallbackCIDRs,omitempty"`
	AllowedResultsCallbackDomains []string `json:"allowedResultsCallbackDomains,omitempty"`
	// AllowedStreamSchemes is the allowlist a camera URL's scheme must match.
	// Defaults to http, https and rtsp.
	AllowedStreamSchemes []string `json:"allowedStreamSchemes,omitempty"`
	// SessionLabel is the label the job template stamps each handler Job with,
	// and the selector Release uses to find that Job again. It must match the
	// label the template sets or handlers will be created and never reaped.
	SessionLabel string `json:"sessionLabel,omitempty"`

	// The remaining fields describe the wire contract with the frontend and
	// handler. They exist so a deployment that renames a header does not need a
	// rebuild, and default to the names the ingestor uses today.
	SessionHeader       string `json:"sessionHeader,omitempty"`
	OriginHeader        string `json:"originHeader,omitempty"`
	TriggerOrigin       string `json:"triggerOrigin,omitempty"`
	StopOrigin          string `json:"stopOrigin,omitempty"`
	StreamURLHeader     string `json:"streamURLHeader,omitempty"`
	FrameIntervalHeader string `json:"frameIntervalHeader,omitempty"`

	// TemplateCacheTTL is how long a fetched job template is reused before being
	// re-read. It keeps provisioning off the API server's back without losing the
	// ability to change the template and have the next session pick it up.
	// Accepts a Go duration string; defaults to 30s. "0" disables caching.
	TemplateCacheTTL string `json:"templateCacheTTL,omitempty"`

	// resultsCallbackNets is AllowedResultsCallbackCIDRs parsed, filled in by validate so
	// the cost is paid at startup rather than on every trigger.
	resultsCallbackNets []netip.Prefix
}

// applyDefaults fills unset fields and normalises the ones used for matching.
// Header names are lowercased because the EPP lowercases every incoming header
// name before the plugin ever sees it.
func (p *Parameters) applyDefaults() {
	setDefault(&p.JobTemplateKey, defaultJobTemplateKey)
	setDefault(&p.DecodeProfile, defaultDecodeProfile)
	setDefault(&p.SessionHeader, defaultSessionHeader)
	setDefault(&p.OriginHeader, defaultOriginHeader)
	setDefault(&p.TriggerOrigin, defaultTriggerOrigin)
	setDefault(&p.StopOrigin, defaultStopOrigin)
	setDefault(&p.StreamURLHeader, defaultStreamURLHeader)
	setDefault(&p.ResultsCallbackHeader, defaultResultsCallbackHeader)
	setDefault(&p.FrameIntervalHeader, defaultFrameIntervalHeader)
	setDefault(&p.SessionLabel, defaultSessionLabel)

	p.SessionHeader = strings.ToLower(p.SessionHeader)
	p.OriginHeader = strings.ToLower(p.OriginHeader)
	p.StreamURLHeader = strings.ToLower(p.StreamURLHeader)
	p.ResultsCallbackHeader = strings.ToLower(p.ResultsCallbackHeader)
	p.FrameIntervalHeader = strings.ToLower(p.FrameIntervalHeader)
	// Origin values are compared lowercased too, so a config that capitalises
	// them still matches what arrives on the wire.
	p.TriggerOrigin = strings.ToLower(p.TriggerOrigin)
	p.StopOrigin = strings.ToLower(p.StopOrigin)

	if len(p.AllowedStreamSchemes) == 0 {
		p.AllowedStreamSchemes = []string{"http", "https", "rtsp"}
	}
	for i, s := range p.AllowedStreamSchemes {
		p.AllowedStreamSchemes[i] = strings.ToLower(strings.TrimSpace(s))
	}

	if len(p.AllowedResultsCallbackCIDRs) == 0 {
		// Every range a Kubernetes pod IP is drawn from in practice: RFC 1918,
		// the carrier-grade NAT block some CNIs allocate from, and IPv6 ULA.
		p.AllowedResultsCallbackCIDRs = []string{
			"10.0.0.0/8",
			"172.16.0.0/12",
			"192.168.0.0/16",
			"100.64.0.0/10",
			"fc00::/7",
		}
	}
	for i, c := range p.AllowedResultsCallbackCIDRs {
		p.AllowedResultsCallbackCIDRs[i] = strings.TrimSpace(c)
	}
	if len(p.AllowedResultsCallbackDomains) == 0 {
		p.AllowedResultsCallbackDomains = []string{"svc.cluster.local"}
	}
	for i, d := range p.AllowedResultsCallbackDomains {
		p.AllowedResultsCallbackDomains[i] = strings.ToLower(strings.Trim(strings.TrimSpace(d), "."))
	}

	p.ResultsCallbackBaseURL = strings.TrimRight(p.ResultsCallbackBaseURL, "/")
}

// validate checks the parameters that have no sensible default.
func (p *Parameters) validate() error {
	if p.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	if p.JobTemplateConfigMap == "" {
		return fmt.Errorf("jobTemplateConfigMap is required")
	}
	if p.TriggerOrigin == p.StopOrigin {
		return fmt.Errorf("triggerOrigin and stopOrigin must differ, both are %q", p.TriggerOrigin)
	}
	// Parsed once here rather than per request, and reported at startup rather
	// than as a puzzling 400 on the first session.
	p.resultsCallbackNets = p.resultsCallbackNets[:0]
	for _, c := range p.AllowedResultsCallbackCIDRs {
		n, err := netip.ParsePrefix(c)
		if err != nil {
			return fmt.Errorf("allowedResultsCallbackCIDRs entry %q is not a valid CIDR: %w", c, err)
		}
		p.resultsCallbackNets = append(p.resultsCallbackNets, n)
	}
	if _, err := p.cacheTTL(); err != nil {
		return err
	}
	return nil
}

func (p *Parameters) cacheTTL() (time.Duration, error) {
	if p.TemplateCacheTTL == "" {
		return defaultTemplateCacheTTL, nil
	}
	d, err := time.ParseDuration(p.TemplateCacheTTL)
	if err != nil {
		return 0, fmt.Errorf("templateCacheTTL %q is not a valid duration: %w", p.TemplateCacheTTL, err)
	}
	if d < 0 {
		return 0, fmt.Errorf("templateCacheTTL must not be negative, got %s", d)
	}
	return d, nil
}

func setDefault(field *string, value string) {
	if *field == "" {
		*field = value
	}
}

// Factory builds the plugin from its YAML parameters. It is registered with the
// framework in cmd/epp/main.go.
func Factory(name string, parameters *json.Decoder, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
	params := Parameters{}
	if parameters != nil {
		if err := parameters.Decode(&params); err != nil {
			return nil, fmt.Errorf("failed to parse parameters of the %s plugin: %w", PluginType, err)
		}
	}
	params.applyDefaults()
	if err := params.validate(); err != nil {
		return nil, fmt.Errorf("invalid parameters for the %s plugin: %w", PluginType, err)
	}

	// Built here rather than lazily so a cluster the EPP cannot reach fails at
	// startup, where it is obvious, instead of on the first user session.
	cfg, err := ctrl.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("%s could not load a Kubernetes client config: %w", PluginType, err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("%s could not build a Kubernetes client: %w", PluginType, err)
	}

	ttl, err := params.cacheTTL()
	if err != nil {
		return nil, err
	}

	return New(name, params, NewJobProvisioner(client, params, ttl)), nil
}
