# llm-d-stream-handler-plugin

/n out-of-tree [llm-d](https://llm-d.ai) EPP plugin that provisions an RTSP
stream handler Job once the router has chosen the inference endpoint a session
will use, and reaps it when the session ends.

It is the "gateway hook (not in this repo)" that [`droidcam-rtsp-ingestor`](https://github.com/Tanchwa/droidcam-rtsp-ingestor)
describes but does not implement.

```
browser ──ws──► frontend ──trigger POST──► llm-d gateway ──► EPP
                                                              │ this plugin, once
                                                              │ the decode endpoint
                                                              ▼ is known
                                              handler Job ◄── creates
                                                   │
                                   frames ─────────┘──► the assigned endpoint
```

## Where it hooks in
This plugin uses two supported request-control extension points, and
forks nothing:

| hook | when | what it does |
|---|---|---|
| `PreAdmit` | before admission | validates a trigger, or services a stop |
| `PreRequest` | after the full scheduling cycle | provisions against the chosen endpoint |

Validation is deliberately in `PreAdmit`: it is the last hook whose returned
error still reaches the caller as an HTTP status, so malformed input becomes a
400 instead of a handler that silently never appears.

The split matters because at llm-d-router v0.9.0 **`PreRequest` has no error
return**. A provisioning failure there can only be logged — the trigger POST
still succeeds and the user sees a session that produces no frames. Upstream
added an error return after v0.9.0; this should return one as soon as the pin
moves. See [Version pin](#version-pin).

### Why pinning the endpoint matters beyond routing

The handler posts every frame as `[{text: prompt}, {image_url: frame}]`. The
text is identical on every request, so it is a **constant prefix** that vLLM's
automatic prefix cache prefills once and reuses from frame two onward.
That reuse only survives if frames keep landing on the **same pod**, which is
exactly what pinning `POOL_ENDPOINT` to the scheduled endpoint guarantees.
Re-scheduling per frame would scatter across pods and cold-miss the prefix every
hop.

**Human's note:** I will explore more detailed caching for frames and further implementation of disagg later, see the [Not done here](#not-done-here) section. 
but the current design for this POC is to handle inference of real time streams, 
and the assumption is that only text is constant.

Note this is *vLLM's own on-pod cache*, not llm-d's `prefix-cache-scorer` or
`mm-embeddings-cache-scorer`. Those act on gateway scheduling decisions, and the
handler's frame POSTs never reach the EPP at all.

## The request contract

Dispatch is on one header, checked first, so ordinary traffic costs a single map
lookup and provisions nothing:

| `x-llmd-frame-source` | effect |
|---|---|
| `frontend-trigger` | validate, then provision against the scheduled endpoint |
| `frontend-stop` | delete the session's handler Job |
| anything else, or absent | **ignored** |

That last row includes the handler's own frame submissions
(`cellphone-camera-handler`), which is what stops a session from provisioning a
second handler for itself.

**Human's note:** This is actually an artifact of when I was first designing this 
with Claud. Claude initially assumed I wanted to go through the gateway for every frame,
but now the design pins the handler to the scheduled endpoint and bypasses the gateway entirely.
This technically makes the above statement currently impossible, 
but I left in the safety guard for when we do impliment in the future.
See the [Not done here](#not-done-here) section for more details.

A trigger carries its session id and camera URL in headers, falling back to the
request body's `cellphone-camera` object when a gateway strips unknown headers.
Headers win. What lands on the handler pod:

| env var | source |
|---|---|
| `SESSION_ID` | `x-llmd-session-id`, or body `session_id` |
| `STREAM_URL` | `x-cellphone-camera-stream-url`, or body `cellphone-camera.stream_url` |
| `POOL_ENDPOINT` | **the endpoint llm-d scheduled**, as `http://<address>:<port>` |
| `RESULTS_CALLBACK_URL` | `x-cellphone-camera-results-callback`, else `resultsCallbackBaseURL`; the session id is appended either way |
| `PROMPT`, `FRAME_INTERVAL` | optional; **the env entry is dropped when unset** |

Dropping rather than blanking the optional pair is required, not cosmetic. They
are overrides layered on an `envFrom` ConfigMap of defaults: a blank `PROMPT`
would override the default with nothing, and a blank `FRAME_INTERVAL` crashes the
handler on `float("")`.

## Results go back to the caller that asked for them

`RESULTS_CALLBACK_URL` is how a session's inference output reaches the browser.
The handler posts frames straight to the assigned pod, bypassing Envoy, so
nothing in llm-d ever observes them — and an ext_proc filter cannot splice one
request's response into another's stream. The callback is the return path, and
it is the only one there is.

**The caller names it, and that beats the configured base.** The frontend runs
two replicas and each session's WebSocket lives entirely in one pod's memory, so
results sent to the frontend *Service* would be load-balanced to a replica that
has never heard of the session. Only the caller knows which pod is holding the
socket, so the trigger carries its own address in
`x-cellphone-camera-results-callback` (or body `cellphone-camera.results_callback`)
and the plugin appends the session id to it.

`resultsCallbackBaseURL` stays as the fallback for a caller that names nothing —
a `curl`-driven test, or a frontend behind a single replica. With neither set,
no env entry is injected at all and the handler falls through to its own
defaults.

**A caller-supplied callback is checked against an allowlist.** This is stricter
than the camera-URL check above, and deliberately so: a camera address is
arbitrary user LAN and cannot be enumerated, but the only legitimate callbacks
are frontend pods inside this cluster. Left unchecked, the header would let
anything that can reach the gateway choose where a handler sends inference
output. A callback must be `http`/`https`, carry no credentials, carry no query
or fragment (the session id is appended to it), and resolve to either an address
inside `allowedResultsCallbackCIDRs` or a name under
`allowedResultsCallbackDomains`. Loopback, link-local — which is where cloud
instance metadata lives — and the unspecified address are refused whatever those
are set to. A callback that fails any of this fails the trigger with a 400
rather than quietly falling back to the configured base: sending a caller's
results somewhere it did not ask for is worse than telling it the header was
wrong.

**Human's note:** Eventually, this is going to move to a version where callers will
*almost* always be outside the cluster.
The allowlist and callback url header are compromises for now, 
and the long term plan is to have a more robust solution, 
possibly with a more robust authentication mechanism, sticky sessions
proxied through the gateway, or a more robust callback mechanism.

## Template rendering is injection-safe by construction

The handler Job comes from a ConfigMap template with `${...}` placeholders, so
its shape stays owned and reviewed by the ingestor repo rather than hardcoded in
this binary.

Substitution happens on the **decoded object**, never on the YAML source. The
template is parsed into a `batchv1.Job` first, and only then are placeholders
replaced in `metadata.name`, label values, and container env values. `STREAM_URL`
is arbitrary text from a browser field; splicing it into YAML before parsing
would let a crafted camera URL containing a quote and a newline rewrite the
surrounding pod spec. Parsing first makes every value a leaf string that cannot
restructure the document no matter what it contains. There is a test for exactly
this.

Two further rendering rules:

- **The configured namespace overrides the template's.** The ingestor's template
  hardcodes `namespace: cellphone-camera` while the deployed namespace is
  `cellphone-cam`; honouring the template would produce Jobs the plugin's
  namespace-scoped RBAC forbids.
- **An unresolvable label is a hard error.** The session-id label is how the stop
  path finds the Job again, so a Job carrying a blank one could never be reaped.

## Configuration

See [`deploy/epp-config.yaml`](deploy/epp-config.yaml) for a complete
`EndpointPickerConfig`.

```yaml
- type: stream-handler-provisioner
  parameters:
    namespace: cellphone-cam
    jobTemplateConfigMap: cellphone-camera-handler-job-template
    decodeProfile: decode
    resultsCallbackBaseURL: http://cellphone-camera-frontend.cellphone-cam.svc.cluster.local:8080/ingest
    allowedStreamSchemes: [http, https, rtsp]
```

| parameter | default | notes |
|---|---|---|
| `namespace` | *required* | where Jobs are created and the template is read |
| `jobTemplateConfigMap` | *required* | ConfigMap holding the Job template |
| `jobTemplateKey` | `job.yaml` | key within it |
| `decodeProfile` | `decode` | **falls back to the result's primary profile** |
| `resultsCallbackBaseURL` | *(none)* | **fallback only**; a caller's own callback header wins |
| `resultsCallbackHeader` | `x-cellphone-camera-results-callback` | where the caller names its callback |
| `allowedResultsCallbackCIDRs` | private + CGNAT + ULA ranges | addresses a caller's callback may resolve to |
| `allowedResultsCallbackDomains` | `svc.cluster.local` | name suffixes a caller's callback may use |
| `allowedStreamSchemes` | `http,https,rtsp` | scheme allowlist for camera URLs |
| `sessionLabel` | `cellphone-camera.io/session-id` | must match the template's label |
| `templateCacheTTL` | `30s` | `0` disables caching |

Two names have to agree and are **not** checked at startup: `decodeProfile`
against the disagg handler's `profiles.decode`, and `sessionLabel` against the
label the template stamps. A mismatch on the first logs instead of provisioning;
a mismatch on the second creates handlers that are never reaped.

The `decodeProfile` fallback is load-bearing in practice. A stock llm-d
quickstart runs a single profile named `default` with no disagg handler at all;
the fallback to `PrimaryProfileName` makes the plugin work there unchanged.

### Scheme allowlist is not an SSRF defence

`allowedStreamSchemes` stops a camera URL naming something like `file://`, but
the legitimate destinations are arbitrary user LAN addresses and cannot be
enumerated in advance. If you need real egress control, pair it with a
NetworkPolicy on the handler pods.

## Installation

**note** this instalation is currently based off the stream handler in [tanchwa/droidcam-rtsp-ingestor](https://github.com/tanchwa/droidcam-rtsp-ingestor) as a POC for getting the llm-d hook working.
Eventually, I will be trying to incorperate this with my company's existing stream handler.

### 0. Build the plugin image

### 1. Deploy the EPP with this plugin config
EPP Plugins and config are exposed in llm-d's routerlib [values.yaml](https://github.com/llm-d/llm-d-router/blob/main/config/charts/routerlib/values.yaml). Although you will have to call the chart through either the llm-d-router-gateway or llm-d-router-standalone charts, it calls this same chart under the hood. Whichever one you use, add your configuration as shown in [Configuration](#Configuration)

### 2. Grant the EPP permission in the handler namespace

The EPP's SA starts with nothing here. Verify that first, so you can tell the
grant actually did something:

```bash
SA=system:serviceaccount:llm-d-quickstart:llm-d-quickstart-epp
kubectl auth can-i create jobs --as=$SA -n <your job namespace>    # expect: no
```

[`deploy/rbac.yaml`](deploy/rbac.yaml) is a RoleBinding only — the Role it references is in the afformentioned [repo](https://github.com/tanchwa/droidcam-rtsp-ingestor)

**Three namespaces are in play and they mean different things**, which is the
usual reason this step goes wrong:

| | namespace | what it controls |
|---|---|---|
| RoleBinding | `<your job namespace>` | **where the permissions apply** |
| Role (`roleRef`) | `<your job namespace>` | must resolve in the binding's *own* namespace |
| Subject (the EPP's SA) | `llm-d-quickstart` | may be **anywhere** |

The binding goes in the namespace you want to act *on*, not the one the EPP runs
*in*. That asymmetry is the entire mechanism behind a cross-namespace grant.

### Troubleshooting

| symptom | cause |
|---|---|
| EPP pod crash-loops with `plugin type '...' is not registered` | image does not contain the plugin, or the type name is misspelled in the config |
| EPP healthy, no Job ever created, nothing in logs | requests are not carrying `x-llmd-frame-source: frontend-trigger` — the plugin ignores everything else by design |
| `no target endpoint for profile "decode"` in logs | `decodeProfile` names a profile that does not exist |
| `jobs.batch is forbidden` in logs | step 2 RBAC missing, or the RoleBinding is in the wrong namespace |
| Jobs created but never deleted | `sessionLabel` does not match the label the job template stamps |
| Everything works, then stops after a few minutes | ArgoCD `selfHeal` reverted a `kubectl` change — redo it in Git |
| Handler pod runs but produces nothing | check the Job's `RESULTS_CALLBACK_URL`: `kubectl -n cellphone-cam get job -l cellphone-camera.io/session-id=<id> -o jsonpath='{.items[0].spec.template.spec.containers[0].env}'`. Absent means the template has no `${RESULTS_CALLBACK_URL}` entry, or neither the caller nor `resultsCallbackBaseURL` named one |
| Trigger returns 400 mentioning a callback | the caller's `x-cellphone-camera-results-callback` is outside the allowlist; widen `allowedResultsCallbackCIDRs`/`allowedResultsCallbackDomains` or fix the caller |

### Version pin

This module pins `github.com/llm-d/llm-d-router v0.9.0`, matching the version the
llm-d umbrella repo references.

## Testing

```bash
go test ./...                                   # unit, no cluster needed
KUBECONFIG=~/.kube/test-single.config \
  go test -tags integration ./pkg/streamhandler/ -run TestLive -v
```

The live test is behind a build tag and **impersonates the EPP's
ServiceAccount**, so it exercises the real RBAC path rather than an admin's
ambient power. It creates one Job and deletes it, with cleanup registered before
the first assertion.

`testdata/job-template.yaml` is captured from the cluster rather than
hand-written, so the renderer is tested against the template that actually
exists. Refresh it with:

```bash
kubectl -n cellphone-cam get cm cellphone-camera-handler-job-template \
  -o jsonpath='{.data.job\.yaml}' > pkg/streamhandler/testdata/job-template.yaml
```

## Not done here

**Pre-warmed handler pooling.** `Provisioner` exists so this stays additive: a
pooled implementation would `Acquire` a lease and `Release` it back. It is not
built because it needs a handler that can receive an assignment after starting
(today's has no listener and reads only env) plus a cross-replica lease protocol,
since the EPP runs multiple replicas. Measure first — if cold start is dominated
by image pull rather than process start, a smaller image or node pre-pull is far
cheaper.

**Frame traffic is invisible to the EPP.** Bypassing the gateway is deliberate —
it is what keeps the prefix cache warm — but it means one pod absorbs a whole
stream unaccounted for by load-awareness, flow control, and metrics. Acceptable
for a POC; revisit as concurrent stream count grows. The alternative is routing
frames through the gateway with `session-id-producer` plus the `sessionaffinity`
scorer, both of which exist in-tree.
