# llm-d-stream-handler-plugin

An out-of-tree [llm-d](https://llm-d.ai) EPP plugin that provisions an RTSP
stream handler Job once the router has chosen the inference endpoint a session
will use, and reaps it when the session ends.

It is the "gateway hook (not in this repo)" that
[`droidcam-rtsp-ingestor`](https://github.com/Tanchwa/droidcam-rtsp-ingestor)
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

## Where it hooks in, and why not where you would expect

The obvious target is the disagg profile handler's `pickDecodeFirst` — that is
where the decode endpoint is chosen. It is the wrong seam twice over:

- it lives inside `pkg/epp/framework/plugins/scheduling/.../disagg`, so using it
  means **forking llm-d-router**; and
- `Pick()` is re-entered once per stage (decode → encode → prefill) until it
  returns an empty map, so a side effect there fires at ambiguous times and can
  fire more than once per request.

This plugin instead uses two supported request-control extension points, and
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
automatic prefix cache prefills once and reuses from frame two onward. The
images change and must be re-encoded every time; that cost is irreducible. The
text is free after the first request.

That reuse only survives if frames keep landing on the **same pod**, which is
exactly what pinning `POOL_ENDPOINT` to the scheduled endpoint guarantees.
Re-scheduling per frame would scatter across pods and cold-miss the prefix every
hop.

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

## Installation runbook

Installing this means three changes: the EPP runs a **different image**, its
**config gains a plugin block**, and its ServiceAccount gains **permission in the
handler namespace**. Get any one of them wrong and the failure is silent — the
EPP serves traffic normally and simply never provisions a handler.

Values below are the ones verified against the `test-single` cluster. Replace
them for other environments; step 0 tells you how to find each one.

> **If ArgoCD manages your EPP, do not use `kubectl` for steps 2 and 3.**
> In test-single the `llm-d-quickstart` and `cellphone-cam` Applications both run
> `selfHeal: true`, so a `kubectl set image` or `kubectl edit cm` is reverted on
> the next sync — usually within minutes, and with no error to tell you why the
> plugin stopped loading. Every change below goes through Git.

### Step 0 — Gather the five facts you need

```bash
export KUBECONFIG=~/.kube/test-single.config
LLMD_NS=llm-d-quickstart          # namespace the EPP runs in
EPP=llm-d-quickstart-epp          # EPP Deployment name

# 1. the EPP ServiceAccount -- the identity that needs RBAC
kubectl -n $LLMD_NS get deploy $EPP \
  -o jsonpath='{.spec.template.spec.serviceAccountName}{"\n"}'

# 2. the config file it loads, and 3. its current image
kubectl -n $LLMD_NS get deploy $EPP -o jsonpath='{.spec.template.spec.containers[0].args}{"\n"}'
kubectl -n $LLMD_NS get deploy $EPP -o jsonpath='{.spec.template.spec.containers[0].image}{"\n"}'

# 4. the scheduling profile names -- decodeProfile must match one of these
kubectl -n $LLMD_NS get cm $EPP -o jsonpath='{.data}' | grep -A3 schedulingProfiles

# 5. is any of it GitOps-managed?
kubectl -n $LLMD_NS get deploy $EPP \
  -o jsonpath='{.metadata.annotations.argocd\.argoproj\.io/tracking-id}{"\n"}'
```

For test-single that yields: SA `llm-d-quickstart-epp`, config
`/config/optimized-baseline-plugins.yaml`, image
`ghcr.io/llm-d/llm-d-router-endpoint-picker:main`, **one profile named
`default`** (no disagg handler), and yes — ArgoCD manages all of it.

That fourth fact matters most. A stock quickstart has no profile called
`decode`, so setting `decodeProfile: decode` would rely on the plugin's
primary-profile fallback. It works, but name the profile explicitly so a later
switch to disagg does not change behaviour silently.

### Step 1 — Build and push the EPP image

```bash
make image push IMAGE_REPO=docker.io/<you>/llm-d-stream-handler-epp IMAGE_TAG=v0.1.0
```

Confirm the plugin is compiled in before shipping it anywhere. A registered type
instantiates; an unregistered one is rejected by name:

```bash
docker run --rm --entrypoint /epp \
  docker.io/<you>/llm-d-stream-handler-epp:v0.1.0 --help >/dev/null && echo "binary ok"
```

### Step 2 — Grant the EPP permission in the handler namespace

The EPP's SA starts with nothing here. Verify that first, so you can tell the
grant actually did something:

```bash
SA=system:serviceaccount:llm-d-quickstart:llm-d-quickstart-epp
kubectl auth can-i create jobs --as=$SA -n cellphone-cam    # expect: no
```

[`deploy/rbac.yaml`](deploy/rbac.yaml) is a RoleBinding only — the Role it
references is owned by the ingestor repo and synced by the `cellphone-cam`
ArgoCD Application, so shipping a duplicate here would be drift that ArgoCD
reverts.

**Three namespaces are in play and they mean different things**, which is the
usual reason this step goes wrong:

| | namespace | what it controls |
|---|---|---|
| RoleBinding | `cellphone-cam` | **where the permissions apply** |
| Role (`roleRef`) | `cellphone-cam` | must resolve in the binding's *own* namespace |
| Subject (the EPP's SA) | `llm-d-quickstart` | may be **anywhere** |

The binding goes in the namespace you want to act *on*, not the one the EPP runs
*in*. That asymmetry is the entire mechanism behind a cross-namespace grant.

Note also that `subjects` is matched **by name string** — no UID, no
ownerReference, and nothing is written back to the ServiceAccount. A binding
naming a ServiceAccount that does not exist is perfectly valid and is stored
without complaint, granting nothing, with no error or event. That is exactly the
state test-single is in today. Never infer from a binding's existence that it
works; only `kubectl auth can-i` tells you that.

Because that Application syncs `k8s/` from
`github.com/tanchwa/droidcam-rtsp-ingestor`, the durable fix is **in that repo**:
edit `k8s/handler/rbac.yaml` and change the RoleBinding subject from the
placeholder `llm-d-gateway-hook`/`llm-d` to your real EPP SA. The placeholder
names a namespace that does not exist in test-single, which is why the binding
grants nothing today.

Validate before committing, then confirm after the sync:

```bash
kubectl apply -f deploy/rbac.yaml --dry-run=server     # validates, creates nothing

kubectl auth can-i create jobs    --as=$SA -n cellphone-cam   # now: yes
kubectl auth can-i delete jobs    --as=$SA -n cellphone-cam   # now: yes
kubectl auth can-i get configmaps --as=$SA -n cellphone-cam   # now: yes
kubectl auth can-i create jobs    --as=$SA -n default         # still: no
```

That last line is not optional. A binding that grants more than one namespace
means the `roleRef` picked up a ClusterRole by mistake.

### Step 3 — Point the EPP at the new image and config

[`deploy/router-values.yaml`](deploy/router-values.yaml) holds both changes as
Helm values for the `llm-d-router-gateway` chart. Render it first — this touches
nothing and catches a moved chart key immediately:

```bash
helm template llm-d-quickstart \
  oci://ghcr.io/llm-d/charts/llm-d-router-gateway --version v0 \
  -f deploy/router-values.yaml | grep -E 'image:|config-file'
```

You should see your image and `/config/stream-handler-plugins.yaml`.

In test-single the EPP comes from an ArgoCD Application that renders that chart
with inline values, so **paste the `router.epp` block into the Application's
`spec.sources[].helm.values`** and commit. Do not `kubectl edit` the ConfigMap;
`selfHeal` will undo it.

Without ArgoCD, pass the file to Helm directly:

```bash
helm upgrade llm-d-quickstart oci://ghcr.io/llm-d/charts/llm-d-router-gateway \
  --version v0 -n llm-d-quickstart --reuse-values -f deploy/router-values.yaml
```

### Step 4 — Roll out and confirm the plugin loaded

The EPP reads its config once at startup, so a ConfigMap change alone does
nothing until the pod restarts.

```bash
kubectl -n $LLMD_NS rollout restart deploy/$EPP
kubectl -n $LLMD_NS rollout status  deploy/$EPP --timeout=180s
kubectl -n $LLMD_NS get deploy $EPP -o jsonpath='{.spec.template.spec.containers[0].image}{"\n"}'
```

A missing or misspelled plugin type is a **hard startup failure**, not a warning,
so a pod that reaches Ready has loaded the plugin:

```bash
kubectl -n $LLMD_NS logs deploy/$EPP | grep -i "stream-handler\|not registered\|Failed to parse configuration"
```

`plugin type '...' is not registered` means the image does not contain the
plugin — step 1 or step 3 landed wrong.

### Step 5 — Smoke-test a session

Start a session from the frontend, then watch for the Job. The plugin only acts
on requests carrying `x-llmd-frame-source: frontend-trigger`, so ordinary traffic
proves nothing here.

```bash
kubectl -n cellphone-cam get jobs -l app.kubernetes.io/name=cellphone-camera-handler -w
```

When one appears, check that the **scheduled** endpoint was injected — this is
the whole point of the plugin:

```bash
SESSION=<session-id>
POD=$(kubectl -n cellphone-cam get pod -l cellphone-camera.io/session-id=$SESSION -o name | head -1)
kubectl -n cellphone-cam get $POD \
  -o jsonpath='{range .spec.containers[0].env[*]}{.name}={.value}{"\n"}{end}'
```

`POOL_ENDPOINT` must be a real decode pod IP and port. Cross-check it against the
pick the EPP logged for that request id. Then hit **Stop** and confirm the Job is
deleted rather than lingering to its 300s backstop:

```bash
kubectl -n cellphone-cam get jobs -l cellphone-camera.io/session-id=$SESSION
```

### Rollback

Revert the Git commit from step 3 and let ArgoCD sync, or:

```bash
helm rollback llm-d-quickstart -n llm-d-quickstart
kubectl -n cellphone-cam delete rolebinding llm-d-epp-handler-provisioner
```

Dropping the RoleBinding alone is a useful half-measure: the plugin stays loaded
but every provision fails with a logged RBAC error, leaving routing untouched.

### Troubleshooting

| symptom | cause |
|---|---|
| EPP pod crash-loops with `plugin type '...' is not registered` | image does not contain the plugin, or the type name is misspelled in the config |
| EPP healthy, no Job ever created, nothing in logs | requests are not carrying `x-llmd-frame-source: frontend-trigger` — the plugin ignores everything else by design |
| `no target endpoint for profile "decode"` in logs | `decodeProfile` names a profile that does not exist; check step 0, fact 4 |
| `jobs.batch is forbidden` in logs | step 2 RBAC missing, or the RoleBinding is in the wrong namespace |
| Jobs created but never deleted | `sessionLabel` does not match the label the job template stamps |
| Everything works, then stops after a few minutes | ArgoCD `selfHeal` reverted a `kubectl` change — redo it in Git |
| Handler pod runs but produces nothing | check the Job's `RESULTS_CALLBACK_URL`: `kubectl -n cellphone-cam get job -l cellphone-camera.io/session-id=<id> -o jsonpath='{.items[0].spec.template.spec.containers[0].env}'`. Absent means the template has no `${RESULTS_CALLBACK_URL}` entry, or neither the caller nor `resultsCallbackBaseURL` named one |
| Trigger returns 400 mentioning a callback | the caller's `x-cellphone-camera-results-callback` is outside the allowlist; widen `allowedResultsCallbackCIDRs`/`allowedResultsCallbackDomains` or fix the caller |

### Version pin

This module pins `github.com/llm-d/llm-d-router v0.9.0`, matching the version the
llm-d umbrella repo references.

**The EPP image running in test-single is
`ghcr.io/llm-d/llm-d-router-endpoint-picker:main`**, which is newer. That is not
a conflict — this repo ships its own EPP binary, so the pin only decides which
router version *this* binary embeds — but moving the pin to `main` would buy
back three things v0.9.0 lacks:

- `PreRequest` returning an `error`, so provisioning failures fail the request
- `Response.TerminationCause` and `StreamedEvents`, enough to tell a truncated
  stream from a completed one
- plugin stability levels on `Register`

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
