# Relay System — Design (V3)

Owner: Jack
Reviewers: Scarlet (team lead), Aidan (deploy), Donovan (prior art + bitdex-server entrypoint), Tom (cluster-side)
Date: 2026-04-25
Status: V3, post-Justin-toggle-clarification (env-var-dispatch + hot-config aspiration) and post-GPT/Gemini design review fold-in. Replaces V2 (separate-image-swap, partial review fold) and V1 (consume-prod-SSE, wrong shape). Locked against Justin's V2 brief + the GPT/Gemini review must-fix lists.

---

## 1. Vision

A single Rust binary that emulates the HTTP surface of `bitdex-server` while doing zero bitmap work when in relay mode. It accepts requests on configured routes, emits an SSE event carrying the request body onto a configured channel, and returns a configured stub response. The binary boots into one of two modes:

- `BITDEX_MODE=server` (default) — the existing `bitdex-server` behavior, unchanged.
- `BITDEX_MODE=relay` — pure dummy relay; no bitmap engine started.

The relay **replaces** the `bitdex-server` runtime in the production StatefulSet during iteration windows via a `kubectl set env` flip — no manifest commit needed for V1.

Justin's framing:

> "It can essentially just be a relay server where any HTTP requests that come in, we route to essentially a listener through SSE."
> "I think b. Best would be hot config."
> "Relay mode should literally just be passing through exactly what requests come in. So, it's just a simple proxy essentially... as soon as there's a listener, it starts streaming. Stick with simple for now."
> "Pure dummy."

### Use cases (V1)

1. **Iterate on the local rig without owning prod compute.** Production pod runs the relay; real PG-sync ops + real client queries hit the relay; engineers consume relay's SSE stream from local rigs to drive their own BitDex instances.
2. **Save prod CPU/RAM during perf experiments (aggressive opt-in mode only — see §9.6.1).** Relay's ~200 MiB process replaces bitdex-server's ~25 GiB. Default rollout keeps the 14 GiB resource request reserved for safe flip-back; only the explicit aggressive mode delivers actual node-pressure savings, with the eviction-on-flip-back tradeoff documented and accepted by the operator.
3. **Foundation for the data-quality verifier** (separate workstream).

### Non-goals (V1)

- Multi-channel emit per route (V2)
- Hot config reload at runtime (V2 — see §10 sketch)
- "Alongside prod" mode (V2)
- Tera/Handlebars/full templating engine (V2 if config grows)
- Upstream forward / replay sender / "post back from SSE consumer" (V2)
- Bitmap/sort/doc work in relay mode (never)

---

## 2. Architecture

```
                            ┌──────────────────────────────────────┐
                            │         bitdex (BITDEX_MODE=relay)   │
                            │                                      │
   any HTTP client ───────▶ │  POST /api/indexes/{idx}/query  ───┐ │
   (model-share, sidecar    │                                    │ │
   pg-sync, ops-poller, …)  │  POST /api/indexes/{idx}/ops    ───┤ │
                            │                                    │ │
                            │  GET  /api/health               ───┤ │
                            │      ┌─────────────────────────────┘ │
                            │      ▼                                │
                            │  route handler                        │
                            │   ├─ check route auth policy          │
                            │   ├─ enforce per-route body limit     │
                            │   ├─ allocate sequence id             │
                            │   ├─ if receivers > 0 OR capture on:  │
                            │   │     compose payload + emit        │
                            │   ├─ stub response (default empty 200)│
                            │   │                                   │
                            │  GET /events/{channel}  ──────────────┼──▶ SSE subscribers (admin bearer)
                            │   ├─ on RecvError::Lagged(n):         │
                            │   │     emit `:lagged n\n\n`, continue │
                            │   └─ ping every keep_alive_seconds    │
                            │                                       │
                            │  /healthz  /metrics                   │
                            └──────────────────────────────────────┘
```

### Direction

- **Ingress:** anything that talks to bitdex-server today. Most importantly: PG-sync sidecar's POSTs to `/api/indexes/{name}/ops`, and any clients pointed at the bitdex Service.
- **Egress:** SSE channels at `GET /events/{channel}`. Bounded broadcast with explicit lag-signaling; subscribers see gaps as `:lagged N\n\n` SSE comments. Capture file (NDJSON gzip-rotated) optional via config block.
- **No upstream call.** Pure dummy. SSE consumers carry the real query work on their side.

### Pod identity (replacement, not coexistence)

The relay binds the same port the bitdex-server container does (3000). It passes the same `/api/health` probe. From cluster, ingress, and PG-sync sidecar viewpoints, the pod is unchanged — only its dispatch is. Toggle = env var change + pod restart.

### Delivery guarantees (explicit)

- **HTTP response success ≠ event delivered.** Relay returns 200 as soon as the route handler completes. SSE subscribers may have lagged, capture may have rotated mid-write — neither blocks the response.
- **SSE stream is best-effort.** Lossy under subscriber lag. Loss is signaled (sequence ID + `:lagged N` comment) but not recoverable without the optional capture file.
- **Capture file is the only durable record.** Enabling capture is the way to guarantee no data loss across iteration windows.

---

## 3. Constraints

| Constraint | Target | Source |
|---|---|---|
| Resident memory | ≤ 4 GiB hard cap, ~200 MiB steady | Justin |
| CPU | 1–2 cores | Justin |
| Sustained inbound | ≥ 70 QPS queries + 10K ops/sec | Ava + bitdex-sync prod measurements |
| Per-route body limit | configurable; defaults: query=4 MiB, ops=32 MiB | Gemini review (PG-sync batches at 10K ops/sec can exceed 4 MiB) |
| SSE channel capacity | bounded broadcast, configurable | Donovan |
| Drop semantics | tokio broadcast drops messages; lag signaled to subscriber via `:lagged N` SSE comment | GPT review |
| Image size delta | additive feature in same `bitdex` image | Justin |
| Toggle latency | one pod restart (~10s) on `kubectl set env` | Justin |

### Ingress / proxy assumptions (must hold for SSE)

- Reverse proxy / ingress must not buffer streaming responses. Set `X-Accel-Buffering: no` on `/events/*` responses (already done in `bitdex-server`'s `handle_query_stream` — copy the pattern).
- Idle timeout > `keep_alive_seconds`. Default 15s. Confirm Cloudflare and Traefik configs support sustained SSE.
- Compression off for SSE responses.
- HTTP/1.1 streaming OK. HTTP/2 also fine.

---

## 4. Components

1. **Mode dispatch** (`src/bin/server.rs` entrypoint, modified)

   At process startup, read `BITDEX_MODE` env var. Default `server`. If `relay`, branch into `relay::main()` from `src/relay/mod.rs` and skip all bitmap-engine bootstrap. Single binary, single image, mode chosen once at boot. Hot-mode swap deferred to V2 (§10).

2. **HTTP server (axum)** — binds `--listen` (default `0.0.0.0:3000`).

3. **Route registry** — built from config at startup. Each route is `{path, methods, emit?, response, auth, max_body_bytes?}`. Path matching uses axum's path-param syntax (`{index}`); methods filter the dispatch. **Config validation at startup**: unique channel names, route uniqueness by (method, path), `emit.channel` exists, payload templates compile, response status valid, etc. Fail-fast on invalid config.

4. **Channel registry** — built from config at startup. Each channel is `tokio::sync::broadcast::Sender<RelayEvent>` with configured capacity. Channels expose at `GET /events/{name}`. Each event carries a monotonic `seq_id` (per-channel, u64) for gap detection.

5. **Emit pipeline (per-route)** — on matching request:
   - Enforce per-route `max_body_bytes` (axum body-limit middleware).
   - Authenticate per route (see §7).
   - Read body.
   - **Gate:** `if sender.receiver_count() > 0 OR capture.enabled { ... }` — skip emit work entirely if nobody listening and no capture.
   - Compose payload via minimal templating (§6). All non-`{body|json}` interpolations are JSON-string-escaped automatically.
   - Allocate next `seq_id` for the channel (atomic fetch_add).
   - `let _ = sender.send(event);` — **ignore SendError**. Last subscriber may have disconnected between the gate check and send (TOCTOU). This is benign.
   - Write to capture writer if enabled.
   - Increment `relay_emit_total{channel,route}` (using route template, not raw path) or `relay_emit_skipped_no_subscriber_total` if skipped.

6. **Stub response (per-route)**:
   - **Default: empty 200 OK** if no `response` block in route config (per Justin V1).
   - If `response` block present: configured status + headers + static or templated body. Useful for routes where pg-sync or model-share parse the response shape (e.g. `accepted` count).
   - Caller compatibility — see §7.5.

7. **Capture (optional, per-channel)** — NDJSON writer fed by a separate broadcast subscriber. Subscriber explicitly handles `RecvError::Lagged(n)` — writes a marker line `{"_lagged":N,"channel":"<name>","seq_id":<id>}` and continues. Gzip-rotated on size threshold. Off by default. File permissions 0640 (owner-rw, group-r); files contain request bodies that may include user PII.

8. **SSE egress** — `GET /events/{name}`:
   - Auth-gated by admin bearer (always; no IP-based bypass for `/events/*`).
   - Subscribes a fresh receiver. Streams events as `id: {seq_id}\ndata: {json}\n\n`.
   - On `RecvError::Lagged(n)`: emit `: lagged {n}\n\n` (SSE comment), keep looping. **Do not panic, do not exit the task.**
   - On client disconnect: drop receiver, decrement `relay_sse_subscribers` gauge.
   - Headers (explicit, do not rely on axum defaults; reverse proxy may override):
     - `X-Accel-Buffering: no` (Cloudflare/Traefik buffer otherwise; events arrive in ~60s bursts)
     - `Cache-Control: no-cache`
     - `Content-Type: text/event-stream`
   - `KeepAlive::default()` matches existing `bitdex-server` `handle_query_stream` (keeps connection open across idle gaps).
   - Lift verbatim from `bitdex-server` `handle_query_stream` (`src/server.rs:5510-5555`); only addition is the `:lagged N` SSE comment + `id:` line per event.

9. **Probes**:
   - `GET /api/health` (matches existing bitdex-server probe path) — always 200 once bound. Returns `{"status":"ok","mode":"relay"}`.
   - `GET /readyz` — 200 once config is parsed and channels are live.

10. **Metrics** — Prometheus at `/metrics` on the same port (or configurable second port).

---

## 5. Config schema (locked V1)

YAML, loaded at startup from `--config /etc/bitdex-relay/config.yaml` or `BITDEX_RELAY_CONFIG`. Cold-restart on change (V1).

```yaml
listen: 0.0.0.0:3000
metrics_path: /metrics
admin_token_env: BITDEX_ADMIN_TOKEN

# Default body limit; routes can override
max_body_bytes: 4194304        # 4 MiB

channels:
  queries:
    capacity: 10000
    keep_alive_seconds: 15
  ops:
    capacity: 50000
    keep_alive_seconds: 15

routes:
  - path: /api/indexes/{index}/query
    methods: [POST]
    auth: none                   # query endpoint is public-equivalent today
    emit:
      channel: queries
      payload: |
        {"seq_id":{seq_id},"ts_ms":{ts_ms},"index":"{path.index}","body":{body|json}}
    response:
      status: 200
      headers: { Content-Type: application/json }
      body: '{"ids":[],"total_matched":0,"cursor":null,"tee_mode":true}'

  - path: /api/indexes/{index}/ops
    methods: [POST]
    auth: loopback_or_bearer     # PG-sync sidecar over loopback (no token); external callers need bearer
    max_body_bytes: 33554432     # 32 MiB — PG-sync batches at 10K ops/sec can exceed 4 MiB
    emit:
      channel: ops
      payload: |
        {"seq_id":{seq_id},"ts_ms":{ts_ms},"index":"{path.index}","body":{body|json}}
    response:
      status: 200
      headers: { Content-Type: application/json }
      # PG-sync's bitdex_client expects {"accepted": N}; mirror the count for compatibility
      body: '{"accepted":0}'

  - path: /api/health
    methods: [GET]
    auth: none
    response:
      status: 200
      headers: { Content-Type: application/json }
      body: '{"status":"ok","mode":"relay"}'

# Egress channels — admin bearer always required, no bypass
egress:
  /events/{channel}:
    auth: bearer

capture:
  enabled: false
  dir: /var/lib/bitdex-relay
  rotate_bytes: 268435456       # 256 MiB
  gzip_after_rotate: true
  max_total_bytes: 21474836480  # 20 GiB total disk budget; oldest files deleted past cap
  fsync: per_rotation           # per-event would crater throughput; per-rotation is durable enough
  file_mode: 0640
```

### Defaults shipped with the image

A default config covering the routes above lives at `/etc/bitdex/relay-config.yaml` in the image. Operators override by mounting a ConfigMap.

---

## 6. Templating (minimal V1)

Hand-rolled placeholder substitution. **Not** Tera/Handlebars. Supported tokens:

| Token | Meaning |
|---|---|
| `{seq_id}` | u64 monotonic sequence ID for the channel |
| `{ts_ms}` | u64 unix epoch milliseconds at relay-receive |
| `{body|json}` | request body parsed as JSON and re-serialized; on parse fail, emits `null` and increments `relay_emit_parse_error_total` |
| `{path.<name>}` | named axum path param, **JSON-string-escaped** (`"`, `\`, control chars) |
| `{header.<name>}` | named header value (lowercased name); missing → empty; **JSON-string-escaped** |
| `{client_ip}` | `X-Forwarded-For` first hop, else peer addr; **JSON-string-escaped** |

Rationale for removing `{body}` (raw): Gemini + GPT both flagged the unfiltered raw-body interpolation as a footgun. `{body|json}` covers all production use cases (every body pg-sync, model-share, or smoke testing posts is valid JSON). Hand-rolling a safe `{body}` token costs more than it earns.

JSON-string escaping for `{path.*}`, `{header.*}`, `{client_ip}` defaults on; turning it off is V2 if a non-JSON template ever needs it.

---

## 7. Auth (per-route policy)

GPT review flagged the V2 doc's "no XFF = internal" bypass as unsafe. V3 splits per-route.

### Auth modes (config'd per route)

- `none` — no auth required. For routes that mirror existing public bitdex-server endpoints (e.g. `/api/indexes/{index}/query`).
- `bearer` — requires `Authorization: Bearer ${BITDEX_ADMIN_TOKEN}`. For SSE egress and admin-only routes.
- `loopback_or_bearer` — bypass auth only when `peer_addr ∈ {127.0.0.1, ::1}` (true sidecar-over-loopback case). External requests require bearer. **No XFF check.** For routes the in-pod sidecar uses (e.g. `/api/indexes/{index}/ops` from PG-sync).

### Token sourcing

- `BITDEX_ADMIN_TOKEN` env var read at startup.
- If unset and any route has `auth: bearer`: relay refuses to start with a clear log line. No silent fall-open.
- Token never logged; never accepted as a flag (process-list leak risk).

### Egress block

`/events/*` is **always** `auth: bearer`. The egress block in config is informational; the code path hard-codes bearer-required for SSE.

### 7.5 Caller compatibility validation (mandatory before prod flip)

Stub responses must match what current callers parse. Smoke before prod flip:

- **PG-sync `bitdex_client.rs`** — verified safe by Donovan (2026-04-25). `BitdexClient::post_ops` (`src/pg_sync/bitdex_client.rs:268-283`) only checks `resp.status().is_success()`; doesn't deserialize body. `{"accepted": 0}` is fine; empty 200 is fine. No retry-forever risk.
- **`/api/health`** — match the current shape `{"status":"ok"}` plus added `mode` field. Verify K8s probes don't enforce a strict schema.
- **Model-share / shadow-mode query callers** — **HARD BLOCKER for prod flip.** Per Donovan (2026-04-25), the comparator at `src/server/bitdex/{client.ts,compare.ts}` in the model-share repo has **no existing `tee_mode` handling**. When relay flips on, every query returns `{"ids":[],"total_matched":0,"tee_mode":true}` while Meili returns real results → 100% divergence on every query → alert spam. **Fix required before relay window:** model-share's `compare.ts` checks `response.tee_mode === true` early and skips comparator path entirely (no record, no divergence count, no alert). Owner: needs assignment (Donovan offline on model-share; coordinate with Aidan or Justin to land the model-share PR).

If any caller breaks on the stub shape, surface as a blocker before prod flip.

---

## 8. Build

- New module `src/relay/` with `mod.rs`, `config.rs`, `route.rs`, `channel.rs`, `template.rs`, `capture.rs`, `sse.rs`.
- Modified `src/bin/server.rs` — early dispatch on `BITDEX_MODE`. Default `server` runs the existing path; `relay` invokes `relay::main()`.
- New Cargo feature `relay = ["dep:axum", "dep:tokio", "dep:tokio-stream", "dep:tower-http", "dep:tracing", "dep:tracing-subscriber", "dep:bytes", "dep:futures-util", "dep:serde_yaml", "dep:flate2"]`. Most of these already exist on the `server` feature; relay adds `flate2` + `serde_yaml` if not already pulled.
- Single image: `bitdex-server` binary contains both modes. Image size delta ~+2-5 MiB (the relay code).
- `bitdex-server` server-mode behavior unchanged.

---

## 9. Deploy & toggle

Per Justin's clarification 2026-04-25: env-var-dispatch lean. Single image, `BITDEX_MODE=relay|server` toggle. Aidan owns K8s wiring; this section captures the requirements.

### Toggle pattern: env-var-dispatch (chosen)

- Single image: `ghcr.io/civitai/bitdex:1.0.X` containing both modes.
- StatefulSet env: `BITDEX_MODE` defaulting to `server`. Toggle via `kubectl set env statefulset/bitdex BITDEX_MODE=relay` (or talos-infra commit, equivalent semantics — Aidan's call on which is the canonical channel).
- Pod restart on env change is acceptable.
- Process-startup decision; no live mode swap V1.

### Considered, not chosen

- **Separate-image-swap (was V2 lean):** rejected after Justin clarified. Maintains two GHCR repos for marginal gain; env-var-dispatch is operationally simpler and matches Justin's "single image" intent.
- **Sidecar always present:** rejected — defeats resource savings.

### Rollout sequence (V1)

1. Land relay code on `feat/relay`. Coordinate with Donovan on the `src/bin/server.rs` entrypoint dispatch (`if mode == "relay" { run_relay_main() }`). Donovan's review on the entrypoint hook is a gate.
2. Land Cargo feature + module structure. Tests pass on both modes.
3. Build + push image (Aidan).
4. Local smoke (containerized): run image with `BITDEX_MODE=relay` on Docker host. Hit each configured route, watch SSE.
5. Per-route caller-compat test (§7.5).
6. Stage flip on a non-prod cluster if available; otherwise gated prod flip during a low-traffic window.
7. **Prod flip preconditions checklist (mandatory):**
   - Local rig connected to `https://bitdex.civitai.com/events/{channel}` BEFORE flip (else first window of events lost).
   - Capture enabled if any data must be guaranteed durable.
   - Operator acknowledges PG-sync divergence invariant (see §12.1).
   - Resource requests **NOT** lowered (see §9.6 scheduling trap), unless aggressive mode opt-in (§9.6.1).
   - **Model-share `compare.ts` lands `tee_mode === true` skip path BEFORE flip** (hard blocker — see §7.5 + §12.7).
   - WAL retention window confirmed > expected relay window (for cheap WAL-replay reseed on flip-back).
8. Prod flip — `kubectl set env statefulset/bitdex BITDEX_MODE=relay`. Pod restarts on relay mode at the same image tag.
9. Local rig consumes events. Verifier runs.
10. Flip back when done — `kubectl set env statefulset/bitdex BITDEX_MODE=server`. Pod restarts on server mode.
11. **Post-flip-back mandatory step (see §9.7):** trigger BitDex full reseed.

### Replica topology

The bitdex StatefulSet has two replicas (`bitdex-0`, `bitdex-1`). Open question for Aidan: **flip both or one at a time?**

- **Both at once** — simplest. Local rig consumes events from whichever pod the Service routes to. PG-sync (sidecar in each pod) advances cursor against both. Standard expectation.
- **One pod at a time** — Service round-robins between server-mode and relay-mode replicas, producing inconsistent client behavior (real query results from one pod, stub responses from the other). **Avoid.** Don't mix modes across replicas.

Recommendation: flip both replicas together. Aidan to confirm; doc this as a hard invariant before prod flip.

### 9.6 Resource budget on toggle (default — safe flip-back mode)

- **Do NOT lower the StatefulSet resource requests during the relay window** (Gemini scheduling-trap finding, critical).
- Reasoning: lowering `requests.memory` from 14 GiB → 256 MiB during the relay window lets the scheduler pack other pods onto the node. On flip-back to server, the 14 GiB request can no longer be satisfied; pod sticks `Pending` indefinitely → outage.
- Default flip keeps the 14 GiB request (or whatever the server-mode request is). Relay just doesn't use most of it. ~13.5 GiB sits reserved-but-idle during the window. That's the cost of guaranteed flip-back.

### 9.6.1 Aggressive mode (resource-savings, opt-in)

For use case 2 ("save prod CPU/RAM"), an aggressive mode lowers `requests.memory` so the freed capacity is genuinely available to other workloads.

**Operator opt-in is mandatory** — this mode does not run by default. Choose explicitly when:
- The savings are needed (e.g. another pod needs the 13.5 GiB now, or node pressure is real).
- The flip-back eviction risk is accepted.

**Mode flip:**
- Aggressive: `kubectl set env statefulset/bitdex BITDEX_MODE=relay` **plus** a `kubectl patch` lowering `requests.memory` to 1 GiB (or whatever is enough for relay + sidecar). Two separate operator actions; doc the second as the "I accept eviction risk" gate.
- Flip-back from aggressive: revert resources first (`kubectl patch`), wait for scheduler to find capacity, then `BITDEX_MODE=server`. If the node has been backfilled, expect either eviction-of-other-workloads or a `Pending` window until capacity frees.

**Pre-flip checklist for aggressive mode:**
- Operator confirms the eviction risk explicitly.
- Other workloads on the node are tagged with priority class so the scheduler can evict them on flip-back.
- Or: operator commits to a maintenance window that tolerates a `Pending` interval.

If the operator does not opt into aggressive mode, the relay window delivers iteration value (use case 1) but no resource savings.

### PodSecurity housekeeping

While editing the StatefulSet manifest, fix Tom's flagged warnings. Apply at pod + container level:

```yaml
spec:
  template:
    spec:
      securityContext:
        runAsNonRoot: true
      containers:
        - name: bitdex            # main container
          securityContext:
            runAsNonRoot: true
            runAsUser: 65532
            allowPrivilegeEscalation: false
            capabilities:
              drop: ["ALL"]
            seccompProfile:
              type: RuntimeDefault
        - name: pg-sync
          securityContext:
            # same block
```

### Cluster-side state (Tom, 2026-04-25)

- Manifest path: `clusters/production/apps/bitdex/deployment.yaml`
- Flux Kustomization: `flux-system/bitdex`
- Namespace: `bitdex`, `ghcr-cred` ImagePullSecret in place
- nodeAffinity pin: `talos-fq9-f3k`
- PVCs: `data-bitdex-0` / `data-bitdex-1`, 200 Gi each, retained
- Resource drift: Tom committed `requests.memory: 14Gi`. **Keep at 14 GiB during relay window** per scheduling-trap finding above.
- PG-sync sidecar continues unchanged

### 9.7 Failure / rollback (mandatory reseed)

GPT + Gemini both flagged: rollback is **not idempotent** without a reseed step. Promoted to operational invariant, not a soft caveat.

**Why:** PG-sync advances its cursor on every batched POST during the relay window (§12.1). The bitdex-server WAL never received those ops. On flip-back, server mode reads its WAL and finds it stale relative to the PG-sync cursor — silently serves missing/wrong data.

**Mandatory flip-back runbook (enforced):**

1. Flip env back: `kubectl set env statefulset/bitdex BITDEX_MODE=server`. Pod restarts.
2. **Wipe + reseed.** Prefer the cheap path:
   - **Default — WAL replay (5-10 min):** reset PG-sync cursor to a pre-relay-window value (the cursor at flip-into-relay time, recorded in the operator runbook). Let WAL replay catch up. **Safe if and only if** `(relay window duration) < (WAL retention window)`. Operator confirms WAL retention before electing this path.
   - **Fallback — full bulk reload (60-90 min):** when WAL retention insufficient, WAL corrupt, or replay fails. Use `.claude/skills/deploy/cli.mjs` (suspend Flux, dump CSV, scale to 0, wipe `data-bitdex-{0,1}` PVC, scale to 1, run dump processor). See `docs/archive/bulk-load-handoff.md` for the V1 procedure.
3. Verify with a known-good query: shadow-mode comparison vs Meili shows expected match rate.
4. Until verification passes, **do not advertise BitDex as authoritative.** Shadow-mode + Meili fallback continues to serve.

This is not optional. The relay mode trades durable consistency for cheap iteration; flip-back rebuilds it. WAL replay is the cheap default; full bulk reload is the rare path.

---

## 10. Hot-config V2 (sketch, deferred)

Justin's V2 aspiration: "best would be hot config." Goal is to swap mode (and route/channel definitions) without a pod restart.

Sketch (V2 work, not built V1):

- `PATCH /api/config` admin endpoint accepts a new mode + (optional) new config YAML.
- Server module + relay module both implement a `RouteHandler` trait. App state holds an `ArcSwap<dyn RouteHandler>`. PATCH constructs a new handler, then `ArcSwap::store`s it. Next request lands on the new handler.
- Bitmap engine bootstrap is lazy: server-mode requires it; flipping from relay → server triggers engine init (slow first request). Flipping server → relay drops the engine to free RAM.
- Probe + auth + metrics surfaces live outside the swap so they're never disrupted.

Risks for V2 (not for V1):
- Engine teardown on relay-mode entry is non-trivial (open file handles, cursors, in-flight ops).
- Memory free-back depends on jemalloc retention; PATCH might not actually release RAM.
- Mid-flight requests at swap time need a quiesce/drain.

V1 just reads `BITDEX_MODE` once at boot. V2 work is filed under task #N (relay-team backlog) and gates on V1 being stable in prod.

---

## 11. Test plan

### Unit
- Templating: each token resolves, missing values → empty string (or `null` for `body|json` parse fail). JSON-string escaping verified for `{path.*}`, `{header.*}`, `{client_ip}` against quotes, backslashes, control chars.
- Route dispatch: method filter, path-param extraction, per-route body limit enforced, content-type-agnostic, rejected routes return 404/405.
- Auth: `none` → no header check; `bearer` → 401 on missing/wrong, 200 on right; `loopback_or_bearer` → loopback peer bypasses, non-loopback requires bearer.
- Channel: `RecvError::Lagged(n)` handled in SSE + capture loops; tasks don't exit. `SendError` from receiver disconnect mid-flight does not panic.
- Capture writer: line atomicity, rotation cross-threshold, gzip post-rotation, `max_total_bytes` deletes oldest, file-mode 0640.
- Config validation: unknown channel referenced by route → fail to start; duplicate route by (method,path) → fail to start; missing token env when bearer required → fail to start.

### Integration
- Spawn relay against in-process axum test client. POST 10K queries with subscriber keeping up. Assert all 10K events delivered with monotonic `seq_id`s.
- Lagged-subscriber test: subscriber reads 1/sec while producer is 1000/sec. Assert: drops counted via `relay_drops_total`, lag-comment events emitted, no panic, no unbounded memory growth.
- Capture round-trip: `capture.enabled=true`, send N events (mixed query + ops), assert NDJSON file has N+M lines (M = `_lagged` markers under deliberately-induced lag), all parseable as JSON, gzip rotates at threshold.
- TOCTOU: subscriber connects, producer fires, subscriber disconnects mid-flight. Confirm no panic, response still 200.
- Caller-compat: send sample PG-sync `OpsBatch` payload; confirm relay returns shape PG-sync's `bitdex_client.rs` accepts as success.
- Mode dispatch: start binary with `BITDEX_MODE=server`, confirm bitdex-server boot path. Restart with `BITDEX_MODE=relay`, confirm relay boot path, no bitmap engine init.

### Smoke (containerized)
- Build single image. Run with `BITDEX_MODE=relay` on Docker host.
- Send 10K synthetic POSTs via curl. Confirm:
  - All return 200 with the configured stub body.
  - SSE subscriber receives 10K events with no gaps.
  - Memory stays ≤ 250 MiB.
  - CPU stays ≤ 1 core average.
- Same image, `BITDEX_MODE=server`: confirm bitdex-server normal startup against fixture data dir.

### End-to-end (production gated, Aidan-owned)
- Flip `BITDEX_MODE=relay` on prod StatefulSet (both replicas) during a low-traffic window.
- Subscribe local rig to `https://bitdex.civitai.com/events/queries` and `/events/ops`.
- Watch metrics: `relay_emit_total`, drops 0 (subscriber keeping up), pod RSS < 500 MiB.
- Monitor PG-sync cursor advancement; verify it continues unimpeded.
- Flip back. Run mandatory reseed runbook (§9.7). Verify shadow-mode parity restored.

---

## 12. Risks (revised post-review)

### 🔴 12.1 PG-sync cursor divergence — operational invariant, not soft caveat

PG-sync advances its cursor on every batched POST while the relay is active. The real bitdex-server WAL never receives those ops. On flip-back:

- bitdex-server boots with stale state relative to PG-sync's cursor.
- BitDex serves silently-wrong data unless reseeded.

**Reseed paths (cheapest first):**
- **WAL replay (default, 5-10 min):** reset PG-sync cursor to pre-relay-window value, let WAL replay catch up. Conditional on `(relay window duration) < (WAL retention)`.
- **Full bulk reload (fallback, 60-90 min):** when WAL retention insufficient or WAL corrupt.

**This is the operating model**, not a side-effect. Flip-back is **not idempotent** without a reseed step. §9.7 documents the mandatory runbook. Operators must acknowledge before flip.

### 🔴 12.2 Scheduling trap on flip-back

Lowering `requests.memory` during the relay window lets the scheduler pack other pods onto the node. On flip-back, the 14 GiB request can't be satisfied → pod stuck `Pending` → outage. **Mitigation:** keep server-mode resource requests during the relay window. §9.6.

### 🔴 12.3 XFF-based auth bypass (now removed)

V2 design's "no `X-Forwarded-For` header = internal pod" trust boundary was unsafe. Pod-direct or NodePort access bypasses the ingress and lacks XFF, granting admin. **Mitigation:** removed entirely. V3 uses peer_addr loopback check (`127.0.0.1` / `::1`) for sidecar-over-loopback bypass; nothing else bypasses bearer.

### 🟡 12.4 Body templating injection

`{body}` raw token removed entirely (V2 risk). `{body|json}` parses + re-serializes, escaping all content. `{path.*}` and `{header.*}` JSON-string-escaped by default. **Mitigation:** removed `{body}` token from the schema; escaping is mandatory and on by default.

### 🟡 12.5 SSE drop semantics + lag visibility

tokio broadcast drops messages, not receivers. SSE/capture loops must explicitly handle `RecvError::Lagged(n)`. **Mitigation:** §4 component 8 mandates `:lagged N` SSE comments; tests verify the loop survives. `seq_id` lets consumers detect gaps even without watching the comment.

### 🟡 12.6 Replica mixed mode

Two-replica StatefulSet flipped one-at-a-time would round-robin server vs relay responses to clients. **Mitigation:** §9.5 mandates both replicas flip together. Documented as a hard invariant.

### 🟡 12.7 Caller compatibility for stubs

PG-sync, model-share shadow comparison, K8s probes all parse responses. Stub shapes might break callers. **Mitigation:** §7.5 mandates pre-flip caller-compat smoke; blockers surface before prod flip.

### 🟡 12.8 Ingress / proxy SSE buffering

Cloudflare or Traefik buffering, idle timeouts, compression can break SSE delivery. **Mitigation:** §3 documents the assumptions; pre-flip checklist confirms `X-Accel-Buffering: no` works through ingress (already validated in `bitdex-server`'s `handle_query_stream`).

### 🟡 12.9 Body size limit too small

PG-sync batches at 10K ops/sec can exceed 4 MiB. **Mitigation:** per-route override; `/api/indexes/{index}/ops` defaults to 32 MiB. Verify against observed prod batch sizes pre-flip.

### 🟢 12.10 Stub-response side effects (clients act on success)

Clients that act on a 200 (e.g. cache empty results, mark ops as durable) operate on relay-mode lies. **Mitigation:** treat the relay window as a controlled experiment; downstream consumers must opt in. Shadow-mode comparison will surface divergence; muting or marking the window prevents alert spam. Operator checklist.

### 🟢 12.11 Privacy of captured bodies

Capture files contain request bodies, which include user IDs / search queries / etc. Sensitive in aggregate. **Mitigation:** file mode 0640, files live on PVC the cluster already trusts, retention bounded by `max_total_bytes`. Don't ship outside the pod without admin gate. Logs do **not** include bodies (relay logs operations only).

### 🟢 12.12 Token absence at startup

If `BITDEX_ADMIN_TOKEN` is unset and any route requires bearer, relay refuses to start with a clear log line. **No silent fall-open.** Verified in §7.

### 🟢 12.13 Metrics cardinality

`route` label uses configured route template (`/api/indexes/{index}/query`), not raw paths. Cardinality is bounded by the config size. **Mitigation:** explicit in §11 metrics naming.

---

## 13. Metrics

| Metric | Type | Labels | Notes |
|---|---|---|---|
| `relay_emit_total` | counter | channel, route | Per emitted event; route uses template |
| `relay_emit_skipped_no_subscriber_total` | counter | channel | Gate skipped emit work because no subscriber + capture off |
| `relay_emit_parse_error_total` | counter | route | `body|json` token parse failure |
| `relay_drops_total` | counter | channel, reason | Lag-induced or capacity drops |
| `relay_request_duration_seconds` | histogram | route | End-to-end handler latency |
| `relay_sse_subscribers` | gauge | channel | Active SSE connections |
| `relay_sse_lagged_events_total` | counter | channel | Sum of `n` from each `RecvError::Lagged(n)` |
| `relay_capture_bytes_written_total` | counter | channel | If capture enabled |
| `relay_capture_rotations_total` | counter | channel | If capture enabled |
| `relay_capture_disk_bytes` | gauge | (none) | Current capture-dir disk use |
| `relay_config_loaded_at` | gauge | (none) | Startup timestamp |
| `relay_mode` | gauge | mode | Const 1 with label = `BITDEX_MODE` |

---

## 14. Decisions log (V3)

| # | Decision | Rationale |
|---|---|---|
| 1 | Single binary, env-var-dispatch toggle (`BITDEX_MODE=server\|relay`) | Justin: "I think b. Best would be hot config." Single image, no separate registry, fastest toggle. |
| 2 | Hot-config swap is V2; V1 reads env at boot only | Engine teardown is non-trivial; V1 keeps it simple |
| 3 | YAML config | Operator-friendly; matches existing sync config format |
| 4 | Hand-rolled placeholder templating, no Tera; `{body}` raw token removed | Smaller surface, smaller deps, fewer footguns |
| 5 | Single channel emit per route in V1 | Don't over-design |
| 6 | NDJSON capture optional, off by default | Off keeps the relay stateless; on serves Donovan's offline-replay need |
| 7 | No upstream forward / replay sender; no "post back from SSE consumer" | Justin: "pure dummy", "stick with simple" |
| 8 | Cold-restart config reload V1 | SIGHUP + hot-config = V2 |
| 9 | Replaces bitdex-server runtime via env, not a separate image | Justin's V1 framing |
| 10 | `/events/*` always bearer; no IP-based bypass | GPT review |
| 11 | `loopback_or_bearer` uses peer_addr, not XFF absence | Gemini review |
| 12 | Per-route body-size override; ops defaults 32 MiB | Gemini review (PG-sync batches) |
| 13 | Sequence IDs + `:lagged N` SSE comments for gap visibility | GPT review |
| 14 | Keep server-mode resource requests during relay window | Gemini scheduling-trap finding |
| 15 | Mandatory reseed runbook on flip-back | GPT + Gemini both flagged |
| 16 | Both replicas flip together, never mixed | GPT review |

---

## 15. Open questions for Aidan + Donovan

### Aidan
1. Confirm env-var-dispatch with `BITDEX_MODE` matches the toggle UX you want.
2. Probe paths used by the StatefulSet today — does `/api/health` cover liveness + readiness, or are different paths in use?
3. `BITDEX_ADMIN_TOKEN` secret name + key. Relay reuses same secret.
4. Replica flip strategy: confirm both replicas flip together. `kubectl set env statefulset/...` flips both atomically; verify Flux doesn't fight that.
5. Pre-flip checklist sign-off — operator runbook ownership.
6. PodSecurity housekeeping: same commit as relay rollout, or separate?
7. Reseed runbook (§9.7) — does the existing `.claude/skills/deploy/` cover the post-flip-back wipe + dump path, or do we need a new skill?

### Donovan (answered 2026-04-25)
1. ✅ Entrypoint dispatch hook — branch right after `parse_config()` in `src/bin/server.rs:264-310`, before `BitdexServer::new`. Mirror panic_hook + tracing init in the relay branch.
2. ✅ PG-sync `bitdex_client.rs` `accepted` tolerance — doesn't deserialize body; only checks `status().is_success()`. `{"accepted":0}` safe.
3. ⚠️ Model-share shadow comparator — **no existing `tee_mode` handling**. Hard blocker; needs PR to model-share's `compare.ts` to skip comparator on `response.tee_mode === true` BEFORE relay flip. See §7.5 + §12.7.
4. ✅ SSE pattern — lift `handle_query_stream` verbatim. Add `Cache-Control: no-cache` + explicit `Content-Type: text/event-stream` (axum sets but reverse proxy may override). `KeepAlive::default()` keeps connection open across idle gaps.

---

## 16. Status & next steps

1. ✅ V3 design doc reflects Justin's env-var clarification + GPT/Gemini must-fix lists.
2. ✅ ≥2 design reviews complete (GPT + Gemini). Verbatim outputs preserved at `docs/_in/relay-review-gpt.md` and `docs/_in/relay-review-gemini.md` for audit.
3. **Mail Scarlet** with V3 doc + review summaries.
4. **Coordinate** with Aidan (toggle pattern, probes, reseed) and Donovan (entrypoint, caller compat).
5. Open `feat/relay` branch. Implement on the locked design.
6. Local smoke (containerized) + caller-compat smoke.
7. Aidan ships image. Operator pre-flip checklist + relay window. Mandatory reseed on flip-back.
8. Mail Justin when V1 is live.

---

## Credits

- Donovan's `tee-receiver-design.md` (2026-04-25) — original Rust + axum + NDJSON capture + drop-oldest framing.
- Scarlet — translated Justin's V2/V3 framing into actionable scope, three rounds of trust-but-verify.
- Tom — cluster-side prep, scheduling math, PodSecurity housekeeping.
- Aidan — deploy authority + image registry.
- GPT (via OpenRouter) — review pass; flagged toggle inconsistency, broadcast-semantics misunderstanding, XFF auth bypass, missing event sequencing, replica topology, caller-compat, ingress/SSE assumptions.
- Gemini (via OpenRouter) — review pass; flagged scheduling trap on flip-back (the most operationally critical finding), TOCTOU on emit gate, header/path JSON injection, body-size limit, mandatory PVC wipe.
- Justin — vision, V2 framing, V3 toggle clarification, ship authority.
