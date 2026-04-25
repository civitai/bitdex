# Relay Config Schema — Sketch

Status: **Thought-fodder, not a commitment.** Drafted while awaiting V2 brief from Scarlet (pivot 2026-04-25). Justin's framing: generic HTTP→SSE relay, config-driven endpoint registration, lives in BitDex namespace as a stand-in for the BitDex pod itself.

The shape below is one plausible config language. Treat it as a discussion artifact, not a spec.

---

## Mental model

The relay is a tiny HTTP server with two halves:

1. **Ingress** — receives HTTP requests on configured routes. For each route, it can:
   - Emit an SSE event onto one or more channels with a configurable payload shape
   - Return a configured stub response to the client (status + JSON body)
   - Optionally forward the request to an upstream HTTP target (passthrough mode)

2. **Egress** — exposes one SSE endpoint per channel. Subscribers connect, receive events as they're emitted by ingress.

That's the whole product. Everything else is config.

```
HTTP client ──▶ POST /api/indexes/civitai/query ──▶ relay
                     │
                     ├──▶ emit event on "queries" SSE channel
                     │       payload = { ts_ms, route, body }
                     │
                     └──▶ return { ids: [], total_matched: 0 } (status 200)

local BitDex ──▶ GET /events/queries ──▶ relay
                     │
                     └──▶ SSE stream of { ts_ms, route, body } events
```

---

## Config shape (YAML, draft)

```yaml
listen: 0.0.0.0:3099
metrics_listen: 0.0.0.0:9090

# Egress: declare named SSE channels. Each gets an HTTP endpoint at /events/{name}.
channels:
  queries:
    capacity: 10000          # bounded broadcast buffer
    keep_alive_seconds: 15
  ops:
    capacity: 50000
    keep_alive_seconds: 15

# Ingress: declare HTTP routes the relay should accept.
routes:
  - path: /api/indexes/{index}/query
    methods: [POST]
    emit:
      channel: queries
      # The event payload — Tera/Liquid-style templating over { ts_ms, route, path_params, headers, body }
      payload: |
        {
          "ts_ms": {{ ts_ms }},
          "received_at_ms": {{ ts_ms }},
          "index": "{{ path_params.index }}",
          "body": {{ body | json }}
        }
    response:
      status: 200
      headers:
        Content-Type: application/json
      body: |
        {"ids": [], "total_matched": 0, "cursor": null, "tee_mode": true}

  - path: /api/indexes/{index}/ops
    methods: [POST]
    emit:
      channel: ops
      payload: |
        {
          "received_at_ms": {{ ts_ms }},
          "index": "{{ path_params.index }}",
          "body": {{ body | json }}
        }
    response:
      status: 200
      body: |
        {"accepted": {{ body.ops | length }}}

  # Health passthrough — relay returns 200 directly, no event
  - path: /api/health
    methods: [GET]
    response:
      status: 200
      body: '{"status":"ok"}'

  # Cursor read — return prod's actual cursor by proxying upstream
  - path: /api/indexes/{index}/cursors/{name}
    methods: [GET]
    proxy:
      upstream: https://bitdex.civitai.com
      timeout_ms: 5000
      # No emit. Pure passthrough.

# Auth gate for selected egress channels (admin-style bearer)
auth:
  bearer_env: BITDEX_ADMIN_TOKEN
  required_for:
    - egress:queries
    - egress:ops

# Optional: persist all emitted events to NDJSON (parallel to SSE fan-out)
capture:
  enabled: true
  dir: /var/lib/bitdex-relay
  rotate_bytes: 256MiB
  gzip_after_rotate: true
```

---

## Key design questions for V2 brief

1. **Templating engine.** YAML literal vs Tera vs Handlebars vs minimal `{var}` substitution. Tera handles `{{ body | json }}` cleanly but adds a dep. Minimal is enough for "echo body, add timestamp" but won't cover edge cases. Lean: Tera (well-trodden Rust, predictable).

2. **Response shape.** Static body works for pure stubs (the BitDex query 200 case). Templating works for "echo back what you got". `proxy` works for "pass through to upstream". Should we allow both `emit` + `proxy` on the same route (capture in flight to upstream)? Probably yes — that's the "tee" pattern.

3. **Auth model.** Bearer per-route? Per-channel egress? Mix? Donovan's admin-token-gate pattern is enough V1. Anything more complex defers.

4. **Persistence.** Donovan's NDJSON capture is independently useful; surface as `capture: { enabled: true }`. Off by default.

5. **Hot reload.** Config-driven systems usually want SIGHUP reload. V1 cold-restart is fine. V2 if needed.

6. **Multi-channel from one route.** Should one HTTP route be able to emit on multiple channels (e.g. queries → both `queries` channel and `audit` channel)? `emit` as a list (`emit: [{channel: queries, ...}, {channel: audit, ...}]`) keeps that door open without committing to it.

7. **Path params + headers in templates.** Useful for `{{ path_params.index }}` and `{{ headers.x_request_id }}`. Lock in.

---

## What survives from Donovan's spec

- Bounded ring buffer per channel (`channels.<name>.capacity`)
- Drop-oldest on overload, increment a metric
- NDJSON gzip-rotated capture (now optional via `capture:`)
- Admin bearer for protected egress
- Prom metrics surface
- Health endpoint
- ~180 MiB steady RSS estimate still holds (the body of the relay is the same)

## What doesn't

- "Subscribes to prod SSE" inversion — relay is now the **source** of SSE
- "Stub BitDex" framing — now generic, BitDex routes are just one config example
- bitdex-sync POSTing to relay's `/ops` — still works, but the route is config'd not hard-coded
- `--target` HTTP forwarder — re-cast as `proxy:` per-route, optional alongside `emit:`

## What's new

- Channel registry as first-class config
- Route → emit mapping with templated payload
- Per-route stub response config
- Config language is the design surface

---

## Sample: full BitDex stand-in config

This is what a single relay config would look like to **replace** the BitDex pod for the iteration window per Justin's framing:

```yaml
listen: 0.0.0.0:3000     # bitdex's prod port
channels:
  queries: { capacity: 10000 }
  ops: { capacity: 50000 }
routes:
  - path: /api/indexes/{index}/query
    methods: [POST]
    emit: { channel: queries, payload: '{"ts_ms":{{ ts_ms }},"index":"{{ path_params.index }}","body":{{ body|json }}}' }
    response: { status: 200, body: '{"ids":[],"total_matched":0,"cursor":null,"tee_mode":true}' }
  - path: /api/indexes/{index}/ops
    methods: [POST]
    emit: { channel: ops, payload: '{"received_at_ms":{{ ts_ms }},"body":{{ body|json }}}' }
    response: { status: 200, body: '{"accepted":{{ body.ops|length }}}' }
  - path: /api/health
    response: { status: 200, body: '{"status":"ok"}' }
capture: { enabled: true, dir: /var/lib/bitdex-relay }
auth:
  bearer_env: BITDEX_ADMIN_TOKEN
  required_for: [egress:queries, egress:ops]
```

bitdex-sync continues to POST `/api/indexes/civitai/ops` as it does today. Relay accepts, emits to `ops` channel, returns 200. A local BitDex (or anything else) subscribes to `https://relay/events/ops` and gets the stream. Same for queries.

---

## Data-quality verifier (next workstream — sketch only)

The verifier subscribes to `events/queries` and `events/ops`, runs equivalent queries against PG/CH, asserts BitDex (downstream of the relay) produces compatible results. The relay is just the fan-out point — verifier doesn't need to know it exists, only that there's an SSE stream to consume.

```
relay /events/queries ──▶ verifier ──▶ run query against PG ──▶ compare with BitDex doc fetch
                       └▶ local BitDex ──▶ run query bitmap-side ──▶ ids, docs
```

Multiple subscribers on `/events/queries` → multiple consumers, no per-consumer config in the relay, fan-out comes free from broadcast channel semantics.

---

## Next steps after V2 brief

1. Confirm: is YAML the right surface, or does Justin prefer TOML / something else?
2. Confirm: templating engine choice (Tera lean).
3. Confirm: relay literally takes over the `bitdex-0` pod's address, or sits at a sibling Service?
4. Confirm: does `bitdex-sync` continue as sidecar in the relay pod, or stays where it is?
5. Lock route schema, ship design doc, ≥2 GPT/Gemini reviews.
