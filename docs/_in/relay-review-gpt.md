Here’s a direct review against your requested focus areas.

## High-level take

The core shape is sound: a tiny axum service that preserves the bitdex HTTP surface, emits request bodies to SSE, and returns stubs is a reasonable replacement pod for iteration windows. The “separate relay image, manifest image swap” approach is cleaner than trying to multiplex modes inside one image.

That said, the doc has several load-bearing inconsistencies and a few dangerous assumptions. The biggest ones are:

- deploy/toggle section is internally contradictory
- the proposed auth bypass is not safe enough as written
- `tokio::broadcast` does **not** implement “drop-oldest queue semantics” in the way the doc seems to assume
- PG-sync cursor advancement is not a minor caveat; it is the central operational risk and needs a real runbook / explicit acceptance
- readiness/health and response compatibility with existing callers are under-specified
- scheduling/resource assumptions are a bit too hand-wavy for a StatefulSet replacement

---

# 1. Architecture soundness

## What is sound

### 1. Relay replacing the server pod
This is the right shape if the goal is “accept prod traffic, do no real work, expose captured requests to external consumers.” Reusing the same service identity/port/probe surface is operationally pragmatic.

### 2. Separate image swap
Option B is the right call for a dummy relay. It keeps:
- image small
- CVE surface small
- runtime behavior obvious
- rollback simple

It also avoids a weird “ship giant server image to run a no-op binary” anti-pattern.

### 3. Config-driven route/channel registry
Good V1 tradeoff. A small declarative route table is enough.

### 4. No upstream forwarding
Correct. Keeps blast radius and failure modes simple.

---

## What is risky / wrong / underspecified

### A. `tokio::broadcast` is not “drop-oldest bounded broadcast” in the operational sense you probably want
The doc says:

- bounded broadcast channel
- drop-oldest on overflow
- `try_send`
- lagged receivers are dropped by tokio broadcast semantics

This is partly true, but the behavior matters:

- `broadcast` stores a ring buffer shared across receivers
- when sender advances beyond receiver position, that receiver gets `RecvError::Lagged(n)`
- the **receiver is not automatically disconnected forever**; it can continue from the newest retained item after observing lag
- “drop-oldest” happens globally at the ring, not per-subscriber queue
- sender-side capacity pressure does not backpressure on slow subscribers in the usual sense; slow subscribers just miss messages

That may be acceptable. But the doc mixes concepts:
- “drop-oldest” queue semantics
- “lagged receivers are dropped”
- “subscriber holding open a slow connection causes broadcast backpressure”

Those are not the same thing.

For SSE, the real behavior is:
- if the HTTP stream writer to a client is slow, your task that bridges `broadcast::Receiver` -> SSE will accumulate lag and miss messages
- that client should receive either:
  - explicit gap/loss signaling, or
  - be disconnected on lag so the consumer can reconnect intentionally

As written, the design implies silent event loss is acceptable. That is okay only if explicitly accepted for both `queries` and `ops`. For `ops`, silent loss may be catastrophic depending on subscriber expectations.

### B. SSE as sole egress for 10K ops/sec is plausible but not yet proven
10K ops/sec over one SSE stream can work, but only if:
- events are small
- subscriber connection is healthy
- serialization path is efficient
- no extra buffering explodes memory
- proxies/ingress don’t coalesce/buffer badly

You need to call out ingress/proxy behavior. Some reverse proxies buffer streaming responses unless configured otherwise. If this is exposed via the existing public bitdex endpoint, check:
- proxy buffering disabled for SSE
- idle timeouts > keepalive interval
- HTTP/1.1 streaming behavior okay
- compression off for SSE unless explicitly tested

This is a real load-bearing assumption missing from the doc.

### C. Request body buffering may break under larger real-world traffic than assumed
Current design reads full body into memory up to `max_body_bytes`. Fine for 4 MiB default and 70 QPS query traffic, but at 10K ops/sec this only stays safe if ops bodies are tiny. The doc should explicitly state expected body sizes for `/ops`.

If `/ops` batches can spike large, memory and CPU assumptions may fail due to:
- body copies
- JSON validation for `{body|json}`
- serialization into SSE frames

### D. Stub responses are probably not protocol-compatible enough
The relay returns:
- query: `{"ids":[],"total_matched":0,"cursor":null,"tee_mode":true}`
- ops: `{"accepted":0}`

That may be enough, but the architecture assumes every existing caller tolerates these exact stubs. That is not proven.

Specific concerns:
- does any query caller treat empty results as valid and proceed silently, causing higher-level corruption/confusion?
- does PG-sync expect `accepted > 0` or any additional fields?
- are there content-type headers on every route that match existing server behavior?
- does health route need exact body shape/status/content-type?

This needs compatibility verification, not just a sketch.

### E. Single SSE subscriber “in practice” is a dangerous design assumption
If only one subscriber matters operationally, then broadcast may be the wrong abstraction. Broadcast is okay, but doc should state whether:
- multiple subscribers are supported best-effort only
- one privileged subscriber is expected
- event delivery guarantees are explicitly at-most-best-effort

Because if verifier + local rig both subscribe, you can now have two independent lag profiles and no replay.

---

# 2. Config schema sufficiency for V1

## Sufficient enough
The schema is close to adequate for V1:
- listen
- metrics path
- admin token env
- max body bytes
- channel capacity/keepalive
- routes with methods/path/emit/response
- optional capture

That is enough to ship a narrow relay.

---

## Missing / under-specified fields

### A. No explicit auth policy per route
Current global auth model is too blunt. You likely need config support or hardcoded route classes for:
- public-compatible endpoints that must remain unauthenticated
- admin-only SSE endpoints
- internal-only endpoints

At minimum, schema should specify auth mode per route or per endpoint class:
- `auth: none | bearer | internal_or_bearer`

Without this, behavior is hidden in code and easy to misapply.

### B. No per-route body limit override
A global `max_body_bytes` is probably okay for V1, but query and ops likely have different realistic body sizes. Per-route override would make V1 safer.

### C. No explicit response content-type defaults
Some routes set headers, some don’t. You should define whether JSON bodies always get `Content-Type: application/json`. Today `/ops` route body lacks explicit content-type.

### D. No config for SSE replay/loss signaling
If drops are allowed, subscribers need to know. You likely need one of:
- event sequence number in payload
- server-side SSE `id:`
- optional disconnect-on-lag policy

At minimum, config or design should include monotonic event IDs.

### E. No route matching precedence rules
If multiple configured routes can match, what wins? Probably first match. Must be specified.

### F. No config validation rules written down
Need startup validation such as:
- unique channel names
- route path uniqueness by method
- emit channel exists
- `keep_alive_seconds > 0`
- capacities sane
- body templates compile/validate at startup
- response status valid
- `admin_token_env` present if auth is enabled

### G. `capture` config is underspecified for prod
If enabled:
- retention policy?
- disk budget?
- fsync strategy?
- behavior on disk full?
- permissions on files?

Not blocking if truly off by default, but should be called out.

---

# 3. Risks list completeness

Current risks list is missing several important ones and soft-pedals one major one.

## Existing risk list comments

### 1. Body templating injection
Correct risk. Mitigation is too weak if `{body}` remains generally available. More on that below.

### 2. Auth bypass via XFF spoofing
Correct risk, but mitigation is not acceptable as written. “same posture bitdex-server already trusts” is not a strong enough argument for a new external SSE endpoint with admin-bearing semantics.

### 3. Toggle race during upgrade
This risk is stale / wrong because the doc chose option B image swap, not env mode toggle. The doc still refers to `BITDEX_MODE` repeatedly. This is a sign the deploy section was not fully reconciled.

### 4. Probe drift
Valid.

### 5. PG-sync silently ahead
This is the biggest operational risk and is understated. It is not just “acceptable in iteration windows”; it is a mode that intentionally creates durable divergence between logical upstream cursor and real BitDex state. This must be elevated.

### 6. SSE hold under load
Valid but explanation is imprecise, as noted above.

### 7. Dead code in old tee mode
Not really a top-tier risk.

---

## Missing risks

### A. Ingress/proxy buffering and timeout breaking SSE
This is a major missing risk.

### B. Loss of ops/query events with no replay path
If subscriber disconnects or lags, messages are gone unless capture is on. That means your external consumer can silently miss part of the iteration window. Need explicit risk.

### C. Flip-back idempotency / stale state assumptions
On rollback to server image:
- does server bootstrap from durable state that is now stale?
- does PG-sync assume current cursor and therefore skip needed replay?
- is manual reseed required every single time?

This is partially mentioned, but not concretely operationalized.

### D. Existing clients may mutate behavior based on successful stub responses
Returning 200 for a request that no real system processed can cause upstream systems to make decisions that are not safe to undo. This is distinct from PG-sync cursor advancement. Example:
- clients may cache empty query results
- workflows may treat accepted ops as durable

### E. Abuse / DoS via SSE endpoint or body size
External SSE endpoint plus body capture can become an exfiltration or resource abuse vector if auth is bypassed incorrectly or token leaks. Also many SSE connections could create memory/task pressure.

### F. Secret/token absence at startup
What happens if `BITDEX_ADMIN_TOKEN` is unset? Does startup fail or is auth effectively disabled? Must be explicit.

### G. StatefulSet + PVC semantics on image swap
Likely okay, but if server image expects certain filesystem ownership and relay changes pod security context, rollback may expose permission issues. Especially with PVC mounted and UID changes.

### H. Metrics cardinality
`route` label likely okay if static, but if implemented using concrete request path instead of configured route pattern, cardinality could explode. Should specify route label uses route template, not raw path.

### I. SSE data privacy
Request bodies may contain sensitive data. Exposing them on an admin SSE endpoint needs explicit risk acknowledgement:
- logs must not include body
- metrics should not include payload-derived labels
- capture files are sensitive

---

# 4. Deploy / rollout plan in §9

## Biggest issue: section is internally inconsistent

You say:

- “Three options were on the table; we're going with A.”
- heading says “Chosen pattern: separate image tag, manifest swap (option B)”
- later multiple references still talk about `BITDEX_MODE=relay`
- rollback says `BITDEX_MODE=server`
- rollout steps mention env var in places
- test plan smoke and e2e mention `BITDEX_MODE`

This must be fixed. Right now the deploy plan is contradictory enough to confuse the implementer/operator.

If option B is chosen, remove all mode env var references and rewrite the toggle/rollback narrative cleanly around image swap only.

---

## Rollout plan concerns

### A. “low-traffic window” is necessary but insufficient
Need explicit preconditions:
- confirm no critical production dependence on real BitDex responses during window
- confirm local subscriber connected before flip or accept event loss from the start
- confirm whether capture is enabled as safety net
- confirm PG-sync divergence acknowledged by operator

### B. No explicit drain / cutover choreography
If using a StatefulSet pod replacement:
- existing clients may see brief failures during restart
- if there are multiple replicas, how is service routing behaving?
- if two pods exist, are both swapped together or one at a time?
- do both have local pg-sync sidecars and separate PVC/cursors?

You mention `bitdex-0` / `bitdex-1` PVCs, implying multiple replicas, but the rollout plan reads like a single-pod service. Needs clarity. If there are 2 replicas and you flip only one, behavior may be mixed. If both flip, local subscriber may need to read from both or only service-routed traffic from one backend.

That is a major under-specification.

### C. Resource request lowering on same commit may be risky operationally
Changing image + requests/limits + pod security settings in one commit increases variables. If this is a production flip, strongly consider:
1. securityContext fix separately first
2. relay image swap + requests/limits together only if necessary and tested

Or at least call out that bundling all three raises rollback/debug complexity.

### D. Need explicit rollback runbook for divergence
Rollback is not just “image reverts.” It should state:
- server will come up with stale index relative to PG-sync cursor
- expected next action: full dump/rebuild or explicit re-sync procedure
- do not assume service correctness immediately after rollback unless rebuild completed

As written, rollback language is too optimistic.

### E. Need staging answer before claiming plan
The doc says “non-prod cluster if available; otherwise gated prod flip.” Fine, but because the relay changes semantics drastically, you should require at least:
- cluster-local smoke in ephemeral namespace
- service/ingress SSE test
- PG-sync stub compatibility test

before prod.

---

# 5. Wrong / load-bearing assumptions

## A. PG-sync cursor advancing while relay does no real work
This is the biggest one.

The doc says this is acceptable because “we expect to do a fresh dump after a relay window.”

That is not just a caveat. That is the operating model. It should be promoted from “risk” to “explicit invariant”:

> Running relay mode intentionally sacrifices continuity of server-applied ops. Returning to server mode requires a rebuild/reseed/replay from authoritative source before trusting results.

If there is no guaranteed rebuild path, this whole design is operationally unsafe.

Questions the doc must answer:
- Where does authoritative state come from on flip-back?
- How long does rebuild take?
- Is the service expected to be unavailable/incorrect until rebuild completes?
- Is there any chance PG-sync’s durable cursor makes replay impossible without manual intervention?
- Is sidecar state on PVC or elsewhere? What exactly advances?

Without this, the “easy toggle” story is misleading.

## B. XFF absence means internal pod call
This is not a safe trust boundary. “No X-Forwarded-For” is not equivalent to “internal pod.” Many legitimate external paths may omit XFF, and many internal paths may include it depending on networking/proxying.

Safer alternatives:
- bypass auth only for loopback / Unix socket / known pod CIDR if you can trust it
- better: require bearer on SSE admin endpoints always, and separately exempt only `/api/indexes/*/ops` from auth if needed
- best: use network policy to scope access plus token for admin egress

At minimum, **never** make absence of XFF the signal for privileged access to `/events/*`.

## C. `{body}` token is too footgunny
You already know it can break JSON. The current mitigation “document to prefer `{body|json}`” is weak. In a hand-rolled templating engine, footguns become production incidents.

For V1:
- either remove `{body}` entirely
- or restrict `{body}` to non-JSON response/emit templates with explicit opt-in
- or provide `{body|json_string}` if you need raw-as-string semantics

Given your stated use case, `{body|json}` likely covers almost everything.

## D. “drop-oldest” is okay for ops subscribers
This might be wrong depending on why ops are being streamed. If downstream consumers intend to reproduce or verify behavior, losing ops means:
- they no longer have a faithful stream
- local rig state diverges from what requests implied
- debugging becomes misleading

If drops are acceptable only for queries but not for ops, split policy by channel:
- `queries`: bounded/drop okay
- `ops`: either much larger durable capture, disconnect on lag, or mandatory file capture

Right now `ops.capacity: 50000` is just a bigger lossy buffer, not a guarantee.

## E. Pod scheduling math
The doc says relay requests should be low so scheduler isn’t stuck, and mentions a node with 17 GiB free. But if current StatefulSet has affinity pinning and retained high requests, image swap alone will not fix scheduling. You do note lowering requests, which is correct, but this should be framed as mandatory for the pending-pod problem, not “should.”

Also:
- if there are 2 replicas pinned oddly, one may still not schedule
- if pg-sync sidecar requests remain high, total pod request may still be too large
- if PVC locality or node affinity is strict, scheduler freedom may be lower than implied

Need full pod-level request math, not just relay container math.

## F. Flip-back to server mode is idempotent
As written, rollback wording implies “same PVC, no migration, normal restart.” That is only mechanically true. Functionally, flip-back is **not** idempotent if PG-sync has advanced state that server did not apply.

This assumption is wrong unless there is an explicit rebuild/reseed step.

---

# Additional specific comments

## Auth model should be split by endpoint type
Recommended V1 hardening:
- `/events/*`: always require bearer, no bypass
- `/metrics`: probably cluster-internal only, no public exposure
- `/health` and `/readyz`: no auth
- `/api/indexes/*/ops`: allow localhost/pod-internal only, or no auth if that matches existing service exposure and ingress doesn’t route there externally
- `/api/indexes/*/query`: likely no auth if current clients call it unauthenticated

Current wording “mutating + egress endpoints require bearer; internal-pod calls bypass auth” is too broad and likely wrong for query compatibility.

## Need event IDs
For SSE, add:
- monotonic sequence number per channel or globally
- send as SSE `id:` and/or JSON field
- increment gap metric on lag
This makes losses diagnosable and consumers resumable-ish even without replay.

## Need explicit lag handling policy
For example:
- if `RecvError::Lagged(n)`, increment metric and emit a synthetic SSE event like `event: gap` with count
- optionally close connection after lag for `ops` channel
Silent continuation is hard to reason about.

## Health/readiness mismatch
Architecture section says relay passes same `/api/health` probe.
Components section says `/health` and `/readyz`.
Config includes `/api/health`.
Need one clear answer matching current StatefulSet probes.

## Metrics naming / semantics nit
`relay_emit_no_subscribers_total` comment says “Emit succeeded but channel had no listeners.” With `tokio::broadcast`, `send` returns number of subscribers; zero-subscriber case can be detected, but it’s not exactly a failure. Fine, just be precise in implementation.

## Test plan has unrealistic assertions
“POST 10K queries, subscribe SSE, assert all 10K events delivered” conflicts with your own drop-allowed semantics. That test only passes under ideal conditions and says little about production. Better:
- assert all 10K delivered when subscriber keeps up
- separately assert controlled lag causes measurable drops and no unbounded memory growth

Also smoke/e2e still reference `BITDEX_MODE`, which is stale.

---

# Recommended changes

## Architectural changes
1. Clarify delivery guarantees explicitly:
   - HTTP response success does **not** imply downstream consumer received event
   - SSE stream is best-effort, lossy under lag unless capture enabled
2. Add event IDs and lag/gap signaling.
3. Consider mandatory capture for `ops` during production relay windows, or explicitly declare ops stream lossy and unsuitable for exact replay.
4. Tighten auth boundaries; do not use XFF absence as privileged signal for admin egress.

## Config/schema changes
Add or document:
- per-route auth mode
- route validation rules
- explicit content-type defaults
- sequence IDs in emitted payloads
- maybe per-route body size override

## Deploy/ops changes
1. Fix option A/B/env-toggle contradictions.
2. Define replica topology and rollout behavior.
3. Add explicit pre-flip / post-flip / rollback runbook.
4. State that rollback to server mode requires rebuild/reseed before trusting outputs, unless proven otherwise.
5. Confirm ingress supports SSE streaming.

---

# MUST-FIX items

1. **Fix the deploy section contradictions.** The doc cannot simultaneously say option A, option B, image swap, and `BITDEX_MODE` toggle. Pick one model and rewrite §9, §10, and rollback text consistently.

2. **Replace the XFF-based auth bypass for admin/SSE endpoints.** “No X-Forwarded-For” is not a safe trust boundary. `/events/*` should always require bearer auth; if needed, exempt only specific internal ingestion routes separately.

3. **Make PG-sync divergence a first-class operational invariant, not a soft risk.** The doc must explicitly state what flip-back requires and whether rebuild/reseed is mandatory before trusting server results again.

4. **Clarify and correct channel/drop semantics.** Document actual `tokio::broadcast` behavior, define lag handling for SSE consumers, and stop describing it as generic “drop-oldest” without precision.

5. **Add event sequencing / gap visibility.** SSE needs sequence IDs and a defined way for consumers to detect loss; otherwise dropped events are operationally opaque.

6. **Specify replica/rollout behavior for the StatefulSet.** If there are multiple pods, define whether all replicas are swapped, how subscribers consume from them, and whether mixed relay/server behavior can occur during rollout.

7. **Validate caller compatibility for stub responses.** Especially `/ops` and `/api/health`. Do not assume `{"accepted":0}` and empty query results are harmless without checking current callers.

8. **Add ingress/proxy SSE assumptions to risks and rollout checks.** Buffering, idle timeout, and streaming support must be confirmed before prod use.

9. **Either remove `{body}` or constrain it hard.** Current mitigation is too weak for a hand-rolled template engine; `{body|json}` should be the default safe path.

10. **Make scheduling/resource changes explicit and pod-level.** If the goal is to solve pending/scheduling issues, reduced requests for the relay pod are mandatory, and sidecar + affinity + replica math must be accounted for.
