# Relay Consumer How-to (Donovan)

Practical brief for driving a local BitDex with real prod traffic via the
relay's SSE channels. Pairs with `scripts/replay-prod-via-relay.mjs`.

---

## 1. SSE endpoints + auth

When `BITDEX_MODE=relay` is set on the prod pod (gated by Scarlet sign-off
+ Tom apply), the relay container exposes two SSE channels at the same
host the bitdex-server normally serves on:

- `GET https://bitdex.civitai.com/events/queries`
- `GET https://bitdex.civitai.com/events/ops`

Both endpoints **always** require admin bearer auth (per design §7):

```
Authorization: Bearer ${BITDEX_ADMIN_TOKEN}
Accept: text/event-stream
```

No XFF bypass for `/events/*`. Internal-pod calls don't bypass either —
this is the egress surface that gets the most untrusted traffic.

Headers the server sets on the response (mirrored from
`bitdex-server`'s pattern; defeats Cloudflare proxy buffering):

- `Content-Type: text/event-stream`
- `Cache-Control: no-cache`
- `X-Accel-Buffering: no`

Keep-alive comments are sent every ~15 s to keep the connection open
across idle gaps.

---

## 2. Event schema

Events on both channels share the same outer shape (`relay-config.default.yaml`
is the source of truth):

```jsonc
// One SSE frame per event, e.g.:
//   id: 4711
//   data: {"seq_id":4711,"ts_ms":1745601623145,"index":"civitai","body":<original body>}
{
  "seq_id": 4711,           // u64, monotonic per channel
  "ts_ms":  1745601623145,  // u64, relay-receive time
  "index":  "civitai",      // path param of the originating route
  "body":   { /* original JSON request body */ }
}
```

The `body` field is **the entire original HTTP request body** that the
client posted to the relay — re-encoded as compact JSON via the
`{body|json}` template token. Replay it verbatim against the matching
local route:

| Channel    | `body` shape                                   | Replay against                          |
|------------|------------------------------------------------|-----------------------------------------|
| `queries`  | `BitdexQuery` JSON (filters, sort, limit, …)   | `POST /api/indexes/civitai/query`       |
| `ops`      | `OpsBatch` JSON (whole batch from pg-sync)     | `POST /api/indexes/civitai/ops`         |

Ops events carry the **whole pg-sync batch** as a single event — pg-sync
groups multiple `BitdexOps` rows into one POST, and the relay emits one
SSE event per POST. Don't re-batch on the consumer side; pass the body
straight through.

### Lag signaling

Tokio's broadcast channel drops *messages* on subscriber lag, not the
subscriber. The SSE stream surfaces this as a comment frame:

```
: lagged 137
```

`scripts/replay-prod-via-relay.mjs` parses these and counts them as
drops without attempting replay. Sequence-id gaps are also tracked; a
non-monotonic `seq_id` indicates the relay-side broadcast buffer
overflowed (unlikely at default 10K capacity but loud-fail-visible if
it does).

---

## 3. Throughput expectations

From the post-150 handoff measurements + the prod replay rig:

| Stream    | Sustained rate    | Notes                                             |
|-----------|-------------------|---------------------------------------------------|
| `queries` | ~70 QPS           | SSE replay rig at 70 QPS sustained for 30 min.    |
| `ops`     | ≤10K ops/sec peak | pg-sync prod cursor saw bursts; sustained lower.  |

Default consumer concurrency in `replay-prod-via-relay.mjs`:

- queries: 16 parallel local POSTs (matches the 70 QPS target with
  ~10ms median local latency)
- ops: 4 parallel — ops batches are larger and the local BitDex's
  ops_processor benefits from serialization

Adjust via `QUERY_CONCURRENCY` + `OPS_CONCURRENCY` env if your perf
stack measurements need different load profiles.

---

## 4. Running the consumer

```bash
# From the bitdex repo root.
export BITDEX_ADMIN_TOKEN=<prod admin token>
export RELAY_URL=https://bitdex.civitai.com
export BITDEX_URL=http://localhost:3002

node scripts/replay-prod-via-relay.mjs 30   # run for 30 minutes
```

Stats print every 10 s on stderr:

```
[60s] q recv=4123 done=4118 err=0 drop=0 lag=0 gap=0/0 qd=0 p50=4 p99=22 |
       o recv=518 done=518 err=0 drop=0 lag=0 gap=0/0 qd=0 p50=11 p99=84
```

Final summary on SIGINT or duration timeout — full snapshot per channel
including p50/p90/p95/p99 latencies, total events received vs. replayed,
queue depth, drops, lag count, gap count.

---

## 5. Gotchas

- **Run before flipping prod.** The pre-flip checklist (see
  `docs/guide/relay-flip-runbook.md`) requires the local rig to be
  connected to the SSE streams **before** `BITDEX_MODE` is flipped to
  `relay`. Events emitted between the flip and the consumer's connect
  are lost (no replay buffer).
- **SSE = best-effort.** No durability guarantee. If you need a
  durable record, enable the relay-side capture (NDJSON gzip-rotated,
  `capture.enabled: true` in the relay config). The consumer doesn't
  capture; it just replays.
- **Local target health.** The consumer doesn't gate on the local
  BitDex being healthy. If `BITDEX_URL` is wrong or down, you'll see
  errors climbing in stats but the SSE side keeps consuming. Useful
  for back-pressure exploration, dangerous for long unattended runs.
- **Bearer leaks via process listing.** The token is read from env;
  don't pass it as a flag. CI/runbook scripts should source from the
  cluster secret rather than copy-paste.
- **Reconnect drops the seq sequence.** On reconnect, the consumer
  starts fresh — `seq_id` continuity is per-connection. Gap counters
  reset on reconnect.

---

## 6. Hand-off

Donovan owns the local BitDex side: pulling the perf-stack image
(`v1.0.165` … `v1.0.171` once tagged), spinning local on `:3002`, and
measuring P99 against the consumer-driven traffic. Relay container +
SSE consumer (this script) are mine; route any consumer issues my way.
