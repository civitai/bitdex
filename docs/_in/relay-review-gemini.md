Here is the critical design review of the `relay-system-design.md` document. 

The core concept—a lightweight, dummy HTTP-to-SSE relay replacing the heavy bitmap server via a K8s image swap—is sound and elegantly solves the local-iteration and resource-saving goals. However, there are critical flaws in the auth posture, Rust concurrency assumptions, and the operational flip-back sequence that will cause security breaches, silent data loss, or cluster scheduling deadlocks if implemented as written.

Here are the direct findings.

### 1. Security: The XFF Auth Bypass is a Critical Vulnerability
**The flaw:** Relying on the *absence* of the `X-Forwarded-For` (XFF) header to identify internal pod calls and bypass the Bearer token auth is highly insecure. If an external actor hits the NodePort, LoadBalancer IP, or Pod IP directly (bypassing the Ingress), the request will lack an XFF header, and the relay will grant them full admin access to mutating and egress endpoints.
**The fix:** Do not use XFF for auth bypass. Since the PG-sync sidecar runs in the same pod, it communicates over the loopback interface. Check the TCP connection's peer IP: if `peer_addr == 127.0.0.1` (or `::1`), bypass auth. Otherwise, require the token.

### 2. Concurrency: Misunderstanding of `tokio::sync::broadcast`
**The flaw:** The doc states, *"Lagged receivers are dropped by tokio broadcast semantics."* This is incorrect. Tokio's broadcast channel drops the *messages*, not the receiver. The receiver stays alive, but its next call to `.recv()` returns `Err(tokio::sync::broadcast::error::RecvError::Lagged(count))`. 
**The impact:** If your SSE egress or Capture writer doesn't explicitly match and handle `RecvError::Lagged` (usually by logging the skipped count and continuing to loop), the subscriber task will likely panic or silently exit, permanently breaking the SSE stream or capture file for that client.
**The fix:** The SSE and Capture subscriber loops must explicitly handle `RecvError::Lagged(n)` by emitting an SSE comment (e.g., `: lagged by n messages\n\n`) or a capture log marker, and continuing to read.

### 3. Architecture: TOCTOU Race Condition in Emit Pipeline
**The flaw:** The emit pipeline checks `sender.receiver_count() > 0`, builds the payload, and then calls `send()`. The doc claims `send()` returns an error *"only when receiver_count is zero, which we already gated above."*
**The impact:** This is a Time-Of-Check to Time-Of-Use (TOCTOU) race condition. A subscriber can disconnect in the microseconds between the check and the `send()`. If the code assumes `send()` cannot fail and `unwrap()`s it, the relay will panic and crash under load.
**The fix:** Ignore the result of `send()`. If it returns a `SendError`, it just means the last subscriber disconnected mid-flight. Drop the error and return the 200 OK.

### 4. Templating: Injection via Headers and Paths
**The flaw:** The doc acknowledges injection risks for `{body}` and mitigates it with `{body|json}`. However, `{path.index}` and `{header.name}` are also interpolated as raw strings. If an attacker sends a header like `X-My-Header: "hello"`, and the template is `{"header":"{header.x-my-header}"}`, the resulting string is `{"header":""hello""}`, which is invalid JSON.
**The impact:** Downstream SSE consumers will crash trying to parse malformed JSON.
**The fix:** Implement a `|json_string` filter (or apply it by default) to `{path.*}` and `{header.*}` tokens so internal quotes and control characters are properly escaped.

### 5. Deployment: The StatefulSet Resource "Flip-Back" Trap
**The flaw:** The deploy plan dictates lowering the StatefulSet memory requests from `14Gi` to `256Mi` during the relay swap to save resources. 
**The impact:** When you flip back to `BITDEX_MODE=server`, you must raise the requests back to `14Gi`. Because you temporarily freed up 13.75Gi of memory on that node (`talos-fq9-f3k`), the K8s scheduler will likely pack other pods onto it. When you attempt to flip back, the node will no longer have 14Gi available. The real `bitdex-server` pod will go into a `Pending` state indefinitely, causing a production outage.
**The fix:** If the goal is purely local iteration (Use Case 1), *do not* lower the resource requests in the manifest. Leave them at 14Gi to "reserve" the space on the node for the flip-back. If the goal is to relieve node pressure (Use Case 2), you must accept that flipping back will require evicting other pods or waiting for cluster capacity.

### 6. Operations: PG-Sync Cursor Data Loss
**The flaw:** The doc correctly identifies that PG-sync will advance its cursor while the relay is active, meaning the real BitDex WAL will miss operations. The doc states this is "acceptable... because we expect to do a fresh dump after a relay window."
**The impact:** A "fresh dump" of a 25GiB index is a massive, destructive operational task. If an operator flips the image back without realizing they *must* trigger a full rebuild, production will serve stale/missing data silently.
**The fix:** This cannot just be a documented risk; it must be an enforced operational runbook step. The flip-back commit *must* include the command/script to wipe the PVC and trigger the PG-sync backfill.

### 7. Config: `max_body_bytes` Limit vs. Batch Sizes
**The flaw:** The default `max_body_bytes` is set to 4 MiB. The doc notes PG-sync pushes 10K ops/sec. 
**The impact:** If PG-sync batches these operations, a single POST body might easily exceed 4 MiB. Axum will reject it with a `413 Payload Too Large`, causing PG-sync to fail or infinitely retry, halting the cursor.
**The fix:** Verify the maximum possible batch size emitted by PG-sync in production and set `max_body_bytes` to at least 2x that value (e.g., 16 MiB or 32 MiB).

---

### MUST-FIX List

1. **Change Auth Bypass Logic:** Replace the `X-Forwarded-For` absence check with a TCP `peer_addr() == 127.0.0.1` check for internal pod bypass.
2. **Handle `RecvError::Lagged`:** Update the SSE and Capture subscriber loops to explicitly match and recover from `tokio::sync::broadcast::error::RecvError::Lagged(n)` instead of assuming the receiver is dropped.
3. **Handle `SendError` Gracefully:** Remove any assumption that `send()` will succeed if `receiver_count() > 0`. Catch and ignore `SendError` to prevent TOCTOU panics.
4. **Escape Template Strings:** Ensure `{path.*}` and `{header.*}` tokens are JSON-escaped when interpolated into payloads to prevent malformed JSON generation.
5. **Adjust `max_body_bytes`:** Increase the 4 MiB default to safely accommodate PG-sync's maximum batch size at 10K ops/sec.
6. **Document the Scheduling Trap:** Explicitly warn Aidan/Tom in the deploy plan that lowering resource requests during the relay window risks a `Pending` state on flip-back due to node backfilling.
7. **Formalize the PVC Wipe:** Add a mandatory step to the "Flip back when done" rollout sequence to wipe the local BitDex PVC / trigger a fresh dump, preventing silent data gaps.
