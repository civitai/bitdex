# Reliability & Error Handling Audit

**Auditor:** Agent D — Reliability & Error Handling
**Date:** 2026-03-13
**Scope:** Production readiness of Bitdex V2 core engine, HTTP server, persistence layer, and pg-sync tooling

---

## Summary of Review

Examined the following files and areas:

- **HTTP Server** (`src/server.rs`, `src/bin/server.rs`) — request handling, error responses, input validation
- **Concurrent Engine** (`src/concurrent_engine.rs`) — thread lifecycle, flush/merge threads, shutdown, panic propagation
- **Write Pipeline** (`src/write_coalescer.rs`) — channel-based mutation batching
- **Persistence** (`src/bitmap_fs.rs`, `src/docstore.rs`) — disk I/O, atomic writes, crash safety
- **PG Sync** (`src/pg_sync/`) — outbox poller, bitdex client, bulk loader, progress server
- **Loader** (`src/loader.rs`) — NDJSON bulk loading pipeline
- **Configuration** (`src/config.rs`) — validation completeness
- **Error types** (`src/error.rs`) — error hierarchy
- **Dependencies** (`Cargo.toml`) — release profile, dependency choices

---

## Findings

### F1: `panic = "abort"` in release profile makes all panics immediately fatal

**File:** `Cargo.toml:92`
**Severity:** HIGH
**Pattern:** `panic = "abort"` in `[profile.release]`

With `panic = "abort"`, any unwrap/expect that hits a `None`/`Err` in production will instantly terminate the process with no cleanup — no Drop handlers run, no flush of in-flight mutations, no final bitmap snapshot write. This interacts badly with every other finding below that identifies a panic-capable code path.

**Impact:** Process death on any unexpected `None`/`Err` in production. Unflushed mutations lost. No opportunity for graceful degradation.

**Suggested fix:** Either (a) remove `panic = "abort"` so Drop handlers and unwind-based cleanup can run, or (b) eliminate ALL unwrap/expect calls on fallible paths in non-test code (see F2, F3). If keeping `abort` for the binary size/performance benefits, at minimum install a panic hook (`std::panic::set_hook`) that logs diagnostics before the process dies.

---

### F2: `expect()` calls in production code paths that can panic on bad input or I/O

**File:** `src/loader.rs:145`
**Severity:** HIGH
**Pattern:** `File::open(&data_path_owned).expect("Failed to open data file")`

The NDJSON reader thread panics on file open failure. Since this runs inside `thread::spawn`, with `panic = "abort"` the entire process dies. Even without abort, the panic would poison the thread and leave the parse pipeline stuck waiting on a channel that never receives data.

Additional production `expect()` calls:
- `src/loader.rs:190` — `engine.prepare_bulk_writer(...).expect("prepare_bulk_writer")`
- `src/bin/server.rs:35` — `args[i].parse().expect("--port must be a number")` (pre-startup, acceptable)
- `src/bin/server.rs:62` — `server.serve(addr).await.expect("Server failed")` (top-level, acceptable)
- `src/pg_sync/bitdex_client.rs:35` — `Client::builder()...build().expect("failed to build HTTP client")` (startup, low risk)
- `src/pg_sync/progress.rs:101` — `TcpListener::bind(addr).await.unwrap()` in spawned task

**Impact:** API-triggered load request with an invalid or permissions-restricted path kills the entire server process.

**Suggested fix:** Replace `expect()` with proper `Result` propagation. The `load_ndjson` function already returns `Result` — use `?` or `map_err` instead of `expect`. The reader thread should send errors through the channel or use a shared error flag.

---

### F3: No graceful shutdown signal handling

**File:** `src/bin/server.rs:52-63`, `src/server.rs:454-456`
**Severity:** HIGH
**Pattern:** No SIGTERM/SIGINT handler; `axum::serve(listener, app).await` blocks forever.

The server has no signal handling. On SIGTERM (Kubernetes pod shutdown, `docker stop`, systemd stop):
1. The process receives SIGTERM
2. Default handler terminates the process
3. `ConcurrentEngine::Drop` never runs (especially with `panic = "abort"`)
4. In-flight mutations in the crossbeam channel are lost
5. The merge thread's periodic snapshot may not have persisted recent changes

The `ConcurrentEngine` has a proper `shutdown()` method and `Drop` impl, but they never get called because the process exits before the axum future completes.

**Impact:** Data loss on every deployment/restart. Mutations accepted by the HTTP API but not yet flushed+persisted are silently dropped.

**Suggested fix:** Add `tokio::signal::ctrl_c()` and/or a SIGTERM handler via `tokio::signal::unix::signal(SignalKind::terminate())`. Use `axum::serve(...).with_graceful_shutdown(shutdown_signal)`. On shutdown signal: (1) stop accepting new requests, (2) drain the flush channel, (3) call `engine.shutdown()`, (4) force a final snapshot save.

---

### F4: Missing fsync before rename in atomic write path

**File:** `src/bitmap_fs.rs:65-79`
**Severity:** HIGH
**Pattern:** `write_bitmap_atomic` writes to `.tmp`, then renames, but never calls `fsync()` on the file or directory.

The code comment on line 6 says "Atomic tmp->fsync->rename pattern" but the implementation skips the fsync step entirely. The code does:
```
std::fs::write(&tmp_path, &buf)  // line 75
std::fs::rename(&tmp_path, path)  // line 77
```

Without `fsync()` on the file before rename, and `fsync()` on the parent directory after rename, a power failure or OS crash can result in:
- Zero-length files (rename completed, data not yet on disk)
- Missing files (directory entry not persisted)

This affects ALL bitmap persistence: filter bitmaps, sort layers, alive bitmap, slot counter, cursors, deferred alive map.

The same issue exists in:
- `src/bitmap_fs.rs:94-105` (`write_bytes_atomic`)
- `src/docstore.rs:434-439` (`write_shard_file`)
- `src/docstore.rs:166-170` (`save_field_dict`)

**Impact:** On power loss/crash, bitmap or docstore data may be corrupted or zeroed out. Next restart would load empty/corrupt bitmaps, potentially losing the entire index.

**Suggested fix:** After `std::fs::write`, open the file, call `.sync_all()`, then rename. After rename, open the parent directory and call `sync_all()` on it (POSIX) or use `FlushFileBuffers` (Windows). Example:
```rust
std::fs::write(&tmp_path, &buf)?;
let f = std::fs::File::open(&tmp_path)?;
f.sync_all()?;
std::fs::rename(&tmp_path, path)?;
```

---

### F5: Flush and merge thread panics are silently swallowed

**File:** `src/concurrent_engine.rs:383` (flush thread), `src/concurrent_engine.rs:894` (merge thread)
**Severity:** HIGH
**Pattern:** Both critical background threads are spawned with bare `thread::spawn`. Panics are swallowed by `handle.join().ok()` at line 3254/3257.

If the flush thread panics (e.g., due to an unexpected bitmap state, arithmetic overflow, or OOM):
1. The crossbeam mutation channel fills up
2. All `put()`/`delete()` calls block or fail with "channel disconnected"
3. No snapshots are published — queries see stale data forever
4. There is zero logging or alerting that the thread died

If the merge thread panics:
1. No more bitmap snapshots are written to disk
2. Data that was in memory is never persisted
3. Next restart loses all data since the last successful snapshot

With `panic = "abort"`, the flush thread panic kills the process immediately. Without abort, it's worse — the process stays alive but non-functional.

**Impact:** Silent data loss and query staleness. The server appears healthy (responds to `/api/health`) but mutations are not being processed.

**Suggested fix:**
1. Wrap the flush/merge thread bodies in `std::panic::catch_unwind`. On panic: log the error, set a health flag to `unhealthy`, attempt recovery or trigger shutdown.
2. Add a health check that verifies the flush thread is alive (e.g., check that `flush_publish_count` is advancing, or use a heartbeat atomic).
3. The `/api/health` endpoint should check flush thread liveness.

---

### F6: Docstore write errors silently dropped — mutations appear successful but docs are lost

**File:** `src/concurrent_engine.rs:837-839`
**Severity:** HIGH
**Pattern:**
```rust
if let Err(e) = docstore.lock().put_batch(&doc_batch) {
    eprintln!("docstore batch write failed: {e}");
}
```

When the flush thread's docstore write fails, the error is printed to stderr and discarded. The bitmap mutations have already been applied and published. This means:
- The document's bitmaps are correct (queries return the ID)
- But the document content is missing from the docstore
- Subsequent upserts cannot diff against the old doc (they'll treat it as a fresh insert)
- Clean deletes will fail to read the doc and can't clear filter bitmaps properly

Same pattern at lines 873-875 (final drain), 2788-2789 (put_bulk writer), 2795-2796 (put_bulk writer), 2811-2812 (write_docs_to_docstore).

**Impact:** Silent data inconsistency. Document retrieval returns empty results for affected IDs. Upsert diffs are incorrect. Delete cleanup is incomplete. No metric or alert signals the failure.

**Suggested fix:**
1. Track docstore write failures as a Prometheus counter.
2. On failure, queue the failed batch for retry (bounded retry buffer).
3. Add a "docstore lag" metric showing how many docs are pending write.
4. Consider making the `put()` path return an error if the docstore channel is disconnected.

---

### F7: Load endpoint accepts arbitrary filesystem paths — path traversal vulnerability

**File:** `src/server.rs:859`
**Severity:** MEDIUM
**Pattern:** `let path = PathBuf::from(&req.path);` — the load endpoint takes a raw file path from the HTTP request body with no sanitization.

A POST to `/api/indexes/{name}/load` with `{"path": "/etc/passwd"}` or `{"path": "C:\\Windows\\System32\\config\\SAM"}` would attempt to parse the file as NDJSON. While it wouldn't succeed in loading secrets (the parser would skip malformed lines), it could:
- Leak information via error messages
- Allow loading data from unexpected locations
- Be used as an SSRF-like primitive to probe the filesystem

**Impact:** Potential information disclosure or unexpected data loading from arbitrary filesystem paths.

**Suggested fix:** Validate that the path is under a configured allowed directory. Reject paths containing `..`, or canonicalize and verify the prefix.

---

### F8: No request body size limit on HTTP endpoints

**File:** `src/server.rs:423-450`
**Severity:** MEDIUM
**Pattern:** No `RequestBodyLimit` layer or body size configuration on the axum router.

The upsert endpoint (`/api/indexes/{name}/documents/upsert`) accepts a JSON array of documents with no size limit. An attacker or misconfigured client could send a multi-GB request body, causing:
- OOM as axum buffers the entire body before deserializing
- Process crash or degraded performance for all concurrent users

Axum's default body limit is 2MB, but this may be insufficient for legitimate bulk upserts, and there's no explicit configuration to make the limit visible/tunable.

**Impact:** Potential OOM crash or resource exhaustion from oversized requests.

**Suggested fix:** Add an explicit `tower_http::limit::RequestBodyLimitLayer` to the router with a configurable limit (e.g., 50MB for upsert endpoints, 1MB for query endpoints).

---

### F9: `unwrap()` in server handler on `guard.as_ref()` after dropping lock

**File:** `src/server.rs:1150`
**Severity:** MEDIUM
**Pattern:**
```rust
let idx = guard.as_ref().unwrap();
```

In `handle_upsert`, the code acquires the mutex, clones the `engine` Arc, releases the lock, then re-acquires the lock and calls `unwrap()` on `guard.as_ref()`. Between the two lock acquisitions, another request could delete the index via `handle_delete_index`, making `guard.as_ref()` return `None` and causing a panic.

With `panic = "abort"`, this crashes the entire server.

**Impact:** Race condition between concurrent delete-index and upsert requests causes server crash.

**Suggested fix:** Replace with `if let Some(idx) = guard.as_ref()` or restructure to hold the lock for both operations.

---

### F10: All logging uses `eprintln!` — no structured logging, no log levels

**File:** Throughout all source files (183 occurrences across 15 files)
**Severity:** MEDIUM
**Pattern:** Despite having `tracing` and `tracing-subscriber` in dependencies, the engine uses raw `eprintln!` for all operational logging.

Consequences:
- No log levels — can't distinguish warnings from informational messages
- No structured fields — can't search/filter by index name, field, slot ID
- No timestamps (unless stderr is piped through something that adds them)
- Can't be sent to log aggregation (Loki, CloudWatch, etc.) without parsing
- High-volume messages (lazy load notifications, eviction reports) can't be suppressed
- The pg-sync binary does use `tracing_subscriber::fmt()` init (line 49-54) but the engine code it calls uses `eprintln!`

**Impact:** Severely impaired production debugging and monitoring. Can't correlate events across components. Can't suppress noisy messages without code changes.

**Suggested fix:** Replace `eprintln!` calls with `tracing::info!`, `tracing::warn!`, `tracing::error!` with structured fields. The dependency is already present — just needs adoption.

---

### F11: Integer overflow in docstore shard write — `data_offset` is u32

**File:** `src/docstore.rs:396-400`
**Severity:** MEDIUM
**Pattern:**
```rust
let mut data_offset: u32 = 0;
for (slot_id, doc_bytes) in entries {
    offsets.push((*slot_id, data_offset, doc_bytes.len() as u32));
    raw_data.extend_from_slice(doc_bytes);
    data_offset += doc_bytes.len() as u32;
}
```

If documents in a single shard total more than 4GB of uncompressed data, `data_offset` wraps around silently. The same issue exists in `bitmap_fs.rs:150-154` and `bitmap_fs.rs:447-448` for bitmap pack files.

At 512 docs/shard with typical document sizes (~200 bytes each), this is unlikely to trigger. But with very large documents or a future increase in shard size, it could cause silent data corruption.

**Impact:** Low probability but catastrophic impact — silent data corruption if triggered.

**Suggested fix:** Use `checked_add` and return an error if the offset would overflow, or use `u64` for the offset and length fields.

---

### F12: Lazy load channel is unbounded — potential memory exhaustion

**File:** `src/concurrent_engine.rs:295-296`
**Severity:** LOW
**Pattern:** `let (lazy_tx, lazy_rx): (Sender<LazyLoad>, Receiver<LazyLoad>) = crossbeam_channel::unbounded();`

The lazy load channel is unbounded. If many concurrent query threads trigger lazy loads simultaneously (e.g., at startup with a burst of queries), each sends a full field's worth of bitmaps through this channel. For tagIds (~6.5GB), multiple copies in the channel could exhaust memory.

In practice, the pending set acts as a gate (only one thread loads per field), but per-value loads for lazy_value_fields could theoretically queue up many `FilterValues` messages.

**Impact:** Unlikely memory exhaustion under specific concurrent load patterns at startup.

**Suggested fix:** Use a bounded channel (e.g., capacity 16) with backpressure, or add a loading-in-progress flag per field.

---

### F13: `serde_json::to_string_pretty(&definition).unwrap()` in request handler

**File:** `src/server.rs:686`
**Severity:** LOW
**Pattern:** `let config_json = serde_json::to_string_pretty(&definition).unwrap();`

Called in `handle_create_index`. If serialization fails (theoretically shouldn't for well-typed structs, but could with custom Serialize impls or extreme nesting), the server panics and (with `panic = "abort"`) dies.

Similar: `src/server.rs:964` — `serde_json::to_value(&status).unwrap()` in load status handler.

**Impact:** Low probability crash. Serialization of well-known types rarely fails.

**Suggested fix:** Replace with `map_err` returning a 500 error.

---

### F14: `/api/health` does not check engine health

**File:** `src/server.rs` (inferred from route at line 445)
**Severity:** LOW
**Pattern:** The health endpoint likely returns 200 unconditionally without checking if the flush thread is alive, if the docstore is writable, or if the engine is in a degraded state.

**Impact:** Kubernetes liveness/readiness probes will report the server as healthy even when the flush thread has died, the disk is full, or the engine is non-functional.

**Suggested fix:** Check flush thread heartbeat, available disk space, and ability to read from the docstore.

---

### F15: Cursor name from HTTP request used directly as filesystem path component

**File:** `src/bitmap_fs.rs:687`
**Severity:** LOW
**Pattern:** `Self::write_bytes_atomic(&dir.join(name), value.as_bytes())` where `name` comes from the HTTP request (`cursor.name` in upsert handler).

A cursor name containing `/` or `..` could write to arbitrary filesystem locations relative to the cursors directory. The pg-sync code uses hardcoded cursor names (`pg-sync-{replica_id}`), but the HTTP API accepts arbitrary names.

**Impact:** Potential filesystem path traversal if an attacker controls cursor names.

**Suggested fix:** Validate cursor names to only contain alphanumeric characters, hyphens, and underscores.

---

### F16: No timeout on docstore reads during delete/upsert

**File:** `src/concurrent_engine.rs:1177`, `src/concurrent_engine.rs:1245`
**Severity:** LOW
**Pattern:** `self.docstore.lock().get(id)?` — the docstore read in the put/delete path uses a parking_lot Mutex with no timeout.

If the docstore lock is held by a long-running operation (e.g., a batch write from the flush thread), individual put/delete requests will block indefinitely. This could cause request timeouts at the HTTP layer.

The docstore `get()` call itself does synchronous disk I/O (decompresses a shard file) which could be slow on a degraded disk.

**Impact:** Request latency spikes when docstore is contended or disk is slow.

**Suggested fix:** Consider using a `try_lock_for` timeout, or move docstore reads to a dedicated thread pool with bounded concurrency.

---

## Prioritized Top 5

| Priority | Finding | Severity | Rationale |
|----------|---------|----------|-----------|
| 1 | **F3: No graceful shutdown** | HIGH | Data loss on every deployment. Mutations in the pipeline are silently dropped. This is the single highest-impact issue for production. |
| 2 | **F4: Missing fsync in atomic writes** | HIGH | Power failure corrupts the entire bitmap index. The code comments claim fsync happens but it doesn't. Crash recovery is broken. |
| 3 | **F5: Flush/merge thread panics silently swallowed** | HIGH | If the flush thread dies, the server looks healthy but is non-functional. Combined with F1 (panic=abort), any panic kills the process with no cleanup. |
| 4 | **F2: expect() in loader thread** | HIGH | API-triggered file load with an invalid path kills the entire server. Easy to trigger, easy to fix. |
| 5 | **F6: Docstore write errors silently dropped** | HIGH | Bitmap and docstore state diverge silently. Queries return IDs for docs that don't exist. Subsequent upserts/deletes produce incorrect results. No way to detect or recover. |

### Honorable mentions
- **F1 (panic=abort)** amplifies every other finding but is a deliberate performance tradeoff
- **F7 (path traversal in load)** is exploitable if the server is network-exposed
- **F10 (eprintln everywhere)** makes all other issues harder to diagnose in production
