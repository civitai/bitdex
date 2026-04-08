use std::time::Duration;

use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;

/// HTTP client for pushing changes to the Bitdex server.
pub struct BitdexClient {
    client: Client,
    base_url: String,
    /// Server root URL (e.g. "http://localhost:3000") for health checks.
    server_root: String,
}

#[derive(Deserialize)]
struct CursorResponse {
    #[allow(dead_code)]
    name: String,
    value: String,
}

impl BitdexClient {
    /// Create a new BitdexClient.
    ///
    /// `base_url` can be either:
    ///   - Full index URL: "http://host:3000/api/indexes/civitai"
    ///   - Server root: "http://host:3000" (index_name required)
    ///
    /// If `base_url` already contains `/api/indexes/`, it's used as-is.
    /// Otherwise, `index_name` is appended as `/api/indexes/{name}`.
    pub fn new(base_url: &str) -> Self {
        Self::with_index(base_url, None)
    }

    pub fn with_index(base_url: &str, index_name: Option<&str>) -> Self {
        let base = base_url.trim_end_matches('/').to_string();
        let base_url = if base.contains("/api/indexes/") {
            base.clone()
        } else if let Some(name) = index_name {
            format!("{}/api/indexes/{}", base, name)
        } else {
            base.clone()
        };
        // Derive server root for health checks
        let server_root = base_url
            .find("/api/indexes/")
            .map(|pos| base_url[..pos].to_string())
            .unwrap_or_else(|| base.clone());
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .build()
            .expect("failed to build HTTP client");
        Self {
            client,
            base_url,
            server_root,
        }
    }

    /// Check if the BitDex server is reachable and healthy.
    /// Returns true if GET /api/health returns 200, false otherwise.
    /// Uses a short timeout so the health gate reacts quickly to failures.
    pub async fn is_healthy(&self) -> bool {
        let url = format!("{}/api/health", self.server_root);
        match self.client.get(&url).timeout(Duration::from_secs(3)).send().await {
            Ok(resp) => resp.status().is_success(),
            Err(_) => false,
        }
    }

    /// Get the alive document count from the server's stats endpoint.
    /// Returns 0 if the server is unreachable or the index isn't loaded.
    pub async fn get_alive_count(&self) -> u64 {
        let url = format!("{}/stats", self.base_url);
        match self.client.get(&url).timeout(Duration::from_secs(5)).send().await {
            Ok(resp) if resp.status().is_success() => {
                resp.json::<serde_json::Value>().await.ok()
                    .and_then(|v| v.get("alive_count")?.as_u64())
                    .unwrap_or(0)
            }
            _ => 0,
        }
    }

    /// Upsert a batch of documents, optionally advancing a named cursor.
    pub async fn upsert_batch(
        &self,
        docs: &[Value],
        cursor: Option<(&str, &str)>,
    ) -> Result<(), String> {
        let url = format!("{}/documents/upsert", self.base_url);
        let mut body = serde_json::json!({ "documents": docs });
        if let Some((name, value)) = cursor {
            body["cursor"] = serde_json::json!({ "name": name, "value": value });
        }
        let resp = self.client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("upsert request failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("upsert returned {status}: {body}"));
        }
        Ok(())
    }

    /// Patch a batch of documents (partial update), optionally advancing a named cursor.
    /// Only provided fields are updated; missing fields are preserved.
    pub async fn patch_batch(
        &self,
        docs: &[Value],
        cursor: Option<(&str, &str)>,
    ) -> Result<(), String> {
        let url = format!("{}/documents/patch", self.base_url);
        let mut body = serde_json::json!({ "documents": docs });
        if let Some((name, value)) = cursor {
            body["cursor"] = serde_json::json!({ "name": name, "value": value });
        }
        let resp = self.client
            .patch(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("patch request failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("patch returned {status}: {body}"));
        }
        Ok(())
    }

    /// Delete a batch of documents by ID, optionally advancing a named cursor.
    pub async fn delete_batch(
        &self,
        ids: &[i64],
        cursor: Option<(&str, &str)>,
    ) -> Result<(), String> {
        let url = format!("{}/documents", self.base_url);
        let mut body = serde_json::json!({ "ids": ids });
        if let Some((name, value)) = cursor {
            body["cursor"] = serde_json::json!({ "name": name, "value": value });
        }
        let resp = self.client
            .delete(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("delete request failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("delete returned {status}: {body}"));
        }
        Ok(())
    }

    /// Sync filter values for a filter_only multi-value field.
    /// Replaces all bitmap memberships for the given slots on the named field.
    pub async fn filter_sync(
        &self,
        field: &str,
        entries: &[(i64, Vec<i64>)],
    ) -> Result<(), String> {
        let url = format!("{}/documents/filter-sync", self.base_url);
        let documents: Vec<Value> = entries
            .iter()
            .map(|(id, values)| {
                debug_assert!(*id >= 0 && *id <= u32::MAX as i64, "image ID {} exceeds u32 range", id);
                serde_json::json!({
                    "id": *id as u32,
                    "values": values.iter().map(|v| *v as u64).collect::<Vec<_>>(),
                })
            })
            .collect();
        let body = serde_json::json!({
            "field": field,
            "documents": documents,
        });
        let resp = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("filter_sync request failed: {e}"))?;

        let status = resp.status();
        let resp_body: Value = resp
            .json()
            .await
            .map_err(|e| format!("filter_sync response parse failed: {e}"))?;

        // Check for HTTP errors
        if status.is_server_error() || status.is_client_error() {
            return Err(format!("filter_sync returned {status}: {resp_body}"));
        }

        // Check for partial failures (207 Multi-Status)
        if let Some(errors) = resp_body.get("errors") {
            if let Some(arr) = errors.as_array() {
                if !arr.is_empty() {
                    return Err(format!(
                        "filter_sync partial failure: {} errors: {}",
                        arr.len(),
                        resp_body,
                    ));
                }
            }
        }

        Ok(())
    }

    /// Read a named cursor from BitDex. Returns None if the cursor doesn't exist.
    /// Signal the engine to reload a field's existence set from disk.
    pub async fn reload_field(&self, field_name: &str) -> Result<(), String> {
        let url = format!("{}/fields/{}/reload", self.base_url, field_name);
        let resp = self.client.post(&url).send().await
            .map_err(|e| format!("reload_field request failed: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("reload_field returned {status}: {body}"));
        }
        Ok(())
    }

    /// Report pg-sync cycle metrics to the BitDex server for Prometheus exposition.
    /// Fire-and-forget: errors are logged but don't affect the poller.
    pub async fn report_pgsync_metrics(
        &self,
        replica_id: &str,
        cycle_seconds: f64,
        rows_fetched: u64,
        cursor_position: i64,
    ) {
        let url = format!("{}/api/internal/pgsync-metrics", self.server_root);
        let body = serde_json::json!({
            "replica": replica_id,
            "cycle_seconds": cycle_seconds,
            "rows_fetched": rows_fetched,
            "cursor_position": cursor_position,
        });
        let _ = self.client
            .post(&url)
            .json(&body)
            .timeout(Duration::from_secs(2))
            .send()
            .await;
    }

    /// POST a batch of V2 ops to the BitDex /ops endpoint.
    pub async fn post_ops(&self, batch: &super::ops::OpsBatch) -> Result<(), String> {
        let url = format!("{}/ops", self.base_url);
        let resp = self.client
            .post(&url)
            .json(batch)
            .send()
            .await
            .map_err(|e| format!("post_ops request failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("post_ops returned {status}: {body}"));
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Dump lifecycle methods (for boot sequence)
    // -----------------------------------------------------------------------

    /// Wait for BitDex to become healthy, retrying with backoff.
    /// Returns Ok(()) when healthy, Err after max_retries.
    pub async fn wait_for_healthy(&self, max_retries: u32, initial_delay_secs: u64) -> Result<(), String> {
        let mut delay = initial_delay_secs;
        for attempt in 1..=max_retries {
            if self.is_healthy().await {
                return Ok(());
            }
            eprintln!(
                "BitDex not healthy (attempt {attempt}/{max_retries}), retrying in {delay}s..."
            );
            tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
            delay = (delay * 2).min(30); // exponential backoff, cap at 30s
        }
        Err(format!("BitDex not healthy after {max_retries} attempts"))
    }

    /// Register a new dump via PUT /dumps.
    /// Returns the dump entry response (name + status).
    pub async fn register_dump(
        &self,
        dump_request: &serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let url = format!("{}/dumps", self.base_url);

        // Retry with backoff for 503 (index not loaded yet after health check passes)
        let mut attempts = 0u32;
        let max_retries = 30; // up to ~5 minutes with backoff
        loop {
            let resp = self
                .client
                .put(&url)
                .json(dump_request)
                .timeout(std::time::Duration::from_secs(30))
                .send()
                .await
                .map_err(|e| format!("register_dump request failed: {e}"))?;

            let status = resp.status();
            let body: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| format!("register_dump response parse failed: {e}"))?;

            if status.as_u16() == 503 && attempts < max_retries {
                attempts += 1;
                let delay = std::cmp::min(2u64.pow(attempts.min(5)), 30);
                eprintln!(
                    "register_dump: 503 (index not loaded), retry {}/{} in {}s",
                    attempts, max_retries, delay
                );
                tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                continue;
            }

            if status.is_client_error() || status.is_server_error() {
                return Err(format!("register_dump returned {status}: {body}"));
            }

            return Ok(body);
        }
    }

    /// Signal that a dump file is complete via POST /dumps/{name}/loaded.
    pub async fn signal_dump_loaded(
        &self,
        dump_name: &str,
        ops_written: u64,
    ) -> Result<(), String> {
        let url = format!("{}/dumps/{}/loaded", self.base_url, dump_name);
        let body = serde_json::json!({ "ops_written": ops_written });
        let resp = self
            .client
            .post(&url)
            .json(&body)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| format!("signal_dump_loaded failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("signal_dump_loaded returned {status}: {body}"));
        }
        Ok(())
    }

    /// Get dump registry via GET /dumps.
    pub async fn get_dumps(&self) -> Result<serde_json::Value, String> {
        let url = format!("{}/dumps", self.base_url);
        let resp = self
            .client
            .get(&url)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| format!("get_dumps failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("get_dumps returned {status}: {body}"));
        }

        resp.json()
            .await
            .map_err(|e| format!("get_dumps parse failed: {e}"))
    }

    /// Poll dump status until all specified dumps are complete.
    /// Returns Ok(()) when all_complete, Err on timeout or failure.
    pub async fn poll_dumps_until_complete(
        &self,
        dump_names: &[String],
        poll_interval_secs: u64,
        timeout_secs: u64,
    ) -> Result<(), String> {
        let start = std::time::Instant::now();
        let poll_interval = std::time::Duration::from_secs(poll_interval_secs);
        let timeout = std::time::Duration::from_secs(timeout_secs);

        loop {
            if start.elapsed() > timeout {
                return Err(format!(
                    "Dump processing timed out after {}s",
                    timeout_secs
                ));
            }

            let dumps = self.get_dumps().await?;
            let dump_map = dumps.get("dumps").and_then(|d| d.as_object());

            let mut all_complete = true;
            let mut any_failed = false;

            for name in dump_names {
                if let Some(map) = &dump_map {
                    if let Some(entry) = map.get(name) {
                        let status = entry.get("status").and_then(|s| s.as_str()).unwrap_or("unknown");
                        match status {
                            "Complete" => {}
                            "Failed" => {
                                any_failed = true;
                                let err = entry.get("status")
                                    .and_then(|s| s.get("Failed"))
                                    .and_then(|s| s.as_str())
                                    .unwrap_or("unknown error");
                                return Err(format!("Dump '{name}' failed: {err}"));
                            }
                            _ => {
                                all_complete = false;
                                let ops_processed = entry.get("ops_processed").and_then(|v| v.as_u64()).unwrap_or(0);
                                let ops_written = entry.get("ops_written").and_then(|v| v.as_u64()).unwrap_or(0);
                                eprintln!(
                                    "  {name}: {status} ({ops_processed}/{ops_written} ops)"
                                );
                            }
                        }
                    } else {
                        all_complete = false;
                        eprintln!("  {name}: not found in dump registry");
                    }
                }
            }

            if all_complete && !any_failed {
                return Ok(());
            }

            tokio::time::sleep(poll_interval).await;
        }
    }

    pub async fn get_cursor(&self, cursor_name: &str) -> Result<Option<String>, String> {
        let url = format!("{}/cursors/{}", self.base_url, cursor_name);
        let resp = self.client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("get_cursor request failed: {e}"))?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("get_cursor returned {status}: {body}"));
        }

        let cursor: CursorResponse = resp
            .json()
            .await
            .map_err(|e| format!("get_cursor parse failed: {e}"))?;
        Ok(Some(cursor.value))
    }

    /// Set a named cursor on the BitDex server. Persisted to disk on the
    /// server side via snapshot save, so the value survives server restarts.
    /// Used by the metrics poller to track its closed-window high-water-mark.
    pub async fn set_cursor(&self, cursor_name: &str, value: &str) -> Result<(), String> {
        let url = format!("{}/cursors/{}", self.base_url, cursor_name);
        let body = serde_json::json!({ "value": value });
        let resp = self.client
            .put(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("set_cursor request failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("set_cursor returned {status}: {body}"));
        }
        Ok(())
    }
}
