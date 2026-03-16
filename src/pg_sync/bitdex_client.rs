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
}
