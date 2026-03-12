use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;

/// HTTP client for pushing changes to the Bitdex server.
pub struct BitdexClient {
    client: Client,
    base_url: String,
}

#[derive(Deserialize)]
struct CursorResponse {
    #[allow(dead_code)]
    name: String,
    value: String,
}

impl BitdexClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
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
