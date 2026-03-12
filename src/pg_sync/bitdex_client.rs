use reqwest::Client;
use serde_json::Value;

/// HTTP client for pushing changes to the Bitdex server.
pub struct BitdexClient {
    client: Client,
    base_url: String,
}

impl BitdexClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    /// Upsert a batch of documents. Each doc is a serde_json::Value matching the data schema.
    pub async fn upsert_batch(&self, docs: &[Value]) -> Result<(), String> {
        let url = format!("{}/documents/upsert", self.base_url);
        let resp = self.client
            .post(&url)
            .json(docs)
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

    /// Delete a batch of documents by ID.
    pub async fn delete_batch(&self, ids: &[i64]) -> Result<(), String> {
        let url = format!("{}/documents", self.base_url);
        let resp = self.client
            .delete(&url)
            .json(ids)
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
}
