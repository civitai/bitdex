use std::collections::HashMap;

use serde_json::{json, Value};

use super::queries::{ImageRow, ResourceRow, TagRow, TechniqueRow, ToolRow};

/// Enrichment data collected from the parallel queries (Q2-Q5).
/// Keyed by image ID.
pub struct EnrichmentData {
    pub tags: HashMap<i64, Vec<i64>>,
    pub tools: HashMap<i64, Vec<i64>>,
    pub techniques: HashMap<i64, Vec<i64>>,
    pub resources: HashMap<i64, ResourceInfo>,
    pub metrics: HashMap<i64, MetricInfo>,
}

pub struct ResourceInfo {
    pub base_model: Option<String>,
    pub model_version_ids: Vec<i64>,
    pub model_version_ids_manual: Vec<i64>,
    pub resource_poi: bool,
}

#[derive(Default)]
pub struct MetricInfo {
    pub reaction_count: i64,
    pub comment_count: i64,
    pub collected_count: i64,
}

impl EnrichmentData {
    /// Build enrichment data from query results.
    pub fn from_rows(
        tags: Vec<TagRow>,
        tools: Vec<ToolRow>,
        techniques: Vec<TechniqueRow>,
        resources: Vec<ResourceRow>,
    ) -> Self {
        let mut tag_map: HashMap<i64, Vec<i64>> = HashMap::new();
        for t in tags {
            tag_map.entry(t.image_id as i64).or_default().push(t.tag_id as i64);
        }

        let mut tool_map: HashMap<i64, Vec<i64>> = HashMap::new();
        for t in tools {
            tool_map.entry(t.image_id as i64).or_default().push(t.tool_id as i64);
        }

        let mut technique_map: HashMap<i64, Vec<i64>> = HashMap::new();
        for t in techniques {
            technique_map
                .entry(t.image_id as i64)
                .or_default()
                .push(t.technique_id as i64);
        }

        let mut resource_map: HashMap<i64, ResourceInfo> = HashMap::new();
        for r in resources {
            resource_map.insert(
                r.image_id as i64,
                ResourceInfo {
                    base_model: r.base_model,
                    model_version_ids: r.model_version_ids,
                    model_version_ids_manual: r.model_version_ids_manual,
                    resource_poi: r.resource_poi.unwrap_or(false),
                },
            );
        }

        EnrichmentData {
            tags: tag_map,
            tools: tool_map,
            techniques: technique_map,
            resources: resource_map,
            metrics: HashMap::new(),
        }
    }
}

/// Assemble a single image row + enrichment data into a serde_json::Value
/// matching the Bitdex data schema.
pub fn assemble_document(image: &ImageRow, enrichment: &EnrichmentData) -> Value {
    let id = image.id;

    // Compute sortAt as unix epoch seconds
    let sort_at_secs = image.sort_at.map(|dt| dt.timestamp()).unwrap_or(0);

    // Compute timestamp milliseconds for doc-only fields
    let sort_at_unix_ms = image.sort_at.map(|dt| dt.timestamp_millis()).unwrap_or(0);
    // publishedAtUnix must be null (not 0) when unpublished — the exists_boolean
    // mapping sets isPublished=true for any non-null value including 0.
    let published_at_unix_ms: Option<i64> = image
        .published_at
        .map(|dt| dt.timestamp_millis())
        .filter(|&ms| ms > 0);
    let existed_at_unix_ms = image
        .created_at
        .map(|dt| dt.timestamp_millis())
        .unwrap_or(0);

    // hasMeta: meta is not null, not JSON null, and not hidden
    let has_meta = match &image.meta {
        Some(v) if !v.is_null() => !image.hide_meta.unwrap_or(false),
        _ => false,
    };

    // onSite: meta contains "civitaiResources"
    let on_site = match &image.meta {
        Some(Value::Object(map)) => map.contains_key("civitaiResources"),
        _ => false,
    };

    // poi: image.poi OR resource_poi
    let resource_info = enrichment.resources.get(&id);
    let poi = image.poi.unwrap_or(false) || resource_info.map_or(false, |r| r.resource_poi);

    let minor = image.minor.unwrap_or(false);

    // Enrichment arrays
    let tag_ids = enrichment.tags.get(&id).cloned().unwrap_or_default();
    let tool_ids = enrichment.tools.get(&id).cloned().unwrap_or_default();
    let technique_ids = enrichment
        .techniques
        .get(&id)
        .cloned()
        .unwrap_or_default();
    let model_version_ids = resource_info
        .map(|r| r.model_version_ids.clone())
        .unwrap_or_default();
    let model_version_ids_manual = resource_info
        .map(|r| r.model_version_ids_manual.clone())
        .unwrap_or_default();
    let base_model = resource_info
        .and_then(|r| r.base_model.clone())
        .unwrap_or_default();

    // Metrics: only include when enriched from ClickHouse. Omitting them
    // prevents the outbox poller from zeroing out bulk-loaded metrics.
    // The metrics_poller handles metric updates separately.
    let metrics = enrichment.metrics.get(&id);

    let mut doc = json!({
        "id": id,
        "nsfwLevel": image.nsfw_level.unwrap_or(0),
        "userId": image.user_id.unwrap_or(0),
        "postId": image.post_id,
        "postedToId": image.posted_to_id,
        "type": image.image_type.as_deref().unwrap_or("image"),
        "baseModel": base_model,
        "availability": image.availability.as_deref().unwrap_or("Public"),
        "blockedFor": image.blocked_for.as_deref(),
        // TODO: isRemix not implemented — remixOfId not available in current
        // COPY query or outbox enrichment. Needs Image.remixOfId column added
        // to the enrichment query. See pipeline audit #4, finding A6.
        "remixOfId": null,
        "tagIds": tag_ids,
        "modelVersionIds": model_version_ids,
        "modelVersionIdsManual": model_version_ids_manual,
        "toolIds": tool_ids,
        "techniqueIds": technique_ids,
        "sortAt": sort_at_secs,
        "sortAtUnix": sort_at_unix_ms,
        "publishedAtUnix": published_at_unix_ms,
        "existedAtUnix": existed_at_unix_ms,
        "url": image.url.as_deref(),
        "hash": image.hash.as_deref(),
    });

    // Only include metrics when we have real data from ClickHouse
    if let Some(m) = metrics {
        if let Some(obj) = doc.as_object_mut() {
            obj.insert("reactionCount".into(), json!(m.reaction_count));
            obj.insert("commentCount".into(), json!(m.comment_count));
            obj.insert("collectedCount".into(), json!(m.collected_count));
        }
    }

    // Exists-boolean fields: only set when true
    if let Some(obj) = doc.as_object_mut() {
        if has_meta {
            obj.insert("hasMeta".into(), json!(true));
        }
        if on_site {
            obj.insert("onSite".into(), json!(true));
        }
        if poi {
            obj.insert("poi".into(), json!(true));
        }
        if minor {
            obj.insert("minor".into(), json!(true));
        }
    }

    doc
}

/// Batch-assemble documents from images + enrichment.
pub fn assemble_batch(images: &[ImageRow], enrichment: &EnrichmentData) -> Vec<Value> {
    images
        .iter()
        .map(|img| assemble_document(img, enrichment))
        .collect()
}
