//! Config preset loader.
//!
//! Loads named TOML preset files and merges them into a Config struct.
//! Presets are partial overlays — only specified fields override defaults.
//! Unspecified fields keep their current values.

use std::path::Path;

use serde::Deserialize;

use crate::config::{CacheConfig, Config};

/// Partial config overlay loaded from a TOML preset file.
/// All fields are optional — only specified fields override the base config.
#[derive(Debug, Deserialize, Default)]
pub struct PresetOverlay {
    /// Preset metadata (name, description). Not applied to config.
    #[serde(default)]
    pub metadata: PresetMetadata,

    /// Top-level config overrides.
    #[serde(default)]
    pub flush_interval_us: Option<u64>,
    #[serde(default)]
    pub merge_interval_ms: Option<u64>,
    #[serde(default)]
    pub channel_capacity: Option<usize>,
    #[serde(default)]
    pub compact_threshold_pct: Option<u64>,
    #[serde(default)]
    pub eviction_sweep_interval: Option<u64>,
    #[serde(default)]
    pub max_page_size: Option<usize>,

    /// Cache config overrides.
    #[serde(default)]
    pub cache: Option<CacheOverlay>,

    /// Unified cache overrides.
    #[serde(default)]
    pub unified_cache: Option<UnifiedCacheOverlay>,

    /// Doc cache overrides.
    #[serde(default)]
    pub doc_cache: Option<DocCacheOverlay>,

    /// ShardStore overrides.
    #[serde(default)]
    pub shard_store: Option<ShardStoreOverlay>,
}

#[derive(Debug, Deserialize, Default)]
pub struct PresetMetadata {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Debug, Deserialize, Default)]
pub struct CacheOverlay {
    pub max_entries: Option<usize>,
    pub decay_rate: Option<f64>,
    pub bound_target_size: Option<usize>,
    pub bound_max_size: Option<usize>,
    pub bound_max_count: Option<usize>,
    pub prefetch_threshold: Option<f64>,
    pub preload_bounds: Option<bool>,
    pub max_maintenance_work: Option<usize>,
    pub max_maintenance_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
pub struct UnifiedCacheOverlay {
    pub max_bytes: Option<usize>,
    pub max_entries: Option<usize>,
    pub initial_capacity: Option<usize>,
    pub max_capacity: Option<usize>,
    pub min_filter_size: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
pub struct DocCacheOverlay {
    pub max_bytes: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
pub struct ShardStoreOverlay {
    pub compact_threshold: Option<u32>,
}

/// Load a preset from a TOML file.
pub fn load_preset(path: &Path) -> Result<PresetOverlay, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("read preset {}: {e}", path.display()))?;
    toml::from_str(&content)
        .map_err(|e| format!("parse preset {}: {e}", path.display()))
}

/// Apply a preset overlay to a Config, modifying it in place.
/// Only fields present in the overlay are changed.
pub fn apply_preset(config: &mut Config, preset: &PresetOverlay) {
    // Top-level overrides
    if let Some(v) = preset.flush_interval_us { config.flush_interval_us = v; }
    if let Some(v) = preset.merge_interval_ms { config.merge_interval_ms = v; }
    if let Some(v) = preset.channel_capacity { config.channel_capacity = v; }
    if let Some(v) = preset.compact_threshold_pct { config.compact_threshold_pct = v; }
    if let Some(v) = preset.eviction_sweep_interval { config.eviction_sweep_interval = v; }
    if let Some(v) = preset.max_page_size { config.max_page_size = v; }

    // Cache overrides
    if let Some(ref c) = preset.cache {
        apply_cache_overlay(&mut config.cache, c);
    }
}

fn apply_cache_overlay(cache: &mut CacheConfig, overlay: &CacheOverlay) {
    if let Some(v) = overlay.max_entries { cache.max_entries = v; }
    if let Some(v) = overlay.decay_rate { cache.decay_rate = v; }
    if let Some(v) = overlay.bound_target_size { cache.bound_target_size = v; }
    if let Some(v) = overlay.bound_max_size { cache.bound_max_size = v; }
    if let Some(v) = overlay.bound_max_count { cache.bound_max_count = v; }
    if let Some(v) = overlay.prefetch_threshold { cache.prefetch_threshold = v; }
    if let Some(v) = overlay.preload_bounds { cache.preload_bounds = v; }
    if let Some(v) = overlay.max_maintenance_work { cache.max_maintenance_work = v; }
    if let Some(v) = overlay.max_maintenance_ms { cache.max_maintenance_ms = v; }
}

/// Load and apply a preset file to a Config.
pub fn load_and_apply(config: &mut Config, path: &Path) -> Result<String, String> {
    let preset = load_preset(path)?;
    let name = preset.metadata.name.clone();
    apply_preset(config, &preset);
    Ok(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_baseline_preset() {
        let toml = r#"
[metadata]
name = "baseline"
description = "No overrides"
"#;
        let overlay: PresetOverlay = toml::from_str(toml).unwrap();
        assert_eq!(overlay.metadata.name, "baseline");
        assert!(overlay.flush_interval_us.is_none());
        assert!(overlay.cache.is_none());
    }

    #[test]
    fn test_apply_cache_overlay() {
        let toml = r#"
[metadata]
name = "test"

[cache]
bound_target_size = 50000
max_maintenance_ms = 5
"#;
        let overlay: PresetOverlay = toml::from_str(toml).unwrap();
        let mut config = Config::default();
        apply_preset(&mut config, &overlay);
        assert_eq!(config.cache.bound_target_size, 50000);
        assert_eq!(config.cache.max_maintenance_ms, 5);
        // Unset fields keep defaults
        assert_eq!(config.cache.bound_max_size, 20000);
    }

    #[test]
    fn test_apply_top_level_overlay() {
        let toml = r#"
flush_interval_us = 50
merge_interval_ms = 2000

[metadata]
name = "test"
"#;
        let overlay: PresetOverlay = toml::from_str(toml).unwrap();
        let mut config = Config::default();
        apply_preset(&mut config, &overlay);
        assert_eq!(config.flush_interval_us, 50);
        assert_eq!(config.merge_interval_ms, 2000);
        // Unset keeps default
        assert_eq!(config.channel_capacity, 100_000);
    }
}
