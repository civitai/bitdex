//! RSS-aware memory pressure detection and cache eviction.
//!
//! Reads the process RSS and compares against a memory budget (from cgroup v2,
//! environment variable, or config). When RSS exceeds the pressure threshold,
//! the flush thread triggers cache eviction to bring memory under the target.
//!
//! This bypasses the serialized_size() accuracy problem in unified cache —
//! real RSS is the eviction signal, not tracked byte counts.

use std::sync::atomic::{AtomicU64, Ordering};

/// Memory pressure configuration.
#[derive(Debug, Clone)]
pub struct MemoryPressureConfig {
    /// Total memory budget in bytes. Auto-detected from cgroup or config.
    pub budget_bytes: u64,
    /// Fraction of budget at which eviction triggers (default 0.80).
    pub pressure_threshold: f64,
    /// Fraction of budget to evict down to (default 0.75).
    pub pressure_target: f64,
    /// How often to check (in flush cycles). Default 100 (~5-10s at 50μs flush interval).
    pub check_interval_cycles: u64,
}

impl Default for MemoryPressureConfig {
    fn default() -> Self {
        Self {
            budget_bytes: 32 * 1024 * 1024 * 1024, // 32 GB default
            pressure_threshold: 0.80,
            pressure_target: 0.75,
            check_interval_cycles: 100,
        }
    }
}

/// Detect memory budget from cgroup v2, environment variable, or config.
///
/// Priority:
/// 1. Config value (if explicitly set)
/// 2. BITDEX_MEMORY_LIMIT_BYTES environment variable
/// 3. K8s downward API: BITDEX_POD_MEMORY_LIMIT env var
/// 4. cgroup v2: /sys/fs/cgroup/memory.max
/// 5. Default: 32 GB
pub fn detect_memory_budget(config_value: Option<u64>) -> u64 {
    // 1. Explicit config
    if let Some(v) = config_value {
        if v > 0 {
            return v;
        }
    }

    // 2. BITDEX_MEMORY_LIMIT_BYTES env var
    if let Ok(v) = std::env::var("BITDEX_MEMORY_LIMIT_BYTES") {
        if let Ok(bytes) = v.parse::<u64>() {
            if bytes > 0 {
                return bytes;
            }
        }
    }

    // 3. K8s downward API env var (set via resourceFieldRef: limits.memory)
    if let Ok(v) = std::env::var("BITDEX_POD_MEMORY_LIMIT") {
        if let Ok(bytes) = v.parse::<u64>() {
            if bytes > 0 {
                return bytes;
            }
        }
    }

    // 4. cgroup v2 memory.max (Linux only)
    #[cfg(target_os = "linux")]
    {
        if let Ok(contents) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
            let trimmed = contents.trim();
            if trimmed != "max" {
                if let Ok(bytes) = trimmed.parse::<u64>() {
                    if bytes > 0 {
                        return bytes;
                    }
                }
            }
        }
    }

    // 5. Default
    32 * 1024 * 1024 * 1024
}

/// Memory pressure state, shared between the flush thread and metrics.
pub struct MemoryPressureState {
    pub config: MemoryPressureConfig,
    /// Total evictions triggered by memory pressure.
    pub pressure_evictions: AtomicU64,
    /// Last observed RSS when pressure check ran.
    pub last_rss_bytes: AtomicU64,
}

impl MemoryPressureState {
    pub fn new(config: MemoryPressureConfig) -> Self {
        Self {
            config,
            pressure_evictions: AtomicU64::new(0),
            last_rss_bytes: AtomicU64::new(0),
        }
    }

    /// Check if RSS exceeds the pressure threshold.
    pub fn is_under_pressure(&self, rss_bytes: u64) -> bool {
        let threshold = (self.config.budget_bytes as f64 * self.config.pressure_threshold) as u64;
        rss_bytes > threshold
    }

    /// Target RSS to evict down to.
    pub fn target_bytes(&self) -> u64 {
        (self.config.budget_bytes as f64 * self.config.pressure_target) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = MemoryPressureConfig::default();
        assert_eq!(config.budget_bytes, 32 * 1024 * 1024 * 1024);
        assert!((config.pressure_threshold - 0.80).abs() < f64::EPSILON);
        assert!((config.pressure_target - 0.75).abs() < f64::EPSILON);
    }

    #[test]
    fn test_pressure_detection() {
        let config = MemoryPressureConfig {
            budget_bytes: 32 * 1024 * 1024 * 1024, // 32 GB
            pressure_threshold: 0.80,
            pressure_target: 0.75,
            check_interval_cycles: 100,
        };
        let state = MemoryPressureState::new(config);

        // 24 GB = 75% of 32 GB — not under pressure
        assert!(!state.is_under_pressure(24 * 1024 * 1024 * 1024));

        // 26 GB = 81.25% of 32 GB — under pressure
        assert!(state.is_under_pressure(26 * 1024 * 1024 * 1024));

        // Target should be 75% = 24 GB
        assert_eq!(state.target_bytes(), 24 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_detect_budget_config_priority() {
        // Config value takes priority
        assert_eq!(detect_memory_budget(Some(16 * 1024 * 1024 * 1024)), 16 * 1024 * 1024 * 1024);

        // Zero config falls through
        let budget = detect_memory_budget(Some(0));
        assert!(budget > 0); // should get env var or default
    }
}
