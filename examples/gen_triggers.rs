/// Generate trigger SQL from the sync config for review and PG testing.
/// Usage: cargo run --example gen_triggers --features pg-sync
use std::fs;

fn main() {
    let yaml = fs::read_to_string("deploy/configs/sync-config-civitai.yaml")
        .expect("Failed to read sync config");

    let config = bitdex_v2::pg_sync::sync_config::FullSyncConfig::from_yaml(&yaml)
        .expect("Failed to parse sync config");

    let mut sql = String::new();
    sql.push_str("-- BitDex V2 Trigger SQL\n");
    sql.push_str("-- Generated from deploy/configs/sync-config-civitai.yaml\n\n");

    for (i, trigger) in config.triggers.iter().enumerate() {
        let trigger_sql = bitdex_v2::pg_sync::trigger_gen::generate_trigger_sql(trigger);
        sql.push_str(&format!(
            "-- [{}/{}] Table: {}\n",
            i + 1,
            config.triggers.len(),
            trigger.table
        ));
        sql.push_str(&trigger_sql);
        sql.push_str("\n\n");
    }

    let out_path = "data/trigger-review.sql";
    fs::write(out_path, &sql).expect("Failed to write SQL file");
    eprintln!("Wrote {} bytes to {}", sql.len(), out_path);
    print!("{}", sql);
}
