//! Effective configuration introspection and explicit commit-trigger mode.

use hakodb::config::{CommitTriggerMode, DurabilityMode, HakoConfig};
use hakodb::engine::Hako;

fn dir(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "hako-effective-config-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&p);
    p
}

#[test]
fn effective_config_reflects_live_setters() {
    let d = dir("live");
    let cfg = HakoConfig {
        durability_mode: DurabilityMode::Interval,
        ..Default::default()
    };
    let db = Hako::open(&d, cfg).expect("open");

    let initial = db.effective_config();
    assert_eq!(initial["durability_mode"], "interval");
    assert_eq!(initial["commit_trigger_mode"], "legacy");
    assert_eq!(initial["group_commit_interval_ms"], 5);

    // Changing the interval must not change the trigger mode implicitly.
    db.set_group_commit_interval_ms_all(30);
    let changed = db.effective_config();
    assert_eq!(changed["group_commit_interval_ms"], 30);
    assert_eq!(changed["commit_trigger_mode"], "legacy");

    db.set_commit_trigger_mode_all(CommitTriggerMode::StrictTime);
    let strict = db.effective_config();
    assert_eq!(strict["commit_trigger_mode"], "strict-time");
    assert_eq!(strict["group_commit_interval_ms"], 30);

    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn explicit_strict_config_is_reported_at_open() {
    let d = dir("open");
    let cfg = HakoConfig {
        durability_mode: DurabilityMode::Interval,
        commit_trigger_mode: CommitTriggerMode::StrictTime,
        group_commit_interval_ms: 30,
        ..Default::default()
    };
    let db = Hako::open(&d, cfg).expect("open");
    let report = db.effective_config();
    assert_eq!(report["commit_trigger_mode"], "strict-time");
    assert_eq!(report["group_commit_max_ops"], 128);
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn effective_config_includes_quiescence_and_stats() {
    let d = dir("status");
    let db = Hako::open(&d, HakoConfig::default()).expect("open");
    let report = db.effective_config();
    let q = &report["quiescence"];
    assert_eq!(q["indexes_ready"], report["indexes_ready"]);
    assert!(q["is_quiescent"].is_boolean());
    assert!(q["pending_index_ops"].is_number());
    assert!(q["index_backfills"].is_number());
    assert!(q["pending_blob_bytes"].is_number());
    assert!(q["queued_blob_items"].is_number());
    assert!(q["maintenance_running"].is_boolean());
    assert!(report["stats"].is_object());
    let _ = std::fs::remove_dir_all(&d);
}
