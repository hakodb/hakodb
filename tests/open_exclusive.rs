//! #217 P1: exclusive data-dir locking — `open_exclusive` refuses a
//! second holder (cross-handle: in-process too), releases on drop, and
//! plain `open` stays lock-free (caller-owned semantics, unchanged).

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;

fn cfg() -> HakoConfig {
    let mut c = HakoConfig::default();
    c.durability_mode = DurabilityMode::Manual;
    c
}

fn tmp(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "fl-test-openx-{label}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[test]
fn open_exclusive_blocks_second_holder_and_releases_on_drop() {
    let dir = tmp("block");
    let db = Hako::open_exclusive(&dir, cfg()).unwrap();
    db.put("docs", "a", &doc()).unwrap();

    let second = Hako::open_exclusive(&dir, cfg());
    let err = match second {
        Err(e) => e,
        Ok(_) => panic!("second exclusive open must fail loudly"),
    };
    assert!(
        err.to_string().contains("already open"),
        "error must name the conflict: {err}"
    );

    // The lock releases with the engine: the next exclusive open works
    // and the data dir is intact.
    drop(db);
    let third = Hako::open_exclusive(&dir, cfg()).unwrap();
    let hits = third.get("docs", "a").unwrap();
    assert!(hits.is_some(), "data survived the lock handover");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn plain_open_stays_lock_free() {
    let dir = tmp("plain");
    // Legacy semantics: two plain opens both succeed — cross-instance
    // visibility is the CALLER's problem (never guaranteed; that is the
    // gap open_exclusive closes). The lock file must not disturb either.
    let a = Hako::open(&dir, cfg()).unwrap();
    let b = Hako::open(&dir, cfg()).unwrap();
    a.put("docs", "a", &doc()).unwrap();
    assert!(a.get("docs", "a").unwrap().is_some());
    drop(b);
    drop(a);
    // After both close, a fresh open sees the state (sanity).
    let c = Hako::open_exclusive(&dir, cfg()).unwrap();
    assert!(c.get("docs", "a").unwrap().is_some());
    let _ = std::fs::remove_dir_all(&dir);
}

fn doc() -> HakoDoc {
    let mut d = HakoDoc::default();
    d.insert("x", Value::String("v".into()));
    d
}
