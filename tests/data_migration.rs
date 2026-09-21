//! Pre-rebrand data-plane migration: `__firelite_*` directories become
//! their `__hako_*` canonical names on open, data intact, exclusion
//! preserved. `__users`/`__groups` never carried the brand and are untouched.

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "hakodb-migtest-{}-{}",
        std::process::id(),
        tag
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn put_simple(db: &Hako, col: &str, id: &str, n: i64) {
    let mut doc = HakoDoc::default();
    doc.insert("v", Value::Int(n));
    db.put(col, id, &doc).unwrap();
}

fn open_manual(dir: &std::path::Path) -> Hako {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    Hako::open(dir, cfg).unwrap()
}

#[test]
fn legacy_dirs_migrate_with_data_intact() {
    let dir = temp_dir("basic");
    {
        let db = open_manual(&dir);
        // Old-binary layout, as FireLite-era versions left it.
        put_simple(&db, "__firelite_system", "probe", 1);
        put_simple(&db, "__firelite_rooms", "room1", 2);
        put_simple(&db, "__firelite_security", "policy", 3);
        put_simple(&db, "__users", "admin", 4);
        put_simple(&db, "users", "alice", 5);
    }
    // Reopen: migration renames the three branded directories.
    // (Note: phase 1 already left an EMPTY canonical dir behind — normal
    // engine reads auto-create shard dirs. Migration must replace it.)
    {
        let db = open_manual(&dir);
        for old in ["__firelite_system", "__firelite_rooms", "__firelite_security"] {
            assert!(
                !dir.join(old).exists(),
                "legacy dir must be gone: {old}"
            );
        }
        for new in ["__hako_system", "__hako_rooms", "__hako_security"] {
            assert!(dir.join(new).is_dir(), "canonical dir missing: {new}");
        }
        let get = |col: &str, id: &str| {
            db.get(col, id)
                .unwrap()
                .expect("migrated doc missing")
                .get("v")
                .cloned()
        };
        assert_eq!(get("__hako_system", "probe"), Some(Value::Int(1)));
        assert_eq!(get("__hako_rooms", "room1"), Some(Value::Int(2)));
        assert_eq!(get("__hako_security", "policy"), Some(Value::Int(3)));
        // Unbranded collections pass through untouched.
        assert_eq!(get("__users", "admin"), Some(Value::Int(4)));
        assert_eq!(get("users", "alice"), Some(Value::Int(5)));

        // Exclusion holds on both spellings: canonical and alias alike.
        for col in [
            "__hako_system",
            "__hako_rooms",
            "__firelite_system",
            "__firelite_rooms",
            "__users",
            "__groups",
        ] {
            assert!(
                db.is_sync_excluded_effective(col),
                "{col} must be sync-excluded"
            );
        }
        assert!(!db.is_sync_excluded_effective("users"));
        assert!(!db.is_sync_excluded_effective("__hako_security"));

        // Enumeration: canonical plane hidden, user data listed.
        let cols = db.sync_collections().unwrap();
        assert!(cols.contains(&"users".to_string()), "{cols:?}");
        for hidden in ["__hako_system", "__hako_rooms", "__users"] {
            assert!(!cols.contains(&hidden.to_string()), "{cols:?}");
        }
        // Security replicates by design (unchanged across the rebrand).
        assert!(cols.contains(&"__hako_security".to_string()), "{cols:?}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn both_present_keeps_canonical_and_orphan() {
    // Downgrade cycle: canonical exists AND legacy reappears (old binary
    // ran, file-level restore). Canonical wins; the orphan stays excluded
    // and inert — never merged, never synced, never deleted.
    let dir = temp_dir("both");
    {
        let db = open_manual(&dir);
        put_simple(&db, "__hako_system", "keep", 10);
        put_simple(&db, "users", "u", 11);
        std::fs::create_dir_all(dir.join("__firelite_system")).unwrap();
        // Seed the orphan directly behind the engine's back, the way a
        // downgraded binary would have left it.
        put_simple(&db, "__firelite_system", "orphan", 99);
    }
    {
        let db = open_manual(&dir);
        let keep = db
            .get("__hako_system", "keep")
            .unwrap()
            .expect("canonical doc missing");
        assert_eq!(keep.get("v").cloned(), Some(Value::Int(10)));
        // Orphan dir still on disk (never deleted) but excluded.
        assert!(dir.join("__firelite_system").is_dir());
        assert!(db.is_sync_excluded_effective("__firelite_system"));
        assert!(!db.sync_collections().unwrap().contains(&"__firelite_system".to_string()));
    }
    let _ = std::fs::remove_dir_all(&dir);
}
