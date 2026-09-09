//! Sync group management (`__groups` collection).
//!
//! A group binds a room name to an admission policy. Absent row (or
//! `mode: "open"`) admits everyone — the historic default. `registered`
//! admits only a valid API key (+ listed members when the list is
//! non-empty). Enforcement lives in the library handshake
//! (`check_group_access`); this module is the admin management surface.
//! Plaintexts keys exist only in the create/rotate response (shown once);
//! only SHA-256 hashes persist (see `hash_api_key`).

use firelite::cloud_sync::hash_api_key;
use firelite::document::firelite_doc::FireLiteDoc;
use firelite::document::value::Value;
use firelite::engine::FireLite;
use serde::{Deserialize, Serialize};

pub const GROUPS_COLLECTION: &str = "__groups";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GroupMode {
    Open,
    Registered,
}

impl GroupMode {
    fn parse(s: &str) -> Option<GroupMode> {
        match s {
            "open" => Some(GroupMode::Open),
            "registered" => Some(GroupMode::Registered),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            GroupMode::Open => "open",
            GroupMode::Registered => "registered",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct GroupView {
    pub room_name: String,
    pub mode: GroupMode,
    pub members: Vec<String>,
    pub created_at: i64,
    pub has_key: bool,
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn get_str(doc: &FireLiteDoc, field: &str) -> Option<String> {
    match doc.get(field) {
        Some(Value::String(s)) => Some(s.clone()),
        _ => None,
    }
}

fn valid_room_name(name: &str) -> bool {
    let n = name.trim();
    !n.is_empty() && n.len() <= 64
}

pub fn get_group(db: &FireLite, room_name: &str) -> Option<GroupView> {
    let doc = db.get(GROUPS_COLLECTION, room_name).ok()??;
    let mode = doc
        .get("mode")
        .and_then(|v| match v {
            Value::String(s) => GroupMode::parse(s),
            _ => None,
        })
        .unwrap_or(GroupMode::Open);
    let members: Vec<String> = match doc.get("members") {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|v| match v {
                Value::String(s) => Some(s.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };
    let created_at = match doc.get("created_at") {
        Some(Value::Int(t)) => *t,
        _ => 0,
    };
    let has_key = doc.get("api_key_hash").is_some();
    Some(GroupView {
        room_name: room_name.to_string(),
        mode,
        members,
        created_at,
        has_key,
    })
}

pub fn list_groups(db: &FireLite) -> Vec<GroupView> {
    let rows = match db.query(firelite::query::query::Query::new(GROUPS_COLLECTION)) {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };
    let mut out: Vec<GroupView> = rows
        .into_iter()
        .filter_map(|(id, _)| get_group(db, &id))
        .collect();
    out.sort_by(|a, b| a.room_name.cmp(&b.room_name));
    out
}

fn write_group(
    db: &FireLite,
    room_name: &str,
    mode: GroupMode,
    key_hash: Option<String>,
    members: Vec<String>,
    created_at: i64,
) -> Result<(), String> {
    let mut doc = FireLiteDoc::default();
    doc.insert("mode", Value::String(mode.as_str().to_string()));
    if let Some(h) = key_hash {
        doc.insert("api_key_hash", Value::String(h));
    }
    doc.insert(
        "members",
        Value::Array(members.into_iter().map(Value::String).collect()),
    );
    doc.insert("created_at", Value::Int(created_at));
    db.put(GROUPS_COLLECTION, room_name, &doc)
        .map(|_| ())
        .map_err(|e| format!("store group: {e}"))
}

/// Create a group. Returns the plaintext API key exactly once (for
/// `registered`; `None` for `open`).
pub fn create_group(
    db: &FireLite,
    room_name: &str,
    mode: GroupMode,
    new_key: impl FnOnce() -> String,
) -> Result<(GroupView, Option<String>), String> {
    if !valid_room_name(room_name) {
        return Err("invalid room name".into());
    }
    if get_group(db, room_name).is_some() {
        return Err("group already exists".into());
    }
    let (key_hash, plaintext) = match mode {
        GroupMode::Open => (None, None),
        GroupMode::Registered => {
            let k = new_key();
            (Some(hash_api_key(&k)), Some(k))
        }
    };
    write_group(db, room_name, mode, key_hash, Vec::new(), now_secs())?;
    Ok((get_group(db, room_name).expect("just wrote"), plaintext))
}

/// Rotate a registered group's key. Returns the new plaintext once.
pub fn rotate_group_key(
    db: &FireLite,
    room_name: &str,
    new_key: impl FnOnce() -> String,
) -> Result<String, String> {
    let g = get_group(db, room_name).ok_or("unknown group")?;
    if g.mode != GroupMode::Registered {
        return Err("only registered groups have keys".into());
    }
    let k = new_key();
    write_group(
        db,
        room_name,
        g.mode,
        Some(hash_api_key(&k)),
        g.members,
        g.created_at,
    )?;
    Ok(k)
}

pub fn set_group_mode(db: &FireLite, room_name: &str, mode: GroupMode) -> Result<GroupView, String> {
    let g = get_group(db, room_name).ok_or("unknown group")?;
    // Switching to open drops the key hash (no stale secrets lingering).
    let key_hash = match mode {
        GroupMode::Open => None,
        GroupMode::Registered => db
            .get(GROUPS_COLLECTION, room_name)
            .ok()
            .flatten()
            .and_then(|d| get_str(&d, "api_key_hash")),
    };
    write_group(db, room_name, mode, key_hash, g.members, g.created_at)?;
    get_group(db, room_name).ok_or("unreachable".into())
}

pub fn add_member(db: &FireLite, room_name: &str, client_id: &str) -> Result<GroupView, String> {
    let g = get_group(db, room_name).ok_or("unknown group")?;
    let client_id = client_id.trim();
    if client_id.is_empty() || client_id.len() > 64 {
        return Err("invalid client id".into());
    }
    let mut members = g.members.clone();
    if !members.iter().any(|m| m == client_id) {
        members.push(client_id.to_string());
    }
    let key_hash = db
        .get(GROUPS_COLLECTION, room_name)
        .ok()
        .flatten()
        .and_then(|d| get_str(&d, "api_key_hash"));
    write_group(db, room_name, g.mode, key_hash, members, g.created_at)?;
    get_group(db, room_name).ok_or("unreachable".into())
}

pub fn remove_member(db: &FireLite, room_name: &str, client_id: &str) -> Result<GroupView, String> {
    let g = get_group(db, room_name).ok_or("unknown group")?;
    let members: Vec<String> = g.members.into_iter().filter(|m| m != client_id).collect();
    let key_hash = db
        .get(GROUPS_COLLECTION, room_name)
        .ok()
        .flatten()
        .and_then(|d| get_str(&d, "api_key_hash"));
    write_group(db, room_name, g.mode, key_hash, members, g.created_at)?;
    get_group(db, room_name).ok_or("unreachable".into())
}

pub fn delete_group(db: &FireLite, room_name: &str) -> Result<(), String> {
    if get_group(db, room_name).is_none() {
        return Err("unknown group".into());
    }
    db.delete(GROUPS_COLLECTION, room_name)
        .map(|_| ())
        .map_err(|e| format!("delete group: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use firelite::config::{DurabilityMode, FireLiteConfig};

    fn temp_db() -> (FireLite, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "fl-cs-groups-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut cfg = FireLiteConfig::default();
        cfg.durability_mode = DurabilityMode::Manual;
        (FireLite::open(&dir, cfg).unwrap(), dir)
    }

    fn keygen() -> String {
        "test-key-0123456789abcdef".to_string()
    }

    #[test]
    fn group_lifecycle_never_exposes_hash() {
        let (db, dir) = temp_db();
        let (view, key) = create_group(&db, "game", GroupMode::Registered, keygen).unwrap();
        assert_eq!(view.room_name, "game");
        assert!(view.has_key);
        assert_eq!(key, Some(keygen()));

        // Stored doc holds the hash, not the key.
        let raw = db.get(GROUPS_COLLECTION, "game").unwrap().unwrap();
        assert!(get_str(&raw, "api_key_hash").is_some());
        let json = serde_json::to_string(&view).unwrap();
        assert!(!json.contains("sekret") && !json.contains("hash"));

        // Duplicate create conflicts.
        assert!(create_group(&db, "game", GroupMode::Open, keygen).is_err());

        // Rotate replaces, mode flip to open drops the hash.
        let k2 = rotate_group_key(&db, "game", || "second-key".to_string()).unwrap();
        assert_eq!(k2, "second-key");
        let g = set_group_mode(&db, "game", GroupMode::Open).unwrap();
        assert!(!g.has_key);

        // Members round-trip, then delete.
        add_member(&db, "game", "alice").unwrap();
        add_member(&db, "game", "alice").unwrap(); // idempotent
        let g = remove_member(&db, "game", "alice").unwrap();
        assert!(g.members.is_empty());
        delete_group(&db, "game").unwrap();
        assert!(get_group(&db, "game").is_none());
        assert!(delete_group(&db, "game").is_err());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn invalid_inputs_rejected() {
        let (db, dir) = temp_db();
        assert!(create_group(&db, "", GroupMode::Open, keygen).is_err());
        assert!(create_group(&db, "ok", GroupMode::Open, keygen).is_ok());
        assert!(rotate_group_key(&db, "ok", keygen).is_err()); // open has no key
        assert!(add_member(&db, "ok", "").is_err());
        assert!(add_member(&db, "missing", "a").is_err());
        assert!(set_group_mode(&db, "missing", GroupMode::Open).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }
}
