//! Admin user management (`__users`).
//!
//! Complements the auth primitives with listing, creation, updates and
//! deletion. Two guards protect against lockout: the last enabled admin
//! can never be disabled, demoted or deleted, and nobody can delete
//! themselves (use another admin for that).

use firelite::document::value::Value;
use firelite::engine::FireLite;
use serde::Serialize;

use crate::auth::{upsert_user, Role, USERS_COLLECTION};

#[derive(Debug, Clone, Serialize)]
pub struct UserView {
    pub username: String,
    pub role: Role,
    pub disabled: bool,
    pub created_at: i64,
}

fn view_of(username: &str, doc: &firelite::document::firelite_doc::FireLiteDoc) -> Option<UserView> {
    let role = match doc.get("role") {
        Some(Value::String(s)) => Role::parse(s)?,
        _ => return None,
    };
    let disabled = matches!(doc.get("disabled"), Some(Value::Bool(true)));
    let created_at = match doc.get("created_at") {
        Some(Value::Int(t)) => *t,
        _ => 0,
    };
    Some(UserView {
        username: username.to_string(),
        role,
        disabled,
        created_at,
    })
}

pub fn list_users(db: &FireLite) -> Vec<UserView> {
    let rows = db
        .query(firelite::query::query::Query::new(USERS_COLLECTION))
        .unwrap_or_default();
    let mut out: Vec<UserView> = rows
        .iter()
        .filter_map(|(id, doc)| view_of(id, doc))
        .collect();
    out.sort_by(|a, b| a.username.cmp(&b.username));
    out
}

pub fn get_user(db: &FireLite, username: &str) -> Option<UserView> {
    db.get(USERS_COLLECTION, username)
        .ok()?
        .and_then(|doc| view_of(username, &doc))
}

/// Other enabled admins besides `except` (empty string = count all).
fn other_enabled_admins(db: &FireLite, except: &str) -> usize {
    list_users(db)
        .iter()
        .filter(|u| u.role == Role::Admin && !u.disabled && u.username != except)
        .count()
}

fn guard_not_last_admin(db: &FireLite, target: &UserView, actor: &str) -> Result<(), String> {
    if target.username == actor {
        return Err("cannot change your own account: ask another admin".into());
    }
    if target.role == Role::Admin && !target.disabled && other_enabled_admins(db, &target.username) == 0 {
        return Err("refusing: last enabled admin".into());
    }
    Ok(())
}

pub fn create_user(
    db: &FireLite,
    username: &str,
    password: &str,
    role: Role,
) -> Result<UserView, String> {
    if get_user(db, username).is_some() {
        return Err("user already exists".into());
    }
    upsert_user(db, username, password, role, false)?;
    get_user(db, username).ok_or("unreachable".into())
}

pub fn update_user(
    db: &FireLite,
    actor: &str,
    username: &str,
    role: Option<Role>,
    disabled: Option<bool>,
    password: Option<&str>,
) -> Result<UserView, String> {
    let current = get_user(db, username).ok_or("unknown user")?;
    let next_role = role.unwrap_or(current.role);
    let next_disabled = disabled.unwrap_or(current.disabled);
    // Guard only when the change *removes* admin power.
    let demoting = next_role != Role::Admin || next_disabled;
    if demoting && current.role == Role::Admin && !current.disabled {
        guard_not_last_admin(db, &current, actor)?;
    }
    if let Some(pw) = password {
        if pw.is_empty() {
            return Err("empty password".into());
        }
        // Validated constructor: length checks + fresh hash, preserving
        // the (possibly updated) role/disabled flags.
        upsert_user(db, username, pw, next_role, next_disabled)?;
    } else {
        // Preserve the existing hash: rewrite the row manually.
        let mut doc = firelite::document::firelite_doc::FireLiteDoc::default();
        let raw = db
            .get(USERS_COLLECTION, username)
            .ok()
            .flatten()
            .ok_or("unknown user")?;
        let hash = match raw.get("password_hash") {
            Some(Value::String(h)) => h.clone(),
            _ => return Err("corrupt user row".into()),
        };
        doc.insert("password_hash", Value::String(hash));
        doc.insert(
            "role",
            Value::String(
                match next_role {
                    Role::Viewer => "viewer",
                    Role::Operator => "operator",
                    Role::Admin => "admin",
                }
                .to_string(),
            ),
        );
        doc.insert("disabled", Value::Bool(next_disabled));
        doc.insert(
            "created_at",
            raw.get("created_at").cloned().unwrap_or(Value::Int(0)),
        );
        db.put(USERS_COLLECTION, username, &doc)
            .map(|_| ())
            .map_err(|e| format!("store user: {e}"))?;
    }
    get_user(db, username).ok_or("unreachable".into())
}

pub fn delete_user(db: &FireLite, actor: &str, username: &str) -> Result<(), String> {
    let current = get_user(db, username).ok_or("unknown user")?;
    guard_not_last_admin(db, &current, actor)?;
    db.delete(USERS_COLLECTION, username)
        .map(|_| ())
        .map_err(|e| format!("delete user: {e}"))
}
