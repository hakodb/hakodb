//! FireLite embedded document database - minimal example.
//!
//! Build & run from this directory:
//!
//!   cargo run            (debug)
//!   cargo run --release  (release)
//!
//! The first build compiles the FireLite engine crate as a dependency, so it
//! can take a couple of minutes.

use firelite::config::{DurabilityMode, FireLiteConfig};
use firelite::document::firelite_doc::FireLiteDoc;
use firelite::document::value::Value;
use firelite::engine::{BatchMutation, FireLite};
use firelite::error::Result;
use firelite::query::query::Query;

fn main() -> Result<()> {
    // ---- Open the database (file is created on first use) ----
    let mut config = FireLiteConfig::default();
    config.durability_mode = DurabilityMode::Always;
    config.query_workers = 4;
    let db = FireLite::open("demo.db", config)?;
    println!("opened demo.db ({} collections)", db.list_collections()?.len());

    // ---- Insert documents ----
    let mut alice = FireLiteDoc::default();
    alice.insert("name", Value::String("Alice".into()));
    alice.insert("age", Value::Int(32));
    alice.insert("active", Value::Bool(true));
    db.put("users", "u1", &alice)?;

    let mut bob = FireLiteDoc::default();
    bob.insert("name", Value::String("Bob".into()));
    bob.insert("age", Value::Int(27));
    bob.insert("tags", Value::Array(vec![Value::String("admin".into())]));
    db.put("users", "u2", &bob)?;

    println!("inserted u1, u2");

    // ---- Read a single document back ----
    if let Some(doc) = db.get("users", "u1")? {
        println!("u1 -> {}", doc.to_json());
    }

    // ---- Query with filter + order + limit ----
    let results = db.query(
        Query::new("users")
            .where_eq("active", Value::Bool(true))
            .order_by("age", true)
            .limit(10),
    )?;
    println!("query(active=true) -> {} hits", results.len());

    // ---- Aggregation ----
    let agg = db.execute_aggregation(
        Query::new("users").aggregate(firelite::query::query::AggregateOp::Avg("age".into())),
    )?;
    println!("avg(age) -> {:?}", agg);

    // ---- Atomic batch ----
    let mut carol = FireLiteDoc::default();
    carol.insert("name", Value::String("Carol".into()));
    carol.insert("age", Value::Int(41));
    let written = db.write_batch(vec![
        BatchMutation::Put {
            collection: "users".into(),
            doc_id: "u3".into(),
            doc: carol,
        },
        BatchMutation::Patch {
            collection: "users".into(),
            doc_id: "u2".into(),
            updates: vec![("age".to_string(), Value::Int(28))],
        },
    ])?;
    println!("batch wrote {} docs", written.len());

    // ---- Serializable transaction ----
    let mut tx = db.begin_serializable_transaction();
    if let Some(doc) = tx.get(&db, "users", "u1")? {
        let mut updated = doc.clone();
        updated.insert("age", Value::Int(33));
        tx.put("users", "u1", updated);
    }
    tx.commit(&db)?;
    println!("transaction committed");

    // ---- Real-time watch (non-blocking poll) ----
    let rx = db.watch_collection("users");
    let mut probe = FireLiteDoc::default();
    probe.insert("probe", Value::Bool(true));
    db.put("users", "u_probe", &probe)?;
    while let Ok(event) = rx.try_recv() {
        println!("watch event: {:?}", event.kind);
    }

    // ---- Cleanup ----
    db.compact()?;
    println!("done");
    Ok(())
}
