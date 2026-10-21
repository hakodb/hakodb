//! hakodb#22: expanded covering projections. Secondary equality should
//! answer the indexed field (and `id`) without a document read; every other
//! projection must still fall back to decode. Parity is checked against the
//! decode path, not against hand-built expectations.

use hakodb::config::HakoConfig;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::query::query::Query;

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("issue22-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn setup(tag: &str) -> Hako {
    let db = Hako::open(&tmp_dir(tag), HakoConfig::default()).unwrap();
    db.create_index("docs", "status").unwrap();
    let mut batch = Vec::new();
    for i in 0..400 {
        let mut d = HakoDoc::default();
        d.insert("status", Value::String(if i % 2 == 0 { "open" } else { "closed" }.into()));
        d.insert("payload", Value::String(format!("p{i}")));
        batch.push(BatchMutation::Put {
            collection: "docs".into(),
            doc_id: format!("d{i:03}"),
            doc: d,
        });
    }
    db.write_batch(batch).unwrap();
    db.await_quiescent(std::time::Duration::from_secs(10));
    db
}

fn open_query() -> Query {
    Query::new("docs").where_eq("status", Value::String("open".into()))
}

#[test]
fn secondary_equality_covers_field_and_id() {
    let db = setup("secondary");

    let rep = db
        .explain_report(open_query().select_fields(vec!["status".to_string()]))
        .unwrap();
    assert!(rep.covering, "indexed field is covered: {rep}");
    assert!(rep.scan.starts_with("secondary_index"), "scan: {}", rep.scan);

    let rep = db
        .explain_report(open_query().select_fields(vec!["id".to_string()]))
        .unwrap();
    assert!(rep.covering, "id is carried by the scan row: {rep}");

    let rep = db
        .explain_report(open_query().select_fields(vec!["payload".to_string()]))
        .unwrap();
    assert!(!rep.covering, "payload still needs a document read: {rep}");

    // Fast path: status comes from the plan; pointer liveness still comes
    // from the scan. Compare against a projection that forces decode.
    let fast = db
        .query_projected_zero_copy(open_query(), &["status".to_string()])
        .unwrap();
    let slow = db
        .query_projected_zero_copy(
            open_query(),
            &["status".to_string(), "payload".to_string()],
        )
        .unwrap();
    let mut fast: Vec<_> = fast
        .into_iter()
        .map(|(id, fields)| {
            let status = fields
                .iter()
                .find(|(k, _)| k == "status")
                .map(|(_, v)| v.clone())
                .expect("covering status");
            (id, status)
        })
        .collect();
    let mut slow: Vec<_> = slow
        .into_iter()
        .map(|(id, fields)| {
            let status = fields
                .iter()
                .find(|(k, _)| k == "status")
                .map(|(_, v)| v.clone())
                .expect("decoded status");
            (id, status)
        })
        .collect();
    fast.sort_by(|a, b| a.0.cmp(&b.0));
    slow.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(fast, slow, "covering rows match decode");
    assert!(fast.iter().all(|(_, v)| v == &Value::String("open".into())));

    // id-only: the output key is the id, so the field list stays empty.
    let fast = db
        .query_projected_zero_copy(open_query(), &["id".to_string()])
        .unwrap();
    let slow = db
        .query_projected_zero_copy(
            open_query(),
            &["id".to_string(), "payload".to_string()],
        )
        .unwrap();
    assert!(fast.iter().all(|(_, fields)| fields.is_empty()));
    let mut fast_ids: Vec<_> = fast.into_iter().map(|(id, _)| id).collect();
    let mut slow_ids: Vec<_> = slow.into_iter().map(|(id, _)| id).collect();
    fast_ids.sort();
    slow_ids.sort();
    assert_eq!(fast_ids, slow_ids, "id-only rows match decode");
}
