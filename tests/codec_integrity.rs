use firelite::config::{DurabilityMode, FireLiteConfig};
use firelite::document::firelite_doc::FireLiteDoc;
use firelite::document::value::Value;
use firelite::engine::{BatchMutation, FireLite};
use firelite::query::query::Query;
use std::collections::HashMap;

/// Codec-level identity (no engine): every Value arm round-trips through
/// encode/decode byte-identically, including server timestamps, references
/// and blob links (engine semantics may rewrite some of these on write —
/// that is tested separately below with inflated reads).
#[test]
fn codec_roundtrip_identity() {
    let mut doc = FireLiteDoc::default();
    doc._time = 1_700_000_000_000_001;
    doc.insert("null", Value::Null);
    doc.insert("sts", Value::ServerTimestamp);
    doc.insert("bool", Value::Bool(true));
    doc.insert("smallint", Value::Int(7));
    doc.insert("bigint", Value::Int(-9_223_372_036_854_775_000));
    doc.insert("tinystr", Value::String("abcdefg".into()));
    doc.insert("str", Value::String("caf\u{e9} \u{2615} \"q\" \\ back".into()));
    doc.insert("float", Value::Float(1.5));
    doc.insert("inf", Value::Float(f64::INFINITY));
    doc.insert("bin", Value::Binary(vec![0u8, 1, 200, 255]));
    doc.insert("emptybin", Value::Binary(vec![]));
    doc.insert("ts", Value::Timestamp(1_700_000_000_000_002));
    doc.insert(
        "ref",
        Value::Reference { collection: "users".into(), doc_id: "u_9".into() },
    );
    doc.insert("link", Value::BlobLink { offset: 12345, len: 678 });
    doc.insert("emptymap", Value::Map(vec![]));
    doc.insert("emptyarr", Value::Array(vec![]));
    doc.insert(
        "nested",
        Value::Map(vec![
            ("zip".into(), Value::Int(90210)),
            ("tags".into(), Value::Array(vec![Value::String("a".into()), Value::Null])),
        ]),
    );

    let bytes = doc.encode();
    let back = FireLiteDoc::decode(&bytes).expect("decode own encoding");
    assert_eq!(back, doc, "codec must be an identity");
    assert_eq!(back.encode(), bytes, "re-encode must be byte-stable");
    assert!(FireLiteDoc::decode(&bytes[..bytes.len() - 1]).is_none() || bytes.len() < 12);
    assert!(FireLiteDoc::decode(b"short").is_none());
    assert!(FireLiteDoc::decode(&[]).is_none());
}

type Expected = HashMap<String, Vec<(String, Value)>>;

fn fields_of(doc: &FireLiteDoc) -> Vec<(String, Value)> {
    doc.fields.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
}

fn test_config() -> FireLiteConfig {
    let mut cfg = FireLiteConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    // Force every pointer state at test scale: blob links above 64B,
    // segment spill above 8KB of inlined bytes.
    cfg.value_blob_threshold_bytes = 64;
    cfg.max_inlined_memory_bytes = 8192;
    cfg
}

fn seed_mixed(db: &FireLite) -> (Expected, Vec<String>) {
    let mut expected: Expected = HashMap::new();
    let mut puts: Vec<BatchMutation> = Vec::new();
    // `fields` is the EXPECTED post-write shape (usually the doc itself;
    // the blob doc below expects its inflated shape).
    let mut put = |id: &str, doc: FireLiteDoc, fields: Vec<(String, Value)>| {
        expected.insert(id.to_string(), fields);
        puts.push(BatchMutation::Put {
            collection: "docs".into(),
            doc_id: id.into(),
            doc,
        });
    };

    let mut d = FireLiteDoc::default();
    put("empty", d.clone(), fields_of(&d));

    d = FireLiteDoc::default();
    d.insert("n", Value::Int(3));
    d.insert("s", Value::String("hi".into()));
    d.insert("b", Value::Bool(false));
    d.insert("z", Value::Null);
    put("small", d.clone(), fields_of(&d));

    d = FireLiteDoc::default();
    d.insert("big", Value::Int(i64::MIN));
    d.insert("f", Value::Float(-0.25));
    d.insert("ts", Value::Timestamp(-5));
    d.insert("uni", Value::String("caf\u{e9} \u{2615}".into()));
    d.insert(
        "ref",
        Value::Reference { collection: "c".into(), doc_id: "k".into() },
    );
    put("scalars", d.clone(), fields_of(&d));

    d = FireLiteDoc::default();
    d.insert(
        "nested",
        Value::Map(vec![
            ("a".into(), Value::Array(vec![Value::Int(1), Value::Map(vec![])])),
            ("e".into(), Value::Array(vec![])),
        ]),
    );
    put("nested", d.clone(), fields_of(&d));

    // Blob docs: values crossing the 64B threshold become BlobLinks.
    // ponytail: blob storage is type-erased raw bytes — inflation restores
    // String iff the bytes are valid UTF-8, else Binary. Lock BOTH sides:
    // invalid-UTF-8 bytes round-trip as Binary, big text as String.
    let big: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
    d = FireLiteDoc::default();
    d.insert("data", Value::Binary(big.clone()));
    d.insert("name", Value::String("blobdoc".into()));
    let mut inflated = FireLiteDoc::default();
    inflated.insert("data", Value::Binary(big));
    inflated.insert("name", Value::String("blobdoc".into()));
    put("blobdoc", d, fields_of(&inflated));

    let bigtext = format!("PHOTO_{}", "y".repeat(4096));
    d = FireLiteDoc::default();
    d.insert("photo", Value::String(bigtext.clone()));
    let mut inflated_t = FireLiteDoc::default();
    inflated_t.insert("photo", Value::String(bigtext));
    put("textblob", d, fields_of(&inflated_t));

    // Volume for scan paths.
    for i in 0..300 {
        let mut v = FireLiteDoc::default();
        v.insert("idx", Value::Int(i as i64));
        v.insert("tag", Value::String(format!("t{}", i % 7)));
        put(&format!("v_{i:04}"), v.clone(), fields_of(&v));
    }

    // Overwrite: latest wins everywhere.
    let mut o = FireLiteDoc::default();
    o.insert("ver", Value::Int(1));
    let of = fields_of(&o);
    put("over", o, of);
    let mut o2 = FireLiteDoc::default();
    o2.insert("ver", Value::Int(2));
    o2.insert("extra", Value::String("second".into()));
    let o2f = fields_of(&o2);
    put("over", o2, o2f);

    db.write_batch(puts).expect("seed");

    // Deletes: absent on every path afterwards.
    let dels = ["v_0001", "v_0002", "v_0003", "small", "nested"]
        .iter()
        .map(|id| {
            expected.remove(*id);
            BatchMutation::Delete { collection: "docs".into(), doc_id: (*id).into() }
        })
        .collect();
    db.write_batch(dels).expect("deletes");

    // Blob doc ids (inflated expectation differs from stored skeleton).
    (expected, vec!["blobdoc".to_string(), "textblob".to_string()])
}

/// Every read path must agree with `expected` field-for-field (_time is
/// engine-assigned and excluded by construction: fields_of skips it).
fn verify_all(db: &FireLite, expected: &Expected, blob_ids: &[String]) {
    // ponytail: queries issued before background index recovery plan
    // FullCollection (bounds ignored, pages repeat) — poll first. Same
    // race the cursor tests guard with wait_ready.
    let t0 = std::time::Instant::now();
    while !db.is_indexes_ready() {
        assert!(t0.elapsed() < std::time::Duration::from_secs(30), "indexes never ready");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    // 1. Point gets.
    for (id, fields) in expected {
        let got = db.get("docs", id).expect("get").unwrap_or_else(|| panic!("{id} missing"));
        assert_eq!(&fields_of(&got), fields, "get mismatch for {id}");
    }
    assert!(db.get("docs", "no-such-id").expect("get").is_none());
    for dead in ["v_0001", "small", "nested"] {
        assert!(db.get("docs", dead).expect("get").is_none(), "{dead} not deleted");
    }

    let mut sorted_ids: Vec<&String> = expected.keys().collect();
    sorted_ids.sort();

    // 2. Decoded scans, both directions + paging.
    let mut fwd = Vec::new();
    let mut anchor: Option<String> = None;
    loop {
        let mut q = Query::new("docs");
        q = q.order_by("id", true);
        q.limit = Some(50);
        if let Some(a) = &anchor {
            q = q.start_after(vec![Value::String(a.clone())]);
        }
        let rows = db.query(q).expect("page");
        if rows.is_empty() {
            break;
        }
        anchor = Some(rows.last().unwrap().0.clone());
        for (id, doc) in rows {
            assert_eq!(&fields_of(&doc), &expected[&id], "scan mismatch for {id}");
            fwd.push(id);
        }
    }
    assert_eq!(fwd.len(), expected.len(), "forward scan count");
    assert!(fwd.windows(2).all(|w| w[0] < w[1]));

    let mut rev = Vec::new();
    anchor = None;
    loop {
        let mut q = Query::new("docs");
        q = q.order_by("id", false);
        q.limit = Some(50);
        if let Some(a) = &anchor {
            q = q.start_after(vec![Value::String(a.clone())]);
        }
        let rows = db.query(q).expect("rev page");
        if rows.is_empty() {
            break;
        }
        anchor = Some(rows.last().unwrap().0.clone());
        for (id, doc) in rows {
            assert_eq!(&fields_of(&doc), &expected[&id], "rev scan mismatch for {id}");
            rev.push(id);
        }
    }
    assert_eq!(rev.len(), expected.len());
    assert!(rev.windows(2).all(|w| w[0] > w[1]));
    assert_eq!(rev, fwd.iter().rev().cloned().collect::<Vec<_>>());

    // 3. Offset + limit slice.
    let mut q = Query::new("docs");
    q = q.order_by("id", true);
    q.limit = Some(10);
    q.offset = Some(5);
    let rows = db.query(q).expect("offset query");
    let want: Vec<String> = sorted_ids[5..15].iter().map(|s| (*s).clone()).collect();
    let got: Vec<String> = rows.iter().map(|(id, _)| id.clone()).collect();
    assert_eq!(got, want, "offset/limit slice");

    // 4. Unordered FullCollection + limit: valid subset.
    let mut q = Query::new("docs");
    q.limit = Some(25);
    let rows = db.query(q).expect("full scan");
    assert_eq!(rows.len(), 25);
    for (id, doc) in &rows {
        assert_eq!(&fields_of(doc), &expected[id], "full scan mismatch for {id}");
    }

    // 5. Raw + decode + encode identity on every row.
    let mut q = Query::new("docs");
    q = q.order_by("id", true);
    let rows = db.query_raw(q).expect("raw scan");
    assert_eq!(rows.len(), expected.len());
    for (id, bytes) in &rows {
        let doc = FireLiteDoc::decode(bytes).expect("raw bytes decode");
        if blob_ids.iter().any(|b| b == id) {
            // Skeleton shape: links, not data (inflation covered in 6).
            assert!(
                doc.fields.iter().any(|(_, v)| matches!(v, Value::BlobLink { .. })),
                "blob skeleton for {id}"
            );
            continue;
        }
        assert_eq!(&fields_of(&doc), &expected[id], "raw mismatch for {id}");
        assert_eq!(&doc.encode(), bytes.as_ref(), "codec identity for {id}");
    }

    // 6. Walk + decode agreement (blob rows resolve through the engine).
    // ponytail: collect under the walk, resolve AFTER it returns — the
    // walk holds the storage read lock and the callback must not re-enter
    // the engine (a waiting writer + second read attempt deadlocks).
    let mut q = Query::new("docs");
    q = q.order_by("id", true);
    let mut walked: Vec<(String, Vec<u8>)> = Vec::new();
    let count = db
        .walk(q, &mut |id: &str, bytes: &[u8]| {
            walked.push((id.to_string(), bytes.to_vec()));
            true
        })
        .expect("walk");
    assert_eq!(count, expected.len());
    assert_eq!(walked.len(), expected.len());
    for (id, bytes) in &walked {
        let doc = FireLiteDoc::decode(bytes).expect("walk bytes decode");
        let mut full = doc;
        db.resolve_document_blobs(&mut full, "docs").expect("resolve");
        assert_eq!(&fields_of(&full), &expected[id], "walk mismatch for {id}");
    }

    // 7. Projection on one known doc.
    let mut q = Query::new("docs");
    q = q.order_by("id", true);
    let projected = db
        .query_projected_zero_copy(q, &["idx".to_string(), "tag".to_string()])
        .expect("projected");
    let mut vrow = projected.into_iter().find(|(id, _)| id == "v_0010").expect("v_0010 projected");
    vrow.1.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        vrow.1,
        vec![
            ("idx".to_string(), Value::Int(10)),
            ("tag".to_string(), Value::String("t3".into())),
        ],
        "projection"
    );
}

#[test]
fn codec_integrity_matrix() {
    let dir = std::env::temp_dir().join(format!(
        "fl-test-integrity-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let cfg = test_config();
    let db = FireLite::open(&dir, cfg.clone()).expect("open");
    let (expected, blob_ids) = seed_mixed(&db);

    // Force every pointer state: spill Inlined->Segment, drain blob queue,
    // flush WAL. Then verify fresh AND reopened (replayed) states.
    db.compact().expect("compact");
    db.flush().expect("flush");
    verify_all(&db, &expected, &blob_ids);
    drop(db);

    let db2 = FireLite::open(&dir, cfg).expect("reopen");
    let t0 = std::time::Instant::now();
    while !db2.is_indexes_ready() {
        assert!(t0.elapsed() < std::time::Duration::from_secs(30), "indexes never ready");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    verify_all(&db2, &expected, &blob_ids);
    drop(db2);
    std::fs::remove_dir_all(&dir).ok();
}

/// Quiescence: an idle engine is settled immediately; a freshly written
/// one settles quickly (index worker + blob worker drain in ms). The
/// status shape reports each background stage independently.
#[test]
fn quiescence_settles() {
    use std::time::Duration;
    let dir = std::env::temp_dir().join(format!(
        "fl-test-quiesce-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut cfg = FireLiteConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    let db = FireLite::open(&dir, cfg).expect("open");

    assert!(db.await_quiescent(Duration::from_secs(10)), "idle engine settles");
    let s = db.quiescence_status();
    assert!(s.is_quiescent(), "idle status all-clear: {s:?}");
    assert!(s.indexes_ready);
    assert_eq!(s.pending_index_ops, 0);
    assert_eq!(s.pending_blob_bytes, 0);
    assert_eq!(s.queued_blob_items, 0);
    assert!(!s.maintenance_running);

    // Writes (one blob-bearing doc to exercise the blob queue) then settle.
    let mut doc = FireLiteDoc::default();
    doc.insert("n", Value::Int(1));
    let mut big = FireLiteDoc::default();
    big.insert("data", Value::Binary(vec![7u8; 2048]));
    db.write_batch(vec![
        BatchMutation::Put { collection: "q".into(), doc_id: "a".into(), doc },
        BatchMutation::Put { collection: "q".into(), doc_id: "b".into(), doc: big },
    ])
    .expect("seed");
    assert!(db.await_quiescent(Duration::from_secs(15)), "seeded engine settles");
    assert!(db.quiescence_status().is_quiescent());
    drop(db);
    std::fs::remove_dir_all(&dir).ok();
}
