use firelite::config::{DurabilityMode, FireLiteConfig};
use firelite::document::firelite_doc::FireLiteDoc;
use firelite::document::value::Value;
use firelite::engine::{BatchMutation, FireLite};
use firelite::query::query::Query;

const PAGE: usize = 1000;

// ponytail: full 20k in release (real numbers); 2k in debug so `cargo test`
// stays fast — the paths are identical, only the volume differs.
#[cfg(debug_assertions)]
const N: usize = 2_000;
#[cfg(not(debug_assertions))]
const N: usize = 20_000;

fn open_bench(dir: &std::path::Path) -> FireLite {
    let mut cfg = FireLiteConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    FireLite::open(dir, cfg).expect("open")
}

fn seed(db: &FireLite) {
    let payload = vec![0xABu8; 100];
    for chunk in (0..N).collect::<Vec<_>>().chunks(2000) {
        let mut batch = Vec::with_capacity(chunk.len());
        for &i in chunk {
            let mut doc = FireLiteDoc::default();
            doc.insert("v", Value::Binary(payload.clone()));
            batch.push(BatchMutation::Put {
                collection: "bench".into(),
                doc_id: format!("{i:016x}"),
                doc,
            });
        }
        db.write_batch(batch).expect("seed batch");
    }
}

fn scan_all(db: &FireLite, ascending: bool) -> (Vec<String>, std::time::Duration) {
    let t0 = std::time::Instant::now();
    let mut ids = Vec::with_capacity(N);
    let mut anchor: Option<String> = None;
    loop {
        let mut q = Query::new("bench");
        q = q.order_by("id", ascending);
        q.limit = Some(PAGE);
        if let Some(a) = &anchor {
            q = q.start_after(vec![Value::String(a.clone())]);
        }
        let rows = db.query(q).expect("page query");
        if rows.is_empty() {
            break;
        }
        anchor = Some(rows.last().unwrap().0.clone());
        ids.extend(rows.into_iter().map(|(id, _)| id));
    }
    (ids, t0.elapsed())
}

#[test]
fn cursor_direction_parity() {
    let dir = std::env::temp_dir().join(format!(
        "fl-test-cursor-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let db = open_bench(&dir);
    seed(&db);

    let (fwd, fwd_dt) = scan_all(&db, true);
    assert_eq!(fwd.len(), N, "forward scan missed docs");
    assert!(fwd.windows(2).all(|w| w[0] < w[1]), "forward not ascending");

    let (rev, rev_dt) = scan_all(&db, false);
    assert_eq!(rev.len(), N, "reverse scan missed docs");
    assert!(rev.windows(2).all(|w| w[0] > w[1]), "reverse not descending");
    assert_eq!(rev.first().unwrap(), fwd.last().unwrap());
    assert_eq!(rev.last().unwrap(), fwd.first().unwrap());

    eprintln!(
        "cursor parity: forward {N} docs in {fwd_dt:?} (= {} docs/s), reverse in {rev_dt:?} (= {} docs/s)",
        N as u128 * 1_000_000_000 / fwd_dt.as_nanos().max(1),
        N as u128 * 1_000_000_000 / rev_dt.as_nanos().max(1),
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn point_get_floor() {
    let dir = std::env::temp_dir().join(format!(
        "fl-test-pget-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let db = open_bench(&dir);
    seed(&db);

    // Deterministic pseudo-random probe sequence (no dev-dependency).
    let mut state: u64 = 0x12345678;
    let t0 = std::time::Instant::now();
    let mut found = 0;
    for _ in 0..N {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let k = format!("{:016x}", (state >> 11) as usize % N);
        if db.get("bench", &k).expect("get").is_some() {
            found += 1;
        }
    }
    let dt = t0.elapsed();
    assert_eq!(found, N);
    eprintln!(
        "point-get floor (native, no FFI/JSON): {N} gets in {dt:?} = {} ops/s",
        N as u128 * 1_000_000_000 / dt.as_nanos().max(1),
    );

    // Differential: hot single-key loop (doc_cache hit path) vs second
    // sequential pass (every key cached) — sizes miss-path vs fixed cost.
    let t0 = std::time::Instant::now();
    for _ in 0..N {
        assert!(db.get("bench", "0000000000000000").expect("get").is_some());
    }
    let hot_dt = t0.elapsed();
    eprintln!(
        "point-get hot same-key: {N} gets in {hot_dt:?} = {} ops/s",
        N as u128 * 1_000_000_000 / hot_dt.as_nanos().max(1),
    );
    let t0 = std::time::Instant::now();
    for i in 0..N {
        assert!(db.get("bench", &format!("{i:016x}")).expect("get").is_some());
    }
    let seq_dt = t0.elapsed();
    eprintln!(
        "point-get sequential 2nd pass: {N} gets in {seq_dt:?} = {} ops/s",
        N as u128 * 1_000_000_000 / seq_dt.as_nanos().max(1),
    );

    // Harness-equivalent path: raw FFI get + full JSON + frees, same as
    // t_firelite.cc db_read(DO_RANDOM). Sizes wrapper+JSON tax vs native.
    // Own dir through the FFI (open takes cfg ownership, like the harness).
    let fdir = std::env::temp_dir().join(format!(
        "fl-test-ffi-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let fdir_c = std::ffi::CString::new(fdir.to_str().unwrap()).unwrap();
    let col = std::ffi::CString::new("bench").unwrap();
    let field = std::ffi::CString::new("v").unwrap();
    let payload = vec![0xABu8; 100];
    let engine_ptr = unsafe {
        let cfg = firelite::ffi::fl_config_new();
        firelite::ffi::fl_config_set_durability(cfg, 2);
        firelite::ffi::fl_engine_open_with_config(fdir_c.as_ptr(), cfg)
    };
    assert!(!engine_ptr.is_null());
    for i in 0..N {
        let k = std::ffi::CString::new(format!("{i:016x}")).unwrap();
        let doc = unsafe { firelite::ffi::fl_doc_new() };
        unsafe {
            assert_eq!(
                firelite::ffi::fl_doc_insert_bin(doc, field.as_ptr(), payload.as_ptr(), payload.len()),
                0
            );
            assert_eq!(
                firelite::ffi::fl_engine_insert_take(engine_ptr, col.as_ptr(), k.as_ptr(), doc),
                0
            );
        }
    }
    let mut state: u64 = 0x12345678;
    let t0 = std::time::Instant::now();
    let mut found = 0;
    for _ in 0..N {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let k = std::ffi::CString::new(format!("{:016x}", (state >> 11) as usize % N)).unwrap();
        let doc = unsafe {
            firelite::ffi::fl_engine_get(engine_ptr, col.as_ptr(), k.as_ptr())
        };
        if !doc.is_null() {
            let js = unsafe { firelite::ffi::fl_doc_to_json(doc) };
            if !js.is_null() {
                unsafe { firelite::ffi::fl_string_free(js) };
            }
            unsafe { firelite::ffi::fl_doc_free(doc) };
            found += 1;
        }
    }
    let ffi_dt = t0.elapsed();
    assert_eq!(found, N);
    eprintln!(
        "point-get FFI+JSON (harness-equivalent): {N} gets in {ffi_dt:?} = {} ops/s",
        N as u128 * 1_000_000_000 / ffi_dt.as_nanos().max(1),
    );
    unsafe { firelite::ffi::fl_engine_free(engine_ptr) };
    std::fs::remove_dir_all(&fdir).ok();

    // Control: same FFI get loop, but seeded via BATCHES (like fillrandbatch)
    // instead of singles. If this is faster, the read gap is downstream of
    // write-path pointer state (BlobPending vs Inlined), not the get path.
    let bdir = std::env::temp_dir().join(format!(
        "fl-test-ffib-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let bdir_c = std::ffi::CString::new(bdir.to_str().unwrap()).unwrap();
    let bengine = unsafe {
        let cfg = firelite::ffi::fl_config_new();
        firelite::ffi::fl_config_set_durability(cfg, 2);
        firelite::ffi::fl_engine_open_with_config(bdir_c.as_ptr(), cfg)
    };
    assert!(!bengine.is_null());
    for chunk in (0..N).collect::<Vec<_>>().chunks(500) {
        let batch = unsafe { firelite::ffi::fl_batch_new() };
        for &i in chunk {
            let k = std::ffi::CString::new(format!("{i:016x}")).unwrap();
            let doc = unsafe { firelite::ffi::fl_doc_new() };
            unsafe {
                assert_eq!(
                    firelite::ffi::fl_doc_insert_bin(doc, field.as_ptr(), payload.as_ptr(), payload.len()),
                    0
                );
                assert_eq!(
                    firelite::ffi::fl_batch_set(batch, col.as_ptr(), k.as_ptr(), doc),
                    0
                );
                firelite::ffi::fl_doc_free(doc);
            }
        }
        unsafe {
            assert_eq!(firelite::ffi::fl_batch_commit(bengine, batch), 0);
            firelite::ffi::fl_batch_free(batch);
        }
    }
    let mut state: u64 = 0x12345678;
    let t0 = std::time::Instant::now();
    let mut found = 0;
    for _ in 0..N {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let k = std::ffi::CString::new(format!("{:016x}", (state >> 11) as usize % N)).unwrap();
        let doc = unsafe {
            firelite::ffi::fl_engine_get(bengine, col.as_ptr(), k.as_ptr())
        };
        if !doc.is_null() {
            let js = unsafe { firelite::ffi::fl_doc_to_json(doc) };
            if !js.is_null() {
                unsafe { firelite::ffi::fl_string_free(js) };
            }
            unsafe { firelite::ffi::fl_doc_free(doc) };
            found += 1;
        }
    }
    let bffi_dt = t0.elapsed();
    assert_eq!(found, N);
    eprintln!(
        "point-get FFI+JSON batch-seeded: {N} gets in {bffi_dt:?} = {} ops/s",
        N as u128 * 1_000_000_000 / bffi_dt.as_nanos().max(1),
    );
    // Split the FFI tax: get-alone (fetch+free, no JSON) vs json-alone
    // (one fetched doc serialized N times). Runs before bengine is freed.
    let t0 = std::time::Instant::now();
    let mut found = 0;
    for _ in 0..N {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let k = std::ffi::CString::new(format!("{:016x}", (state >> 11) as usize % N)).unwrap();
        let doc = unsafe {
            firelite::ffi::fl_engine_get(bengine, col.as_ptr(), k.as_ptr())
        };
        if !doc.is_null() {
            unsafe { firelite::ffi::fl_doc_free(doc) };
            found += 1;
        }
    }
    let getonly_dt = t0.elapsed();
    assert_eq!(found, N);
    eprintln!(
        "point-get FFI get-alone: {N} gets in {getonly_dt:?} = {} ops/s",
        N as u128 * 1_000_000_000 / getonly_dt.as_nanos().max(1),
    );
    let k0 = std::ffi::CString::new("0000000000000000").unwrap();
    let one = unsafe { firelite::ffi::fl_engine_get(bengine, col.as_ptr(), k0.as_ptr()) };
    assert!(!one.is_null());
    let t0 = std::time::Instant::now();
    for _ in 0..N {
        let js = unsafe { firelite::ffi::fl_doc_to_json(one) };
        assert!(!js.is_null());
        unsafe { firelite::ffi::fl_string_free(js) };
    }
    let jsononly_dt = t0.elapsed();
    eprintln!(
        "fl_doc_to_json alone x{N}: {jsononly_dt:?} = {} ops/s",
        N as u128 * 1_000_000_000 / jsononly_dt.as_nanos().max(1),
    );
    unsafe { firelite::ffi::fl_doc_free(one) };
    // Control: same serialize loop over a STRING-valued doc (no byte array).
    // If this is multiples faster, the Binary-array arm is the confirmed hog.
    let sdoc = unsafe { firelite::ffi::fl_doc_new() };
    let sval = std::ffi::CString::new("x".repeat(100)).unwrap();
    unsafe {
        firelite::ffi::fl_doc_insert_str(sdoc, field.as_ptr(), sval.as_ptr());
    }
    let t0 = std::time::Instant::now();
    for _ in 0..N {
        let js = unsafe { firelite::ffi::fl_doc_to_json(sdoc) };
        assert!(!js.is_null());
        unsafe { firelite::ffi::fl_string_free(js) };
    }
    let sjson_dt = t0.elapsed();
    eprintln!(
        "fl_doc_to_json string-doc x{N}: {sjson_dt:?} = {} ops/s",
        N as u128 * 1_000_000_000 / sjson_dt.as_nanos().max(1),
    );
    unsafe { firelite::ffi::fl_doc_free(sdoc) };
    unsafe { firelite::ffi::fl_engine_free(bengine) };
    std::fs::remove_dir_all(&dir).ok();
    std::fs::remove_dir_all(&bdir).ok();
}
