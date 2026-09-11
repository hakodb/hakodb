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
    // ponytail: open() returns while index recovery still runs in the
    // background; queries issued before `indexes_ready` silently plan
    // FullCollection (planner's index_ready gate) — pages then ignore
    // cursor bounds and repeat rows. Poll before scanning.
    wait_ready(db);
}

/// Poll until background open-recovery finishes (see `seed`).
fn wait_ready(db: &FireLite) {
    let t0 = std::time::Instant::now();
    while !db.is_indexes_ready() {
        assert!(t0.elapsed() < std::time::Duration::from_secs(30), "indexes never ready");
        std::thread::sleep(std::time::Duration::from_millis(5));
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
fn cursor_direction_parity() {    let dir = std::env::temp_dir().join(format!(
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

/// Steady-state control: fresh writes sit as BlobPending (re-encode per
/// read) until background conversion; a reopen replays WAL PutInlined
/// straight into Inlined pointers. Seed → close → reopen → scan measures
/// the TRUE Inlined ceiling with zero code changes.
#[test]
fn reopen_steady_state() {
    let dir = std::env::temp_dir().join(format!(
        "fl-test-reopen-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    {
        let db = open_bench(&dir);
        seed(&db);
    }
    let db2 = open_bench(&dir);
    wait_ready(&db2);

    let (ids, _, dt) = scan_all_raw(&db2, true);
    assert_eq!(ids.len(), N, "reopened raw scan missed docs");
    eprintln!(
        "raw scan REOPENED (Inlined steady state): {N} docs in {dt:?} = {} docs/s",
        N as u128 * 1_000_000_000 / dt.as_nanos().max(1),
    );
    let (dids, ddt) = scan_all(&db2, true);
    assert_eq!(dids, ids, "reopened decoded ids match raw ids");
    eprintln!(
        "decoded scan REOPENED: {N} docs in {ddt:?} = {} docs/s",
        N as u128 * 1_000_000_000 / ddt.as_nanos().max(1),
    );
    drop(db2);
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

fn scan_all_raw(db: &FireLite, ascending: bool) -> (Vec<String>, usize, std::time::Duration) {
    let t0 = std::time::Instant::now();
    let mut ids = Vec::with_capacity(N);
    let mut total_bytes = 0usize;
    let mut anchor: Option<String> = None;
    let mut pages = 0;
    loop {
        let mut q = Query::new("bench");
        q = q.order_by("id", ascending);
        q.limit = Some(PAGE);
        if let Some(a) = &anchor {
            q = q.start_after(vec![Value::String(a.clone())]);
        }
        let rows = db.query_raw(q).expect("raw page query");
        if rows.is_empty() {
            break;
        }
        anchor = Some(rows.last().unwrap().0.clone());
        // Decodability spot-check on first/last page only — decoding every
        // row would measure decode, not the raw path.
        pages += 1;
        if pages == 1 {
            for (id, bytes) in &rows {
                let doc = FireLiteDoc::decode(bytes).expect("raw bytes decode");
                assert!(doc.get("v").is_some(), "decoded raw doc {id} has v");
            }
        }
        total_bytes += rows.iter().map(|(_, b)| b.len()).sum::<usize>();
        ids.extend(rows.into_iter().map(|(id, _)| id));
    }
    (ids, total_bytes, t0.elapsed())
}

#[test]
fn raw_scan_parity() {    let dir = std::env::temp_dir().join(format!(
        "fl-test-raw-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let db = open_bench(&dir);
    seed(&db);

    let (fwd, fwd_bytes, fwd_dt) = scan_all_raw(&db, true);
    assert_eq!(fwd.len(), N, "raw forward missed docs");
    assert!(fwd.windows(2).all(|w| w[0] < w[1]), "raw forward not ascending");

    let (rev, _, rev_dt) = scan_all_raw(&db, false);
    assert_eq!(rev.len(), N, "raw reverse missed docs");
    assert!(rev.windows(2).all(|w| w[0] > w[1]), "raw reverse not descending");
    assert_eq!(rev.first().unwrap(), fwd.last().unwrap());
    assert_eq!(rev.last().unwrap(), fwd.first().unwrap());

    eprintln!(
        "raw scan: forward {N} docs / {} bytes in {fwd_dt:?} (= {} docs/s), reverse in {rev_dt:?} (= {} docs/s)",
        fwd_bytes,
        N as u128 * 1_000_000_000 / fwd_dt.as_nanos().max(1),
        N as u128 * 1_000_000_000 / rev_dt.as_nanos().max(1),
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// FFI raw round-trip: pages through fl_query_execute_raw with
/// fl_query_start_after_raw anchors (descending, like readreverse), reads
/// bytes borrowed via fl_rawdoc_bytes, resolves one row via
/// fl_rawdoc_to_doc. Locks the C ABI contract of the raw surface.
#[test]
fn raw_ffi_roundtrip() {
    use firelite::ffi;
    let dir = std::env::temp_dir().join(format!(
        "fl-test-rawffi-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let dir_c = std::ffi::CString::new(dir.to_str().unwrap()).unwrap();
    let col = std::ffi::CString::new("bench").unwrap();
    let field = std::ffi::CString::new("v").unwrap();
    let idf = std::ffi::CString::new("id").unwrap();
    let payload = vec![0xABu8; 100];

    let engine = unsafe {
        let cfg = ffi::fl_config_new();
        ffi::fl_config_set_durability(cfg, 2);
        ffi::fl_engine_open_with_config(dir_c.as_ptr(), cfg)
    };
    assert!(!engine.is_null());
    for i in 0..N {
        let k = std::ffi::CString::new(format!("{i:016x}")).unwrap();
        let doc = unsafe { ffi::fl_doc_new() };
        unsafe {
            assert_eq!(ffi::fl_doc_insert_bin(doc, field.as_ptr(), payload.as_ptr(), payload.len()), 0);
            assert_eq!(ffi::fl_engine_insert_take(engine, col.as_ptr(), k.as_ptr(), doc), 0);
        }
    }
    let t0 = std::time::Instant::now();
    while unsafe { !ffi::fl_engine_is_indexes_ready(engine) } {
        assert!(t0.elapsed() < std::time::Duration::from_secs(30), "indexes never ready");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    // Descending raw pages of PAGE rows. The anchor binds from the live
    // previous set (start_after_raw copies the id out), so the previous
    // set is freed only after the next query is bound.
    let t0 = std::time::Instant::now();
    let mut ids: Vec<String> = Vec::with_capacity(N);
    let mut total_bytes = 0usize;
    let mut resolved_json = String::new();
    let mut first_page = true;
    let mut prev_rs: *mut ffi::FL_RawResultSet = std::ptr::null_mut();
    let mut prev_last: *mut ffi::FL_RawDoc = std::ptr::null_mut();
    loop {
        let q = unsafe { ffi::fl_query_new(col.as_ptr()) };
        unsafe {
            assert_eq!(ffi::fl_query_order_by(q, idf.as_ptr(), false), 0);
            assert_eq!(ffi::fl_query_limit(q, PAGE), 0);
            if !prev_last.is_null() {
                assert_eq!(ffi::fl_query_start_after_raw(q, prev_last), 0);
            }
            if !prev_rs.is_null() {
                ffi::fl_rawresult_free(prev_rs);
                prev_rs = std::ptr::null_mut();
                prev_last = std::ptr::null_mut();
            }
        }
        let rs = unsafe { ffi::fl_query_execute_raw(engine, q) };
        assert!(!rs.is_null());
        let n = unsafe { ffi::fl_rawresult_count(rs) };
        if n == 0 {
            unsafe { ffi::fl_rawresult_free(rs) };
            unsafe { ffi::fl_query_free(q) };
            break;
        }
        let mut last_raw: *mut ffi::FL_RawDoc = std::ptr::null_mut();
        for i in 0..n {
            let r = unsafe { ffi::fl_rawresult_get(rs, i) };
            assert!(!r.is_null());
            let mut len = 0usize;
            let ptr = unsafe { ffi::fl_rawdoc_bytes(r, &mut len as *mut usize) };
            assert!(!ptr.is_null() && len > 0, "raw row has bytes");
            total_bytes += len;
            let mut idlen = 0usize;
            let idp = unsafe { ffi::fl_rawdoc_id(r, &mut idlen as *mut usize) };
            assert!(!idp.is_null() && idlen > 0);
            let id = unsafe { std::slice::from_raw_parts(idp as *const u8, idlen) };
            ids.push(String::from_utf8_lossy(id).into_owned());
            last_raw = r;
        }
        // Resolve the first row of the first page end-to-end.
        if first_page {
            first_page = false;
            let first = unsafe { ffi::fl_rawresult_get(rs, 0) };
            let d = unsafe { ffi::fl_rawdoc_to_doc(engine, first, col.as_ptr()) };
            assert!(!d.is_null(), "raw row resolves");
            let js = unsafe { ffi::fl_doc_to_json(d) };
            assert!(!js.is_null());
            resolved_json = unsafe { std::ffi::CStr::from_ptr(js) }.to_str().unwrap().to_string();
            unsafe { ffi::fl_string_free(js) };
            unsafe { ffi::fl_doc_free(d) };
        }
        prev_rs = rs;
        prev_last = last_raw;
        unsafe { ffi::fl_query_free(q) };
    }
    let dt = t0.elapsed();
    assert_eq!(ids.len(), N, "raw FFI scan missed docs");
    assert!(ids.windows(2).all(|w| w[0] > w[1]), "raw FFI not descending");
    assert!(resolved_json.contains('v'), "resolved row has field v");
    eprintln!(
        "raw FFI scan: {N} docs / {total_bytes} bytes in {dt:?} = {} docs/s",
        N as u128 * 1_000_000_000 / dt.as_nanos().max(1),
    );
    unsafe { ffi::fl_engine_free(engine) };
    std::fs::remove_dir_all(&dir).ok();
}

/// Zero-alloc walk: single call per direction, borrowed rows, early-stop
/// and contract-error coverage. Caller-side id collection is test-only;
/// the engine allocates nothing per row.
#[test]
fn walk_scan_parity() {
    let dir = std::env::temp_dir().join(format!(
        "fl-test-walk-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let db = open_bench(&dir);
    seed(&db);

    for ascending in [true, false] {
        let mut ids: Vec<String> = Vec::with_capacity(N);
        let mut total_bytes = 0usize;
        let t0 = std::time::Instant::now();
        let mut q = Query::new("bench");
        q = q.order_by("id", ascending);
        let visited = db
            .walk(q, &mut |id: &str, bytes: &[u8]| {
                ids.push(id.to_string());
                total_bytes += bytes.len();
                true
            })
            .expect("walk");
        let dt = t0.elapsed();
        assert_eq!(visited, N);
        assert_eq!(ids.len(), N, "walk missed docs (asc={ascending})");
        if ascending {
            assert!(ids.windows(2).all(|w| w[0] < w[1]), "walk not ascending");
        } else {
            assert!(ids.windows(2).all(|w| w[0] > w[1]), "walk not descending");
        }
        eprintln!(
            "walk {}: {N} docs / {total_bytes} bytes in {dt:?} = {} docs/s",
            if ascending { "forward" } else { "reverse" },
            N as u128 * 1_000_000_000 / dt.as_nanos().max(1),
        );
    }

    // Count-only: no caller-side id allocs — isolates engine cost (the
    // MDBX-equivalent shape: keys available, nothing copied).
    let mut total = 0usize;
    let mut nrows = 0usize;
    let t0 = std::time::Instant::now();
    let q = Query::new("bench").order_by("id", true);
    let visited = db
        .walk(q, &mut |_: &str, bytes: &[u8]| {
            total += bytes.len();
            nrows += 1;
            true
        })
        .expect("count-only walk");
    let dt = t0.elapsed();
    assert_eq!(visited, N);
    assert_eq!(nrows, N);
    eprintln!(
        "walk count-only: {N} docs / {total} bytes in {dt:?} = {} docs/s",
        N as u128 * 1_000_000_000 / dt.as_nanos().max(1),
    );

    // Early-stop: callback false after 100 rows.
    let mut seen = 0usize;
    let q = Query::new("bench").order_by("id", true);
    let visited = db
        .walk(q, &mut |_: &str, _: &[u8]| {
            seen += 1;
            seen < 100
        })
        .expect("early-stop walk");
    assert_eq!(visited, 100, "early stop visits exactly 100");
    assert_eq!(seen, 100);

    // Contract: non-index filter without decode is an error, not a guess.
    let q = Query::new("bench")
        .where_filter("v", firelite::query::filter::Operator::Eq, Value::Binary(vec![1u8]))
        .order_by("id", true);
    let mut n = 0;
    let err = db
        .walk(q, &mut |_: &str, _: &[u8]| {
            n += 1;
            true
        })
        .expect_err("unmatched-filter walk must error");
    let msg = format!("{err:?}");
    assert!(msg.contains("index-satisfied"), "unexpected error: {msg}");
    assert_eq!(n, 0);
    std::fs::remove_dir_all(&dir).ok();
}

/// Borrowed reads: get_view agreement + floor, walk_view lazy scan vs
/// decoded, escape hatch, miss contract.
#[test]
fn view_reads() {
    use firelite::document::firelite_doc::DocView;
    let dir = std::env::temp_dir().join(format!(
        "fl-test-view-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let db = open_bench(&dir);
    seed(&db);

    // 1. Agreement: view pulls match owned gets, field for field.
    for i in 0..N {
        let k = format!("{i:016x}");
        let doc = db.get("bench", &k).expect("get").expect("present");
        let view = db.get_view("bench", &k).expect("view").expect("present");
        assert_eq!(view.len(), doc.fields.len(), "field count {k}");
        assert_eq!(view.time(), doc.get_logical_time());
        for (fk, fv) in &doc.fields {
            assert_eq!(&view.get(fk).expect("field present"), fv, "field {fk}");
        }
        assert!(view.get("no-such-field").is_none());
        assert_eq!(&view.to_owned_doc().expect("escape hatch").fields, &doc.fields);
    }
    assert!(db.get_view("bench", "no-such-id").expect("view").is_none());

    // 2. View floor: random borrows, no field pulls.
    let mut state: u64 = 0x12345678;
    let t0 = std::time::Instant::now();
    let mut found = 0;
    for _ in 0..N {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let k = format!("{:016x}", (state >> 11) as usize % N);
        if db.get_view("bench", &k).expect("view").is_some() {
            found += 1;
        }
    }
    let dt = t0.elapsed();
    assert_eq!(found, N);
    eprintln!(
        "view floor (borrowed, no pulls): {N} gets in {dt:?} = {} ops/s",
        N as u128 * 1_000_000_000 / dt.as_nanos().max(1),
    );

    // 3. Lazy scan: full walk pulling nothing (count only).
    let t0 = std::time::Instant::now();
    let q = Query::new("bench").order_by("id", true);
    let mut nrows = 0;
    let visited = db
        .walk_view(q, &mut |_: &str, _: &DocView| {
            nrows += 1;
            true
        })
        .expect("view walk");
    let dt = t0.elapsed();
    assert_eq!(visited, N);
    assert_eq!(nrows, N);
    eprintln!(
        "walk_view count-only: {N} docs in {dt:?} = {} docs/s",
        N as u128 * 1_000_000_000 / dt.as_nanos().max(1),
    );

    // 4. Lazy scan pulling ONE field per row (the sqlite SELECT a,b analog
    // at its cheapest honest shape): ids collected for order check.
    let mut ids: Vec<String> = Vec::with_capacity(N);
    let mut hits = 0;
    let t0 = std::time::Instant::now();
    let q = Query::new("bench").order_by("id", false);
    let visited = db
        .walk_view(q, &mut |id: &str, v: &DocView| {
            if v.get("v").is_some() {
                hits += 1;
            }
            ids.push(id.to_string());
            true
        })
        .expect("view walk pulls");
    let dt = t0.elapsed();
    assert_eq!(visited, N);
    assert_eq!(hits, N);
    assert!(ids.windows(2).all(|w| w[0] > w[1]), "view walk descending");
    eprintln!(
        "walk_view +1 pull: {N} docs in {dt:?} = {} docs/s",
        N as u128 * 1_000_000_000 / dt.as_nanos().max(1),
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// Codec floor: pure decode + encode loops over one representative row
/// (100B binary doc). Sizes the codec vs the fetch machinery around it.
#[test]
fn codec_floor() {
    use firelite::document::firelite_doc::FireLiteDoc;
    let dir = std::env::temp_dir().join(format!(
        "fl-test-codec-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let db = open_bench(&dir);
    seed(&db);

    let mut q = Query::new("bench").order_by("id", true);
    q.limit = Some(1);
    let rows = db.query_raw(q).expect("one raw row");
    assert_eq!(rows.len(), 1);
    let bytes: Vec<u8> = rows[0].1.as_ref().clone();

    const K: usize = 200_000;
    let t0 = std::time::Instant::now();
    let mut nfields = 0;
    for _ in 0..K {
        let doc = FireLiteDoc::decode(&bytes).expect("decode");
        nfields += doc.fields.len();
    }
    let ddt = t0.elapsed();
    assert_eq!(nfields, K);
    eprintln!(
        "codec floor decode x{K}: {ddt:?} = {} ops/s ({} ns/op)",
        K as u128 * 1_000_000_000 / ddt.as_nanos().max(1),
        ddt.as_nanos() / K as u128,
    );

    let doc = FireLiteDoc::decode(&bytes).expect("decode");
    let t0 = std::time::Instant::now();
    let mut nbytes = 0;
    for _ in 0..K {
        nbytes += doc.encode().len();
    }
    let edt = t0.elapsed();
    assert_eq!(nbytes, K * bytes.len());
    eprintln!(
        "codec floor encode x{K}: {edt:?} = {} ops/s ({} ns/op)",
        K as u128 * 1_000_000_000 / edt.as_nanos().max(1),
        edt.as_nanos() / K as u128,
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[derive(Default)]
struct WalkState {
    rows: usize,
    bytes: usize,
    first_id: String,
    last_id: String,
}

/// extern "C" counting callback — mirrors what t_firelite.cc will do.
unsafe extern "C" fn walk_count_cb(
    id: *const std::os::raw::c_char,
    id_len: usize,
    bytes: *const u8,
    bytes_len: usize,
    userdata: *mut std::ffi::c_void,
) -> bool {
    let st = &mut *(userdata as *mut WalkState);
    let idb = std::slice::from_raw_parts(id as *const u8, id_len);
    let s = String::from_utf8_lossy(idb).into_owned();
    if st.rows == 0 {
        st.first_id = s.clone();
    }
    st.last_id = s;
    st.rows += 1;
    st.bytes += bytes_len;
    let _ = std::slice::from_raw_parts(bytes, bytes_len);
    true
}

unsafe extern "C" fn walk_stop_cb(
    _id: *const std::os::raw::c_char,
    _id_len: usize,
    _bytes: *const u8,
    _bytes_len: usize,
    userdata: *mut std::ffi::c_void,
) -> bool {
    let n = &mut *(userdata as *mut usize);
    *n += 1;
    *n < 100
}

/// FFI walk: one call per direction over the ABI, early-stop + null
/// callback coverage. Locks the fl_cursor_walk contract.
#[test]
fn walk_ffi() {
    use firelite::ffi;
    let dir = std::env::temp_dir().join(format!(
        "fl-test-walkffi-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let dir_c = std::ffi::CString::new(dir.to_str().unwrap()).unwrap();
    let col = std::ffi::CString::new("bench").unwrap();
    let field = std::ffi::CString::new("v").unwrap();
    let idf = std::ffi::CString::new("id").unwrap();
    let payload = vec![0xABu8; 100];

    let engine = unsafe {
        let cfg = ffi::fl_config_new();
        ffi::fl_config_set_durability(cfg, 2);
        ffi::fl_engine_open_with_config(dir_c.as_ptr(), cfg)
    };
    assert!(!engine.is_null());
    for i in 0..N {
        let k = std::ffi::CString::new(format!("{i:016x}")).unwrap();
        let doc = unsafe { ffi::fl_doc_new() };
        unsafe {
            assert_eq!(ffi::fl_doc_insert_bin(doc, field.as_ptr(), payload.as_ptr(), payload.len()), 0);
            assert_eq!(ffi::fl_engine_insert_take(engine, col.as_ptr(), k.as_ptr(), doc), 0);
        }
    }
    let t0 = std::time::Instant::now();
    while unsafe { !ffi::fl_engine_is_indexes_ready(engine) } {
        assert!(t0.elapsed() < std::time::Duration::from_secs(30), "indexes never ready");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    // Full descending walk, no limit — one call.
    let q = unsafe { ffi::fl_query_new(col.as_ptr()) };
    unsafe {
        assert_eq!(ffi::fl_query_order_by(q, idf.as_ptr(), false), 0);
    }
    let mut st = WalkState::default();
    let t0 = std::time::Instant::now();
    let n = unsafe {
        ffi::fl_cursor_walk(
            engine,
            q,
            Some(walk_count_cb),
            &mut st as *mut WalkState as *mut std::ffi::c_void,
        )
    };
    let dt = t0.elapsed();
    assert_eq!(n, N as i64, "walk visited all rows");
    assert_eq!(st.rows, N);
    assert!(st.first_id > st.last_id, "descending order");
    eprintln!(
        "walk FFI: {N} docs / {} bytes in {dt:?} = {} docs/s",
        st.bytes,
        N as u128 * 1_000_000_000 / dt.as_nanos().max(1),
    );
    unsafe { ffi::fl_query_free(q) };

    // Early-stop + null callback.
    let q2 = unsafe { ffi::fl_query_new(col.as_ptr()) };
    unsafe {
        assert_eq!(ffi::fl_query_order_by(q2, idf.as_ptr(), true), 0);
    }
    let mut cnt = 0usize;
    let n = unsafe {
        ffi::fl_cursor_walk(
            engine,
            q2,
            Some(walk_stop_cb),
            &mut cnt as *mut usize as *mut std::ffi::c_void,
        )
    };
    assert_eq!(n, 100, "early stop visits exactly 100");
    assert_eq!(cnt, 100);
    let n = unsafe { ffi::fl_cursor_walk(engine, q2, None, std::ptr::null_mut()) };
    assert_eq!(n, -1, "null callback errors");
    unsafe { ffi::fl_query_free(q2) };

    unsafe { ffi::fl_engine_free(engine) };
    std::fs::remove_dir_all(&dir).ok();
}
