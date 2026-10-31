#![cfg(feature = "cloud-sync")]
//! Regression: `hk_cloud_sync_start` must return when called from a plain
//! (non-tokio) thread. The C ABI parks the runtime inside the handle;
//! without that, `Runtime::drop` waits forever on the `spawn_blocking`
//! sync tailers (they only exit after `stop()`) and the FFI call
//! deadlocks every ctypes/C consumer.

use std::time::Duration;

#[cfg_attr(windows, link(name = "hakodb.dll", kind = "dylib"))]
#[cfg_attr(not(windows), link(name = "hakodb", kind = "dylib"))]
extern "C" {
    fn hk_engine_open(path: *const std::os::raw::c_char) -> *mut std::ffi::c_void;
    fn hk_engine_free(engine: *mut std::ffi::c_void);
    fn hk_cloud_sync_client_new(
        engine: *mut std::ffi::c_void,
        client_id: *const std::os::raw::c_char,
        room_name: *const std::os::raw::c_char,
        room_key: *const std::os::raw::c_char,
        auth_token: *const std::os::raw::c_char,
    ) -> *mut std::ffi::c_void;
    fn hk_cloud_sync_start(
        cloud_sync: *mut std::ffi::c_void,
        address: *const std::os::raw::c_char,
    ) -> std::os::raw::c_int;
    fn hk_cloud_sync_status(cloud_sync: *mut std::ffi::c_void) -> *mut std::os::raw::c_char;
    fn hk_cloud_sync_stop(cloud_sync: *mut std::ffi::c_void);
    fn hk_cloud_sync_free(cloud_sync: *mut std::ffi::c_void);
    fn hk_string_free(s: *mut std::os::raw::c_char);
}

fn cs(s: &str) -> std::ffi::CString {
    std::ffi::CString::new(s).unwrap()
}

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("fl-ffi-cloud-{tag}-{nanos}"))
}

/// Client start against a dead port must (a) return promptly - the connect
/// loop retries in the background - and (b) not wedge `stop`/`free`.
#[test]
fn ffi_cloud_client_start_returns_on_plain_thread() {
    let dir = temp_dir("client");
    std::fs::create_dir_all(&dir).unwrap();
    let engine = unsafe { hk_engine_open(cs(dir.to_str().unwrap()).as_ptr()) };
    assert!(!engine.is_null(), "engine open failed");

    let syncer = unsafe {
        hk_cloud_sync_client_new(
            engine,
            cs("ffi-client").as_ptr(),
            cs("room").as_ptr(),
            cs("key").as_ptr(),
            cs("token").as_ptr(),
        )
    };
    assert!(!syncer.is_null(), "cloud client new failed");

    // start(): parked-runtime path (no ambient runtime in a plain test
    // thread). Without the fix this call never returns.
    let (tx, rx) = std::sync::mpsc::channel();
    let syncer_thread = syncer as usize;
    std::thread::spawn(move || {
        let rc = unsafe { hk_cloud_sync_start(syncer_thread as *mut std::ffi::c_void, cs("ws://127.0.0.1:1").as_ptr()) };
        let _ = tx.send(rc);
    });
    let rc = rx
        .recv_timeout(Duration::from_secs(30))
        .expect("hk_cloud_sync_start deadlocked (runtime parked on the FFI path)");
    assert_eq!(rc, 0, "offline-first client start must succeed");

    // Status JSON must be readable while the (failing) connect loop runs.
    let status = unsafe { hk_cloud_sync_status(syncer) };
    assert!(!status.is_null(), "null status from cloud sync");
    let text = unsafe { std::ffi::CStr::from_ptr(status) }
        .to_string_lossy()
        .into_owned();
    unsafe { hk_string_free(status) };
    assert!(text.contains("client"), "unexpected status: {text}");

    // stop + free must not hang: tailers exit because running=false.
    unsafe {
        hk_cloud_sync_stop(syncer);
        hk_cloud_sync_free(syncer);
        hk_engine_free(engine);
    }
}
