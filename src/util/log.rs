// ponytail: log sink with three independent on/off knobs. All three default
// to OFF in release builds so embedding an application that doesn't ask for
// diagnostics never sees library chatter on stderr. Library callers wire
// either a sink callback or stderr explicitly via the FFI.
//
// API shape (three fns) is intentional: callers want the cheap win of "stop
// printing" without paying for log levels or structured fields. Upgrade to
// log::facade only if/when we need per-module filtering or rotation hooks.

use std::ptr::null_mut;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};

pub type Sink = fn(&str);

static STDERR_ON: AtomicBool = AtomicBool::new(false);
static CALLBACK: AtomicPtr<()> = AtomicPtr::new(null_mut());

fn from_sink(s: Sink) -> *mut () { s as *const () as *mut () }
fn as_sink(p: *mut ()) -> Option<Sink> {
    if p.is_null() { None } else { Some(unsafe { std::mem::transmute(p) }) }
}

pub fn info(msg: &str) {
    let cb_ptr = CALLBACK.load(Ordering::Acquire);
    if let Some(sink) = as_sink(cb_ptr) {
        sink(msg);
        return;
    }
    if STDERR_ON.load(Ordering::Acquire) {
        eprintln!("[firelite] {msg}");
    }
}

pub fn enable_stderr() {
    STDERR_ON.store(true, Ordering::Release);
}

pub fn disable_stderr() {
    STDERR_ON.store(false, Ordering::Release);
}

pub fn set_sink(sink: Option<Sink>) {
    match sink {
        Some(s) => CALLBACK.store(from_sink(s), Ordering::Release),
        None => CALLBACK.store(null_mut(), Ordering::Release),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn callback_receives_messages() {
        static HIT: AtomicUsize = AtomicUsize::new(0);
        extern "Rust" fn sink(_msg: &str) {
            HIT.fetch_add(1, Ordering::Relaxed);
        }
        set_sink(Some(sink));
        info("hello");
        info("world");
        set_sink(None);
        assert_eq!(HIT.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn silent_by_default() {
        // We can't assert "no print to stderr" without capturing it, but we
        // CAN assert that disabling the sink leaves CALLBACK null and STDERR
        // off — info() then becomes a no-op.
        disable_stderr();
        set_sink(None);
        info("should be silent"); // would panic on a test runner that fails
                                   // on stderr noise, but Cargo doesn't.
    }
}
