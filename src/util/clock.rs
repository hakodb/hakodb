pub fn unix_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|v| v.as_millis())
        .unwrap_or(0)
}

/// Wall-clock microseconds (the `_time`/LWW clock unit). Saturates instead
/// of wrapping on exotic clocks; 0 only before the epoch (never in practice).
pub fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|v| v.as_micros().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}
