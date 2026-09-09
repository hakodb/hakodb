//! Server-sent realtime stream (`GET /api/events`).
//!
//! Multiplexes three sources into one SSE stream, per connection:
//!   1. document changes via `watch_collection` (one pump thread per
//!      requested collection — admin connections are rare, so no global
//!      hub; dead receivers are pruned by the engine on next write),
//!   2. peer membership diffs (polled, peers only change on connect/drop),
//!   3. per-room version-clock diffs (polled; the version map has no push).
//! An initial snapshot (peers + versions) is sent first so the UI renders
//! without waiting for the first change.
//!
//! Lifecycle without leaks: pumps push into a *bounded* channel and exit
//! when the send fails (client gone, receiver dropped); the poll loop
//! checks `is_closed()` every tick. No global state, nothing outlives the
//! connection.

use axum::{
    extract::{Query, State},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse,
    },
};
use serde::Deserialize;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::mpsc;

use crate::app::{AppState, AuthedUser};
use crate::data::room_prefixes;

const CHANNEL_DEPTH: usize = 128;
const PEER_POLL: Duration = Duration::from_secs(2);
const VERSION_POLL: Duration = Duration::from_secs(5);

#[derive(Debug, Deserialize, Default)]
pub struct EventsQuery {
    /// Comma-separated collections to watch. Default: all current
    /// non-internal collections at connect time.
    collections: Option<String>,
}

/// Minimal `Stream` adapter over a tokio mpsc receiver (avoids a
/// stream-adapter dependency for one use site).
struct MpscStream {
    rx: mpsc::Receiver<Result<Event, Infallible>>,
}

impl futures_core::Stream for MpscStream {
    type Item = Result<Event, Infallible>;

    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

/// Snapshot of peer membership for diffing.
fn peer_snapshot(state: &AppState) -> HashSet<String> {
    match state.sync.as_ref() {
        Some(s) => s.peer_list().into_iter().map(|p| p.peer_key).collect(),
        None => HashSet::new(),
    }
}

/// Snapshot of all room version clocks: prefix -> {collection -> version}.
fn version_snapshot_synced(
    state: &AppState,
    prefixes: &[String],
) -> HashMap<String, HashMap<String, i64>> {
    let Some(sync) = state.sync.as_ref() else {
        return HashMap::new();
    };
    prefixes
        .iter()
        .map(|p| (p.clone(), sync.room_versions(p)))
        .collect()
}

fn send_event(
    tx: &mpsc::Sender<Result<Event, Infallible>>,
    kind: &str,
    payload: serde_json::Value,
) -> bool {
    let ev = match Event::default().event(kind).json_data(payload) {
        Ok(e) => e,
        Err(_) => return false,
    };
    tx.blocking_send(Ok(ev)).is_ok()
}

async fn send_event_async(
    tx: &mpsc::Sender<Result<Event, Infallible>>,
    kind: &str,
    payload: serde_json::Value,
) -> bool {
    let ev = match Event::default().event(kind).json_data(payload) {
        Ok(e) => e,
        Err(_) => return false,
    };
    tx.send(Ok(ev)).await.is_ok()
}

pub async fn events(
    State(state): State<Arc<AppState>>,
    _user: AuthedUser,
    Query(q): Query<EventsQuery>,
) -> impl IntoResponse {
    let wanted: Option<HashSet<String>> = q.collections.map(|raw| {
        raw.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    });
    // Explicitly requested collections are watched verbatim — even ones
    // that don't exist yet (first write creates them and fires). Without
    // the parameter we watch what exists now; collections born later on an
    // open-ended stream can't be anticipated.
    let collections: Vec<String> = match wanted {
        Some(w) => w.into_iter().collect(),
        None => state
            .db
            .list_collections()
            .unwrap_or_default()
            .into_iter()
            .filter(|c| !c.starts_with('_'))
            .collect(),
    };

    let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(CHANNEL_DEPTH);

    // 1. Document pumps: one blocking thread per collection. Exits when the
    // client goes away (send fails on the dropped receiver).
    for col in collections {
        let tx_pump = tx.clone();
        let db_pump = state.db.clone();
        std::thread::spawn(move || {
            let rx_watch = db_pump.watch_collection(&col);
            while let Ok(ev) = rx_watch.recv() {
                let kind = match ev.kind {
                    firelite::engine::ChangeKind::Put => "put",
                    firelite::engine::ChangeKind::Delete => "delete",
                };
                if !send_event(
                    &tx_pump,
                    "doc",
                    json!({"collection": col, "id": ev.path, "kind": kind}),
                ) {
                    break;
                }
            }
        });
    }

    // Initial snapshot so the UI renders immediately.
    let prefixes = room_prefixes(&state.db);
    let peers_now = peer_snapshot(&state);
    let versions_now = version_snapshot_synced(&state, &prefixes);
    let _ = send_event_async(&tx, "peers", json!({ "peers": peers_now.iter().collect::<Vec<_>>() })).await;
    let _ = send_event_async(&tx, "versions", json!({ "rooms": versions_now })).await;

    // 2+3. Poll loop for peer and version diffs. Ends when the client
    // disconnects (receiver dropped => is_closed).
    let tx_poll = tx.clone();
    let state_poll = state.clone();
    tokio::spawn(async move {
        let mut last_peers = peers_now;
        let mut last_versions = versions_now;
        let mut peer_tick = tokio::time::interval(PEER_POLL);
        let mut ver_tick = tokio::time::interval(VERSION_POLL);
        // Skip the immediate first ticks (snapshot already sent).
        peer_tick.tick().await;
        ver_tick.tick().await;
        loop {
            tokio::select! {
                _ = peer_tick.tick() => {
                    if tx_poll.is_closed() {
                        break;
                    }
                    let now = peer_snapshot(&state_poll);
                    if now != last_peers {
                        last_peers = now.clone();
                        if !send_event_async(&tx_poll, "peers", json!({ "peers": now.into_iter().collect::<Vec<_>>() })).await {
                            break;
                        }
                    }
                }
                _ = ver_tick.tick() => {
                    if tx_poll.is_closed() {
                        break;
                    }
                    let prefixes = room_prefixes(&state_poll.db);
                    let now = version_snapshot_synced(&state_poll, &prefixes);
                    if now != last_versions {
                        last_versions = now.clone();
                        if !send_event_async(&tx_poll, "versions", json!({ "rooms": now })).await {
                            break;
                        }
                    }
                }
            }
        }
    });

    Sse::new(MpscStream { rx }).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}
