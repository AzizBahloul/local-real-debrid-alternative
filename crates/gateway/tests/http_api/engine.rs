//! The engine's own bookkeeping, as the router drives it: what may be started,
//! what counts as activity, and what survives a restart.

use std::time::Duration;

use axum::http::StatusCode;

use crate::{get, Gateway};

const HASH: &str = "0123456789abcdef0123456789abcdef01234567";

#[tokio::test]
async fn idle_reaper_leaves_a_freshly_streamed_torrent_alone() {
    // The reaper decides purely from stream-activity recency, so it can be
    // checked without a real swarm: a torrent touched just now must never be
    // considered idle, or playback would be paused out from under a viewer.
    let gateway = Gateway::start().await;
    let engine = &gateway.state.engine;
    engine.remember_advertised(HASH, &[]).await;
    engine.touch_stream(HASH, "127.0.0.1".parse().unwrap());

    assert!(
        engine.is_recently_active(HASH, Duration::from_secs(120)),
        "a just-streamed torrent must count as active"
    );
    // Nothing is in the session, so this is a no-op that must not panic.
    assert_eq!(
        engine.pause_idle_torrents(Duration::from_secs(120)).await,
        0
    );
}

/// Activity is recorded before a request is judged, so it has to be limited
/// to torrents that exist in some sense -- or every made-up hash a client
/// asks for is kept for the life of the process.
#[tokio::test]
async fn requests_for_unknown_hashes_leave_no_trace() {
    let gateway = Gateway::start().await;
    for n in 0..20 {
        let (status, _) = get(gateway.router(), &format!("/videos/{n:040}/0")).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }
    assert!(gateway.state.engine.recent_streams().is_empty());
}

#[tokio::test]
async fn videos_endpoint_refuses_to_start_a_hash_it_never_advertised() {
    // `/videos` starts torrents on demand, so without this gate anyone who
    // can reach the port could make the gateway join an arbitrary swarm.
    // It must refuse immediately -- no metadata fetch, no network at all.
    let gateway = Gateway::start().await;

    let started = std::time::Instant::now();
    let (status, body) = get(gateway.router(), &format!("/videos/{HASH}/0")).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body["error"].as_str().unwrap().contains("not offered"));
    // A rejection that went to the network would take seconds, not millis.
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "refusal must not touch the network"
    );
}

#[tokio::test]
async fn videos_endpoint_accepts_a_hash_the_gateway_advertised() {
    // Mirror of the test above. Asserted on timing rather than on a final
    // response: the gate refuses in microseconds, whereas passing it means
    // dropping into a metadata fetch that cannot finish here (no DHT, no
    // peers). "Still running after a moment" is therefore proof it got
    // through -- and keeps this test at ~1s instead of the 60s a real
    // request timeout would cost.
    let gateway = Gateway::start().await;
    let engine = &gateway.state.engine;

    let unknown = tokio::time::timeout(Duration::from_secs(1), engine.ensure_started(HASH, 0))
        .await
        .expect("gate must refuse without blocking")
        .expect("refusal is not an error");
    assert!(!unknown, "an unadvertised hash must be refused");

    engine.remember_advertised(HASH, &[]).await;
    let advertised =
        tokio::time::timeout(Duration::from_secs(1), engine.ensure_started(HASH, 0)).await;
    assert!(
        advertised.is_err(),
        "an advertised hash must pass the gate and reach the torrent engine"
    );
}

/// The advertised set used to live only in memory, so every restart turned
/// every link already sitting in a Stremio client into a 404.
#[tokio::test]
async fn advertised_links_survive_a_restart() {
    let first = Gateway::start().await;
    let trackers = vec!["udp://tracker.example:1337/announce".to_string()];
    first
        .state
        .engine
        .remember_advertised_many([
            (HASH, trackers.as_slice()),
            ("ab".repeat(20).as_str(), &[][..]),
        ])
        .await;
    let dir = first.dir.clone();
    drop(first);

    let second = Gateway::start_in(dir).await;
    assert!(second.state.engine.is_advertised(HASH));
    assert!(second.state.engine.is_advertised(&"ab".repeat(20)));
    assert!(!second.state.engine.is_advertised(&"cd".repeat(20)));
}
