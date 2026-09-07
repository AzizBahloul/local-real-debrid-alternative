//! Integration tests against the real HTTP router (real `TorrentEngine`,
//! real `librqbit::Session`, a temp directory for cache/session state).
//!
//! What these do *not* do: complete a real BitTorrent download. That needs
//! live peers/trackers on the public network, which would make the suite
//! flaky and dependent on external state -- not appropriate for a
//! repeatable test run. Instead they exercise everything that does not
//! require a real swarm: routing, the Stremio protocol shapes, `/health`,
//! and every input-validation rejection path (bad info hashes, bad magnets,
//! oversized input) on the real handlers.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

use streaming_gateway::cache::CacheManager;
use streaming_gateway::config::AppConfig;
use streaming_gateway::torrent::TorrentEngine;
use streaming_gateway::{build_router, AppState};

async fn test_state() -> AppState {
    let dir = std::env::temp_dir().join(format!(
        "streaming-gateway-it-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    // Started from the real defaults and then overridden, rather than written
    // out field by field. A struct literal here has to name every field, so
    // adding one to `AppConfig` breaks this file for reasons that have nothing
    // to do with what it tests -- and the fix is always "copy the default in",
    // which is what this does once instead of on every future field.
    let mut config = AppConfig::defaults();

    config.port = 0;
    config.fallback_port = 0;
    config.bind_addr = "127.0.0.1".parse().unwrap();
    config.cache_dir = dir;
    // Tests use a disabled `AuditLog`, so nothing is written and this path is
    // never created.
    config.log_dir = None;
    config.auto_cleanup = false;
    config.cleanup_interval_secs = 3600;
    config.max_concurrent_torrents = 4;

    // No rate limiting and no client timeout: there is no swarm to throttle,
    // and the router is driven in-process rather than over a real socket, so
    // neither setting can influence the outcome.
    config.max_upload_mb_s = 0;
    config.max_download_mb_s = 0;
    config.client_timeout_secs = 0;

    config.disable_dht = true; // tests must not depend on the public DHT network
    // An ephemeral peer port so concurrent test sessions never clash over one,
    // and no UPnP: a test must not reconfigure the developer's router.
    config.peer_port = 0;
    config.disable_upnp = true;
    // Both would join real swarms.
    config.browse_prefetch_count = 0;
    config.mp4_tail_warm = false;

    config.monitor_interval_secs = 3600;
    config.log_level = "error".to_string();

    // Discovery off: these tests must not reach out to a public index. Indexer
    // parsing/id-validation is covered by unit tests instead.
    config.indexer_url = "https://indexer.invalid".to_string();
    config.disable_indexer = true;

    config.idle_pause_secs = 120;
    config.idle_check_interval_secs = 30;

    // No pre-buffering: there is no swarm, so waiting for bytes that can never
    // arrive would just add the timeout to every case.
    config.prebuffer_bytes = 0;
    config.prebuffer_timeout_secs = 1;
    // Stall recovery off: with no swarm every read stalls by definition, so
    // leaving it on would re-open streams in a loop.
    config.stall_timeout_secs = 0;

    // No https listener: it would fetch a certificate over the network, which
    // these tests must never depend on.
    config.disable_https = true;
    config.https_port = 0;
    config.tls_cert_url = "https://tls.invalid/server.pem".to_string();
    config.tls_key_url = "https://tls.invalid/server.key".to_string();

    let engine = TorrentEngine::new(&config).await.expect("engine starts");
    let cache = CacheManager::new(
        config.downloads_dir(),
        config.max_cache_size_bytes(),
        config.auto_cleanup,
        Arc::clone(&engine),
    );

    AppState {
        engine,
        cache,
        stream_base_url: "http://127.0.0.1:8080".to_string(),
        indexer: None,
        tls_host: None,
        audit: streaming_gateway::audit::AuditLog::disabled(),
    }
}

async fn get(app: axum::Router, uri: &str) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .extension(axum::extract::ConnectInfo(SocketAddr::from((
                    [127, 0, 0, 1],
                    9,
                ))))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, json)
}

#[tokio::test]
async fn manifest_matches_stremio_addon_protocol() {
    let app = build_router(test_state().await);
    let (status, body) = get(app, "/manifest.json").await;

    assert_eq!(status, StatusCode::OK);
    assert!(body["id"].is_string());
    assert!(body["resources"].is_array());
    assert_eq!(body["catalogs"], serde_json::json!([]));
    assert!(body["types"]
        .as_array()
        .unwrap()
        .contains(&Value::from("movie")));
}

#[tokio::test]
async fn health_reports_zero_activity_on_a_fresh_gateway() {
    let app = build_router(test_state().await);
    let (status, body) = get(app, "/health").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");
    assert_eq!(body["active_streams"], 0);
    assert_eq!(body["active_torrents"], serde_json::json!([]));
}

#[tokio::test]
async fn stream_endpoint_returns_empty_list_when_discovery_is_disabled() {
    // Stremio queries every installed addon for every title id. With
    // discovery off we cannot resolve a catalog id, so the answer must be a
    // valid, empty stream list -- never an error, and never an outbound call.
    let app = build_router(test_state().await);
    let (status, body) = get(app, "/stream/movie/tt1234567.json").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["streams"], serde_json::json!([]));
}

#[tokio::test]
async fn manifest_only_advertises_magnet_ids_when_discovery_is_disabled() {
    // Advertising `tt` while discovery is off would make Stremio query us for
    // every title and always get nothing back, which reads as a broken addon.
    let app = build_router(test_state().await);
    let (_, body) = get(app, "/manifest.json").await;

    let prefixes = body["resources"][0]["idPrefixes"].as_array().unwrap();
    assert_eq!(prefixes, &vec![Value::from("magnet:")]);
}

#[tokio::test]
async fn stream_endpoint_ignores_ids_shaped_to_reach_the_index() {
    // Defense in depth: a catalog id is interpolated into an outbound URL, so
    // anything not strictly `tt<digits>` / `kitsu:<digits>` must be refused
    // here even before the indexer's own encoding would neutralize it.
    let app = build_router(test_state().await);
    let (status, body) = get(app, "/stream/movie/tt123%2F..%2F..%2Fadmin.json").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["streams"], serde_json::json!([]));
}

#[tokio::test]
async fn play_rejects_malformed_magnet() {
    let app = build_router(test_state().await);
    let (status, body) = get(app, "/play?magnet=not-a-magnet-link").await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"]
        .as_str()
        .unwrap()
        .contains("could not resolve"));
}

#[tokio::test]
async fn play_rejects_oversized_magnet() {
    let app = build_router(test_state().await);
    let huge = "a".repeat(9000);
    let (status, body) = get(app, &format!("/play?magnet={huge}")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"].as_str().unwrap().contains("too long"));
}

#[tokio::test]
async fn videos_endpoint_rejects_invalid_info_hash_shapes() {
    let app = build_router(test_state().await);
    let (status, _) = get(app, "/videos/../../etc/passwd/0").await;
    // Axum's path matcher itself won't even route a literal `..` segment the
    // same way, but any non-40-hex value must be rejected before it reaches
    // a lookup.
    assert!(status == StatusCode::NOT_FOUND || status == StatusCode::BAD_REQUEST);

    let app = build_router(test_state().await);
    let (status, body) = get(app, "/videos/short/0").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"].as_str().unwrap().contains("40 hex"));
}

#[tokio::test]
async fn idle_reaper_leaves_a_freshly_streamed_torrent_alone() {
    // The reaper decides purely from stream-activity recency, so it can be
    // checked without a real swarm: a torrent touched just now must never be
    // considered idle, or playback would be paused out from under a viewer.
    let state = test_state().await;
    let hash = "0123456789abcdef0123456789abcdef01234567";
    state
        .engine
        .touch_stream(hash, "127.0.0.1".parse().unwrap())
        .await;

    assert!(
        state
            .engine
            .is_recently_active(hash, std::time::Duration::from_secs(120))
            .await,
        "a just-streamed torrent must count as active"
    );
    // Nothing is in the session, so this is a no-op that must not panic.
    assert_eq!(
        state
            .engine
            .pause_idle_torrents(std::time::Duration::from_secs(120))
            .await,
        0
    );
}

#[tokio::test]
async fn videos_endpoint_refuses_to_start_a_hash_it_never_advertised() {
    // `/videos` starts torrents on demand, so without this gate anyone who
    // can reach the port could make the gateway join an arbitrary swarm.
    // It must refuse immediately -- no metadata fetch, no network at all.
    let app = build_router(test_state().await);
    let hash = "0123456789abcdef0123456789abcdef01234567";

    let started = std::time::Instant::now();
    let (status, body) = get(app, &format!("/videos/{hash}/0")).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body["error"].as_str().unwrap().contains("not offered"));
    // A rejection that went to the network would take seconds, not millis.
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
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
    let state = test_state().await;
    let hash = "0123456789abcdef0123456789abcdef01234567";

    let unknown = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        state.engine.ensure_started(hash, 0),
    )
    .await
    .expect("gate must refuse without blocking")
    .expect("refusal is not an error");
    assert!(!unknown, "an unadvertised hash must be refused");

    state.engine.remember_advertised(hash, &[]).await;
    let advertised = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        state.engine.ensure_started(hash, 0),
    )
    .await;
    assert!(
        advertised.is_err(),
        "an advertised hash must pass the gate and reach the torrent engine"
    );
}

/// Same as `get`, but lets the test choose the peer address the router sees.
async fn get_from(app: axum::Router, uri: &str, from: SocketAddr) -> StatusCode {
    app.oneshot(
        Request::builder()
            .uri(uri)
            .extension(axum::extract::ConnectInfo(from))
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap()
    .status()
}

#[tokio::test]
async fn audit_export_is_refused_to_everything_but_loopback() {
    // The audit log records every client IP that ever connected and every
    // title it played. Every *other* route here is deliberately open to the
    // whole LAN, because that is the product -- so this one being closed is a
    // property worth a test rather than a convention worth trusting. The
    // gateway has no authentication of any kind, so "reachable from the WiFi"
    // means "readable by anything on the WiFi".
    let app = build_router(test_state().await);
    let status = get_from(
        app,
        "/audit/export",
        SocketAddr::from(([192, 168, 1, 214], 51000)),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn audit_export_is_served_over_loopback() {
    // The other half: the desktop app's "save logs" button asks over
    // 127.0.0.1, so a guard that refused everyone would be just as broken.
    let app = build_router(test_state().await);
    let status = get_from(app, "/audit/export", SocketAddr::from(([127, 0, 0, 1], 9))).await;
    assert_eq!(status, StatusCode::OK);
}
