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

    let config = AppConfig {
        port: 0,
        fallback_port: 0,
        bind_addr: "127.0.0.1".parse().unwrap(),
        cache_dir: dir,
        max_cache_size_gb: 20,
        auto_cleanup: false,
        cleanup_interval_secs: 3600,
        max_concurrent_torrents: 4,
        disable_dht: true, // tests must not depend on the public DHT network
        monitor_interval_secs: 3600,
        log_level: "error".to_string(),
    };

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
        base_url: "http://127.0.0.1:11470".to_string(),
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
async fn stream_endpoint_returns_empty_list_for_non_magnet_ids() {
    // Stremio queries every installed addon for every title id (usually an
    // IMDB id). We only know how to resolve magnet-shaped ids, so anything
    // else must come back as a valid, empty stream list -- never an error.
    let app = build_router(test_state().await);
    let (status, body) = get(app, "/stream/movie/tt1234567.json").await;

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
async fn videos_endpoint_404s_for_unknown_but_valid_hash() {
    let app = build_router(test_state().await);
    let hash = "0123456789abcdef0123456789abcdef01234567";
    let (status, _) = get(app, &format!("/videos/{hash}/0")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
