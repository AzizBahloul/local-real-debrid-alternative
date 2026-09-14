//! The protocol surface: manifest, stream lists, health, input validation, and
//! which routes answer whom.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::http::{Method, StatusCode};
use serde_json::Value;

use streaming_gateway::indexer::{IndexedTorrent, SearchFuture, StreamIndexer};

use crate::{get, request, request_from, send, Gateway};

#[tokio::test]
async fn manifest_matches_stremio_addon_protocol() {
    let gateway = Gateway::start().await;
    let (status, body) = get(gateway.router(), "/manifest.json").await;

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
async fn the_manifest_is_served_as_json() {
    let gateway = Gateway::start().await;
    let reply = send(gateway.router(), request(Method::GET, "/manifest.json")).await;
    assert_eq!(reply.header("content-type"), Some("application/json"));
}

#[tokio::test]
async fn health_reports_zero_activity_on_a_fresh_gateway() {
    let gateway = Gateway::start().await;
    let (status, body) = get(gateway.router(), "/health").await;

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
    let gateway = Gateway::start().await;
    let (status, body) = get(gateway.router(), "/stream/movie/tt1234567.json").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["streams"], serde_json::json!([]));
}

#[tokio::test]
async fn manifest_only_advertises_magnet_ids_when_discovery_is_disabled() {
    // Advertising `tt` while discovery is off would make Stremio query us for
    // every title and always get nothing back, which reads as a broken addon.
    let gateway = Gateway::start().await;
    let (_, body) = get(gateway.router(), "/manifest.json").await;

    let prefixes = body["resources"][0]["idPrefixes"].as_array().unwrap();
    assert_eq!(prefixes, &vec![Value::from("magnet:")]);
}

#[tokio::test]
async fn stream_endpoint_ignores_ids_shaped_to_reach_the_index() {
    // Defense in depth: a catalog id is interpolated into an outbound URL, so
    // anything not strictly `tt<digits>` / `kitsu:<digits>` must be refused
    // here even before the indexer's own encoding would neutralize it.
    let gateway = Gateway::start().await;
    let (status, body) = get(
        gateway.router(),
        "/stream/movie/tt123%2F..%2F..%2Fadmin.json",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["streams"], serde_json::json!([]));
}

/// An index that always finds the same two releases.
struct FakeIndex;

impl StreamIndexer for FakeIndex {
    fn search<'a>(&'a self, _: &'a str, _: &'a str) -> SearchFuture<'a> {
        Box::pin(async {
            Ok(vec![
                IndexedTorrent {
                    info_hash: "1111111111111111111111111111111111111111".into(),
                    file_idx: Some(2),
                    title: "Show.S01E01.1080p".into(),
                    quality: "1080p".into(),
                    size_bytes: Some(1024 * 1024 * 1024),
                    seeders: Some(12),
                    trackers: vec!["udp://tracker.example:1337/announce".into()],
                },
                IndexedTorrent {
                    info_hash: "2222222222222222222222222222222222222222".into(),
                    file_idx: None,
                    title: "Show.S01E01.720p".into(),
                    quality: "720p".into(),
                    size_bytes: None,
                    seeders: None,
                    trackers: Vec::new(),
                },
            ])
        })
    }
}

/// Listing a stream is what makes its `/videos` URL startable, so a stream
/// list the gateway forgot to advertise is a list of 404s.
#[tokio::test]
async fn a_stream_list_offers_lan_urls_and_makes_every_one_startable() {
    let mut gateway = Gateway::start().await;
    gateway.state.indexer = Some(Arc::new(FakeIndex));

    let (status, body) = get(gateway.router(), "/stream/series/tt0944947:1:1.json").await;
    assert_eq!(status, StatusCode::OK);
    let streams = body["streams"].as_array().unwrap();
    assert_eq!(streams.len(), 2, "no away entries without a public host");
    assert_eq!(
        streams[0]["url"],
        "http://127.0.0.1:8080/videos/1111111111111111111111111111111111111111/2"
    );
    assert_eq!(
        streams[1]["url"],
        "http://127.0.0.1:8080/videos/2222222222222222222222222222222222222222/0"
    );

    let engine = &gateway.state.engine;
    assert!(engine.is_advertised("1111111111111111111111111111111111111111"));
    assert!(engine.is_advertised("2222222222222222222222222222222222222222"));
}

#[tokio::test]
async fn play_rejects_malformed_magnet() {
    let gateway = Gateway::start().await;
    let (status, body) = get(gateway.router(), "/play?magnet=not-a-magnet-link").await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"]
        .as_str()
        .unwrap()
        .contains("could not resolve"));
}

#[tokio::test]
async fn play_rejects_oversized_magnet() {
    let gateway = Gateway::start().await;
    let huge = "a".repeat(9000);
    let (status, body) = get(gateway.router(), &format!("/play?magnet={huge}")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"].as_str().unwrap().contains("too long"));
}

#[tokio::test]
async fn videos_endpoint_rejects_invalid_info_hash_shapes() {
    let gateway = Gateway::start().await;
    let (status, _) = get(gateway.router(), "/videos/../../etc/passwd/0").await;
    // Axum's path matcher itself won't even route a literal `..` segment the
    // same way, but any non-40-hex value must be rejected before it reaches
    // a lookup.
    assert!(status == StatusCode::NOT_FOUND || status == StatusCode::BAD_REQUEST);

    let (status, body) = get(gateway.router(), "/videos/short/0").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"].as_str().unwrap().contains("40 hex"));
}

/// Required by the Stremio addon protocol: every route must permit every
/// origin, or Stremio Web and TV apps cannot read the answers.
#[tokio::test]
async fn every_route_permits_any_origin() {
    let gateway = Gateway::start().await;
    let reply = send(
        gateway.router(),
        request(Method::GET, "/manifest.json").header("origin", "https://web.stremio.com"),
    )
    .await;
    assert_eq!(reply.header("access-control-allow-origin"), Some("*"));
}

#[tokio::test]
async fn audit_export_is_refused_to_everything_but_loopback() {
    // The audit log records every client IP that ever connected and every
    // title it played. Every *other* route here is deliberately open to the
    // whole LAN, because that is the product -- so this one being closed is a
    // property worth a test rather than a convention worth trusting. The
    // gateway has no authentication of any kind, so "reachable from the WiFi"
    // means "readable by anything on the WiFi".
    let gateway = Gateway::start().await;
    let reply = send(
        gateway.router(),
        request_from(
            Method::GET,
            "/audit/export",
            SocketAddr::from(([192, 168, 1, 214], 51000)),
        ),
    )
    .await;
    assert_eq!(reply.status, StatusCode::FORBIDDEN);
    assert!(
        reply.json()["error"].is_string(),
        "a refusal says why, in the same JSON shape as every other error"
    );
}

#[tokio::test]
async fn audit_export_is_served_over_loopback() {
    // The other half: the desktop app's "save logs" button asks over
    // 127.0.0.1, so a guard that refused everyone would be just as broken.
    let gateway = Gateway::start().await;
    let reply = send(gateway.router(), request(Method::GET, "/audit/export")).await;
    assert_eq!(reply.status, StatusCode::OK);
}

#[tokio::test]
async fn the_destructive_routes_refuse_the_lan() {
    let gateway = Gateway::start().await;
    let phone = SocketAddr::from(([192, 168, 1, 214], 51000));
    for uri in [
        "/cache/clear",
        "/torrents/0123456789abcdef0123456789abcdef01234567/delete",
    ] {
        let reply = send(gateway.router(), request_from(Method::POST, uri, phone)).await;
        assert_eq!(reply.status, StatusCode::FORBIDDEN, "{uri}");
    }
}

#[tokio::test]
async fn an_unknown_torrent_action_is_a_bad_request_that_says_so() {
    let gateway = Gateway::start().await;
    let reply = send(
        gateway.router(),
        request(
            Method::POST,
            "/torrents/0123456789abcdef0123456789abcdef01234567/explode",
        ),
    )
    .await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
    assert!(reply.json()["error"].as_str().unwrap().contains("explode"));
}

/// The desktop app polls `/health` every second, and those polls were 99.7%
/// of the log. Everything else must still be recorded.
#[tokio::test]
async fn the_request_log_skips_health_polls_but_keeps_everything_else() {
    let mut gateway = Gateway::start().await;
    let log_dir = tempfile::tempdir().unwrap();
    let audit = streaming_gateway::audit::AuditLog::open(log_dir.path());
    assert!(audit.is_enabled(), "the test needs a real log to read back");
    gateway.state.audit = audit.clone();

    for _ in 0..3 {
        assert_eq!(get(gateway.router(), "/health").await.0, StatusCode::OK);
    }
    get(gateway.router(), "/manifest.json").await;

    // The writer is a background thread; give it a moment to land the line.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut log = audit.read_all();
    while !log.contains("/manifest.json") && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
        log = audit.read_all();
    }
    assert!(
        log.contains("\"path\":\"/manifest.json\""),
        "log was: {log}"
    );
    assert!(!log.contains("\"path\":\"/health\""), "log was: {log}");
}
