//! Integration tests against the real HTTP router (real `TorrentEngine`,
//! real `librqbit::Session`, a temp directory for cache/session state).
//!
//! What these do *not* do: download from a real swarm. That needs live
//! peers/trackers on the public network, which would make the suite flaky and
//! dependent on external state -- not appropriate for a repeatable test run.
//! Instead they exercise everything that does not require one: routing, the
//! Stremio protocol shapes, `/health`, every input-validation rejection path,
//! and -- through a torrent built on the spot and laid out on disk as if it
//! had finished downloading (see `seed_torrent`) -- the byte-serving path
//! itself.
//!
//! One test binary, split by subject: `routes` for the protocol surface,
//! `engine` for the engine's own bookkeeping as seen through it, `streaming`
//! for serving bytes.

mod engine;
mod routes;
mod streaming;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::http::{HeaderMap, Method, Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

use streaming_gateway::cache::CacheManager;
use streaming_gateway::config::AppConfig;
use streaming_gateway::torrent::TorrentEngine;
use streaming_gateway::{build_router, AppState};

/// A gateway running against its own temporary cache directory.
pub struct Gateway {
    pub state: AppState,
    pub config: AppConfig,
    /// Held so the directory outlives the engine using it; shared so a second
    /// gateway can be started over the same one (a restart).
    pub dir: Arc<tempfile::TempDir>,
}

impl Gateway {
    pub async fn start() -> Self {
        Self::start_in(Arc::new(tempfile::tempdir().expect("create a temp dir"))).await
    }

    pub async fn start_in(dir: Arc<tempfile::TempDir>) -> Self {
        let config = test_config(dir.path());
        let engine = TorrentEngine::new(&config).await.expect("engine starts");
        let cache = CacheManager::new(
            config.downloads_dir(),
            config.max_cache_size_bytes(),
            config.max_cached_torrents,
            config.auto_cleanup,
            Arc::clone(&engine),
        );

        let state = AppState {
            engine,
            cache,
            stream_base_url: "http://127.0.0.1:8080".to_string(),
            indexer: None,
            tls_host: None,
            audit: streaming_gateway::audit::AuditLog::disabled(),
        };
        Self { state, config, dir }
    }

    pub fn router(&self) -> axum::Router {
        build_router(self.state.clone())
    }
}

fn test_config(cache_dir: &std::path::Path) -> AppConfig {
    // Started from the real defaults and then overridden, rather than written
    // out field by field. A struct literal here has to name every field, so
    // adding one to `AppConfig` breaks this file for reasons that have nothing
    // to do with what it tests -- and the fix is always "copy the default in",
    // which is what this does once instead of on every future field.
    let mut config = AppConfig::defaults();

    config.port = 0;
    config.fallback_port = 0;
    config.bind_addr = "127.0.0.1".parse().unwrap();
    config.cache_dir = cache_dir.to_path_buf();
    // Tests use a disabled `AuditLog` unless they build their own, so nothing
    // is written and this path is never created.
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

    // Discovery off: these tests must not reach out to a public index. Tests
    // that need one install a fake.
    config.indexer_url = "https://indexer.invalid".to_string();
    config.disable_indexer = true;

    config.idle_pause_secs = 120;
    config.idle_check_interval_secs = 30;

    // No pre-buffering: waiting for bytes that may never arrive would just
    // add the timeout to every case.
    config.prebuffer_bytes = 0;
    config.prebuffer_timeout_secs = 1;
    // Stall recovery off: with no swarm a read that stalls stays stalled, so
    // leaving it on would re-open streams in a loop.
    config.stall_timeout_secs = 0;

    // No https listener: it would fetch a certificate over the network, which
    // these tests must never depend on.
    config.disable_https = true;
    config.https_port = 0;
    config.tls_cert_url = "https://tls.invalid/server.pem".to_string();
    config.tls_key_url = "https://tls.invalid/server.key".to_string();
    config
}

/// A response, read to the end.
pub struct Reply {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl Reply {
    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or(Value::Null)
    }

    pub fn header(&self, name: impl axum::http::header::AsHeaderName) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }
}

/// A request as the router sees one from `from`.
pub fn request_from(method: Method, uri: &str, from: SocketAddr) -> axum::http::request::Builder {
    Request::builder()
        .method(method)
        .uri(uri)
        .extension(axum::extract::ConnectInfo(from))
}

/// A request from this machine.
pub fn request(method: Method, uri: &str) -> axum::http::request::Builder {
    request_from(method, uri, SocketAddr::from(([127, 0, 0, 1], 9)))
}

pub async fn send(app: axum::Router, request: axum::http::request::Builder) -> Reply {
    let response = app
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    Reply {
        status,
        headers,
        body,
    }
}

pub async fn get(app: axum::Router, uri: &str) -> (StatusCode, Value) {
    let reply = send(app, request(Method::GET, uri)).await;
    (reply.status, reply.json())
}

/// A torrent this gateway can serve with no swarm at all.
pub struct SeededTorrent {
    pub info_hash: String,
    pub name: String,
    pub content: Vec<u8>,
}

/// Builds a real single-file torrent and lays it out exactly as a finished,
/// later-reclaimed download of it would have left things: the file under
/// `downloads/<hash>/`, its `.torrent` in the metadata archive, and the hash
/// on the advertised list a stream list would have put it on.
///
/// Starting it then goes through the real path -- advertised-hash gate, the
/// archive, librqbit's add and hash-check -- and finds every piece already on
/// disk, so it serves bytes without a single peer.
pub async fn seed_torrent(gateway: &Gateway) -> SeededTorrent {
    seed_torrent_variant(gateway, 0).await
}

/// Like [`seed_torrent`], but `variant` changes every byte, so each variant
/// is a different torrent with its own info hash.
pub async fn seed_torrent_variant(gateway: &Gateway, variant: u8) -> SeededTorrent {
    use librqbit::spawn_utils::BlockingSpawner;
    use librqbit::{create_torrent, CreateTorrentOptions};

    let name = "Fixture.Movie.2024.1080p.mkv".to_string();
    // Several pieces, and a length that is not a multiple of the piece size,
    // so the last piece is a short one.
    let content: Vec<u8> = (0..300_001u32)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 11) as u8 ^ variant)
        .collect();

    let staging = tempfile::tempdir().unwrap();
    let source = staging.path().join(&name);
    std::fs::write(&source, &content).unwrap();
    let created = create_torrent(
        &source,
        CreateTorrentOptions {
            name: Some(&name),
            trackers: Vec::new(),
            piece_length: Some(32 * 1024),
        },
        &BlockingSpawner::new(1),
    )
    .await
    .expect("build a torrent from the fixture file");
    let info_hash = created.info_hash().as_string();

    let download_dir = gateway.config.downloads_dir().join(&info_hash);
    std::fs::create_dir_all(&download_dir).unwrap();
    std::fs::write(download_dir.join(&name), &content).unwrap();

    let archive = gateway.config.session_state_dir().join("metadata");
    std::fs::create_dir_all(&archive).unwrap();
    std::fs::write(
        archive.join(format!("{info_hash}.torrent")),
        created.as_bytes().unwrap(),
    )
    .unwrap();

    gateway
        .state
        .engine
        .remember_advertised(&info_hash, &[])
        .await;

    SeededTorrent {
        info_hash,
        name,
        content,
    }
}
