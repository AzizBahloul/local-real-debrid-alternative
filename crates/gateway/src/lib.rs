pub mod cache;
pub mod config;
pub mod error;
pub mod monitoring;
pub mod network;
pub mod streaming;
pub mod stremio;
pub mod torrent;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::FromRef;
use axum::routing::get;
use axum::Router;
use tower_http::cors::CorsLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};

use cache::CacheManager;
use config::AppConfig;
use torrent::TorrentEngine;

/// Shared application state. Cheap to clone (everything inside is an `Arc`
/// or a small `String`) -- axum clones it per request.
#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<TorrentEngine>,
    pub cache: Arc<CacheManager>,
    /// This gateway's own address as reachable from the LAN, e.g.
    /// `http://192.168.1.42:11470`. Used to build absolute stream URLs for
    /// Stremio (relative URLs are not valid in a stream response).
    pub base_url: String,
}

// Lets handlers ask for `State<Arc<TorrentEngine>>` directly instead of the
// whole `AppState`, without duplicating the engine behind two owners.
impl FromRef<AppState> for Arc<TorrentEngine> {
    fn from_ref(state: &AppState) -> Self {
        Arc::clone(&state.engine)
    }
}

/// Builds the full HTTP router. Split out from `run` so integration tests can
/// exercise real routing/middleware/handlers without binding a socket.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/manifest.json", get(stremio::manifest))
        .route("/stream/{content_type}/{id}", get(stremio::stream))
        .route("/play", get(streaming::play))
        .route(
            "/videos/{info_hash}/{file_idx}",
            get(streaming::stream_video),
        )
        .route("/health", get(monitoring::health))
        .layer(TraceLayer::new_for_http())
        // Required by the Stremio addon protocol ("all routes must serve
        // CORS headers permitting all origins"); also what lets a phone's
        // browser or a smart TV app fetch across origins on the LAN.
        .layer(CorsLayer::permissive())
        // Every endpoint here is a GET with query/path params, no uploads --
        // this is defense in depth against an oversized request body, not a
        // limit anything legitimate should ever hit.
        .layer(RequestBodyLimitLayer::new(16 * 1024))
        // Bounds time-to-first-byte (metadata resolution, opening a file
        // stream), not total playback duration: a streaming response body is
        // handed back to the client as soon as headers are ready, so a long
        // watch session is never cut off by this.
        .layer(TimeoutLayer::with_status_code(
            axum::http::StatusCode::GATEWAY_TIMEOUT,
            Duration::from_secs(60),
        ))
        .with_state(state)
}

/// Binds the primary port, falling back to `fallback_port` if it's already
/// taken (e.g. another instance running, or something else on 11470).
pub async fn bind_with_fallback(
    config: &AppConfig,
) -> anyhow::Result<(tokio::net::TcpListener, u16)> {
    let primary = SocketAddr::new(config.bind_addr, config.port);
    match tokio::net::TcpListener::bind(primary).await {
        Ok(listener) => Ok((listener, config.port)),
        Err(e) => {
            warn!(
                port = config.port,
                error = %e,
                "primary port unavailable, trying fallback port {}",
                config.fallback_port
            );
            let fallback = SocketAddr::new(config.bind_addr, config.fallback_port);
            let listener = tokio::net::TcpListener::bind(fallback)
                .await
                .map_err(|e2| {
                    anyhow::anyhow!(
                        "failed to bind both port {} and fallback port {}: {e2}",
                        config.port,
                        config.fallback_port
                    )
                })?;
            Ok((listener, config.fallback_port))
        }
    }
}

pub async fn run() -> anyhow::Result<()> {
    let config = AppConfig::load();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(config.log_level.clone()))
        .init();

    if let Err(e) = librqbit::try_increase_nofile_limit() {
        warn!("could not raise open-file limit (streaming many torrents may hit OS limits): {e:#}");
    }

    info!("starting torrent engine...");
    let engine = TorrentEngine::new(&config).await?;

    let cache = CacheManager::new(
        config.downloads_dir(),
        config.max_cache_size_bytes(),
        config.auto_cleanup,
        Arc::clone(&engine),
    );
    cache.spawn_janitor(Duration::from_secs(config.cleanup_interval_secs));

    let lan_ip = network::detect_lan_ip();
    let (listener, bound_port) = bind_with_fallback(&config).await?;
    let base_url = format!("http://{lan_ip}:{bound_port}");

    let state = AppState {
        engine,
        cache,
        base_url,
    };

    monitoring::spawn_terminal_monitor(
        state.clone(),
        Duration::from_secs(config.monitor_interval_secs),
    );

    let app = build_router(state);

    network::print_banner(lan_ip, bound_port);
    info!(%lan_ip, port = bound_port, "streaming gateway listening");

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("shutdown signal received, stopping gracefully");
}
