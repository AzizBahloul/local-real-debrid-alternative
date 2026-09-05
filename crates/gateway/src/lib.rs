pub mod cache;
pub mod config;
pub mod error;
pub mod indexer;
pub mod monitoring;
pub mod network;
pub mod streaming;
pub mod stremio;
pub mod tls;
pub mod torrent;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::FromRef;
use axum::routing::{get, post};
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
    /// `http://192.168.1.42:8080`. Used to build absolute stream URLs for
    /// Stremio (relative URLs are not valid in a stream response).
    ///
    /// Deliberately the LAN address even when the addon itself is reached
    /// through an https tunnel: the manifest is kilobytes and may travel
    /// through the tunnel, but the video is gigabytes and must not. See the
    /// `stremio` module header.
    pub stream_base_url: String,
    /// Torrent discovery. `None` when `--disable-indexer` is set, in which
    /// case the addon only answers for ids that already carry a magnet.
    pub indexer: Option<Arc<indexer::TorrentioIndexer>>,
    /// This gateway's own https hostname, when it serves one (e.g.
    /// `192-168-1-67.local-ip.sh`).
    ///
    /// A request arriving on this name came over the LAN, even though the
    /// name is a public one that resolves through public DNS. Without
    /// knowing that, the addon would see an unfamiliar public-looking host
    /// and offer its "you must be away from home" fallback for a request
    /// that never left the building.
    pub tls_host: Option<String>,
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
        // Loopback-only; see the handler. The desktop app's "clear" button.
        .route("/cache/clear", post(monitoring::clear_cache))
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
/// taken (e.g. another instance running, or something else on 8080).
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

    if config.idle_pause_secs > 0 {
        engine.spawn_idle_reaper(
            Duration::from_secs(config.idle_check_interval_secs.max(1)),
            Duration::from_secs(config.idle_pause_secs),
        );
        info!(
            idle_after_secs = config.idle_pause_secs,
            "idle torrents will be paused so bandwidth goes to what is being watched"
        );
    }

    let lan_ip = network::detect_lan_ip();
    let (listener, bound_port) = bind_with_fallback(&config).await?;
    // Applied to the listener, and thereby to every connection accepted on it,
    // so a viewer who walks away stops holding a torrent open. See
    // `network::harden_listener`.
    let client_timeout = Duration::from_secs(config.client_timeout_secs);
    network::harden_listener(&listener, client_timeout);
    let stream_base_url = config
        .public_stream_url
        .clone()
        .map(|url| url.trim_end_matches('/').to_string())
        .unwrap_or_else(|| format!("http://{lan_ip}:{bound_port}"));

    let indexer = if config.disable_indexer {
        info!("torrent discovery disabled; addon will only answer magnet ids");
        None
    } else {
        match indexer::TorrentioIndexer::new(
            config.indexer_url.clone(),
            config.indexer_max_results,
            Duration::from_secs(config.indexer_timeout_secs),
        ) {
            Ok(ix) => {
                info!(url = %config.indexer_url, "torrent discovery enabled");
                Some(Arc::new(ix))
            }
            Err(e) => {
                // Discovery is an enhancement -- failing to build its client
                // must not stop the gateway from serving magnets.
                warn!("could not initialize torrent index, discovery disabled: {e:#}");
                None
            }
        }
    };

    // Known before the certificate is: it is derived purely from the LAN
    // address, and the stream handler needs it to recognise its own https
    // origin as a local one.
    let tls_host =
        (!config.disable_https).then(|| tls::hostname_for(lan_ip, &config.tls_host_suffix));

    let state = AppState {
        engine,
        cache,
        stream_base_url,
        indexer,
        tls_host: tls_host.clone(),
    };

    monitoring::spawn_terminal_monitor(
        state.clone(),
        Duration::from_secs(config.monitor_interval_secs),
    );

    let app = build_router(state);

    // The https listener is what lets Stremio's Android app add the addon
    // without a tunnel -- see the `tls` module. It serves the same router on
    // its own port, so http keeps working exactly as before and a failure
    // here degrades to "no addon URL" rather than "no gateway".
    let addon_url = if config.disable_https {
        None
    } else {
        match tls::load(
            config.tls_cert_file.as_deref(),
            config.tls_key_file.as_deref(),
            &config.tls_cert_url,
            &config.tls_key_url,
            &config.cache_dir,
        )
        .await
        {
            Ok(tls_config) => {
                tls::spawn_refresh(
                    tls_config.clone(),
                    config.tls_cert_url.clone(),
                    config.tls_key_url.clone(),
                    config.cache_dir.clone(),
                );

                let host = tls::hostname_for(lan_ip, &config.tls_host_suffix);
                let addr = SocketAddr::new(config.bind_addr, config.https_port);
                let https_app = app.clone();

                // Bound here rather than letting `bind_rustls` do it, purely so
                // the same client timeout can be applied to this socket too.
                // Video does travel over https in some setups (a tunnelled
                // client, or VLC simply pointed at this port), and a wedged
                // connection pins its torrent exactly as it would over http.
                match std::net::TcpListener::bind(addr)
                    .and_then(|l| l.set_nonblocking(true).map(|()| l))
                {
                    Ok(https_listener) => {
                        network::harden_listener(&https_listener, client_timeout);
                        tokio::spawn(async move {
                            let served = match axum_server::from_tcp_rustls(
                                https_listener,
                                tls_config,
                            ) {
                                Ok(server) => {
                                    server
                                        .serve(
                                            https_app
                                                .into_make_service_with_connect_info::<SocketAddr>(),
                                        )
                                        .await
                                }
                                Err(e) => Err(e),
                            };
                            if let Err(e) = served {
                                warn!(
                                    "https listener stopped: {e:#} -- the addon URL will not \
                                     work, but http streaming is unaffected"
                                );
                            }
                        });
                        info!(%host, port = config.https_port, "https listening");
                        Some(format!("https://{host}:{}", config.https_port))
                    }
                    Err(e) => {
                        warn!(
                            port = config.https_port,
                            "could not bind the https port ({e}); Stremio's Android app will \
                             not be able to add this addon without a tunnel"
                        );
                        None
                    }
                }
            }
            Err(e) => {
                warn!(
                    "could not start https ({e:#}); Stremio's Android app will not be able to \
                     add this addon without a tunnel"
                );
                None
            }
        }
    };

    network::print_banner(lan_ip, bound_port, addon_url.as_deref());
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
