pub mod audit;
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

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::FromRef;
use axum::routing::{get, post};
use axum::Router;
use tower_http::cors::CorsLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;
use tracing::{debug, info, warn};

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
    /// Persistent event log. Cheap to clone; see the `audit` module.
    pub audit: audit::AuditLog,
}

// Lets handlers ask for `State<Arc<TorrentEngine>>` directly instead of the
// whole `AppState`, without duplicating the engine behind two owners.
impl FromRef<AppState> for Arc<TorrentEngine> {
    fn from_ref(state: &AppState) -> Self {
        Arc::clone(&state.engine)
    }
}

/// The liveness endpoint, named because `audit_requests` has to recognise it.
const HEALTH_PATH: &str = "/health";

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
        .route(HEALTH_PATH, get(monitoring::health))
        // Loopback-only; see the handler. The desktop app's "clear" button.
        .route("/cache/clear", post(monitoring::clear_cache))
        // Loopback-only; see the handler. The desktop app's per-title
        // pause/resume/delete buttons.
        .route(
            "/torrents/{info_hash}/{action}",
            post(monitoring::torrent_action),
        )
        // Loopback-only; see the handler. The desktop app's "export logs".
        .route("/audit/export", get(monitoring::export_audit_log))
        // Records every request with its time-to-first-byte. Added here, so it
        // sits *inside* the timeout layer below and therefore still logs a
        // request that the timeout cut off -- a 504 is precisely the event
        // worth having in the file.
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            audit_requests,
        ))
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

/// Records one `Request` event per HTTP request, with its time-to-first-byte.
///
/// TTFB rather than total duration, because the response returns as soon as
/// headers are ready and the body then streams for the length of the video --
/// timing that would measure how long someone watched, not how long they
/// waited. The wait is the number that decides whether a player starts or
/// gives up, so it is the one worth recording.
async fn audit_requests(
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::extract::ConnectInfo(addr): axum::extract::ConnectInfo<SocketAddr>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let path = request.uri().path().to_string();
    let range = request
        .headers()
        .get(axum::http::header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    let started = std::time::Instant::now();
    let response = next.run(request).await;
    let status = response.status();

    // A successful health poll is not evidence of anything, and the GUI issues
    // one per second for as long as it is open. Left in, they were 35 402 of
    // 35 519 lines -- 99.7% -- which at 8 MB per rotation and one generation
    // kept meant the log evicted a real investigation inside two days and made
    // the rest of it something you had to grep past. A *failing* poll still
    // records: that one is a fact about the server.
    let is_routine_health_poll = path == HEALTH_PATH && status.is_success();

    if !is_routine_health_poll {
        state.audit.record(audit::Event::Request {
            client: addr.ip().to_string(),
            path,
            status: status.as_u16(),
            latency_ms: started.elapsed().as_millis() as u64,
            range,
        });
    }
    response
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

    // Opened before anything that can fail, so a startup failure is itself
    // recorded rather than being the one class of problem the log misses.
    let log_dir = config.resolved_log_dir();
    let audit = audit::AuditLog::open(&log_dir);
    audit::install_panic_hook(audit.clone());
    match audit.path() {
        Some(path) => info!(path = %path.display(), "audit log"),
        None => warn!("audit log is disabled; no session record will be kept"),
    }

    if let Err(e) = librqbit::try_increase_nofile_limit() {
        warn!("could not raise open-file limit (streaming many torrents may hit OS limits): {e:#}");
    }

    info!("starting torrent engine...");
    let engine = TorrentEngine::new(&config).await.inspect_err(|e| {
        audit.record(audit::Event::Problem {
            context: "torrent engine startup".to_string(),
            message: format!("{e:#}"),
        });
    })?;
    engine.attach_audit(audit.clone());

    let cache = CacheManager::new(
        config.downloads_dir(),
        config.max_cache_size_bytes(),
        config.max_cached_torrents,
        config.auto_cleanup,
        Arc::clone(&engine),
    );
    cache.spawn_janitor(Duration::from_secs(config.cleanup_interval_secs));

    // Unconditional, unlike the idle reaper below: this is what hands a
    // finished download's slot to the next title in the queue, and a viewer
    // who turned idle pausing off still wants their episodes to arrive.
    engine.spawn_download_queue(Duration::from_secs(config.idle_check_interval_secs.max(1)));
    info!(
        max_active_downloads = config.max_active_downloads,
        "downloading titles in request order, this many at a time"
    );

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

    audit.record(audit::Event::ServerStart {
        version: env!("CARGO_PKG_VERSION").to_string(),
        http_port: bound_port,
        https_port: (!config.disable_https).then_some(config.https_port),
        cache_dir: config
            .cache_dir
            .canonicalize()
            .unwrap_or_else(|_| config.cache_dir.clone())
            .display()
            .to_string(),
    });

    let state = AppState {
        engine,
        cache,
        stream_base_url,
        indexer,
        tls_host: tls_host.clone(),
        audit: audit.clone(),
    };

    monitoring::spawn_terminal_monitor(
        state.clone(),
        Duration::from_secs(config.monitor_interval_secs),
    );
    let ip_check_engine = state.engine.clone();

    let app = build_router(state);

    // The banner prints *before* the https setup: fetching the certificate
    // can take up to 15 seconds against a slow provider, and holding the
    // whole gateway (and the http address everyone copies) hostage to it
    // made every cold start feel broken. The https task announces the addon
    // URL itself the moment it is actually servable.
    if config.disable_https {
        network::print_banner(lan_ip, bound_port, network::AddonStatus::Disabled);
    } else {
        network::print_banner(lan_ip, bound_port, network::AddonStatus::Preparing);
        spawn_https(&config, lan_ip, client_timeout, app.clone());
    }
    info!(%lan_ip, port = bound_port, "streaming gateway listening");

    // DHCP can hand this machine a new address (most often after sleep/wake),
    // which leaves the addon URL Stremio already has stale. There is no way to
    // rebind the https hostname/cert without a fresh process, so this restarts
    // the gateway to pick it up -- systemd's Restart=always brings it back.
    //
    // Two guards against doing that destructively:
    //   - the new address must be seen on two consecutive checks a minute
    //     apart, so a one-tick flap (a virtual bridge like virbr0 briefly
    //     looking preferred) never fires this; and
    //   - it never fires while a stream is actually open, so a restart never
    //     cuts off someone mid-episode -- it waits for the next quiet check
    //     after they stop instead.
    let ip_check_lan_ip = lan_ip;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        let mut pending: Option<IpAddr> = None;
        loop {
            interval.tick().await;
            let current_ip = network::detect_lan_ip();
            if current_ip == ip_check_lan_ip {
                pending = None;
                continue;
            }
            if pending != Some(current_ip) {
                warn!(
                    from = %ip_check_lan_ip,
                    to = %current_ip,
                    "lan ip looks changed; confirming on the next check before restarting"
                );
                pending = Some(current_ip);
                continue;
            }
            if ip_check_engine.open_stream_count() > 0 {
                debug!(to = %current_ip, "lan ip change confirmed but a stream is open; deferring restart");
                continue;
            }
            // Panic instead of process::exit so the panic hook records it in the audit log.
            panic!(
                "LAN IP changed from {ip_check_lan_ip} to {current_ip} -- exiting to allow systemd to restart and rebind"
            );
        }
    });

    let started = std::time::Instant::now();
    let served = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await;

    // Recorded on both paths: a clean stop and a serve error are the two ways
    // this process ends without panicking, and telling them apart afterwards
    // is most of "why did it stop?".
    match &served {
        Ok(()) => audit.record(audit::Event::ServerStop {
            uptime_secs: started.elapsed().as_secs(),
        }),
        Err(e) => audit.record(audit::Event::Problem {
            context: "http server stopped".to_string(),
            message: format!("{e:#}"),
        }),
    }
    // The writer flushes after draining each batch, so the record above lands
    // within milliseconds -- but it is queued, not written, at this point, and
    // returning from `run` ends the process. This is the margin that keeps the
    // last line of a session (the one saying how the session ended) from being
    // the one line that never makes it to disk.
    std::thread::sleep(Duration::from_millis(200));

    served?;
    Ok(())
}

/// Brings the https listener up in the background -- see the `tls` module for
/// why https exists at all. It serves the same router on its own port, so
/// http works from the first moment either way and a failure here degrades to
/// "no addon URL" rather than "no gateway".
fn spawn_https(
    config: &AppConfig,
    lan_ip: std::net::IpAddr,
    client_timeout: Duration,
    app: Router,
) {
    let config = config.clone();
    tokio::spawn(async move {
        let tls_config = match tls::load(
            config.tls_cert_file.as_deref(),
            config.tls_key_file.as_deref(),
            &config.tls_cert_url,
            &config.tls_key_url,
            &config.cache_dir,
        )
        .await
        {
            Ok(tls_config) => tls_config,
            Err(e) => {
                warn!(
                    "could not start https ({e:#}); Stremio's Android app will not be able to \
                     add this addon without a tunnel"
                );
                return;
            }
        };

        tls::spawn_refresh(
            tls_config.clone(),
            config.tls_cert_url.clone(),
            config.tls_key_url.clone(),
            config.cache_dir.clone(),
        );

        let host = tls::hostname_for(lan_ip, &config.tls_host_suffix);
        let addr = SocketAddr::new(config.bind_addr, config.https_port);

        // Bound here rather than letting `bind_rustls` do it, purely so the
        // same client timeout can be applied to this socket too. Video does
        // travel over https in some setups (a tunnelled client, or VLC simply
        // pointed at this port), and a wedged connection pins its torrent
        // exactly as it would over http.
        let https_listener = match std::net::TcpListener::bind(addr)
            .and_then(|l| l.set_nonblocking(true).map(|()| l))
        {
            Ok(listener) => listener,
            Err(e) => {
                warn!(
                    port = config.https_port,
                    "could not bind the https port ({e}); Stremio's Android app will not be \
                     able to add this addon without a tunnel"
                );
                return;
            }
        };
        network::harden_listener(&https_listener, client_timeout);

        let server = match axum_server::from_tcp_rustls(https_listener, tls_config) {
            Ok(server) => server,
            Err(e) => {
                warn!("could not start the https server: {e:#}");
                return;
            }
        };

        info!(%host, port = config.https_port, "https listening");
        network::print_addon_ready(&format!("https://{host}:{}", config.https_port));

        if let Err(e) = server
            .serve(app.into_make_service_with_connect_info::<SocketAddr>())
            .await
        {
            warn!(
                "https listener stopped: {e:#} -- the addon URL will not work, but http \
                 streaming is unaffected"
            );
        }
    });
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
