//! `/health` (for scripts/dashboards) and a periodic terminal status printer
//! (for a human watching the gateway run), both reading from the same
//! `TorrentEngine`/`CacheManager` state so the two views never disagree.

use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::SocketAddr;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use axum::extract::{ConnectInfo, State};
use axum::Json;
use serde::Serialize;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

use crate::error::ApiErrorResponse;
use crate::torrent::ActiveTorrentSummary;
use crate::util::{human_bytes, lock};
use crate::AppState;

#[derive(Serialize)]
pub struct HealthResponse {
    status: &'static str,
    version: &'static str,
    uptime_seconds: u64,
    active_torrents: Vec<ActiveTorrentSummary>,
    active_streams: usize,
    cache_usage_bytes: u64,
    cache_max_bytes: u64,
    process_memory_bytes: u64,
    process_cpu_percent: f32,
    /// The last few measured start/seek waits, oldest first.
    ///
    /// Here rather than only in the log file because "was that slow start the
    /// metadata fetch or the swarm?" is a question asked while the thing is
    /// still slow, by someone who is not going to go and grep a JSONL file on
    /// another machine to find out.
    recent_starts: Vec<crate::audit::StartSample>,
}

pub async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let active_torrents = state.engine.list_active();
    let cache_usage_bytes = state.cache.usage_bytes().await;
    let (process_memory_bytes, process_cpu_percent) = self_process_usage();

    Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
        uptime_seconds: state.engine.session_uptime_secs(),
        // Videos being served right now, not titles touched at some point --
        // see `open_stream_count`.
        active_streams: state.engine.open_stream_count(),
        active_torrents,
        cache_usage_bytes,
        cache_max_bytes: state.cache.max_size_bytes(),
        process_memory_bytes,
        process_cpu_percent,
        recent_starts: state.audit.recent_starts(),
    })
}

/// Refuses anyone not on this machine.
///
/// The gateway has no authentication of any kind, and most of its routes are
/// deliberately reachable from the whole LAN because that is the product: a
/// phone streams from the PC. The routes that call this destroy data or hand
/// out the audit log, and nothing that can reach the WiFi has any business
/// asking for either. The desktop app runs on this machine and calls them over
/// 127.0.0.1.
fn require_loopback(addr: SocketAddr) -> Result<(), ApiErrorResponse> {
    if addr.ip().is_loopback() {
        Ok(())
    } else {
        Err(ApiErrorResponse::forbidden(
            "this route only answers requests from the machine running the gateway",
        ))
    }
}

#[derive(Serialize)]
pub struct ClearResponse {
    deleted: usize,
}

/// `POST /cache/clear` — delete every torrent and all of its data.
///
/// **Loopback only** (see `require_loopback`): exposing it to the WiFi would
/// let any device on the network wipe the cache.
pub async fn clear_cache(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> Result<Json<ClearResponse>, ApiErrorResponse> {
    require_loopback(addr)?;
    let deleted = state.engine.discard_everything().await;
    // The window's cache figure should drop the moment the button is pressed,
    // not a few seconds later when the measured value ages out.
    state.cache.invalidate_usage().await;
    Ok(Json(ClearResponse { deleted }))
}

/// `GET /audit/export` — the whole audit log as plain text.
///
/// **Loopback only** (see `require_loopback`): this file records every client
/// IP that ever connected plus every title they played. That is exactly the
/// sort of thing that must not be readable by anything that can reach the
/// WiFi.
///
/// Served as one response rather than a file path so the GUI does not need to
/// know, or guess, where the server chose to write it — the two processes can
/// disagree about the working directory (see `default_log_dir`).
pub async fn export_audit_log(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> Result<String, ApiErrorResponse> {
    require_loopback(addr)?;

    let path = state
        .audit
        .path()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "(logging is disabled)".to_string());
    let dropped = state.audit.dropped();

    // A header, then the raw JSONL. The header is what makes an exported file
    // self-describing when it arrives as an email attachment with no context,
    // and `dropped` is what stops a gap in the events being read as a quiet
    // period rather than a lost one.
    let mut out = format!(
        "# NovaStream audit log\n\
         # exported: {}\n\
         # version: {}\n\
         # source: {path}\n\
         # events dropped (queue full): {dropped}\n\
         # one JSON object per line; try: grep '\"event\":\"cold_start\"'\n\n",
        chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false),
        env!("CARGO_PKG_VERSION"),
    );
    // Off the async workers: the log and its rotated predecessor are read
    // whole, and that is megabytes of blocking file I/O.
    let audit = state.audit.clone();
    let body = tokio::task::spawn_blocking(move || audit.read_all())
        .await
        .map_err(|e| ApiErrorResponse::internal(format!("could not read the audit log: {e}")))?;
    out.push_str(&body);
    Ok(out)
}

#[derive(Serialize)]
pub struct TorrentActionResponse {
    info_hash: String,
    action: String,
    /// False when the session holds no such torrent — the caller's view of the
    /// swarm list was stale, which is worth saying rather than reporting a
    /// success that changed nothing.
    applied: bool,
}

/// `POST /torrents/{info_hash}/{action}` — pause, resume or delete one title.
///
/// **Loopback only** (see `require_loopback`): `delete` destroys data.
///
/// Deliberately one route rather than three: the three actions share their
/// guard, their argument and their response, and splitting them only spreads
/// that agreement across three places to keep in step.
pub async fn torrent_action(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    axum::extract::Path((info_hash, action)): axum::extract::Path<(String, String)>,
) -> Result<Json<TorrentActionResponse>, ApiErrorResponse> {
    require_loopback(addr)?;
    let info_hash = info_hash.to_ascii_lowercase();

    let applied = match action.as_str() {
        "pause" => state.engine.hold_paused(&info_hash).await,
        "resume" => state.engine.release_hold(&info_hash).await,
        // Deletes the torrent *and* its files, which is what "delete the
        // cache for this one" means to whoever pressed it -- pausing already
        // covers "stop, but keep what you have".
        "delete" => {
            let deleted = state.engine.discard_cached(&info_hash).await;
            state.cache.invalidate_usage().await;
            deleted
        }
        _ => {
            return Err(ApiErrorResponse::bad_request(format!(
                "unknown action {action:?}; expected pause, resume or delete"
            )))
        }
    }
    .map_err(|e| ApiErrorResponse::internal(format!("{action} failed: {e:#}")))?;

    Ok(Json(TorrentActionResponse {
        info_hash,
        action,
        applied,
    }))
}

/// This process's resident memory and CPU share.
///
/// The `System` is kept for the life of the process, because CPU usage is a
/// difference between two samples: a fresh `System` per call has only ever
/// taken one, so the figure it reported was always 0%. Only this one process
/// is refreshed, and only its memory and CPU times.
fn self_process_usage() -> (u64, f32) {
    static SYSTEM: OnceLock<Mutex<System>> = OnceLock::new();
    let pid = Pid::from_u32(std::process::id());
    let mut sys = lock(SYSTEM.get_or_init(|| Mutex::new(System::new())));
    sys.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing().with_memory().with_cpu(),
    );
    sys.process(pid)
        .map(|p| (p.memory(), p.cpu_usage()))
        .unwrap_or((0, 0.0))
}

/// How recently a client must have asked for something to be described as
/// streaming rather than merely remembered.
///
/// The activity map is keyed by torrent and is never cleared while the
/// torrent lives, which is right for the cache janitor -- it wants the last
/// time anything touched those bytes, however long ago. It is wrong for a
/// status block: it listed a phone that stopped watching two hours earlier
/// under "Connected clients", which reads as a live viewer and is the exact
/// thing someone consults this block to find out.
const ACTIVE_CLIENT_WINDOW: Duration = Duration::from_secs(300);

/// Spawns the periodic terminal status printer (the "Gateway Status" block).
/// Purely informational -- a failure here must never affect streaming.
pub fn spawn_terminal_monitor(state: AppState, interval: Duration) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // Fingerprint of the last block printed, so an unchanged one is not
        // printed again. See `print_status`.
        let mut last = None;
        loop {
            ticker.tick().await;
            print_status(&state, &mut last).await;
        }
    });
}

/// Prints the status block, but only when something in it actually moved.
///
/// In always-on mode this goes to the journal, and at the default five-second
/// interval an idle gateway wrote a seventeen-line block twelve times a
/// minute forever. The cost is not the disk, it is that every real event --
/// an eviction, a failed start, the shutdown -- ends up buried under
/// thousands of identical blocks, so the log cannot be read at the moment it
/// is actually needed.
///
/// The fingerprint deliberately covers `progress_bytes` and peer counts, so
/// this stays fully verbose while anything is downloading (those change every
/// tick) and goes quiet only when the gateway is genuinely doing nothing.
async fn print_status(state: &AppState, last: &mut Option<u64>) {
    let torrents = state.engine.list_active();
    let recent = state.engine.recent_streams();
    let cache_usage = state.cache.usage_bytes().await;

    let fingerprint = status_fingerprint(&torrents, &recent, cache_usage);
    if *last == Some(fingerprint) {
        return;
    }
    *last = Some(fingerprint);

    let (mem_bytes, cpu) = self_process_usage();

    println!("\n----- Gateway Status --------------------------------------");
    println!(
        "  Process: {} RAM, {cpu:.1}% CPU  |  Cache: {} / {}",
        human_bytes(mem_bytes, 1),
        human_bytes(cache_usage, 1),
        human_bytes(state.cache.max_size_bytes(), 1),
    );

    if torrents.is_empty() {
        println!("  Active torrents: none");
    } else {
        println!("  Active torrents:");
        for t in &torrents {
            println!("    {} ({})", t.name, t.info_hash);
            println!(
                "      progress: {:>5.1}%   down: {:>6.2} MiB/s   up: {:>6.2} MiB/s   peers: {:<3}  state: {}",
                t.progress_percent, t.download_speed_mib_s, t.upload_speed_mib_s, t.peers, t.state
            );
        }
    }

    let window = ACTIVE_CLIENT_WINDOW.as_secs();
    let (active, past): (Vec<_>, Vec<_>) = recent
        .iter()
        .partition(|(_, _, secs_ago)| *secs_ago <= window);

    if active.is_empty() {
        println!("  Streaming now: none");
    } else {
        println!("  Streaming now:");
        for (hash, ip, secs_ago) in &active {
            println!("    {ip}  streaming {hash}  (last request {secs_ago}s ago)");
        }
    }
    // Kept, but under a heading that does not claim they are watching. These
    // are what the cache janitor is ordering its retention by, so seeing them
    // explains an eviction; calling them connected explains nothing.
    if !past.is_empty() {
        println!("  Seen earlier:");
        for (hash, ip, secs_ago) in &past {
            println!("    {ip}  last read {hash}  ({}m ago)", secs_ago / 60);
        }
    }
    println!("-------------------------------------------------------------");
}

/// What has to change before the block is worth printing again.
///
/// Speeds are excluded on purpose: they jitter by a few hundred bytes on an
/// otherwise idle torrent, which would defeat the whole suppression. Progress
/// and peer count cover everything that is actually happening.
///
/// A hash rather than a string built from every field, since it is only ever
/// compared with the previous one; the chance of two different blocks
/// colliding is one missed status print.
fn status_fingerprint(
    torrents: &[ActiveTorrentSummary],
    recent: &[(String, std::net::IpAddr, u64)],
    cache_usage: u64,
) -> u64 {
    let mut hasher = DefaultHasher::new();
    cache_usage.hash(&mut hasher);
    for t in torrents {
        (&t.info_hash, &t.state, t.progress_bytes, t.peers).hash(&mut hasher);
    }
    // The activity map is a `HashMap`, so its order is not stable between
    // two calls; sorted, or an unchanged map would read as news.
    let mut clients: Vec<(std::net::IpAddr, bool)> = recent
        .iter()
        // Bucketed, so the seconds-ago counter ticking up on an idle client
        // does not by itself count as news.
        .map(|(_, ip, secs_ago)| (*ip, *secs_ago <= ACTIVE_CLIENT_WINDOW.as_secs()))
        .collect();
    clients.sort_unstable();
    clients.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn torrent(progress_bytes: u64, peers: u32) -> ActiveTorrentSummary {
        ActiveTorrentSummary {
            info_hash: "aaaa".to_string(),
            name: "movie".to_string(),
            state: "live".to_string(),
            progress_percent: 0.0,
            progress_bytes,
            total_bytes: 100,
            download_speed_mib_s: 1.5,
            upload_speed_mib_s: 0.25,
            peers,
            held: false,
        }
    }

    #[test]
    fn an_unchanged_gateway_prints_its_status_once() {
        let ip: std::net::IpAddr = "192.168.1.5".parse().unwrap();
        let first = status_fingerprint(&[torrent(10, 3)], &[("aaaa".into(), ip, 4)], 7);
        let mut jittered = torrent(10, 3);
        jittered.download_speed_mib_s = 1.49;
        // Seconds-ago ticking up inside the same bucket is not news either.
        let again = status_fingerprint(&[jittered], &[("aaaa".into(), ip, 9)], 7);
        assert_eq!(first, again);
    }

    #[test]
    fn progress_peers_and_cache_usage_are_news() {
        let base = status_fingerprint(&[torrent(10, 3)], &[], 7);
        assert_ne!(base, status_fingerprint(&[torrent(11, 3)], &[], 7));
        assert_ne!(base, status_fingerprint(&[torrent(10, 4)], &[], 7));
        assert_ne!(base, status_fingerprint(&[torrent(10, 3)], &[], 8));
    }

    #[test]
    fn a_client_going_quiet_is_news() {
        let ip: std::net::IpAddr = "192.168.1.5".parse().unwrap();
        let watching = status_fingerprint(&[], &[("aaaa".into(), ip, 10)], 0);
        let gone = status_fingerprint(&[], &[("aaaa".into(), ip, 10_000)], 0);
        assert_ne!(watching, gone);
    }

    #[test]
    fn remote_callers_get_a_json_refusal() {
        let remote: SocketAddr = "192.168.1.5:5000".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:5000".parse().unwrap();
        assert!(require_loopback(local).is_ok());
        let refused = require_loopback(remote).unwrap_err();
        assert_eq!(refused.status, axum::http::StatusCode::FORBIDDEN);
    }
}
