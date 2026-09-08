//! `/health` (for scripts/dashboards) and a periodic terminal status printer
//! (for a human watching the gateway run), both reading from the same
//! `TorrentEngine`/`CacheManager` state so the two views never disagree.

use std::net::SocketAddr;
use std::time::Duration;

use axum::extract::{ConnectInfo, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Serialize;
use sysinfo::{Pid, ProcessesToUpdate, System};

use crate::torrent::ActiveTorrentSummary;
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

#[derive(Serialize)]
pub struct ClearResponse {
    deleted: usize,
}

/// `POST /cache/clear` — delete every torrent and all of its data.
///
/// **Loopback only.** Every other route here is deliberately reachable from
/// the whole LAN, because that is the product: a phone streams from the PC.
/// This one destroys data, and the gateway has no authentication of any kind,
/// so exposing it to the WiFi would let any device on the network wipe the
/// cache. The desktop app runs on this machine and calls it over 127.0.0.1;
/// anything else has no business asking.
pub async fn clear_cache(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> Result<Json<ClearResponse>, StatusCode> {
    if !addr.ip().is_loopback() {
        return Err(StatusCode::FORBIDDEN);
    }
    let deleted = state.engine.discard_everything().await;
    Ok(Json(ClearResponse { deleted }))
}

/// `GET /audit/export` — the whole audit log as plain text.
///
/// **Loopback only**, for the same reason as `clear_cache`: the gateway has no
/// authentication, and this file records every client IP that ever connected
/// plus every title they played. That is exactly the sort of thing that must
/// not be readable by anything that can reach the WiFi. The desktop app runs
/// on this machine and asks over 127.0.0.1.
///
/// Served as one response rather than a file path so the GUI does not need to
/// know, or guess, where the server chose to write it — the two processes can
/// disagree about the working directory (see `default_log_dir`).
pub async fn export_audit_log(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> Result<String, StatusCode> {
    if !addr.ip().is_loopback() {
        return Err(StatusCode::FORBIDDEN);
    }

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
    out.push_str(&state.audit.read_all());
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
/// **Loopback only**, for the same reason as `clear_cache`: `delete` destroys
/// data and the gateway has no authentication, so this must not be reachable
/// from the WiFi the phone is on. The desktop app runs on this machine.
///
/// Deliberately one route rather than three: the three actions share their
/// guard, their argument and their response, and splitting them only spreads
/// that agreement across three places to keep in step.
pub async fn torrent_action(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    axum::extract::Path((info_hash, action)): axum::extract::Path<(String, String)>,
) -> Result<Json<TorrentActionResponse>, StatusCode> {
    if !addr.ip().is_loopback() {
        return Err(StatusCode::FORBIDDEN);
    }
    let info_hash = info_hash.to_ascii_lowercase();

    let applied = match action.as_str() {
        "pause" => state.engine.hold_paused(&info_hash).await,
        "resume" => state.engine.release_hold(&info_hash).await,
        // Deletes the torrent *and* its files, which is what "delete the
        // cache for this one" means to whoever pressed it -- pausing already
        // covers "stop, but keep what you have".
        "delete" => state.engine.discard_cached(&info_hash).await,
        _ => return Err(StatusCode::BAD_REQUEST),
    }
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(TorrentActionResponse {
        info_hash,
        action,
        applied,
    }))
}

fn self_process_usage() -> (u64, f32) {
    let pid = Pid::from_u32(std::process::id());
    let mut sys = System::new();
    sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
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
        let mut last = String::new();
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
async fn print_status(state: &AppState, last: &mut String) {
    let torrents = state.engine.list_active();
    let recent = state.engine.recent_streams().await;
    let cache_usage = state.cache.usage_bytes().await;

    let fingerprint = status_fingerprint(&torrents, &recent, cache_usage);
    if fingerprint == *last {
        return;
    }
    *last = fingerprint;

    let (mem_bytes, cpu) = self_process_usage();

    println!("\n----- Gateway Status --------------------------------------");
    println!(
        "  Process: {} RAM, {cpu:.1}% CPU  |  Cache: {} / {}",
        human_bytes(mem_bytes),
        human_bytes(cache_usage),
        human_bytes(state.cache.max_size_bytes()),
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
    let (active, past): (Vec<_>, Vec<_>) =
        recent.iter().partition(|(_, _, secs_ago)| *secs_ago <= window);

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
fn status_fingerprint(
    torrents: &[ActiveTorrentSummary],
    recent: &[(String, std::net::IpAddr, u64)],
    cache_usage: u64,
) -> String {
    let mut out = format!("{cache_usage}|");
    for t in torrents {
        out.push_str(&format!(
            "{}:{}:{}:{};",
            t.info_hash, t.state, t.progress_bytes, t.peers
        ));
    }
    out.push('|');
    for (_, ip, secs_ago) in recent {
        // Bucketed, so the seconds-ago counter ticking up on an idle client
        // does not by itself count as news.
        out.push_str(&format!(
            "{ip}:{};",
            if *secs_ago <= ACTIVE_CLIENT_WINDOW.as_secs() {
                "active"
            } else {
                "past"
            }
        ));
    }
    out
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_human_readable_sizes() {
        assert_eq!(human_bytes(0), "0.0 B");
        assert_eq!(human_bytes(1024), "1.0 KB");
        assert_eq!(human_bytes(1024 * 1024 * 5), "5.0 MB");
    }
}
