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

fn self_process_usage() -> (u64, f32) {
    let pid = Pid::from_u32(std::process::id());
    let mut sys = System::new();
    sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
    sys.process(pid)
        .map(|p| (p.memory(), p.cpu_usage()))
        .unwrap_or((0, 0.0))
}

/// Spawns the periodic terminal status printer (the "Gateway Status" block).
/// Purely informational -- a failure here must never affect streaming.
pub fn spawn_terminal_monitor(state: AppState, interval: Duration) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            print_status(&state).await;
        }
    });
}

async fn print_status(state: &AppState) {
    let torrents = state.engine.list_active();
    let recent = state.engine.recent_streams().await;
    let cache_usage = state.cache.usage_bytes().await;
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

    if recent.is_empty() {
        println!("  Connected clients: none");
    } else {
        println!("  Connected clients:");
        for (hash, ip, secs_ago) in &recent {
            println!("    {ip}  streaming {hash}  (last request {secs_ago}s ago)");
        }
    }
    println!("-------------------------------------------------------------");
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
