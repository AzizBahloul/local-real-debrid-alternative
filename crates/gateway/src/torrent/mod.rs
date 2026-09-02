//! Torrent engine: a thin, typed wrapper around `librqbit`.
//!
//! `librqbit` already implements the hard parts correctly (piece selection
//! that prioritizes the first/last piece of a file then fills in sequentially,
//! a streaming reader that blocks on missing pieces and wakes up as they
//! arrive, DHT/tracker/peer-exchange, fastresume). Reimplementing any of that
//! here would just be a worse copy of a battle-tested implementation, so this
//! module only adds what librqbit does not provide: restricting downloads to
//! the file actually being played, picking which file that is, and tracking
//! who is currently watching what (for the cache janitor).

pub mod resolver;

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use librqbit::{
    AddTorrent, AddTorrentOptions, Api, DhtSessionConfig, Session, SessionOptions,
    SessionPersistenceConfig,
};
use tokio::sync::{Mutex, Semaphore};

use crate::config::AppConfig;
use resolver::{
    info_hash_from_magnet, is_video_file, suggest_video_file, MagnetSource, ResolvedTorrent,
    TorrentFile, TorrentSource,
};

/// Recency of activity on a given torrent's stream, so the cache janitor
/// never evicts something someone is actively watching. HTTP range requests
/// are stateless (a player reconnects for every seek/buffer refill rather
/// than holding one connection open for the whole playback), so "active" is
/// tracked as "seen a request recently" rather than "has an open connection".
#[derive(Debug, Clone)]
pub struct StreamActivity {
    pub last_access: Instant,
    pub last_client: IpAddr,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ActiveTorrentSummary {
    pub info_hash: String,
    pub name: String,
    pub state: String,
    pub progress_percent: f64,
    pub progress_bytes: u64,
    pub total_bytes: u64,
    /// Mebibytes per second (not megabits) -- matches librqbit's own `Speed` unit.
    pub download_speed_mib_s: f64,
    pub upload_speed_mib_s: f64,
    pub peers: u32,
}

pub struct TorrentEngine {
    api: Api,
    /// How an identifier turns into a magnet link. Only `MagnetSource` today;
    /// swapping in a different provider (e.g. resolving a `.torrent` file URL)
    /// only means constructing `TorrentEngine` with a different `Box` here.
    source: Box<dyn TorrentSource>,
    max_concurrent: Semaphore,
    active_streams: Mutex<HashMap<String, StreamActivity>>,
}

impl TorrentEngine {
    pub async fn new(config: &AppConfig) -> Result<Arc<Self>> {
        tokio::fs::create_dir_all(config.downloads_dir())
            .await
            .context("creating downloads directory")?;
        tokio::fs::create_dir_all(config.session_state_dir())
            .await
            .context("creating session state directory")?;

        let dht = if config.disable_dht {
            None
        } else {
            Some(DhtSessionConfig::default())
        };

        let opts = SessionOptions {
            dht,
            fastresume: true,
            persistence: Some(SessionPersistenceConfig::Json {
                folder: Some(config.session_state_dir()),
            }),
            ..Default::default()
        };

        let session = Session::new_with_opts(config.downloads_dir(), opts)
            .await
            .context("failed to start torrent session")?;

        let api = Api::new(session, None);

        Ok(Arc::new(Self {
            api,
            source: Box::new(MagnetSource),
            max_concurrent: Semaphore::new(config.max_concurrent_torrents.max(1)),
            active_streams: Mutex::new(HashMap::new()),
        }))
    }

    /// Fetches torrent metadata only (fast, no data downloaded) and lists its files.
    pub async fn resolve(&self, input: &str) -> Result<ResolvedTorrent> {
        let magnet = self.source.to_magnet(input)?;
        let info_hash = info_hash_from_magnet(&magnet)?;

        let opts = AddTorrentOptions {
            list_only: true,
            ..Default::default()
        };
        let response = self
            .api
            .api_add_torrent(AddTorrent::from_url(magnet.clone()), Some(opts))
            .await
            .context(
                "failed to fetch torrent metadata (no peers found yet, or invalid torrent?)",
            )?;

        let files: Vec<TorrentFile> = response
            .details
            .files
            .unwrap_or_default()
            .into_iter()
            .enumerate()
            .map(|(index, f)| TorrentFile {
                index,
                is_video: is_video_file(&f.name),
                name: f.name,
                length: f.length,
            })
            .collect();

        let suggested_file_idx = suggest_video_file(&files);

        Ok(ResolvedTorrent {
            info_hash,
            name: response.details.name,
            magnet,
            files,
            suggested_file_idx,
        })
    }

    /// Starts (or resumes) downloading a single file from a torrent, restricted
    /// to that file only so the rest of a multi-file torrent is never touched.
    /// Returns the info hash, used to address the torrent for streaming/monitoring.
    pub async fn start_file(&self, input: &str, file_idx: usize) -> Result<String> {
        let magnet = self.source.to_magnet(input)?;
        let info_hash = info_hash_from_magnet(&magnet)?;

        // Throttles how many torrents can be *added* concurrently (each add
        // does a burst of tracker/DHT/peer-handshake work) -- not how many
        // can stream at once, so the permit is dropped as soon as add returns.
        let _permit = self
            .max_concurrent
            .acquire()
            .await
            .context("torrent concurrency limiter closed")?;

        let opts = AddTorrentOptions {
            only_files: Some(vec![file_idx]),
            overwrite: true,
            // Every torrent lands under `downloads/<info_hash>/`, single- or
            // multi-file alike. This gives the cache janitor (see the `cache`
            // module) an unambiguous, collision-free way to map a directory
            // on disk back to the torrent it belongs to.
            sub_folder: Some(info_hash.clone()),
            ..Default::default()
        };

        self.api
            .api_add_torrent(AddTorrent::from_url(magnet), Some(opts))
            .await
            .context("failed to start torrent")?;

        Ok(info_hash)
    }

    pub fn api(&self) -> &Api {
        &self.api
    }

    pub async fn touch_stream(&self, info_hash: &str, client: IpAddr) {
        let mut guard = self.active_streams.lock().await;
        guard.insert(
            info_hash.to_string(),
            StreamActivity {
                last_access: Instant::now(),
                last_client: client,
            },
        );
    }

    /// Whether this torrent has been read from within `within` -- used by the
    /// cache janitor to decide it must not delete the file out from under a
    /// player that is mid-playback (or about to issue its next range request).
    pub async fn is_recently_active(&self, info_hash: &str, within: Duration) -> bool {
        self.active_streams
            .lock()
            .await
            .get(info_hash)
            .map(|a| a.last_access.elapsed() < within)
            .unwrap_or(false)
    }

    /// Snapshot of recently-active streams for the `/health` and terminal
    /// monitoring views: (info_hash, last client IP, seconds since last request).
    pub async fn recent_streams(&self) -> Vec<(String, IpAddr, u64)> {
        self.active_streams
            .lock()
            .await
            .iter()
            .map(|(hash, a)| {
                (
                    hash.clone(),
                    a.last_client,
                    a.last_access.elapsed().as_secs(),
                )
            })
            .collect()
    }

    /// Snapshot of every torrent currently managed by the session, for
    /// terminal monitoring and `/health`.
    pub fn list_active(&self) -> Vec<ActiveTorrentSummary> {
        self.api.session().with_torrents(|iter| {
            iter.map(|(_, handle)| {
                let stats = handle.stats();
                let (download_speed_mib_s, upload_speed_mib_s, peers) = stats
                    .live
                    .as_ref()
                    .map(|l| {
                        (
                            l.download_speed.mbps,
                            l.upload_speed.mbps,
                            l.snapshot.peer_stats.live,
                        )
                    })
                    .unwrap_or((0.0, 0.0, 0));
                let progress_percent = if stats.total_bytes == 0 {
                    0.0
                } else {
                    stats.progress_bytes as f64 / stats.total_bytes as f64 * 100.0
                };
                ActiveTorrentSummary {
                    info_hash: handle.info_hash().as_string(),
                    name: handle.name().unwrap_or_else(|| "unknown".to_string()),
                    state: stats.state.to_string(),
                    progress_percent,
                    progress_bytes: stats.progress_bytes,
                    total_bytes: stats.total_bytes,
                    download_speed_mib_s,
                    upload_speed_mib_s,
                    peers,
                }
            })
            .collect()
        })
    }

    pub fn session_uptime_secs(&self) -> u64 {
        self.api.session().stats_snapshot().uptime_seconds
    }
}
