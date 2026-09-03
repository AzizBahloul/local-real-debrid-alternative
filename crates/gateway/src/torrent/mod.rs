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

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use librqbit::api::TorrentIdOrHash;
use librqbit::{
    AddTorrent, AddTorrentOptions, Api, DhtSessionConfig, Session, SessionOptions,
    SessionPersistenceConfig,
};
use tokio::sync::{Mutex, Semaphore};
use tracing::{debug, info, warn};

// Cold-start budget. A first request for a torrent that is not running yet
// does three things in sequence, and their sum must stay under the router's
// 60s request timeout -- otherwise the client gets a 504 having waited the
// full minute for nothing, which is strictly worse than a fast failure:
//
//     ADD_TORRENT_TIMEOUT (25s)  fetch metadata from DHT/trackers
//   + INITIALIZE_TIMEOUT  (15s)  leave `Initializing` (hash-check on disk)
//   + prebuffer timeout   (15s)  wait for real bytes (see `streaming`)
//   = 55s worst case
//
/// Bounds the metadata fetch. Unbounded, a torrent with no reachable peers
/// hangs here until the router gives up.
const ADD_TORRENT_TIMEOUT: Duration = Duration::from_secs(25);

/// How long `start_file` waits for a freshly-added torrent to become
/// streamable.
const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(15);

/// How many recently-advertised info hashes stay startable. Generous relative
/// to how many streams a browse session shows (15 per title by default).
const ADVERTISED_HASH_CAPACITY: usize = 512;

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

/// Info hashes this gateway has itself offered to a client, newest last.
///
/// `/videos/<hash>/<idx>` starts a torrent that isn't running yet, which is
/// what lets the addon hand out a short URL. Without a gate, that endpoint
/// would make the gateway join *any* swarm a caller names -- a stranger who
/// can reach the port could use it to download arbitrary content in your
/// name. So a hash is only startable if we advertised it first.
///
/// Bounded so a long-running session cannot grow this without limit; evicting
/// the oldest entry only costs a re-browse to make it playable again.
struct AdvertisedHashes {
    order: VecDeque<String>,
    set: HashSet<String>,
    cap: usize,
}

impl AdvertisedHashes {
    fn new(cap: usize) -> Self {
        Self {
            order: VecDeque::new(),
            set: HashSet::new(),
            cap,
        }
    }

    fn remember(&mut self, hash: String) {
        if self.set.contains(&hash) {
            return;
        }
        if self.order.len() >= self.cap {
            if let Some(oldest) = self.order.pop_front() {
                self.set.remove(&oldest);
            }
        }
        self.set.insert(hash.clone());
        self.order.push_back(hash);
    }

    fn contains(&self, hash: &str) -> bool {
        self.set.contains(hash)
    }
}

pub struct TorrentEngine {
    api: Api,
    /// How an identifier turns into a magnet link. Only `MagnetSource` today;
    /// swapping in a different provider (e.g. resolving a `.torrent` file URL)
    /// only means constructing `TorrentEngine` with a different `Box` here.
    source: Box<dyn TorrentSource>,
    max_concurrent: Semaphore,
    active_streams: Mutex<HashMap<String, StreamActivity>>,
    advertised: Mutex<AdvertisedHashes>,
    prebuffer_bytes: usize,
    prebuffer_timeout: Duration,
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
            advertised: Mutex::new(AdvertisedHashes::new(ADVERTISED_HASH_CAPACITY)),
            prebuffer_bytes: config.prebuffer_bytes,
            prebuffer_timeout: Duration::from_secs(config.prebuffer_timeout_secs),
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

        tokio::time::timeout(
            ADD_TORRENT_TIMEOUT,
            self.api
                .api_add_torrent(AddTorrent::from_url(magnet), Some(opts)),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "timed out fetching torrent metadata after {}s -- the torrent likely has no \
                 reachable peers (try a release with more seeders)",
                ADD_TORRENT_TIMEOUT.as_secs()
            )
        })?
        .context("failed to start torrent")?;

        // Adding a torrent returns before it is streamable: librqbit still has
        // to move it out of `Initializing` (hashing/checking any data already
        // on disk). Returning here would hand the caller a URL that 404s with
        // "invalid state: initializing" for the first second or two -- which a
        // player treats as a dead link rather than retrying, so playback fails
        // outright. Waiting here makes "started" actually mean "playable".
        self.wait_until_streamable(&info_hash, INITIALIZE_TIMEOUT)
            .await?;

        Ok(info_hash)
    }

    /// Makes sure `info_hash` is present in the session and streamable,
    /// starting it from a bare info hash if it isn't.
    ///
    /// This is what lets the Stremio addon hand out a short, clean
    /// `/videos/<hash>/<idx>` URL instead of a long percent-encoded
    /// `/play?magnet=...` one. That matters because Stremio may pass the URL
    /// to an *external* player (VLC) through an Android intent, where a long
    /// URL full of `%` and `&` is far easier to mangle than a plain path.
    /// Returns `Ok(false)` when the torrent is neither running nor something
    /// this gateway advertised -- the caller turns that into a 404 without
    /// ever touching the network. See `AdvertisedHashes` for why.
    pub async fn ensure_started(&self, info_hash: &str, file_idx: usize) -> Result<bool> {
        let idx = TorrentIdOrHash::parse(info_hash)
            .map_err(|e| anyhow::anyhow!("invalid info hash {info_hash}: {e}"))?;

        if let Some(handle) = self.api.session().get(idx) {
            // Resume anything the idle reaper paused. Cheap to check, and
            // without it a torrent you come back to would serve only the
            // bytes already on disk and then stall forever.
            if handle.is_paused() {
                debug!(%info_hash, "resuming paused torrent for a new request");
                self.api
                    .api_torrent_action_start(idx)
                    .await
                    .map_err(|e| anyhow::anyhow!("failed to resume torrent: {e}"))?;
            }
            // It may still be initializing if a player raced us here, so wait
            // rather than returning immediately.
            self.wait_until_streamable(info_hash, INITIALIZE_TIMEOUT)
                .await?;
            return Ok(true);
        }

        if !self.is_advertised(info_hash).await {
            return Ok(false);
        }

        let magnet = resolver::magnet_with_trackers(info_hash, None);
        self.start_file(&magnet, file_idx).await?;
        Ok(true)
    }

    /// Records that we handed this info hash to a client, making it eligible
    /// for lazy start via `ensure_started`.
    pub async fn remember_advertised(&self, info_hash: &str) {
        self.advertised
            .lock()
            .await
            .remember(info_hash.to_ascii_lowercase());
    }

    /// Whether `/videos/<hash>/...` is allowed to *start* this torrent.
    pub async fn is_advertised(&self, info_hash: &str) -> bool {
        self.advertised
            .lock()
            .await
            .contains(&info_hash.to_ascii_lowercase())
    }

    /// Blocks until the torrent has left `Initializing`, or `timeout` elapses.
    ///
    /// A timeout is not treated as fatal: the torrent stays in the session and
    /// keeps initializing, so the caller can still hand out its URL and the
    /// player's own retry will pick it up shortly after.
    pub async fn wait_until_streamable(&self, info_hash: &str, timeout: Duration) -> Result<()> {
        let idx = TorrentIdOrHash::parse(info_hash)
            .map_err(|e| anyhow::anyhow!("invalid info hash {info_hash}: {e}"))?;
        let handle = self
            .api
            .mgr_handle(idx)
            .context("torrent vanished from the session right after being added")?;

        match tokio::time::timeout(timeout, handle.wait_until_initialized()).await {
            Ok(Ok(())) => Ok(()),
            // The torrent itself errored out (bad metadata, storage failure) --
            // that is worth surfacing, since no amount of retrying will help.
            Ok(Err(e)) => Err(e).context("torrent failed while initializing"),
            Err(_) => {
                warn!(
                    %info_hash,
                    "torrent still initializing after {timeout:?}; \
                     handing out its URL anyway, playback may need a retry"
                );
                Ok(())
            }
        }
    }

    pub fn api(&self) -> &Api {
        &self.api
    }

    pub fn prebuffer_bytes(&self) -> usize {
        self.prebuffer_bytes
    }

    pub fn prebuffer_timeout(&self) -> Duration {
        self.prebuffer_timeout
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

    /// Whether this torrent is currently running (present in the session and
    /// not paused).
    ///
    /// This is the authoritative "in use" signal for the cache janitor.
    /// Last-HTTP-request recency is not: a video player reads a large chunk,
    /// buffers several minutes of playback, then goes silent -- so an actively
    /// watched movie looks idle by that measure and gets evicted mid-playback.
    /// Since the idle reaper pauses anything genuinely unwatched, "still
    /// unpaused" is both accurate and self-maintaining.
    pub fn is_running(&self, info_hash: &str) -> bool {
        let Ok(idx) = TorrentIdOrHash::parse(info_hash) else {
            return false;
        };
        self.api
            .session()
            .get(idx)
            .is_some_and(|handle| !handle.is_paused())
    }

    /// Pauses torrents nobody has streamed from in `idle_after`.
    ///
    /// Every running torrent competes for the same finite upstream bandwidth.
    /// A 23 GB 4K release someone opened once and abandoned will happily eat
    /// most of it, starving the movie actually being watched -- which shows up
    /// as buffering that looks like a network problem but is really
    /// self-inflicted. Pausing idle torrents hands that bandwidth back.
    ///
    /// Paused torrents keep their data and resume instantly on the next
    /// request (see `ensure_started`), so this is invisible in normal use.
    /// Completed torrents are left alone: they cost no download bandwidth and
    /// seeding them back is good manners.
    pub async fn pause_idle_torrents(&self, idle_after: Duration) -> usize {
        let candidates: Vec<(TorrentIdOrHash, String)> = self.api.session().with_torrents(|iter| {
            iter.filter_map(|(_, handle)| {
                if handle.is_paused() {
                    return None;
                }
                let stats = handle.stats();
                // Finished: no download bandwidth being consumed.
                if stats.total_bytes > 0 && stats.progress_bytes >= stats.total_bytes {
                    return None;
                }
                let hash = handle.info_hash().as_string();
                Some((handle.info_hash().into(), hash))
            })
            .collect()
        });

        let mut paused = 0;
        for (idx, hash) in candidates {
            if self.is_recently_active(&hash, idle_after).await {
                continue;
            }
            match self.api.api_torrent_action_pause(idx).await {
                Ok(_) => {
                    info!(info_hash = %hash, "paused idle torrent to free bandwidth");
                    paused += 1;
                }
                // Racing a state change here is normal and harmless.
                Err(e) => debug!(info_hash = %hash, "could not pause idle torrent: {e}"),
            }
        }
        paused
    }

    /// Runs `pause_idle_torrents` on a timer for the lifetime of the process.
    pub fn spawn_idle_reaper(self: &Arc<Self>, interval: Duration, idle_after: Duration) {
        let engine = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // The first tick fires immediately; skip it so a torrent started
            // moments before startup is not paused before anyone can play it.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                engine.pause_idle_torrents(idle_after).await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advertised_set_remembers_and_rejects() {
        let mut set = AdvertisedHashes::new(4);
        set.remember("aaaa".into());
        assert!(set.contains("aaaa"));
        assert!(!set.contains("bbbb"));
    }

    #[test]
    fn advertised_set_evicts_oldest_beyond_capacity() {
        let mut set = AdvertisedHashes::new(2);
        set.remember("one".into());
        set.remember("two".into());
        set.remember("three".into());

        assert!(!set.contains("one"), "oldest entry must be evicted");
        assert!(set.contains("two"));
        assert!(set.contains("three"));
        // Bookkeeping must stay consistent, or the set leaks past its cap.
        assert_eq!(set.order.len(), 2);
        assert_eq!(set.set.len(), 2);
    }

    #[test]
    fn advertised_set_does_not_double_count_repeats() {
        // Re-browsing the same title must not push other entries out.
        let mut set = AdvertisedHashes::new(2);
        set.remember("one".into());
        set.remember("one".into());
        set.remember("two".into());

        assert!(set.contains("one"));
        assert!(set.contains("two"));
        assert_eq!(set.order.len(), 2);
    }
}
