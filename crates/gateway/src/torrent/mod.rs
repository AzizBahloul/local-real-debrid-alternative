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

use std::collections::{HashMap, VecDeque};
use std::io::SeekFrom;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use librqbit::api::TorrentIdOrHash;
use librqbit::limits::LimitsConfig;
use librqbit::{
    AddTorrent, AddTorrentOptions, Api, DhtSessionConfig, Session, SessionOptions,
    SessionPersistenceConfig,
};
use tokio::io::{AsyncRead, AsyncSeekExt};
use tokio::sync::{Mutex, Semaphore};
use tracing::{debug, info, warn};

/// A boxed read side of a torrent file. `librqbit::FileStream` lives in a
/// private module and cannot be named from outside the crate, so it is boxed
/// behind the trait it implements. Seeking is done before boxing.
pub type BoxedReader = Box<dyn AsyncRead + Send + Unpin>;

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

/// Cap on how many already-known torrents a single browse will wake up. A
/// title's stream list can name a dozen releases we have partial data for;
/// resuming all of them would have them compete for the same upstream
/// bandwidth, which is the exact problem the idle reaper exists to prevent.
const MAX_WARM_ON_BROWSE: usize = 3;

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

/// One advertised info hash and the trackers the index reported for exactly
/// that release. Persisted so both survive a restart.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct AdvertisedEntry {
    hash: String,
    #[serde(default)]
    trackers: Vec<String>,
}

/// Info hashes this gateway has itself offered to a client, newest last.
///
/// `/videos/<hash>/<idx>` starts a torrent that isn't running yet, which is
/// what lets the addon hand out a short URL. Without a gate, that endpoint
/// would make the gateway join *any* swarm a caller names -- a stranger who
/// can reach the port could use it to download arbitrary content in your
/// name. So a hash is only startable if we advertised it first.
///
/// Each hash also remembers the trackers the index reported for that
/// specific release. A lazy start otherwise begins from a bare info hash
/// with nothing but DHT and the generic default trackers to find peers on --
/// the release's own trackers are where its seeders actually announce, so
/// carrying them to the start call is a large part of "press play, get
/// bytes" being fast the first time.
///
/// Bounded so a long-running session cannot grow this without limit; evicting
/// the oldest entry only costs a re-browse to make it playable again.
struct AdvertisedHashes {
    order: VecDeque<String>,
    trackers: HashMap<String, Vec<String>>,
    cap: usize,
}

impl AdvertisedHashes {
    fn new(cap: usize) -> Self {
        Self {
            order: VecDeque::new(),
            trackers: HashMap::new(),
            cap,
        }
    }

    /// Rebuilds the set from what was on disk, newest last, honouring the cap.
    fn seed(cap: usize, entries: Vec<AdvertisedEntry>) -> Self {
        let mut set = Self::new(cap);
        for entry in entries {
            set.remember(entry.hash, entry.trackers);
        }
        set
    }

    fn snapshot(&self) -> Vec<AdvertisedEntry> {
        self.order
            .iter()
            .map(|hash| AdvertisedEntry {
                hash: hash.clone(),
                trackers: self.trackers.get(hash).cloned().unwrap_or_default(),
            })
            .collect()
    }

    fn remember(&mut self, hash: String, trackers: Vec<String>) {
        if let Some(known) = self.trackers.get_mut(&hash) {
            // Re-advertised: keep the entry, but adopt trackers if this
            // sighting knows some and the stored one does not (a legacy
            // entry, or a magnet-path advert that carried none).
            if known.is_empty() && !trackers.is_empty() {
                *known = trackers;
            }
            return;
        }
        if self.order.len() >= self.cap {
            if let Some(oldest) = self.order.pop_front() {
                self.trackers.remove(&oldest);
            }
        }
        self.trackers.insert(hash.clone(), trackers);
        self.order.push_back(hash);
    }

    fn contains(&self, hash: &str) -> bool {
        self.trackers.contains_key(hash)
    }

    fn trackers_for(&self, hash: &str) -> Vec<String> {
        self.trackers.get(hash).cloned().unwrap_or_default()
    }
}

/// Decodes a persisted advertised set, accepting both the current format
/// (entries with trackers) and the pre-0.9 one (a bare list of hashes).
/// A file written by the previous version must keep every already-installed
/// Stremio link startable, not silently reset the whole set.
fn parse_advertised(bytes: &[u8]) -> Vec<AdvertisedEntry> {
    if let Ok(entries) = serde_json::from_slice::<Vec<AdvertisedEntry>>(bytes) {
        return entries;
    }
    serde_json::from_slice::<Vec<String>>(bytes)
        .map(|hashes| {
            hashes
                .into_iter()
                .map(|hash| AdvertisedEntry {
                    hash,
                    trackers: Vec::new(),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Marks a torrent as "being read right now", for exactly as long as the HTTP
/// response body that owns it is alive.
///
/// The idle reaper must never pause a torrent that has a live reader.
/// librqbit's reader parks on `Poll::Pending` waiting for one specific piece
/// and is only ever woken by *that piece completing*; pausing does not wake
/// it, and there is no timeout. So pausing under a live reader freezes it
/// permanently -- the video stops dead and only a fresh HTTP request (what a
/// viewer does by hand when they skip forward) recovers it.
///
/// Request recency cannot stand in for this. A player fills its buffer, then
/// reads nothing for minutes while it plays what it already holds, which is
/// indistinguishable from an abandoned torrent by that measure.
pub struct StreamGuard {
    engine: Arc<TorrentEngine>,
    info_hash: String,
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        self.engine.open_streams().release(&self.info_hash);
    }
}

/// How many response bodies are reading each torrent right now.
///
/// Reference-counted rather than a flag because one player legitimately holds
/// several reads at once -- a container header, an index at the end of the
/// file, and the playback position -- and the torrent must stay protected
/// until the last of them is done, not the first.
#[derive(Default)]
struct OpenStreamCounts(HashMap<String, usize>);

impl OpenStreamCounts {
    fn acquire(&mut self, info_hash: &str) {
        *self.0.entry(info_hash.to_string()).or_insert(0) += 1;
    }

    fn release(&mut self, info_hash: &str) {
        if let Some(count) = self.0.get_mut(info_hash) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                // Removed rather than left at zero so the map tracks live
                // readers only, and cannot grow for the life of the process.
                self.0.remove(info_hash);
            }
        }
    }

    fn is_open(&self, info_hash: &str) -> bool {
        self.0.get(info_hash).is_some_and(|count| *count > 0)
    }

    /// How many distinct torrents have a live reader. Torrents rather than
    /// readers because one player routinely holds several reads on the same
    /// file at once (a container header, an index near the end, the playback
    /// position), and reporting "3 active streams" for one viewer watching one
    /// episode describes the implementation rather than what is happening.
    fn torrents_open(&self) -> usize {
        self.0.values().filter(|count| **count > 0).count()
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
    /// Where the advertised set is kept between runs.
    advertised_path: PathBuf,
    /// Which torrents have a response body reading them right now. A plain
    /// `std` mutex because `StreamGuard::drop` cannot await, and every
    /// critical section here is a single map lookup.
    open_streams: StdMutex<OpenStreamCounts>,
    /// The torrent the viewer is watching *now*.
    ///
    /// Every other unfinished torrent is paused the moment this changes, so
    /// the whole line goes to the thing on screen. Without it the previous
    /// title kept downloading until the idle reaper noticed, which is up to
    /// `idle_pause_secs` (five minutes) of a finished-with title competing
    /// with the one that just started.
    focused: StdMutex<Option<String>>,
    prebuffer_bytes: usize,
    prebuffer_timeout: Duration,
    stall_timeout: Duration,
}

/// What to do with a torrent that is not the one being watched.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum UnfocusedAction {
    /// Leave it exactly as it is.
    Leave,
    /// Stop downloading and delete the partial file.
    Discard,
}

/// What should happen to the title the viewer just switched away from.
///
/// Starting a different title means that one has been abandoned — nobody
/// changes episode intending to come back to a half-downloaded file — so its
/// partial data is dead weight in a cache that sits permanently at its cap.
/// It is deleted rather than paused, which is the only thing here that
/// actually frees space.
///
/// This applies to *the previous focus only*, never to the whole session. An
/// earlier version swept every unfinished torrent on each switch, which wiped
/// eight queued titles the first time anything was played — the viewer's
/// backlog is not the same thing as the title they just left.
///
/// Two exemptions, and they are not stylistic:
///
/// * an **open stream** means some response body is still reading those bytes
///   (a second device, or this player's own header/index reads). Deleting
///   underneath it truncates a video someone is watching — see `StreamGuard`.
/// * a **finished** torrent is a complete file. It costs no download
///   bandwidth, and throwing away a fully-downloaded movie because the viewer
///   started the next episode is destructive in a way nobody asks for. The
///   cache janitor already reclaims those, oldest-first, once the cap forces
///   it.
fn action_for_abandoned(has_open_stream: bool, finished: bool) -> UnfocusedAction {
    if has_open_stream || finished {
        return UnfocusedAction::Leave;
    }
    UnfocusedAction::Discard
}

/// Turns a megabytes-per-second setting into the bytes-per-second rate limiter
/// librqbit wants, with 0 (and anything that will not fit) meaning "no limit".
fn bytes_per_second(mb_per_s: u64) -> Option<std::num::NonZeroU32> {
    u32::try_from(mb_per_s.saturating_mul(1024 * 1024))
        .ok()
        .and_then(std::num::NonZeroU32::new)
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
            // Both of these are left at librqbit's defaults by most callers,
            // and both of those defaults are wrong for this gateway --
            // see the field docs on `AppConfig` for the reasoning. In short:
            // a torrent client wants to finish a download, while this wants to
            // keep one video flowing over a link it is sharing with the
            // torrent traffic itself.
            peer_limit: Some(config.max_peers_per_torrent.max(1)),
            ratelimits: LimitsConfig {
                upload_bps: bytes_per_second(config.max_upload_mb_s),
                download_bps: bytes_per_second(config.max_download_mb_s),
            },
            // Announced for every torrent, metadata fetch included. This is
            // what makes a cold start fast on networks where DHT is slow or
            // blocked: a magnet add only reads `tr=` params out of the magnet
            // itself, but these session-wide trackers are merged into every
            // torrent's peer discovery regardless of how it was added.
            trackers: resolver::DEFAULT_TRACKERS
                .iter()
                .filter_map(|t| url::Url::parse(t).ok())
                .collect(),
            ..Default::default()
        };

        let session = Session::new_with_opts(config.downloads_dir(), opts)
            .await
            .context("failed to start torrent session")?;

        let api = Api::new(session, None);

        let advertised_path = config.session_state_dir().join("advertised.json");
        let remembered_advertised = Self::load_advertised(&advertised_path).await;
        if !remembered_advertised.is_empty() {
            info!(
                count = remembered_advertised.len(),
                "restored advertised info hashes, so links already in a client keep working"
            );
        }

        Ok(Arc::new(Self {
            api,
            source: Box::new(MagnetSource),
            max_concurrent: Semaphore::new(config.max_concurrent_torrents.max(1)),
            active_streams: Mutex::new(HashMap::new()),
            advertised: Mutex::new(AdvertisedHashes::seed(
                ADVERTISED_HASH_CAPACITY,
                remembered_advertised,
            )),
            advertised_path,
            open_streams: StdMutex::new(OpenStreamCounts::default()),
            focused: StdMutex::new(None),
            prebuffer_bytes: config.prebuffer_bytes,
            prebuffer_timeout: Duration::from_secs(config.prebuffer_timeout_secs),
            stall_timeout: Duration::from_secs(config.stall_timeout_secs),
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
            seen_peers: response.seen_peers.unwrap_or_default(),
        })
    }

    /// Starts (or resumes) downloading a single file from a torrent, restricted
    /// to that file only so the rest of a multi-file torrent is never touched.
    /// Returns the info hash, used to address the torrent for streaming/monitoring.
    ///
    /// `initial_peers` seeds the swarm with peers already known to be alive
    /// (typically the ones a preceding metadata resolve just talked to), so
    /// the download connects immediately instead of re-running the same
    /// DHT/tracker discovery a second time. Empty means "discover normally".
    pub async fn start_file(
        &self,
        input: &str,
        file_idx: usize,
        initial_peers: Vec<std::net::SocketAddr>,
    ) -> Result<String> {
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
            initial_peers: (!initial_peers.is_empty()).then_some(initial_peers),
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

        // The release's own trackers (remembered from the index) travel in
        // the magnet -- a magnet add reads trackers only from the URI itself.
        // The generic defaults are announced session-wide and need no ride.
        let trackers = self.advertised_trackers(info_hash).await;
        let magnet = resolver::magnet_with_trackers(info_hash, None, &trackers);
        self.start_file(&magnet, file_idx, Vec::new()).await?;
        Ok(true)
    }

    /// Records that we handed this info hash to a client, making it eligible
    /// for lazy start via `ensure_started` -- along with the trackers the
    /// index reported for this specific release, so that lazy start announces
    /// where this torrent's seeders actually are (see `AdvertisedHashes`).
    ///
    /// Persisted immediately. The set used to live only in memory, so every
    /// restart invalidated every link already sitting in a Stremio client:
    /// the phone kept requesting a perfectly good hash and kept getting
    /// "this gateway has not offered that info hash", with no way to tell
    /// that re-picking the stream would fix it.
    pub async fn remember_advertised(&self, info_hash: &str, trackers: &[String]) {
        let snapshot = {
            let mut advertised = self.advertised.lock().await;
            advertised.remember(info_hash.to_ascii_lowercase(), trackers.to_vec());
            advertised.snapshot()
        };
        self.save_advertised(snapshot).await;
    }

    /// The trackers remembered for an advertised hash (empty when none were
    /// ever reported for it).
    pub async fn advertised_trackers(&self, info_hash: &str) -> Vec<String> {
        self.advertised
            .lock()
            .await
            .trackers_for(&info_hash.to_ascii_lowercase())
    }

    /// Writes the advertised set out. Best effort: failing to persist costs a
    /// re-pick after the next restart, which is not worth failing a request.
    async fn save_advertised(&self, entries: Vec<AdvertisedEntry>) {
        let path = self.advertised_path.clone();
        if let Some(parent) = path.parent() {
            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                debug!("could not create state dir for advertised hashes: {e}");
                return;
            }
        }
        match serde_json::to_vec(&entries) {
            Ok(bytes) => {
                if let Err(e) = tokio::fs::write(&path, bytes).await {
                    debug!("could not persist advertised hashes: {e}");
                }
            }
            Err(e) => debug!("could not encode advertised hashes: {e}"),
        }
    }

    /// Reads the advertised set back, or an empty list if it is absent or
    /// unreadable — a corrupt file must not stop the gateway from starting.
    async fn load_advertised(path: &Path) -> Vec<AdvertisedEntry> {
        let Ok(bytes) = tokio::fs::read(path).await else {
            return Vec::new();
        };
        parse_advertised(&bytes)
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

    pub fn stall_timeout(&self) -> Duration {
        self.stall_timeout
    }

    /// A lock poisoned by a panic elsewhere still holds a perfectly usable
    /// map -- refusing to serve video over it would be a worse outcome than
    /// the inconsistency it guards against.
    fn open_streams(&self) -> std::sync::MutexGuard<'_, OpenStreamCounts> {
        self.open_streams
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Registers a live reader on `info_hash`. Hold the returned guard for as
    /// long as the response body exists. See `StreamGuard`.
    pub fn open_stream_guard(self: &Arc<Self>, info_hash: &str) -> StreamGuard {
        self.open_streams().acquire(info_hash);
        StreamGuard {
            engine: Arc::clone(self),
            info_hash: info_hash.to_string(),
        }
    }

    /// Whether any response body is currently reading this torrent.
    pub fn has_open_stream(&self, info_hash: &str) -> bool {
        self.open_streams().is_open(info_hash)
    }

    /// How many torrents are being read right now -- i.e. how many videos this
    /// gateway is actually serving.
    ///
    /// This is what `/health` reports as `active_streams`. The obvious
    /// alternative, the size of the `active_streams` activity map, counts
    /// every torrent touched since the last clear and never shrinks, so after
    /// three episodes it claims three streams with one player connected.
    pub fn open_stream_count(&self) -> usize {
        self.open_streams().torrents_open()
    }

    /// Declares `info_hash` the thing being watched, discarding what was
    /// being watched before it.
    ///
    /// Called on every stream open, but only does work when the focus
    /// actually moves — a seek re-opens the reader on the same torrent dozens
    /// of times and must not re-sweep each time.
    ///
    /// The sweep runs detached: it makes one API call per torrent and the
    /// viewer's first byte should not wait on any of them.
    pub fn focus_stream(self: &Arc<Self>, info_hash: &str) {
        let previous = {
            let mut focused = self
                .focused
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if focused.as_deref() == Some(info_hash) {
                return;
            }
            focused.replace(info_hash.to_string())
        };

        // Nothing was playing before, so nothing has been abandoned. This is
        // what keeps the first play of a session from touching the backlog.
        let Some(previous) = previous else {
            return;
        };

        let engine = Arc::clone(self);
        let focus = info_hash.to_string();
        tokio::spawn(async move {
            if engine.discard_abandoned(&previous).await {
                info!(
                    focused = %focus,
                    abandoned = %previous,
                    "switched title: dropped the one left behind and freed its partial data"
                );
            }
        });
    }

    /// Deletes the abandoned torrent and its partial data. Returns whether it
    /// actually went.
    ///
    /// Uses librqbit's delete (torrent *and* files) rather than forget: a
    /// forgotten torrent leaves its partial file on disk with nothing
    /// tracking it, which is exactly the orphaned bulk the cache cap is
    /// already fighting.
    pub async fn discard_abandoned(&self, info_hash: &str) -> bool {
        let matches: Vec<(TorrentIdOrHash, bool)> = self.api.session().with_torrents(|iter| {
            iter.filter_map(|(_, handle)| {
                if handle.info_hash().as_string() != info_hash {
                    return None;
                }
                let stats = handle.stats();
                let finished = stats.total_bytes > 0 && stats.progress_bytes >= stats.total_bytes;
                Some((handle.info_hash().into(), finished))
            })
            .collect()
        });

        let Some((idx, finished)) = matches.into_iter().next() else {
            return false;
        };
        if action_for_abandoned(self.has_open_stream(info_hash), finished)
            != UnfocusedAction::Discard
        {
            return false;
        }

        match self.api.api_torrent_action_delete(idx).await {
            Ok(_) => {
                info!(info_hash = %info_hash, "discarded abandoned torrent and its partial data");
                self.forget_activity(info_hash).await;
                true
            }
            // Racing a state change here is normal and harmless.
            Err(e) => {
                debug!(info_hash = %info_hash, "could not discard abandoned torrent: {e}");
                false
            }
        }
    }

    /// Deletes every torrent and all of its data. Returns how many went.
    ///
    /// Unlike the automatic paths this spares nothing — not a finished
    /// download, not one being streamed right now. It only ever runs from an
    /// explicit "clear everything" click, where second-guessing the operator
    /// would be the surprising behaviour. A stream in flight dies with it,
    /// which is the honest consequence of wiping the file underneath it.
    pub async fn discard_everything(&self) -> usize {
        let all: Vec<(TorrentIdOrHash, String)> = self.api.session().with_torrents(|iter| {
            iter.map(|(_, handle)| {
                let hash = handle.info_hash().as_string();
                (handle.info_hash().into(), hash)
            })
            .collect()
        });

        let mut deleted = 0;
        for (idx, hash) in all {
            match self.api.api_torrent_action_delete(idx).await {
                Ok(_) => {
                    self.forget_activity(&hash).await;
                    deleted += 1;
                }
                Err(e) => warn!(info_hash = %hash, "could not delete torrent: {e}"),
            }
        }

        // Nothing is playing any more, so the next stream is a first play and
        // must not be treated as a switch away from a torrent that is gone.
        *self
            .focused
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;

        info!(deleted, "cleared every torrent and its data on request");
        deleted
    }

    /// Drops the bookkeeping for a torrent that no longer exists, so a later
    /// request for the same title starts clean instead of matching a stale
    /// activity record.
    async fn forget_activity(&self, info_hash: &str) {
        self.active_streams.lock().await.remove(info_hash);
    }

    /// Length of one file inside a torrent, without opening a read stream.
    pub fn file_length(&self, info_hash: &str, file_idx: usize) -> Result<u64> {
        let idx = TorrentIdOrHash::parse(info_hash)
            .map_err(|e| anyhow::anyhow!("invalid info hash {info_hash}: {e}"))?;
        let details = self
            .api
            .api_torrent_details(idx)
            .map_err(|e| anyhow::anyhow!("torrent details unavailable: {e}"))?;
        details
            .files
            .unwrap_or_default()
            .get(file_idx)
            .map(|f| f.length)
            .context("fileIdx out of range for this torrent")
    }

    /// Opens a fresh read stream positioned at `pos`, resuming the torrent
    /// first if the reaper paused it.
    ///
    /// Opening a stream is also the only thing that makes librqbit look for
    /// peers holding this particular file, and the only thing that puts the
    /// new read position into its piece-priority set. That makes re-opening
    /// the correct response to a stream that has stopped producing bytes:
    /// it is not merely a retry, it actively re-drives peer discovery and
    /// re-points the download at where the viewer actually is.
    pub async fn open_stream_at(
        &self,
        info_hash: &str,
        file_idx: usize,
        pos: u64,
    ) -> Result<BoxedReader> {
        let idx = TorrentIdOrHash::parse(info_hash)
            .map_err(|e| anyhow::anyhow!("invalid info hash {info_hash}: {e}"))?;

        if !self.ensure_started(info_hash, file_idx).await? {
            anyhow::bail!("torrent {info_hash} is no longer startable");
        }

        let mut stream = self
            .api
            .api_stream(idx, file_idx)
            .await
            .map_err(|e| anyhow::anyhow!("could not open torrent stream: {e}"))?;

        if pos > 0 {
            stream
                .seek(SeekFrom::Start(pos))
                .await
                .with_context(|| format!("seeking to byte {pos}"))?;
        }

        Ok(Box::new(stream))
    }

    /// Resumes torrents we already have partial data for, so they are already
    /// connected to peers by the time the viewer picks one.
    ///
    /// Called when Stremio lists the streams for a title. Resuming a paused
    /// torrent drops it back to zero peers and it must rediscover them over
    /// DHT/trackers, which is most of what "it took 30 seconds to start"
    /// actually is. Doing that while the viewer is still reading the stream
    /// list spends that time where nobody is waiting on it.
    ///
    /// Only touches torrents already in the session, so browsing never joins
    /// a swarm on its own.
    pub async fn warm_known_torrents(&self, info_hashes: &[String]) {
        let mut warmed = 0usize;
        for hash in info_hashes {
            if warmed >= MAX_WARM_ON_BROWSE {
                break;
            }
            let Ok(idx) = TorrentIdOrHash::parse(hash) else {
                continue;
            };
            let Some(handle) = self.api.session().get(idx) else {
                continue; // never played -- nothing to warm
            };
            if !handle.is_paused() {
                continue;
            }
            let stats = handle.stats();
            // Finished torrents need no peers, and ones with no data yet gain
            // nothing from a head start they would only spend on metadata.
            if stats.progress_bytes == 0 || stats.progress_bytes >= stats.total_bytes {
                continue;
            }
            match self.api.api_torrent_action_start(idx).await {
                Ok(_) => {
                    debug!(info_hash = %hash, "pre-warming previously watched torrent");
                    warmed += 1;
                }
                Err(e) => debug!(info_hash = %hash, "could not pre-warm: {e}"),
            }
        }
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
            // Two independent checks, because request recency alone is not
            // enough. A live reader is parked inside librqbit waiting for a
            // piece and issues no HTTP requests at all; pausing it there
            // freezes it permanently with no timeout. See `StreamGuard`.
            if self.has_open_stream(&hash) || self.is_recently_active(&hash, idle_after).await {
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
        set.remember("aaaa".into(), Vec::new());
        assert!(set.contains("aaaa"));
        assert!(!set.contains("bbbb"));
    }

    #[test]
    fn advertised_set_evicts_oldest_beyond_capacity() {
        let mut set = AdvertisedHashes::new(2);
        set.remember("one".into(), Vec::new());
        set.remember("two".into(), Vec::new());
        set.remember("three".into(), Vec::new());

        assert!(!set.contains("one"), "oldest entry must be evicted");
        assert!(set.contains("two"));
        assert!(set.contains("three"));
        // Bookkeeping must stay consistent, or the set leaks past its cap.
        assert_eq!(set.order.len(), 2);
        assert_eq!(set.trackers.len(), 2);
    }

    #[test]
    fn advertised_set_keeps_trackers_per_hash() {
        let mut set = AdvertisedHashes::new(4);
        set.remember(
            "aaaa".into(),
            vec!["udp://a.example:1337/announce".into()],
        );
        set.remember("bbbb".into(), Vec::new());

        assert_eq!(
            set.trackers_for("aaaa"),
            vec!["udp://a.example:1337/announce".to_string()],
            "a lazy start must announce where this release's seeders are"
        );
        assert!(set.trackers_for("bbbb").is_empty());
        assert!(set.trackers_for("unknown").is_empty());
    }

    #[test]
    fn re_advertising_upgrades_a_trackerless_entry_but_never_downgrades() {
        let mut set = AdvertisedHashes::new(4);
        set.remember("aaaa".into(), Vec::new());
        set.remember("aaaa".into(), vec!["udp://a.example:1337/announce".into()]);
        assert_eq!(
            set.trackers_for("aaaa"),
            vec!["udp://a.example:1337/announce".to_string()],
            "a later sighting that knows trackers must fill in an empty entry"
        );

        // A later sighting with none must not wipe what is known.
        set.remember("aaaa".into(), Vec::new());
        assert!(!set.trackers_for("aaaa").is_empty());
    }

    /// A pre-0.9 `advertised.json` is a bare list of hashes. It must load as
    /// entries (with no trackers) rather than parse-fail into an empty set --
    /// an empty set silently 404s every link already installed in Stremio.
    #[test]
    fn legacy_advertised_file_still_loads() {
        let legacy = br#"["aaaa","bbbb"]"#;
        let entries = parse_advertised(legacy);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].hash, "aaaa");
        assert!(entries[0].trackers.is_empty());

        let current = br#"[{"hash":"cccc","trackers":["udp://t.example:80/announce"]}]"#;
        let entries = parse_advertised(current);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].trackers.len(), 1);

        assert!(parse_advertised(b"not json").is_empty());
    }

    #[test]
    fn open_stream_counts_track_a_single_reader() {
        let mut counts = OpenStreamCounts::default();
        assert!(!counts.is_open("aaaa"));

        counts.acquire("aaaa");
        assert!(
            counts.is_open("aaaa"),
            "a live reader must protect its torrent"
        );

        counts.release("aaaa");
        assert!(!counts.is_open("aaaa"));
        assert!(counts.0.is_empty(), "released entries must not linger");
    }

    #[test]
    fn switching_title_discards_the_one_the_viewer_left() {
        // The whole point: starting a new episode abandons the old one, so
        // its half-downloaded file is deleted rather than kept forever in a
        // cache that is permanently at its cap.
        assert_eq!(action_for_abandoned(false, false), UnfocusedAction::Discard);
    }

    #[test]
    fn a_torrent_another_reader_still_holds_is_never_deleted() {
        // A second device watching something else, or this player's own
        // header/index reads. Deleting underneath a live reader truncates a
        // video someone is watching -- see `StreamGuard`.
        assert_eq!(action_for_abandoned(true, false), UnfocusedAction::Leave);
    }

    #[test]
    fn a_finished_download_survives_the_switch() {
        // A complete file costs no download bandwidth, and deleting a movie
        // that finished downloading because the viewer started the next
        // episode is destructive. The cache janitor reclaims it under
        // pressure, oldest-first.
        assert_eq!(action_for_abandoned(false, true), UnfocusedAction::Leave);
    }

    #[test]
    fn open_stream_counts_protect_until_the_last_reader_finishes() {
        // A player opens several reads at once (header, index, playback
        // position). Releasing one must not expose the torrent to the reaper
        // while the others are still going -- that is the freeze this whole
        // mechanism exists to prevent.
        let mut counts = OpenStreamCounts::default();
        counts.acquire("aaaa");
        counts.acquire("aaaa");
        counts.acquire("aaaa");

        counts.release("aaaa");
        assert!(counts.is_open("aaaa"));
        counts.release("aaaa");
        assert!(counts.is_open("aaaa"));

        counts.release("aaaa");
        assert!(!counts.is_open("aaaa"));
    }

    #[test]
    fn open_stream_counts_ignore_unbalanced_releases() {
        // Must not underflow into a huge count, which would pin a torrent
        // as "in use" forever and defeat both the reaper and the janitor.
        let mut counts = OpenStreamCounts::default();
        counts.release("never-acquired");
        assert!(!counts.is_open("never-acquired"));

        counts.acquire("aaaa");
        counts.release("aaaa");
        counts.release("aaaa");
        assert!(!counts.is_open("aaaa"));
    }

    #[test]
    fn open_stream_counts_keep_torrents_independent() {
        let mut counts = OpenStreamCounts::default();
        counts.acquire("aaaa");
        counts.acquire("bbbb");
        counts.release("aaaa");

        assert!(!counts.is_open("aaaa"));
        assert!(
            counts.is_open("bbbb"),
            "one torrent's reader must not release another's"
        );
    }

    #[test]
    fn advertised_set_does_not_double_count_repeats() {
        // Re-browsing the same title must not push other entries out.
        let mut set = AdvertisedHashes::new(2);
        set.remember("one".into(), Vec::new());
        set.remember("one".into(), Vec::new());
        set.remember("two".into(), Vec::new());

        assert!(set.contains("one"));
        assert!(set.contains("two"));
        assert_eq!(set.order.len(), 2);
    }
}
