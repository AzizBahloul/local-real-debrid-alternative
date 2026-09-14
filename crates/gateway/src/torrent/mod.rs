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
//!
//! Split by concern:
//!
//! * this file -- starting torrents and serving reads from them;
//! * `queue` -- which torrents run: the download queue, the idle reaper, hand
//!   pauses, and discarding the title the viewer walked away from;
//! * `metadata` -- `.torrent` metadata in memory and on disk, and the set of
//!   hashes this gateway has offered;
//! * `readers` -- bookkeeping for the video responses reading right now.

mod metadata;
mod queue;
mod readers;
pub mod resolver;

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::io::SeekFrom;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bytes::Bytes;
use librqbit::api::TorrentIdOrHash;
use librqbit::limits::LimitsConfig;
use librqbit::{
    AddTorrent, AddTorrentOptions, AddTorrentResponse, Api, DhtSessionConfig, ListenerMode,
    ListenerOptions, ManagedTorrent, Session, SessionOptions, SessionPersistenceConfig,
};
use tokio::io::{AsyncRead, AsyncSeekExt};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::config::AppConfig;
use crate::util::lock;
use metadata::{
    load_advertised, session_metadata, AdvertisedHashes, MetadataArchive, MetadataCache,
    ADVERTISED_HASH_CAPACITY,
};
use readers::{
    has_tail_index, OpenStreamCounts, ReaderSlot, LIBRQBIT_STREAM_WINDOW, READAHEAD_ADVANCE_BYTES,
    READAHEAD_REFRESH, TAIL_WARM_BYTES, TAIL_WARM_TIMEOUT,
};
pub use readers::{PieceGeometry, ReadaheadHandle, ReaderCancel, ReaderTicket, StreamGuard};
use resolver::{
    info_hash_from_magnet, is_video_file, suggest_video_file, MagnetSource, ResolvedTorrent,
    TorrentFile, TorrentSource,
};

/// A boxed read side of a torrent file. `librqbit::FileStream` lives in a
/// private module and cannot be named from outside the crate, so it is boxed
/// behind the trait it implements. Seeking is done before boxing.
pub type BoxedReader = Box<dyn AsyncRead + Send + Unpin>;

// Cold-start budget. A first request for a torrent that is not running yet
// does three things in sequence, and their sum must stay under the router's
// request timeout (`crate::REQUEST_TIMEOUT`, 60s) -- otherwise the client
// gets a 504 having waited the full minute for nothing, which is strictly
// worse than a fast failure:
//
//     ADD_TORRENT_TIMEOUT (25s)  fetch metadata from DHT/trackers
//   + INITIALIZE_TIMEOUT  (15s)  leave `Initializing` (hash-check on disk)
//   + prebuffer timeout   (15s)  wait for real bytes (see `streaming`)
//   = 55s worst case
//
// The prebuffer timeout is configurable, so `TorrentEngine::new` warns when a
// configuration breaks the budget, and a test pins the defaults.
//
/// Bounds the metadata fetch. Unbounded, a torrent with no reachable peers
/// hangs here until the router gives up.
const ADD_TORRENT_TIMEOUT: Duration = Duration::from_secs(25);

/// How long `start_file` waits for a freshly-added torrent to become
/// streamable.
const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(15);

/// How long a hash that just failed to find any peers is refused a second
/// full search.
///
/// A release with no reachable seeders is not a transient failure that a
/// retry fixes -- it fails the same way every time, for the same
/// `ADD_TORRENT_TIMEOUT` (25s), and a player that auto-retries a dead stream
/// (resuming "continue watching", or its own error-recovery) does so roughly
/// every 25-30s on its own. Without this, every one of those retries pays the
/// full 25-second search again, which is indistinguishable from the gateway
/// hanging even though it is doing exactly what was asked. Longer than
/// `ADD_TORRENT_TIMEOUT` so the very next automatic retry is the one that
/// gets the fast, honest "still no peers" instead of another full wait.
const START_FAILURE_COOLDOWN: Duration = Duration::from_secs(45);

/// How long a metadata search may keep running after the request that started
/// it has already given up. See `spawn_search`.
///
/// Generous on purpose, because the thing it is waiting for is genuinely slow:
/// a magnet with no trackers has to bootstrap DHT, iterate `get_peers` across
/// the routing table, and then pull the metadata from whichever peer answers.
/// Two minutes is comfortably past the point where a sparsely-seeded release
/// either shows up or does not, while still bounding the task and its
/// concurrency permit so a dead hash cannot park either indefinitely.
const BACKGROUND_SEARCH_TIMEOUT: Duration = Duration::from_secs(120);

/// How long after its last read a torrent is still considered "being watched"
/// for the purpose of *deleting* it — see `queue::action_for_abandoned`.
///
/// Sized off the retry cycle rather than off playback: a player that fails to
/// start re-attempts roughly every 25-30s (the same figure
/// `START_FAILURE_COOLDOWN` is built on), and a viewer picking another source
/// by hand takes longer. This spans a couple of those cycles so the retry
/// lands on a torrent that kept its data and its warmed-up peers, which is the
/// difference between the second attempt starting instantly and starting cold
/// again. It only delays reclaiming space; the cache janitor still enforces
/// the cap, and the idle reaper still parks the torrent in the meantime.
const ABANDON_GRACE: Duration = Duration::from_secs(90);

/// Cap on how many already-known torrents a single browse will wake up. A
/// title's stream list can name a dozen releases we have partial data for;
/// resuming all of them would have them compete for the same upstream
/// bandwidth, which is the exact problem the idle reaper exists to prevent.
const MAX_WARM_ON_BROWSE: usize = 3;

/// Upper bound on the activity map.
///
/// Only hashes the gateway advertised or already holds are ever recorded (see
/// `touch_stream`), and both of those are bounded, so this is a backstop
/// rather than a limit anything legitimate reaches: it is what keeps a client
/// looping over made-up hashes from growing the map for the life of the
/// process.
const ACTIVE_STREAMS_CAPACITY: usize = 1024;

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

/// One row of a title's stream list, as far as pre-warming cares.
#[derive(Debug, Clone)]
pub struct BrowseCandidate {
    pub info_hash: String,
    /// Which file inside the torrent the viewer would play.
    pub file_idx: usize,
    /// Trackers the index reported for this exact release.
    pub trackers: Vec<String>,
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
    /// Paused by the operator rather than by the download queue.
    ///
    /// Separate from `state` because the two answer different questions: a
    /// torrent parked by the queue also reads "paused", and offering the
    /// viewer a Resume button for that would be a lie -- the queue would
    /// simply park it again. Only a hand pause is the viewer's to undo.
    pub held: bool,
}

/// One file inside a running torrent, as a video response needs it.
#[derive(Debug, Clone)]
pub struct StreamFile {
    pub len: u64,
    /// The file's path inside the torrent; only its extension is ever
    /// consulted (see `warm_mp4_tail`).
    pub name: String,
}

/// The configured knobs a video response reads on every request, gathered so
/// they are converted from the config once rather than on each read.
#[derive(Debug, Clone)]
pub struct StreamTuning {
    /// Ceiling on the pre-buffer, in bytes. 0 disables it.
    pub prebuffer_bytes: usize,
    pub prebuffer_timeout: Duration,
    /// How long a body may produce nothing before its read is re-opened.
    /// Zero disables that.
    pub stall_timeout: Duration,
    /// Whether a new read retires the same client's reads that never produced
    /// a byte. See `register_reader`.
    pub seek_supersede: bool,
    /// Whether a trailing container index is fetched alongside the head. See
    /// `warm_mp4_tail`.
    pub tail_warm: bool,
    /// Extra read-ahead past librqbit's own window, in bytes. 0 disables it.
    pub readahead_extra: u64,
    pub readahead_settle: Duration,
}

impl StreamTuning {
    pub fn from_config(config: &AppConfig) -> Self {
        Self {
            prebuffer_bytes: config.prebuffer_bytes,
            prebuffer_timeout: Duration::from_secs(config.prebuffer_timeout_secs),
            stall_timeout: Duration::from_secs(config.stall_timeout_secs),
            seek_supersede: config.seek_supersede,
            tail_warm: config.mp4_tail_warm,
            readahead_extra: config.readahead_extra_mb.saturating_mul(1024 * 1024),
            readahead_settle: Duration::from_secs(config.readahead_settle_secs),
        }
    }
}

/// How far along a torrent is, derived once from librqbit's byte counts so
/// every caller agrees on what "finished" means.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Progress {
    /// Every wanted byte is on disk. A torrent whose size is still unknown is
    /// never finished.
    finished: bool,
    percent: f64,
}

impl Progress {
    fn of(progress_bytes: u64, total_bytes: u64) -> Self {
        Self {
            finished: total_bytes > 0 && progress_bytes >= total_bytes,
            percent: if total_bytes == 0 {
                0.0
            } else {
                progress_bytes as f64 / total_bytes as f64 * 100.0
            },
        }
    }
}

/// How much of `cooldown` is left for `info_hash` since its last failure,
/// or `None` if it was never marked or the cooldown already elapsed. Prunes
/// every expired entry it passes over, so the map cannot grow for the life of
/// the process. `cooldown` is a parameter (rather than reading the module
/// constant directly) so this logic is testable without waiting out a real
/// 45-second window.
fn remaining_cooldown(
    failures: &mut HashMap<String, Instant>,
    info_hash: &str,
    cooldown: Duration,
) -> Option<Duration> {
    failures.retain(|_, at| at.elapsed() < cooldown);
    failures.get(info_hash).map(|at| cooldown - at.elapsed())
}

/// A per-hash lock held for the whole of a start, across its awaits.
type StartLock = Arc<tokio::sync::Mutex<()>>;

/// Hands out the start lock for one info hash, creating it on first use.
///
/// Every caller for the same hash must get the *same* `Arc`, or the lock
/// guards nothing. Entries whose only remaining owner is the map are dropped
/// as we pass over them, so a long browsing session cannot grow this without
/// bound -- while a lock somebody is currently holding or waiting on (strong
/// count above one) is never removed.
fn lock_for(starts: &mut HashMap<String, StartLock>, info_hash: &str) -> StartLock {
    starts.retain(|hash, lock| hash == info_hash || Arc::strong_count(lock) > 1);
    Arc::clone(starts.entry(info_hash.to_string()).or_default())
}

/// Every map in the engine is keyed by the lowercase hex form. Borrowed when
/// the caller already has it, which is nearly always.
fn lowercase(info_hash: &str) -> Cow<'_, str> {
    if info_hash.bytes().any(|b| b.is_ascii_uppercase()) {
        Cow::Owned(info_hash.to_ascii_lowercase())
    } else {
        Cow::Borrowed(info_hash)
    }
}

fn parse_idx(info_hash: &str) -> Result<TorrentIdOrHash> {
    TorrentIdOrHash::parse(info_hash)
        .map_err(|e| anyhow::anyhow!("invalid info hash {info_hash}: {e}"))
}

/// The engine.
///
/// # Locks
///
/// Every `std` mutex here is a leaf. It is held for one map operation and
/// released: never across an `.await`, never while another engine lock is
/// held, and never inside `session().with_torrents` -- librqbit holds its own
/// session lock for the length of that closure, so taking one of ours in
/// there is how two threads come to wait on each other in opposite orders.
/// A pass that needs several of them copies what it needs out first; see
/// `queue_snapshot` and `held_snapshot`.
///
/// The two tokio mutexes are the exceptions, and are held across awaits by
/// design: a per-hash start lock (`starts` hands them out), and
/// `advertised_save`, which keeps two writes of the advertised file in order.
pub struct TorrentEngine {
    api: Api,
    /// How an identifier turns into a magnet link. Only `MagnetSource` today;
    /// swapping in a different provider (e.g. resolving a `.torrent` file URL)
    /// only means constructing `TorrentEngine` with a different `Box` here.
    source: Box<dyn TorrentSource>,
    max_concurrent: Arc<Semaphore>,
    /// Last request per torrent. Bounded; see `touch_stream`.
    active_streams: StdMutex<HashMap<String, StreamActivity>>,
    advertised: StdMutex<AdvertisedHashes>,
    /// Serialises writes of the advertised file, so a slow older write can
    /// never land after a newer one.
    advertised_save: tokio::sync::Mutex<()>,
    /// Where the advertised set is kept between runs.
    advertised_path: PathBuf,
    /// Which torrents have a response body reading them right now. See
    /// `StreamGuard`.
    open_streams: StdMutex<OpenStreamCounts>,
    /// The torrent the viewer is watching *now*. See `focus_stream`.
    focused: StdMutex<Option<String>>,
    tuning: StreamTuning,
    /// How many unknown torrents a single browse may start fetching metadata
    /// for. See `warm_for_browse`.
    browse_prefetch: usize,
    /// How many torrents may download at once. See `download_slots`.
    max_active_downloads: usize,
    /// One lock per info hash being started, so the same torrent is never
    /// added twice at once. See `start_lock`.
    starts: StdMutex<HashMap<String, StartLock>>,
    /// Metadata from torrents already resolved, so starting one does not
    /// re-fetch what a resolve just pulled. See `MetadataCache`.
    resolved_metadata: StdMutex<MetadataCache>,
    /// The same metadata on disk, so replaying a title the janitor reclaimed
    /// weeks ago still needs no swarm lookup. See `MetadataArchive`.
    metadata_archive: MetadataArchive,
    /// When a hash last failed to find any peers. See `START_FAILURE_COOLDOWN`.
    /// Shared with the background search tasks, which are what record it.
    failed_starts: Arc<StdMutex<HashMap<String, Instant>>>,
    /// Hashes with a metadata search running right now, so a retry joins it
    /// instead of starting a competing one. See `spawn_search`.
    searching: Arc<StdMutex<HashSet<String>>>,
    /// Torrents the operator paused by hand, which the download queue must
    /// leave alone. See `hold_paused`.
    held: StdMutex<HashSet<String>>,
    /// Event log. Set after construction by `attach_audit` (the engine is
    /// built before the log's owner exists), and inert until then.
    audit: StdMutex<crate::audit::AuditLog>,
    /// Every video response currently in flight. See `register_reader`.
    readers: StdMutex<Vec<ReaderSlot>>,
    next_reader_id: AtomicU64,
    /// Files whose trailing index has already been warmed, by torrent, so the
    /// warmer runs once per file rather than once per seek -- and the check
    /// on every later seek allocates nothing.
    tail_warmed: StdMutex<HashMap<String, HashSet<usize>>>,
    /// Per-torrent peer cap applied at add time, when set.
    cold_start_peer_limit: Option<usize>,
}

/// Where the time went during one cold start, filled in as it proceeds so the
/// record is complete on the error paths too — a start that *failed* after 25
/// seconds is the one worth explaining, and it is exactly the case a
/// success-only measurement misses.
#[derive(Debug, Default)]
struct ColdStartPhases {
    info_hash: String,
    /// False when the torrent was already running, i.e. not a cold start at
    /// all and not worth a line in the log.
    was_cold: bool,
    from_cached_metadata: bool,
    metadata_ms: u64,
    initialize_ms: u64,
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

        // Rebuilt per attempt: `SessionOptions` is consumed by the call and
        // is not `Clone`, and the peer port needs a second try (see below).
        let make_opts = |peer_port: u16| SessionOptions {
            dht: (!config.disable_dht).then(DhtSessionConfig::default),
            fastresume: true,
            persistence: Some(SessionPersistenceConfig::Json {
                folder: Some(config.session_state_dir()),
            }),
            // Accepting incoming peer connections is not optional for a fast
            // start. librqbit does not listen at all unless told to, which
            // leaves the gateway dialling out only: every seeder that would
            // have connected to *us* after our tracker announce -- a large
            // share of a healthy swarm -- can never arrive. The symptom is a
            // first play stuck at a handful of peers and a few dozen KB/s.
            listen: Some(ListenerOptions {
                mode: ListenerMode::TcpOnly,
                listen_addr: (std::net::Ipv6Addr::UNSPECIFIED, peer_port).into(),
                // What makes that listener reachable from outside this LAN,
                // which for a public swarm is where nearly every peer is.
                enable_upnp_port_forwarding: !config.disable_upnp,
                ..Default::default()
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

        // A peer port already in use is fatal inside librqbit, and the
        // default is a well-known one that another torrent client may well
        // hold. Falling back to an ephemeral port keeps the listener (and so
        // the incoming peers it exists for) rather than refusing to start.
        let session =
            match Session::new_with_opts(config.downloads_dir(), make_opts(config.peer_port)).await
            {
                Ok(session) => session,
                Err(e) if config.peer_port != 0 => {
                    warn!(
                        port = config.peer_port,
                        "could not start the torrent session on that peer port ({e:#}); \
                         retrying on an ephemeral one"
                    );
                    Session::new_with_opts(config.downloads_dir(), make_opts(0))
                        .await
                        .context("failed to start torrent session")?
                }
                Err(e) => return Err(e).context("failed to start torrent session"),
            };

        let api = Api::new(session, None);

        let advertised_path = config.session_state_dir().join("advertised.json");
        let remembered_advertised = load_advertised(&advertised_path).await;
        if !remembered_advertised.is_empty() {
            info!(
                count = remembered_advertised.len(),
                "restored advertised info hashes, so links already in a client keep working"
            );
        }

        let tuning = StreamTuning::from_config(config);
        let budget = ADD_TORRENT_TIMEOUT + INITIALIZE_TIMEOUT + tuning.prebuffer_timeout;
        if budget >= crate::REQUEST_TIMEOUT {
            warn!(
                budget_secs = budget.as_secs(),
                request_timeout_secs = crate::REQUEST_TIMEOUT.as_secs(),
                "PREBUFFER_TIMEOUT_SECS pushes a worst-case cold start past the request \
                 timeout; a slow first play will end in a 504 instead of a retryable 503"
            );
        }

        let engine = Arc::new(Self {
            api,
            source: Box::new(MagnetSource),
            max_concurrent: Arc::new(Semaphore::new(config.max_concurrent_torrents.max(1))),
            // At least one, or nothing could ever download -- including the
            // title on screen, which would look like the gateway hanging.
            max_active_downloads: config.max_active_downloads.max(1),
            active_streams: StdMutex::new(HashMap::new()),
            advertised: StdMutex::new(AdvertisedHashes::seed(
                ADVERTISED_HASH_CAPACITY,
                remembered_advertised,
            )),
            advertised_save: tokio::sync::Mutex::new(()),
            advertised_path,
            open_streams: StdMutex::new(OpenStreamCounts::default()),
            focused: StdMutex::new(None),
            tuning,
            browse_prefetch: config.browse_prefetch_count,
            starts: StdMutex::new(HashMap::new()),
            resolved_metadata: StdMutex::new(MetadataCache::default()),
            metadata_archive: MetadataArchive::new(config.session_state_dir().join("metadata")),
            failed_starts: Arc::new(StdMutex::new(HashMap::new())),
            searching: Arc::new(StdMutex::new(HashSet::new())),
            held: StdMutex::new(HashSet::new()),
            audit: StdMutex::new(crate::audit::AuditLog::disabled()),
            readers: StdMutex::new(Vec::new()),
            next_reader_id: AtomicU64::new(0),
            tail_warmed: StdMutex::new(HashMap::new()),
            cold_start_peer_limit: (config.cold_start_peer_limit > 0)
                .then_some(config.cold_start_peer_limit),
        });

        // Bank what librqbit just restored. These blobs are already in memory
        // and are about to become unreachable: librqbit deletes its own copy
        // when the janitor removes the torrent, and that is exactly the title
        // whose replay next month would otherwise need the swarm. Doing it
        // here rather than only on play makes the archive cover the library
        // that already exists, not just what gets watched from now on.
        //
        // Detached: it is disk work for the future, and the listener should
        // not wait on it.
        let archiving = Arc::clone(&engine);
        tokio::spawn(async move { archiving.archive_session_metadata().await });

        Ok(engine)
    }

    /// Copies every torrent currently in the session into the metadata
    /// archive. Best effort, and cheap: the bytes are already resident, and a
    /// blob an earlier run banked is skipped without being read.
    async fn archive_session_metadata(&self) {
        let known: Vec<(String, Bytes)> = self.api.session().with_torrents(|iter| {
            iter.filter_map(|(_, handle)| {
                let bytes = handle.with_metadata(|m| m.torrent_bytes.clone()).ok()?;
                Some((handle.info_hash().as_string(), bytes))
            })
            .collect()
        });
        let mut archived = 0;
        for (info_hash, bytes) in known {
            if self.metadata_archive.store(&info_hash, &bytes).await {
                archived += 1;
            }
        }
        if archived > 0 {
            info!(
                count = archived,
                "archived torrent metadata, so these titles restart without a swarm lookup"
            );
        }
    }

    /// Gives the engine somewhere to record events.
    ///
    /// Separate from `new` because the engine is constructed before the rest
    /// of the application state exists. Until this is called the engine logs
    /// nothing, which is what tests and the integration harness want.
    pub fn attach_audit(&self, log: crate::audit::AuditLog) {
        *lock(&self.audit) = log;
    }

    pub fn audit(&self) -> crate::audit::AuditLog {
        lock(&self.audit).clone()
    }

    /// Fetches torrent metadata only (fast, no data downloaded) and lists its files.
    ///
    /// Metadata this gateway already holds -- a running torrent, the memory
    /// cache, the archive -- is listed from those bytes without touching the
    /// network. Only a genuinely unknown hash asks the swarm, and that ask is
    /// bounded by `ADD_TORRENT_TIMEOUT` and refused while a search for the
    /// same hash is already running or has just come back empty, exactly as
    /// a start is.
    ///
    /// Goes through `Session::add_torrent` rather than the `Api` wrapper for
    /// one reason: the wrapper discards the assembled `.torrent` bytes, and
    /// those are exactly what lets the subsequent start skip a second
    /// metadata fetch. See `MetadataCache`.
    pub async fn resolve(&self, input: &str) -> Result<ResolvedTorrent> {
        let magnet = self.source.to_magnet(input)?;
        let info_hash = info_hash_from_magnet(&magnet)?;

        let known = self.known_metadata(&info_hash).await;
        let fetched = known.is_none();
        let source = match known {
            Some(bytes) => AddTorrent::from_bytes(bytes),
            None => {
                self.refuse_repeat_search(&info_hash, false)?;
                AddTorrent::from_url(magnet.clone())
            }
        };

        let opts = AddTorrentOptions {
            list_only: true,
            ..Default::default()
        };
        let response = tokio::time::timeout(
            ADD_TORRENT_TIMEOUT,
            self.api.session().add_torrent(source, Some(opts)),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "no metadata after {}s -- no peer holding this release answered yet",
                ADD_TORRENT_TIMEOUT.as_secs()
            )
        })?
        .context("failed to fetch torrent metadata (no peers found yet, or invalid torrent?)")?;

        // `list_only` always takes the `ListOnly` arm; the others would mean
        // librqbit started a download we explicitly asked it not to.
        let AddTorrentResponse::ListOnly(listed) = response else {
            anyhow::bail!("torrent engine started a torrent for a metadata-only request");
        };

        let files: Vec<TorrentFile> = listed
            .info
            .iter_file_details()
            .enumerate()
            .map(|(index, d)| {
                let name = d.filename.to_string();
                TorrentFile {
                    index,
                    is_video: is_video_file(&name),
                    name,
                    length: d.len,
                }
            })
            .collect();

        let suggested_file_idx = suggest_video_file(&files);
        let name = listed.info.name().map(|n| n.into_owned());

        // Whoever resolved is about to start this same torrent, so keep what
        // the fetch cost so `start_file` does not pay it again -- and on disk
        // too, so a replay long after this session is over does not either.
        if fetched {
            self.metadata_archive
                .store(&info_hash, &listed.torrent_bytes)
                .await;
        }
        lock(&self.resolved_metadata).remember(info_hash.clone(), listed.torrent_bytes);

        Ok(ResolvedTorrent {
            info_hash,
            name,
            magnet,
            files,
            suggested_file_idx,
            seen_peers: listed.seen_peers,
        })
    }

    /// Metadata for `info_hash` from anywhere this gateway keeps it: a
    /// torrent already in the session first, then what `cached_metadata`
    /// finds.
    async fn known_metadata(&self, info_hash: &str) -> Option<Bytes> {
        match session_metadata(&self.api, info_hash) {
            Some(bytes) => Some(bytes),
            None => self.cached_metadata(info_hash).await,
        }
    }

    /// Metadata this gateway pulled for exactly this hash at some earlier
    /// point: memory first, then the on-disk archive, which is what covers a
    /// title whose files the janitor reclaimed days ago. See `MetadataCache`
    /// and `MetadataArchive`.
    async fn cached_metadata(&self, info_hash: &str) -> Option<Bytes> {
        let in_memory = lock(&self.resolved_metadata).get(info_hash);
        match in_memory {
            Some(bytes) => Some(bytes),
            None => self.metadata_archive.load(info_hash).await,
        }
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
        let mut phases = ColdStartPhases::default();
        let began = Instant::now();
        let result = self
            .start_file_timed(input, file_idx, initial_peers, &mut phases)
            .await;

        // Only a genuinely cold start is worth a record. A warm one is a map
        // lookup, and logging those would bury the slow starts -- the ones
        // this exists to explain -- under one line per seek.
        if phases.was_cold {
            self.audit().record(crate::audit::Event::ColdStart {
                info_hash: phases.info_hash.clone(),
                file_idx,
                metadata_ms: phases.metadata_ms,
                initialize_ms: phases.initialize_ms,
                total_ms: began.elapsed().as_millis() as u64,
                from_cached_metadata: phases.from_cached_metadata,
                outcome: match &result {
                    Ok(_) => "ok".to_string(),
                    Err(e) => format!("{e:#}"),
                },
            });
        }
        result
    }

    /// The body of `start_file`, recording where the time went as it goes.
    async fn start_file_timed(
        &self,
        input: &str,
        file_idx: usize,
        initial_peers: Vec<std::net::SocketAddr>,
        phases: &mut ColdStartPhases,
    ) -> Result<String> {
        let magnet = self.source.to_magnet(input)?;
        let info_hash = info_hash_from_magnet(&magnet)?;
        phases.info_hash = info_hash.clone();

        // Serializes starts of *this* torrent. librqbit resolves a magnet's
        // metadata before the torrent appears in the session, so for the
        // whole of that fetch -- the slowest part of a cold start -- the
        // torrent is invisible to `session.get()`. Without this lock a viewer
        // tapping a title whose prefetch is still resolving looks it up, sees
        // nothing, and kicks off a *second* metadata fetch alongside the
        // first: two fetches competing for the same peers, and the head start
        // the prefetch bought thrown away. Waiting here instead means the tap
        // joins the fetch already in flight and returns the moment it lands.
        let start_lock = self.start_lock(&info_hash);
        let _start_guard = start_lock.lock().await;

        // Playing a title is the clearest possible statement that its hand
        // pause is over, and clearing it here means the resume below is not
        // undone by the download queue a second later.
        self.forget_hold(&info_hash);

        // Whoever held the lock may have finished the job while we waited.
        let idx = parse_idx(&info_hash)?;
        if let Some(handle) = self.api.session().get(idx) {
            if let Err(e) = self.resume_if_paused(&handle, idx).await {
                // Almost always a race with something else resuming it.
                debug!(%info_hash, "resume before start returned an error: {e:#}");
            }
            self.wait_handle_streamable(&handle, &info_hash, INITIALIZE_TIMEOUT)
                .await?;
            return Ok(info_hash);
        }

        // Adding from metadata already held resolves nothing over the
        // network, so the slowest step of a cold start disappears entirely --
        // which is the whole point of keeping it.
        let cached_metadata = self.cached_metadata(&info_hash).await;

        // Past the warm-path return above, so everything from here is a cold
        // start and worth a record however it ends.
        phases.was_cold = true;
        phases.from_cached_metadata = cached_metadata.is_some();

        self.refuse_repeat_search(&info_hash, cached_metadata.is_some())?;

        // Throttles how many torrents can be *added* concurrently (each add
        // does a burst of tracker/DHT/peer-handshake work) -- not how many
        // can stream at once, so the permit is released as soon as the add
        // returns, background or not.
        let permit = Arc::clone(&self.max_concurrent)
            .acquire_owned()
            .await
            .context("torrent concurrency limiter closed")?;

        let opts = self.start_options(&info_hash, file_idx, initial_peers);
        let source = match cached_metadata {
            Some(torrent_bytes) => {
                debug!(%info_hash, "starting from metadata already fetched; no second swarm lookup");
                AddTorrent::from_bytes(torrent_bytes)
            }
            None => AddTorrent::from_url(magnet),
        };

        // The search runs on its own task so that *this request* timing out
        // does not cancel it.
        //
        // It used to be awaited inline under a 25-second timeout, and dropping
        // that future is what killed the fetch. For a magnet with no trackers
        // -- which is nearly every replayed link, since the index only reports
        // trackers on the browse that first offered the stream -- 25 seconds
        // is simply not long enough to bootstrap DHT, iterate get_peers, and
        // pull the metadata from whoever answers. So a title that was merely
        // slow to find was declared to have "no reachable peers", nothing was
        // left in the session, and every retry re-ran the same doomed 25
        // seconds from scratch. Letting it run means the swarm list shows the
        // torrent the moment metadata lands, and the viewer's next tap takes
        // the warm path above and plays.
        let metadata_began = Instant::now();
        let search = self.spawn_search(info_hash.clone(), source, opts, permit);
        let added = tokio::time::timeout(ADD_TORRENT_TIMEOUT, search).await;
        phases.metadata_ms = metadata_began.elapsed().as_millis() as u64;

        match added {
            // Still going. Deliberately no failure recorded: nothing has
            // failed, and marking it would gag the retry that is about to
            // succeed.
            Err(_elapsed) => anyhow::bail!(
                "no metadata yet after {}s -- still searching in the background, so press \
                 play again shortly",
                ADD_TORRENT_TIMEOUT.as_secs()
            ),
            Ok(Err(join)) => anyhow::bail!("torrent search task failed: {join}"),
            Ok(Ok(Err(e))) => return Err(e).context("failed to start torrent"),
            Ok(Ok(Ok(()))) => {}
        }

        // Bank the metadata this start just proved good. `resolve` only covers
        // the browse path; a viewer who taps straight through to a stream
        // reaches here without ever resolving, and that is precisely the title
        // whose replay would otherwise pay a full swarm lookup months later.
        self.archive_metadata(&info_hash).await;

        // Adding a torrent returns before it is streamable: librqbit still has
        // to move it out of `Initializing` (hashing/checking any data already
        // on disk). Returning here would hand the caller a URL that 404s with
        // "invalid state: initializing" for the first second or two -- which a
        // player treats as a dead link rather than retrying, so playback fails
        // outright. Waiting here makes "started" actually mean "playable".
        let initialize_began = Instant::now();
        let streamable = self
            .wait_until_streamable(&info_hash, INITIALIZE_TIMEOUT)
            .await;
        phases.initialize_ms = initialize_began.elapsed().as_millis() as u64;
        streamable?;

        Ok(info_hash)
    }

    /// Resumes a torrent the idle reaper, the download queue, or a prefetch
    /// parked. A no-op for one already running.
    async fn resume_if_paused(
        &self,
        handle: &Arc<ManagedTorrent>,
        idx: TorrentIdOrHash,
    ) -> Result<()> {
        if handle.is_paused() {
            self.api
                .api_torrent_action_start(idx)
                .await
                .map_err(|e| anyhow::anyhow!("failed to resume torrent: {e}"))?;
        }
        Ok(())
    }

    /// Refuses a swarm search this hash should not run right now. Both
    /// refusals exist to make a repeated ask cheap and honest instead of a
    /// second full wait.
    ///
    /// * A search already running is joined, not duplicated: the answer it is
    ///   about to produce is the same answer this request wants, and a second
    ///   concurrent search only splits the same peers between two lookups.
    /// * A hash whose search just came back empty is not asked again for a
    ///   short while, so a client auto-retrying a dead stream gets an instant
    ///   answer. Skipped when the metadata is already held, since the cooldown
    ///   exists to avoid re-running a search that such an add does not run.
    ///   See `START_FAILURE_COOLDOWN`.
    fn refuse_repeat_search(&self, info_hash: &str, have_metadata: bool) -> Result<()> {
        if self.search_in_flight(info_hash) {
            anyhow::bail!(
                "still searching the swarm for this release -- it keeps looking in the \
                 background, so try again in a moment"
            );
        }
        if have_metadata {
            return Ok(());
        }
        if let Some(remaining) = self.recent_start_failure(info_hash) {
            anyhow::bail!(
                "found no peers {}s ago; waiting {}s before searching again -- if this \
                 release stays dead, pick another source for the same episode",
                (START_FAILURE_COOLDOWN - remaining).as_secs(),
                remaining.as_secs()
            );
        }
        Ok(())
    }

    /// How a start adds its torrent.
    fn start_options(
        &self,
        info_hash: &str,
        file_idx: usize,
        initial_peers: Vec<std::net::SocketAddr>,
    ) -> AddTorrentOptions {
        AddTorrentOptions {
            only_files: Some(vec![file_idx]),
            overwrite: true,
            // Every torrent lands under `downloads/<info_hash>/`, single- or
            // multi-file alike. This gives the cache janitor (see the `cache`
            // module) an unambiguous, collision-free way to map a directory
            // on disk back to the torrent it belongs to.
            sub_folder: Some(info_hash.to_string()),
            initial_peers: (!initial_peers.is_empty()).then_some(initial_peers),
            // Overrides the session-wide cap for this torrent only, and only
            // when asked for. `None` keeps the session default. librqbit reads
            // this once, here, so it cannot be walked back down after the
            // start -- see the field docs on `cold_start_peer_limit`.
            peer_limit: self.cold_start_peer_limit,
            ..Default::default()
        }
    }

    /// Whether a background metadata search is already running for this hash.
    fn search_in_flight(&self, info_hash: &str) -> bool {
        lock(&self.searching).contains(info_hash)
    }

    /// Starts the add on a detached task and returns a handle to it.
    ///
    /// The caller waits on that handle under a timeout; letting the handle go
    /// abandons the *wait*, not the work, because tokio keeps a spawned task
    /// running after its `JoinHandle` is dropped. That asymmetry is the whole
    /// point: an HTTP request cannot hang for two minutes, but the swarm
    /// lookup behind it can happily take that long and still be worth having.
    ///
    /// Bounded by `BACKGROUND_SEARCH_TIMEOUT` so a genuinely dead hash cannot
    /// leave a task and a concurrency permit parked forever.
    ///
    /// Recording and clearing a start failure happens in here, because this
    /// task is the only thing that knows how the search actually ended -- the
    /// request that kicked it off has usually timed out and gone by then.
    fn spawn_search(
        &self,
        info_hash: String,
        source: AddTorrent<'static>,
        opts: AddTorrentOptions,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> tokio::task::JoinHandle<Result<()>> {
        lock(&self.searching).insert(info_hash.clone());

        let api = self.api.clone();
        let searching = Arc::clone(&self.searching);
        let failed_starts = Arc::clone(&self.failed_starts);
        let archive = self.metadata_archive.clone();

        tokio::spawn(async move {
            let outcome = tokio::time::timeout(
                BACKGROUND_SEARCH_TIMEOUT,
                api.api_add_torrent(source, Some(opts)),
            )
            .await;

            // Released here rather than at the caller's timeout, so a search
            // that outlives its request still counts against the add limit.
            drop(permit);
            lock(&searching).remove(&info_hash);

            let failed = |reason: &str| {
                lock(&failed_starts).insert(info_hash.clone(), Instant::now());
                warn!(info_hash = %info_hash, "swarm search gave up: {reason}");
            };

            match outcome {
                Err(_) => {
                    failed("no peers answered within the background search window");
                    anyhow::bail!(
                        "no peers found for this release after {}s of searching",
                        BACKGROUND_SEARCH_TIMEOUT.as_secs()
                    );
                }
                Ok(Err(e)) => {
                    failed("the torrent engine refused the add");
                    return Err(anyhow::anyhow!("{e}"));
                }
                Ok(Ok(_)) => {}
            }

            // Peers answered, so any earlier failure is stale -- a later retry
            // must not be judged by a search that no longer reflects the swarm.
            lock(&failed_starts).remove(&info_hash);

            // Bank the metadata this search just paid for, whether or not the
            // request that asked for it is still waiting. A search that landed
            // after its viewer gave up is exactly the one worth keeping.
            if let Some(bytes) = session_metadata(&api, &info_hash) {
                archive.store(&info_hash, &bytes).await;
            }
            info!(info_hash = %info_hash, "swarm search found metadata; torrent is in the session");
            Ok(())
        })
    }

    /// Copies a live torrent's assembled `.torrent` blob into the archive and
    /// the memory cache.
    ///
    /// Reads it off the session rather than off the add call because that is
    /// the one place it exists whatever route the torrent took in -- magnet,
    /// cached bytes, or restored from librqbit's own persistence.
    async fn archive_metadata(&self, info_hash: &str) {
        // `None` while still resolving; the next start banks it instead.
        let Some(torrent_bytes) = session_metadata(&self.api, info_hash) else {
            return;
        };
        self.metadata_archive.store(info_hash, &torrent_bytes).await;
        lock(&self.resolved_metadata).remember(info_hash.to_string(), torrent_bytes);
    }

    /// How much of `START_FAILURE_COOLDOWN` is left for this hash, or `None`
    /// if it never failed or the cooldown has already elapsed.
    fn recent_start_failure(&self, info_hash: &str) -> Option<Duration> {
        remaining_cooldown(
            &mut lock(&self.failed_starts),
            info_hash,
            START_FAILURE_COOLDOWN,
        )
    }

    /// The lock guarding starts of one particular torrent. See `lock_for`.
    fn start_lock(&self, info_hash: &str) -> StartLock {
        lock_for(&mut lock(&self.starts), info_hash)
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
        let idx = parse_idx(info_hash)?;

        if let Some(handle) = self.api.session().get(idx) {
            // Resume anything the idle reaper (or a prefetch that decided
            // this was not the pick) paused. Cheap to check, and without it a
            // torrent you come back to would serve only the bytes already on
            // disk and then stall forever.
            //
            // Deliberately does NOT wait for peers to reconnect. An earlier
            // version did, reasoning that a resumed torrent has zero peers and
            // would stall the moment playback caught up to what was on disk.
            // But most resumes are of a torrent that *has* data on disk -- a
            // part-watched episode, or a finished one -- and those start
            // playing instantly from that data while peers reconnect in the
            // background. Blocking here made every one of them wait seconds
            // for bytes it already had. A genuinely empty resume is handled
            // where it belongs, by the pre-buffer and the stall/re-open path.
            if handle.is_paused() {
                debug!(%info_hash, "resuming paused torrent for a new request");
            }
            self.resume_if_paused(&handle, idx).await?;
            // It may still be initializing if a player raced us here, so wait
            // rather than returning immediately.
            self.wait_handle_streamable(&handle, info_hash, INITIALIZE_TIMEOUT)
                .await?;
            return Ok(true);
        }

        // One lookup answers both "was it offered?" and "with which trackers?".
        let Some(trackers) = self.advertised_trackers(info_hash) else {
            return Ok(false);
        };

        // The release's own trackers (remembered from the index) travel in
        // the magnet -- a magnet add reads trackers only from the URI itself.
        // The generic defaults are announced session-wide and need no ride.
        let magnet = resolver::magnet_with_trackers(info_hash, None, &trackers);
        self.start_file(&magnet, file_idx, Vec::new()).await?;
        Ok(true)
    }

    /// Records that we handed this info hash to a client, making it eligible
    /// for lazy start via `ensure_started` -- along with the trackers the
    /// index reported for this specific release, so that lazy start announces
    /// where this torrent's seeders actually are (see `AdvertisedHashes`).
    pub async fn remember_advertised(&self, info_hash: &str, trackers: &[String]) {
        self.remember_advertised_many([(info_hash, trackers)]).await;
    }

    /// Records one browse's worth of offered hashes at once.
    ///
    /// Persisted, because the set used to live only in memory and every
    /// restart invalidated every link already sitting in a Stremio client:
    /// the phone kept requesting a perfectly good hash and kept getting
    /// "this gateway has not offered that info hash", with no way to tell
    /// that re-picking the stream would fix it.
    ///
    /// Written once per batch and only when something changed. A stream list
    /// is fifteen rows, and it used to rewrite the whole file fifteen times --
    /// and then fifteen more on every re-browse of the same title, which
    /// changed nothing.
    pub async fn remember_advertised_many<'a>(
        &self,
        offered: impl IntoIterator<Item = (&'a str, &'a [String])>,
    ) {
        let batch: Vec<(String, Vec<String>)> = offered
            .into_iter()
            .map(|(hash, trackers)| (hash.to_ascii_lowercase(), trackers.to_vec()))
            .collect();
        let changed = lock(&self.advertised).remember_all(batch);
        if changed {
            self.save_advertised().await;
        }
    }

    /// The trackers remembered for an advertised hash (empty when none were
    /// ever reported for it), or `None` if the hash was never advertised.
    fn advertised_trackers(&self, info_hash: &str) -> Option<Vec<String>> {
        lock(&self.advertised)
            .trackers_if_advertised(&lowercase(info_hash))
            .map(<[String]>::to_vec)
    }

    /// Writes the advertised set out. Best effort: failing to persist costs a
    /// re-pick after the next restart, which is not worth failing a request.
    ///
    /// The snapshot is taken while holding the save lock, so whichever write
    /// lands last also carries the newest state. Atomic, so a crash mid-write
    /// cannot leave a torn file that parses as an empty set.
    async fn save_advertised(&self) {
        let _in_order = self.advertised_save.lock().await;
        let entries = lock(&self.advertised).snapshot();
        match serde_json::to_vec(&entries) {
            Ok(bytes) => {
                if let Err(e) = crate::util::write_atomic(&self.advertised_path, bytes, false).await
                {
                    debug!("could not persist advertised hashes: {e}");
                }
            }
            Err(e) => debug!("could not encode advertised hashes: {e}"),
        }
    }

    /// Whether `/videos/<hash>/...` is allowed to *start* this torrent.
    pub fn is_advertised(&self, info_hash: &str) -> bool {
        lock(&self.advertised).contains(&lowercase(info_hash))
    }

    /// Blocks until the torrent has left `Initializing`, or `timeout` elapses.
    ///
    /// A timeout is not treated as fatal: the torrent stays in the session and
    /// keeps initializing, so the caller can still hand out its URL and the
    /// player's own retry will pick it up shortly after.
    pub async fn wait_until_streamable(&self, info_hash: &str, timeout: Duration) -> Result<()> {
        let handle = self
            .api
            .mgr_handle(parse_idx(info_hash)?)
            .context("torrent vanished from the session right after being added")?;
        self.wait_handle_streamable(&handle, info_hash, timeout)
            .await
    }

    async fn wait_handle_streamable(
        &self,
        handle: &Arc<ManagedTorrent>,
        info_hash: &str,
        timeout: Duration,
    ) -> Result<()> {
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

    /// The stream settings every video response reads.
    pub fn tuning(&self) -> &StreamTuning {
        &self.tuning
    }

    /// Registers an in-flight video response and, in doing so, retires the
    /// ones this client has already given up on.
    ///
    /// librqbit interleaves piece requests round-robin across every open
    /// stream, so an abandoned read is not merely idle — it keeps taking its
    /// share of the request slots for a position nobody is watching. Ten quick
    /// scrubs therefore leave the position the viewer finally landed on
    /// receiving a tenth of the download, which is why a burst of seeks feels
    /// like ten cold starts rather than one.
    ///
    /// What gets retired is deliberately narrow. Only reads that have not
    /// delivered a single byte, only from the same client address, and never
    /// an index probe:
    ///
    /// * **Zero bytes served** is the proof that nobody is watching it. A read
    ///   that has produced output is feeding a picture on a screen and is left
    ///   alone no matter how old it is.
    /// * **Same client** keeps two devices playing the same title from
    ///   cancelling each other's cold start — and then each other's retry, in
    ///   a loop where neither ever starts.
    /// * **Not a tail probe** because an mp4's index read is a second read the
    ///   same player needs *concurrently*, not a superseded seek. Cancelling
    ///   it would break exactly the players §4.3 exists to help.
    pub fn register_reader(
        self: &Arc<Self>,
        info_hash: &str,
        file_idx: usize,
        client: IpAddr,
        tail_probe: bool,
    ) -> ReaderTicket {
        let id = self.next_reader_id.fetch_add(1, Ordering::Relaxed);
        let served = Arc::new(AtomicU64::new(0));
        let cancel = Arc::new(ReaderCancel::new());

        let superseded: Vec<Arc<ReaderCancel>> = {
            let mut readers = lock(&self.readers);
            let superseded = if self.tuning.seek_supersede && !tail_probe {
                readers
                    .iter()
                    .filter(|slot| slot.superseded_by(info_hash, file_idx, client))
                    .map(|slot| Arc::clone(&slot.cancel))
                    .collect()
            } else {
                Vec::new()
            };
            readers.push(ReaderSlot {
                id,
                info_hash: info_hash.to_string(),
                file_idx,
                client,
                tail_probe,
                served: Arc::clone(&served),
                cancel: Arc::clone(&cancel),
            });
            superseded
        };

        if !superseded.is_empty() {
            debug!(
                %info_hash,
                count = superseded.len(),
                "dropping reads this client abandoned before they produced a byte"
            );
            for cancel in superseded {
                cancel.cancel();
            }
        }

        ReaderTicket {
            engine: Arc::clone(self),
            id,
            served,
            cancel,
        }
    }

    /// Where `file_offset` sits inside its piece, or `None` while the torrent
    /// has no metadata yet.
    ///
    /// The piece is the smallest thing BitTorrent will transfer, so this is
    /// the floor under any read at that offset — and knowing it is what lets
    /// the pre-buffer avoid asking for one byte more than the piece it is
    /// already waiting for, which would silently double the wait.
    ///
    /// Recording it next to the measured wait is also what separates "this
    /// gateway is slow" from "this release has 8 MB pieces", which look
    /// identical from a stopwatch and call for completely different responses.
    pub fn piece_geometry(
        &self,
        info_hash: &str,
        file_idx: usize,
        file_offset: u64,
    ) -> Option<PieceGeometry> {
        let idx = TorrentIdOrHash::parse(info_hash).ok()?;
        let handle = self.api.session().get(idx)?;
        let metadata = handle.metadata.load_full()?;
        let file = metadata.file_infos.get(file_idx)?;
        if file_offset >= file.len {
            return None;
        }
        // librqbit's own arithmetic rather than a re-derivation of it: this is
        // the same function its reader uses to decide which piece it is
        // parked on, so the number reported here cannot drift from the number
        // actually being waited for.
        let lengths = metadata.lengths();
        let current = lengths.compute_current_piece(file_offset, file.offset_in_torrent)?;
        Some(PieceGeometry {
            piece_len: u64::from(lengths.piece_length(current.id)),
            // Clamped to the end of the file: the last piece of a file inside
            // a multi-file torrent runs on into the next file, and those bytes
            // are not ones this request could ever be served.
            remainder: u64::from(current.piece_remaining).min(file.len - file_offset),
        })
    }

    /// Length and name of one file inside a running torrent, read straight
    /// off its metadata.
    ///
    /// This used to go through `api_torrent_details`, which builds a response
    /// describing every file in the torrent -- names, path components,
    /// attributes -- to read one length, and a second time for the name, on
    /// every range request.
    pub fn stream_file(&self, info_hash: &str, file_idx: usize) -> Result<StreamFile> {
        let handle = self
            .api
            .session()
            .get(parse_idx(info_hash)?)
            .context("torrent is not in the session")?;
        let metadata = handle
            .metadata
            .load_full()
            .context("torrent metadata is not available yet")?;
        let file = metadata
            .file_infos
            .get(file_idx)
            .context("fileIdx out of range for this torrent")?;
        Ok(StreamFile {
            len: file.len,
            name: file.relative_filename.to_string_lossy().into_owned(),
        })
    }

    /// Fetches the tail of the file in parallel with its opening frames.
    ///
    /// A player that needs a trailing index — a non-faststart mp4's `moov`, an
    /// mkv's Cues — issues a range request for the tail *first*, at an offset no
    /// peer has been asked for, and only then requests byte 0. That is two full
    /// piece-fetch waits in sequence, which is one whole cold start hiding
    /// inside another, and it is where 12.2 s of a measured 20 s start went.
    ///
    /// Opening a short-lived read at the tail as soon as the stream opens puts
    /// that piece into the priority set alongside the first one, so the two are
    /// fetched together. Runs once per file, detached, and gives up quietly:
    /// it is an optimisation, and nothing downstream may wait on it.
    ///
    /// The cost when it guesses wrong (the file was faststart after all) is one
    /// piece of bandwidth, once.
    pub fn warm_mp4_tail(
        self: &Arc<Self>,
        info_hash: &str,
        file_idx: usize,
        file_len: u64,
        file_name: &str,
    ) {
        if !self.tuning.tail_warm || file_len <= TAIL_WARM_BYTES || !has_tail_index(file_name) {
            return;
        }

        // Claimed once per file, and claimed before the read is attempted, so
        // a warm that failed is not retried on the next seek -- the behaviour
        // this is meant to remove rather than add. This runs on every range
        // request, so the already-claimed path allocates nothing.
        {
            let mut warmed = lock(&self.tail_warmed);
            match warmed.get_mut(info_hash) {
                Some(files) => {
                    if !files.insert(file_idx) {
                        return;
                    }
                }
                None => {
                    warmed.insert(info_hash.to_string(), HashSet::from([file_idx]));
                }
            }
        }

        let engine = Arc::clone(self);
        let info_hash = info_hash.to_string();
        tokio::spawn(async move {
            let from = file_len - TAIL_WARM_BYTES;
            let Ok(mut reader) = engine.open_stream_at(&info_hash, file_idx, from).await else {
                debug!(%info_hash, "could not open a tail read to warm the mp4 index");
                return;
            };
            let mut sink = [0u8; 16 * 1024];
            let warmed = tokio::time::timeout(TAIL_WARM_TIMEOUT, async {
                use tokio::io::AsyncReadExt;
                // One read is enough: it returns only once the piece holding
                // the tail has arrived, which is the entire point.
                reader.read(&mut sink).await
            })
            .await;
            match warmed {
                Ok(Ok(n)) if n > 0 => {
                    debug!(%info_hash, "mp4 index fetched alongside the opening frames")
                }
                _ => debug!(%info_hash, "mp4 tail warm did not complete; harmless"),
            }
        });
    }

    /// Claims piece priority further ahead than librqbit's fixed 32 MB window,
    /// once playback has been running long enough to look settled.
    ///
    /// Returns `None` when the feature is off, which is the default — see
    /// `readahead_extra_mb` in the config for why widening the window is a
    /// trade rather than a win. The returned handle stops the claim when
    /// dropped, so it lives exactly as long as the response it belongs to.
    pub fn spawn_readahead(
        self: &Arc<Self>,
        info_hash: &str,
        file_idx: usize,
        position: Arc<AtomicU64>,
        file_len: u64,
    ) -> Option<ReadaheadHandle> {
        let extra = self.tuning.readahead_extra;
        if extra == 0 {
            return None;
        }

        let engine = Arc::clone(self);
        let info_hash = info_hash.to_string();
        let settle = self.tuning.readahead_settle;

        let task = tokio::spawn(async move {
            tokio::time::sleep(settle).await;

            // Held across iterations: dropping the reader is what releases the
            // claim, so the claim only exists while this task does.
            let mut claim: Option<BoxedReader> = None;
            let mut claimed_at = 0u64;

            loop {
                let target = position
                    .load(Ordering::Relaxed)
                    .saturating_add(LIBRQBIT_STREAM_WINDOW)
                    .saturating_add(extra);
                if target >= file_len {
                    // Past the end of the file there is nothing left to claim,
                    // and librqbit's own window already covers the remainder.
                    return;
                }
                if claim.is_none() || target.saturating_sub(claimed_at) >= READAHEAD_ADVANCE_BYTES {
                    match engine.open_stream_at(&info_hash, file_idx, target).await {
                        Ok(reader) => {
                            claim = Some(reader);
                            claimed_at = target;
                        }
                        Err(e) => debug!(%info_hash, "could not claim extra read-ahead: {e:#}"),
                    }
                }
                tokio::time::sleep(READAHEAD_REFRESH).await;
            }
        });

        Some(ReadaheadHandle(task))
    }

    /// Registers a live reader on `info_hash`. Hold the returned guard for as
    /// long as the response body exists. See `StreamGuard`.
    pub fn open_stream_guard(self: &Arc<Self>, info_hash: &str) -> StreamGuard {
        lock(&self.open_streams).acquire(info_hash);
        StreamGuard {
            engine: Arc::clone(self),
            info_hash: info_hash.to_string(),
        }
    }

    /// Whether any response body is currently reading this torrent.
    pub fn has_open_stream(&self, info_hash: &str) -> bool {
        lock(&self.open_streams).is_open(info_hash)
    }

    /// How many torrents are being read right now -- i.e. how many videos this
    /// gateway is actually serving.
    ///
    /// This is what `/health` reports as `active_streams`. The obvious
    /// alternative, the size of the `active_streams` activity map, counts
    /// every torrent touched since the last clear and never shrinks, so after
    /// three episodes it claims three streams with one player connected.
    pub fn open_stream_count(&self) -> usize {
        lock(&self.open_streams).torrents_open()
    }

    /// Drops the bookkeeping for a torrent that no longer exists, so a later
    /// request for the same title starts clean instead of matching a stale
    /// record.
    fn forget_activity(&self, info_hash: &str) {
        lock(&self.active_streams).remove(info_hash);
        // A re-download of the same title is a new file on disk, and its tail
        // has not been fetched.
        lock(&self.tail_warmed).remove(info_hash);
        // A hand pause on a torrent that no longer exists would otherwise
        // silently apply to the next download of the same title.
        self.forget_hold(info_hash);
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
        if !self.ensure_started(info_hash, file_idx).await? {
            anyhow::bail!("torrent {info_hash} is no longer startable");
        }
        self.open_started_stream_at(info_hash, file_idx, pos).await
    }

    /// [`open_stream_at`](Self::open_stream_at) for a caller that has just
    /// run `ensure_started` itself.
    ///
    /// Only for that caller: a paused torrent opens a stream here just fine,
    /// and then never produces a byte.
    pub async fn open_started_stream_at(
        &self,
        info_hash: &str,
        file_idx: usize,
        pos: u64,
    ) -> Result<BoxedReader> {
        let mut stream = self
            .api
            .api_stream(parse_idx(info_hash)?, file_idx)
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

    /// Prepares the torrents behind a title's stream list while the viewer is
    /// still reading it, so that pressing play is not where the waiting
    /// happens.
    ///
    /// Two different jobs, because a candidate is in one of two states:
    ///
    /// * **already in the session but paused** -- resume it. Pausing drops
    ///   every peer connection and they have to be rediscovered over
    ///   DHT/trackers, which is most of what "it took 30 seconds to start"
    ///   actually is. Only a torrent the download queue would run anyway: a
    ///   hand pause is the operator's, and resuming anything outside the
    ///   queue's slots only has the queue park it again a tick later, after
    ///   it has competed with the stream being watched in between.
    /// * **never seen** -- fetch its metadata and join its swarm now. This is
    ///   the expensive half of a cold start (a magnet carries only an info
    ///   hash; the file list has to be pulled from a peer that has it), and
    ///   until this existed *all* of it happened inside the request the
    ///   player made after the tap, where the viewer watches a spinner for
    ///   every second of it.
    ///
    /// Deliberately bounded (`browse_prefetch` new torrents, three resumes):
    /// each prefetch joins a swarm the viewer may never pick, and every
    /// running torrent competes for the same line as the one being watched.
    pub async fn warm_for_browse(self: &Arc<Self>, candidates: &[BrowseCandidate]) {
        let entries = self.queue_snapshot();
        let held = self.held_snapshot();
        let slots = self.download_slots(&entries, &held);

        let mut resumed = 0usize;
        let mut prefetch: Vec<BrowseCandidate> = Vec::new();

        for candidate in candidates {
            let hash = lowercase(&candidate.info_hash);
            let Ok(idx) = TorrentIdOrHash::parse(&hash) else {
                continue;
            };
            match self.api.session().get(idx) {
                Some(handle) => {
                    if resumed >= MAX_WARM_ON_BROWSE
                        || !handle.is_paused()
                        || held.contains(hash.as_ref())
                        || !slots.contains(hash.as_ref())
                    {
                        continue;
                    }
                    let stats = handle.stats();
                    // Finished torrents need no peers, and ones with no data
                    // yet gain nothing from a head start they would only
                    // spend on metadata.
                    if stats.progress_bytes == 0 || stats.progress_bytes >= stats.total_bytes {
                        continue;
                    }
                    match self.api.api_torrent_action_start(idx).await {
                        Ok(_) => {
                            debug!(info_hash = %hash, "pre-warming previously watched torrent");
                            resumed += 1;
                        }
                        Err(e) => debug!(info_hash = %hash, "could not pre-warm: {e}"),
                    }
                }
                None if prefetch.len() < self.browse_prefetch => {
                    prefetch.push(candidate.clone());
                }
                None => {}
            }
        }

        // Concurrently, because each one is a network round-trip of its own
        // and running them in sequence would mean the second candidate only
        // starts warming after the first has finished or timed out.
        let tasks: Vec<_> = prefetch
            .into_iter()
            .map(|candidate| {
                let engine = Arc::clone(self);
                tokio::spawn(async move { engine.prefetch_candidate(candidate).await })
            })
            .collect();
        for task in tasks {
            let _ = task.await;
        }
    }

    /// Fetches one unknown torrent's metadata and joins its swarm, ahead of
    /// the viewer picking it.
    async fn prefetch_candidate(self: &Arc<Self>, candidate: BrowseCandidate) {
        let magnet =
            resolver::magnet_with_trackers(&candidate.info_hash, None, &candidate.trackers);
        if let Err(e) = self
            .start_file(&magnet, candidate.file_idx, Vec::new())
            .await
        {
            debug!(info_hash = %candidate.info_hash, "could not prefetch candidate: {e:#}");
            return;
        }

        // Metadata only, deliberately never bytes. An earlier version also
        // opened an unguarded read to pull the head of the file onto disk --
        // but librqbit splits piece-download priority evenly across every
        // *open read* on a file (`iter_next_pieces` interleaves them), guard
        // or no guard. That read has no timeout on an individual `read()`
        // call, so it could still be sitting there, priority slot and all,
        // when the viewer's own request opened a second one a moment later --
        // silently halving the bandwidth going to the stream someone was
        // actually waiting on. Measured live: switching to a freshly-tapped
        // title stalled hard while a candidate nobody was watching anymore
        // kept a read open. Metadata + connected peers (this) is the part
        // that is safe to want ahead of time; bytes are not, because getting
        // them requires holding exactly the resource a real viewer needs
        // undiluted.
        //
        // Parking here rather than waiting for the idle reaper's next tick is
        // what keeps that bandwidth with the viewer once one exists.
        if let Ok(idx) = TorrentIdOrHash::parse(&candidate.info_hash) {
            let _ = self.api.api_torrent_action_pause(idx).await;
        }
        debug!(info_hash = %candidate.info_hash, "prefetched metadata only");
    }

    /// The session's handle for a hash, or `None` if it holds no such torrent.
    fn session_idx(&self, info_hash: &str) -> Option<TorrentIdOrHash> {
        let idx = TorrentIdOrHash::parse(info_hash).ok()?;
        self.api.session().get(idx).map(|_| idx)
    }

    /// The session's handle for a hash and how far along it is, captured in
    /// one lookup while the torrent still exists -- so a deletion can still
    /// say how much it threw away.
    fn session_progress(&self, info_hash: &str) -> Option<(TorrentIdOrHash, Progress)> {
        let idx = TorrentIdOrHash::parse(info_hash).ok()?;
        let handle = self.api.session().get(idx)?;
        let stats = handle.stats();
        Some((idx, Progress::of(stats.progress_bytes, stats.total_bytes)))
    }

    /// Records a request against a torrent, for the cache janitor, the idle
    /// reaper and the status views.
    ///
    /// Only for a torrent the gateway advertised or already holds. Called
    /// before the request is validated any further, and recording anything
    /// else let a client grow this map without limit by asking for made-up
    /// hashes -- each one a 404, and each one kept for the life of the
    /// process. `ACTIVE_STREAMS_CAPACITY` backstops the rest.
    pub fn touch_stream(&self, info_hash: &str, client: IpAddr) {
        let now = Instant::now();
        // A known torrent is updated in place: this runs on every chunk of
        // every body, so the common case must not allocate.
        if let Some(activity) = lock(&self.active_streams).get_mut(info_hash) {
            activity.last_access = now;
            activity.last_client = client;
            return;
        }
        if !self.is_advertised(info_hash) && self.session_idx(info_hash).is_none() {
            return;
        }

        let mut streams = lock(&self.active_streams);
        if streams.len() >= ACTIVE_STREAMS_CAPACITY && !streams.contains_key(info_hash) {
            let oldest = streams
                .iter()
                .min_by_key(|(_, activity)| activity.last_access)
                .map(|(hash, _)| hash.clone());
            if let Some(oldest) = oldest {
                streams.remove(&oldest);
            }
        }
        streams.insert(
            info_hash.to_string(),
            StreamActivity {
                last_access: now,
                last_client: client,
            },
        );
    }

    /// Whether this torrent has been read from within `within` -- used by the
    /// cache janitor to decide it must not delete the file out from under a
    /// player that is mid-playback (or about to issue its next range request).
    pub fn is_recently_active(&self, info_hash: &str, within: Duration) -> bool {
        lock(&self.active_streams)
            .get(info_hash)
            .is_some_and(|a| a.last_access.elapsed() < within)
    }

    /// Every torrent read from within `within`, for a pass that would
    /// otherwise ask `is_recently_active` once per torrent.
    pub fn recently_active(&self, within: Duration) -> HashSet<String> {
        lock(&self.active_streams)
            .iter()
            .filter(|(_, a)| a.last_access.elapsed() < within)
            .map(|(hash, _)| hash.clone())
            .collect()
    }

    /// Snapshot of recently-active streams for the `/health` and terminal
    /// monitoring views: (info_hash, last client IP, seconds since last request).
    pub fn recent_streams(&self) -> Vec<(String, IpAddr, u64)> {
        lock(&self.active_streams)
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
        // Copied out first: see the lock notes on `TorrentEngine`.
        let held = self.held_snapshot();
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
                let info_hash = handle.info_hash().as_string();
                ActiveTorrentSummary {
                    held: held.contains(&info_hash),
                    info_hash,
                    name: handle.name().unwrap_or_else(|| "unknown".to_string()),
                    state: stats.state.to_string(),
                    progress_percent: Progress::of(stats.progress_bytes, stats.total_bytes).percent,
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

    /// Whether this torrent is actively fetching bytes: present in the
    /// session, unpaused, and not yet complete.
    ///
    /// This is the signal the cache janitor's retention rule needs, and
    /// [`is_running`](Self::is_running) cannot stand in for it in that
    /// direction: a *finished* torrent sits unpaused ("live") forever, so
    /// sparing everything running would exempt exactly the watched-and-done
    /// backlog retention exists to throw away.
    ///
    /// The reverse mistake is the one that matters now that several titles
    /// download at once. A queued download has no reader and issues no HTTP
    /// requests -- nobody is watching it yet, that is the whole point -- so
    /// by the janitor's other two tests it looks like cold backlog, and
    /// retention would delete it while it was still being written.
    pub fn is_downloading(&self, info_hash: &str) -> bool {
        let Ok(idx) = TorrentIdOrHash::parse(info_hash) else {
            return false;
        };
        self.api.session().get(idx).is_some_and(|handle| {
            if handle.is_paused() {
                return false;
            }
            let stats = handle.stats();
            stats.total_bytes == 0 || stats.progress_bytes < stats.total_bytes
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two callers racing to start the same torrent must contend on one lock,
    /// or the second one launches a duplicate metadata fetch -- which is
    /// exactly the case this exists for (a viewer tapping a title whose
    /// prefetch is still resolving).
    #[test]
    fn the_same_torrent_always_yields_the_same_start_lock() {
        let mut starts = HashMap::new();
        let first = lock_for(&mut starts, "aaaa");
        let second = lock_for(&mut starts, "aaaa");
        assert!(
            Arc::ptr_eq(&first, &second),
            "both starts of one torrent must contend on a single lock"
        );

        let other = lock_for(&mut starts, "bbbb");
        assert!(
            !Arc::ptr_eq(&first, &other),
            "unrelated torrents must start concurrently, not queue behind each other"
        );
    }

    #[test]
    fn start_locks_are_reclaimed_but_never_out_from_under_a_holder() {
        let mut starts = HashMap::new();
        let held = lock_for(&mut starts, "held");
        drop(lock_for(&mut starts, "finished"));

        // Passing over the map reclaims the finished entry; the held one --
        // someone is still inside its critical section -- must survive, or
        // two callers would end up with different locks for one torrent.
        let _ = lock_for(&mut starts, "unrelated");
        assert!(starts.contains_key("held"));
        assert!(
            !starts.contains_key("finished"),
            "a browsing session would otherwise grow this map without bound"
        );

        assert!(Arc::ptr_eq(&lock_for(&mut starts, "held"), &held));
    }

    /// A hash that just failed a peer search must refuse a second one until
    /// the cooldown elapses -- this is the whole point: a client
    /// auto-retrying a dead stream gets an instant failure instead of paying
    /// the full search again on every retry.
    #[test]
    fn a_hash_that_just_failed_is_on_cooldown() {
        let mut failures = HashMap::new();
        let cooldown = Duration::from_millis(50);
        failures.insert("dead".to_string(), Instant::now());

        let remaining = remaining_cooldown(&mut failures, "dead", cooldown);
        assert!(
            remaining.is_some_and(|r| r <= cooldown && r > Duration::ZERO),
            "a fresh failure must report time left, not none and not the full window"
        );
        assert_eq!(
            remaining_cooldown(&mut failures, "healthy", cooldown),
            None,
            "a hash that never failed must search normally"
        );
    }

    #[test]
    fn cooldown_expires_and_is_pruned() {
        let mut failures = HashMap::new();
        let cooldown = Duration::from_millis(20);
        failures.insert("dead".to_string(), Instant::now());
        std::thread::sleep(Duration::from_millis(40));

        assert_eq!(
            remaining_cooldown(&mut failures, "dead", cooldown),
            None,
            "an expired cooldown must allow a fresh search, not refuse forever"
        );
        assert!(
            failures.is_empty(),
            "checking must prune the expired entry, or this map grows without bound"
        );
    }

    /// The grace window has to outlast a player's retry cycle (~25-30s, the
    /// same figure `START_FAILURE_COOLDOWN` is built on) or it spares nothing
    /// that matters -- the retry arrives after the torrent is already gone.
    #[test]
    fn abandon_grace_outlasts_a_player_retry_cycle() {
        assert!(
            ABANDON_GRACE >= START_FAILURE_COOLDOWN,
            "a torrent must not be deleted while the client is still in the \
             retry cycle that would reuse it"
        );
    }

    /// The background search must outlast the request that started it, or the
    /// fix is not a fix: a 25-second foreground wait is shorter than a
    /// trackerless DHT lookup usually takes, so cancelling on that boundary is
    /// what made a merely-slow release look permanently dead.
    #[test]
    fn a_swarm_search_may_run_far_longer_than_the_request_that_started_it() {
        assert!(
            BACKGROUND_SEARCH_TIMEOUT > ADD_TORRENT_TIMEOUT,
            "a search that dies with its request cannot find anything the \
             request could not have found itself"
        );
    }

    /// The cooldown only makes sense once a search has actually concluded. If
    /// it could outlast the search window, a hash would sit refused while
    /// nothing was looking for it -- the stall this whole path exists to end.
    #[test]
    fn the_failure_cooldown_never_outlives_the_search_that_justifies_it() {
        assert!(START_FAILURE_COOLDOWN < BACKGROUND_SEARCH_TIMEOUT);
    }

    /// The cold-start budget at the top of this file, checked rather than
    /// only written down: with the default pre-buffer timeout, a start that
    /// uses every one of its allowances still answers before the router's
    /// timeout turns it into a 504.
    #[test]
    fn a_worst_case_cold_start_fits_inside_the_request_timeout() {
        let tuning = StreamTuning::from_config(&AppConfig::defaults());
        assert!(
            ADD_TORRENT_TIMEOUT + INITIALIZE_TIMEOUT + tuning.prebuffer_timeout
                < crate::REQUEST_TIMEOUT
        );
    }

    #[test]
    fn progress_agrees_on_what_finished_means() {
        assert!(Progress::of(100, 100).finished);
        assert!(!Progress::of(99, 100).finished);
        assert!(
            !Progress::of(0, 0).finished,
            "a torrent whose size is not known yet has not finished anything"
        );
        assert_eq!(Progress::of(0, 0).percent, 0.0);
        assert_eq!(Progress::of(50, 200).percent, 25.0);
    }

    #[test]
    fn hashes_are_lowercased_without_copying_when_already_lowercase() {
        assert!(matches!(lowercase("abcdef0123"), Cow::Borrowed(_)));
        assert_eq!(lowercase("ABCDEF0123"), "abcdef0123");
    }
}
