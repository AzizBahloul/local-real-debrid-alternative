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
use std::io::SeekFrom;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use librqbit::api::TorrentIdOrHash;
use librqbit::limits::LimitsConfig;
use librqbit::{
    AddTorrent, AddTorrentOptions, AddTorrentResponse, Api, DhtSessionConfig, ListenerMode,
    ListenerOptions, Session, SessionOptions, SessionPersistenceConfig,
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
/// for the purpose of *deleting* it — see `action_for_abandoned`.
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


/// How many recently-advertised info hashes stay startable. Generous relative
/// to how many streams a browse session shows (15 per title by default).
const ADVERTISED_HASH_CAPACITY: usize = 512;

/// How many resolved torrents keep their metadata in memory for a later start.
///
/// One entry is a whole `.torrent` file (chiefly the piece hashes: 20 bytes
/// per piece, so a few hundred KB for a large release), which is why this is
/// small rather than generous — in memory it only has to bridge the gap
/// between reading a stream list and tapping one of its rows. The on-disk
/// archive behind it (`METADATA_ARCHIVE_CAPACITY`) is what makes a title
/// playable weeks later. See `MetadataCache`.
const RESOLVED_METADATA_CAPACITY: usize = 16;

/// How many `.torrent` blobs the on-disk archive keeps.
///
/// Sized against the advertised set rather than against memory: an advertised
/// hash is a link a client may still tap, and the whole point of the archive
/// is that tapping one never costs a swarm lookup. At a few hundred KB each
/// the worst case is well under a gigabyte, against a cache measured in tens
/// of them. See `MetadataCache::archive_dir`.
const METADATA_ARCHIVE_CAPACITY: usize = 512;

/// Cap on how many already-known torrents a single browse will wake up. A
/// title's stream list can name a dozen releases we have partial data for;
/// resuming all of them would have them compete for the same upstream
/// bandwidth, which is the exact problem the idle reaper exists to prevent.
const MAX_WARM_ON_BROWSE: usize = 3;

use crate::config::AppConfig;
use resolver::{
    info_hash_from_magnet, is_hex40, is_video_file, suggest_video_file, MagnetSource,
    ResolvedTorrent, TorrentFile, TorrentSource,
};

/// Torrent metadata already fetched from the swarm, keyed by info hash.
///
/// Resolving a magnet is the expensive half of a cold start: the magnet
/// carries only an info hash, so the file list has to be pulled from a peer
/// that holds it, and `list_only` (how `resolve` asks for it) deliberately
/// leaves nothing behind in the session. Every caller that resolves goes on to
/// *start* the same torrent moments later — `/play` immediately, the addon's
/// magnet path when the viewer taps the row — and without this that start
/// pays the identical fetch a second time.
///
/// What is stored is the full `.torrent` blob librqbit assembled from the
/// fetched info (trackers included), which `AddTorrent::from_bytes` takes
/// directly. Adding from it needs no peers at all: the metadata step goes from
/// a network round-trip to a parse.
///
/// Bounded, newest last, oldest evicted — a miss costs only the fetch that
/// would have happened anyway, or a read from `MetadataArchive` behind it.
#[derive(Default)]
struct MetadataCache {
    order: VecDeque<String>,
    bytes: HashMap<String, bytes::Bytes>,
}

impl MetadataCache {
    fn remember(&mut self, info_hash: String, torrent_bytes: bytes::Bytes) {
        if torrent_bytes.is_empty() {
            return;
        }
        if self.bytes.insert(info_hash.clone(), torrent_bytes).is_some() {
            return;
        }
        self.order.push_back(info_hash);
        while self.order.len() > RESOLVED_METADATA_CAPACITY {
            if let Some(oldest) = self.order.pop_front() {
                self.bytes.remove(&oldest);
            }
        }
    }

    fn get(&self, info_hash: &str) -> Option<bytes::Bytes> {
        self.bytes.get(info_hash).cloned()
    }
}

/// The same `.torrent` blobs as `MetadataCache`, kept on disk so they outlive
/// both the process and the cache janitor.
///
/// This exists because of a specific, reproducible dead end. Replaying a title
/// watched days ago has to re-add its torrent, and everything that made the
/// first play fast is gone by then: the files were reclaimed by the retention
/// sweep, librqbit deletes its own `<hash>.torrent` alongside the torrent it
/// belongs to, and the in-memory cache above did not survive the restart. So
/// the add falls back to resolving the magnet from the swarm — and a magnet
/// carries nothing but an info hash, so that is a DHT lookup against a release
/// whose seeders have since moved on. It times out at `ADD_TORRENT_TIMEOUT`,
/// the torrent never enters the session, and from the sofa the gateway simply
/// does not react to pressing play.
///
/// A `.torrent` blob is the cure because it is the *whole* answer to that
/// lookup: piece hashes, file list, trackers. `AddTorrent::from_bytes` needs no
/// peers at all, so the torrent joins the session immediately and shows up as
/// downloading, and finding seeders becomes a background concern instead of a
/// precondition for reacting to the tap.
///
/// Deliberately its own subdirectory: librqbit writes `<hash>.torrent` into the
/// session directory itself and removes it when the torrent leaves the session,
/// which is exactly the deletion this archive exists to survive.
#[derive(Clone)]
struct MetadataArchive {
    dir: PathBuf,
}

impl MetadataArchive {
    fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    fn path_for(&self, info_hash: &str) -> PathBuf {
        self.dir.join(format!("{info_hash}.torrent"))
    }

    /// Best effort throughout: a missing or unreadable archive only costs the
    /// swarm lookup that would have happened without it, so nothing here is
    /// worth failing a request over.
    async fn load(&self, info_hash: &str) -> Option<bytes::Bytes> {
        if !is_hex40(info_hash) {
            return None;
        }
        let path = self.path_for(info_hash);
        let bytes = tokio::fs::read(&path).await.ok()?;
        if bytes.is_empty() {
            return None;
        }
        // Re-stamp on use so pruning evicts by last *use*, not by when the
        // blob was first written -- a series you keep coming back to should
        // not age out behind titles you opened once.
        touch(&path).await;
        Some(bytes::Bytes::from(bytes))
    }

    async fn store(&self, info_hash: &str, torrent_bytes: &bytes::Bytes) {
        if torrent_bytes.is_empty() || !is_hex40(info_hash) {
            return;
        }
        if let Err(e) = tokio::fs::create_dir_all(&self.dir).await {
            debug!("could not create torrent metadata archive: {e}");
            return;
        }
        let path = self.path_for(info_hash);
        if let Err(e) = tokio::fs::write(&path, torrent_bytes.as_ref()).await {
            debug!("could not archive torrent metadata: {e}");
            return;
        }
        self.prune().await;
    }

    /// Drops the least recently used blobs once the archive outgrows
    /// `METADATA_ARCHIVE_CAPACITY`.
    async fn prune(&self) {
        let Ok(mut dir) = tokio::fs::read_dir(&self.dir).await else {
            return;
        };
        let mut entries: Vec<(std::time::SystemTime, PathBuf)> = Vec::new();
        while let Ok(Some(entry)) = dir.next_entry().await {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("torrent") {
                continue;
            }
            let used = entry
                .metadata()
                .await
                .ok()
                .and_then(|m| m.modified().ok())
                .unwrap_or(std::time::UNIX_EPOCH);
            entries.push((used, path));
        }
        if entries.len() <= METADATA_ARCHIVE_CAPACITY {
            return;
        }
        entries.sort_by(|a, b| b.0.cmp(&a.0));
        for (_, path) in entries.split_off(METADATA_ARCHIVE_CAPACITY) {
            let _ = tokio::fs::remove_file(&path).await;
        }
    }
}

/// A live torrent's assembled `.torrent` blob, or `None` if the session does
/// not hold it or has not resolved it yet.
fn session_metadata(api: &Api, info_hash: &str) -> Option<bytes::Bytes> {
    let idx = TorrentIdOrHash::parse(info_hash).ok()?;
    let handle = api.session().get(idx)?;
    handle.with_metadata(|m| m.torrent_bytes.clone()).ok()
}

/// Stamps a file as used just now, so `MetadataArchive::prune` evicts by last
/// use rather than by first write. Best effort: a failed stamp only means the
/// blob ages from when it was written, which is the behaviour without this.
async fn touch(path: &Path) {
    let path = path.to_path_buf();
    let _ = tokio::task::spawn_blocking(move || {
        let now = std::time::SystemTime::now();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .and_then(|f| f.set_times(std::fs::FileTimes::new().set_modified(now)))
    })
    .await;
}

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

/// How much of `cooldown` is left since `info_hash` was marked in `failures`,
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

/// Hands out the start lock for one info hash, creating it on first use.
///
/// Every caller for the same hash must get the *same* `Arc`, or the lock
/// guards nothing. Entries whose only remaining owner is the map are dropped
/// as we pass over them, so a long browsing session cannot grow this without
/// bound -- while a lock somebody is currently holding or waiting on (strong
/// count above one) is never removed.
fn lock_for(starts: &mut HashMap<String, Arc<Mutex<()>>>, info_hash: &str) -> Arc<Mutex<()>> {
    starts.retain(|hash, lock| hash == info_hash || Arc::strong_count(lock) > 1);
    Arc::clone(
        starts
            .entry(info_hash.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(()))),
    )
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

/// librqbit's per-stream look-ahead window, from
/// `torrent_state::streaming::PER_STREAM_BUF_DEFAULT`.
///
/// Not configurable from outside the library, which is the whole reason
/// `spawn_readahead` exists: the only way to prioritise further ahead than
/// this is to hold a second read positioned there.
const LIBRQBIT_STREAM_WINDOW: u64 = 32 * 1024 * 1024;

/// How much of an mp4's tail the warmer pulls. One piece is enough to make
/// the range request the player is about to issue land on data already here;
/// this is comfortably over the largest piece size in practice.
const TAIL_WARM_BYTES: u64 = 24 * 1024 * 1024;

/// How long the tail warmer waits for those bytes before giving up. Generous
/// because it costs nothing to wait -- it is a background read nobody is
/// watching, and once its pieces arrive it stops asking for anything.
const TAIL_WARM_TIMEOUT: Duration = Duration::from_secs(120);

/// Containers whose players read the tail of the file before they will play
/// the head.
///
/// For mp4/m4v/mov this is the `moov` atom, and it is a certainty on a
/// non-faststart file. mkv/webm are here for a measured reason rather than a
/// structural one: Matroska puts its SeekHead at the front, so the theory says
/// it needs none of this — but muxers routinely write the Cues at the end, and
/// players probe for them anyway. On 2026-09-07 a 1.16 GB mkv start spent
/// **12.2 s of its 20 s** on exactly one such request (`bytes=1162008830-`,
/// the last 21 KB), serialised after the head read instead of beside it. The
/// warmer costs one piece when it guesses wrong, and that trade is not close.
const TAIL_INDEXED_EXTENSIONS: [&str; 5] = ["mp4", "m4v", "mov", "mkv", "webm"];

/// How often the read-ahead claim is re-pointed at the current position.
const READAHEAD_REFRESH: Duration = Duration::from_secs(5);

/// How far the playback position must move before the read-ahead claim is
/// re-opened. Re-opening is a real API call, so it is not done per refresh.
const READAHEAD_ADVANCE_BYTES: u64 = 8 * 1024 * 1024;

/// A one-way "stop what you are doing" signal for a single response.
///
/// Built on a closed semaphore rather than a flag plus a `Notify` because the
/// obvious version of that has a race: check the flag, find it clear, await
/// the notification, and miss the one that fired in between. A semaphore with
/// no permits can only ever resolve by being closed, and closing is
/// idempotent, observable, and cannot be missed by a late waiter.
pub struct ReaderCancel(Semaphore);

impl ReaderCancel {
    fn new() -> Self {
        Self(Semaphore::new(0))
    }

    /// A signal that will never fire, for callers with nothing to supersede
    /// them (tests, and any future non-HTTP reader).
    pub fn never_cancelled() -> Self {
        Self::new()
    }

    /// Fires the signal by hand. Only for tests -- in the gateway itself the
    /// decision to supersede belongs to `register_reader`, which is the one
    /// place that can see every reader and apply the same rules to all of them.
    #[cfg(test)]
    pub fn cancel_for_test(&self) {
        self.cancel();
    }

    fn cancel(&self) {
        self.0.close();
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.is_closed()
    }

    /// Resolves when, and only when, this reader has been superseded.
    pub async fn cancelled(&self) {
        let _ = self.0.acquire().await;
    }
}

/// One in-flight video response, as far as deciding who still deserves piece
/// priority is concerned.
struct ReaderSlot {
    id: u64,
    info_hash: String,
    file_idx: usize,
    /// Superseding is scoped to one client, so a second device starting the
    /// same title cannot cancel the first device's cold start (and be
    /// cancelled in turn by its retry, forever).
    client: IpAddr,
    /// An index/tail probe. Exempt, because it is a second read the same
    /// player genuinely needs at the same time, not a superseded seek.
    tail_probe: bool,
    served: Arc<AtomicU64>,
    cancel: Arc<ReaderCancel>,
}

impl ReaderSlot {
    /// Whether a new read of `(info_hash, file_idx)` from `client` makes this
    /// one redundant.
    ///
    /// Every clause is load-bearing; see `register_reader` for why each one is
    /// there and what breaks without it.
    fn superseded_by(&self, info_hash: &str, file_idx: usize, client: IpAddr) -> bool {
        self.info_hash == info_hash
            && self.file_idx == file_idx
            && self.client == client
            && !self.tail_probe
            && self.served.load(Ordering::Relaxed) == 0
    }
}

/// Registration of a live video response. Held for as long as the response is,
/// and removed from the registry when dropped.
pub struct ReaderTicket {
    engine: Arc<TorrentEngine>,
    id: u64,
    served: Arc<AtomicU64>,
    cancel: Arc<ReaderCancel>,
}

impl ReaderTicket {
    /// The signal to stop. Selected on by the pre-buffer and by the response
    /// body, which are the two places a superseded read would otherwise sit
    /// holding piece priority it is never going to use.
    pub fn cancel_handle(&self) -> Arc<ReaderCancel> {
        Arc::clone(&self.cancel)
    }

    /// Reports progress. A reader that has delivered even one byte is someone
    /// watching something and is never superseded.
    pub fn record_served(&self, bytes: u64) {
        self.served.fetch_add(bytes, Ordering::Relaxed);
    }
}

impl Drop for ReaderTicket {
    fn drop(&mut self) {
        self.engine
            .readers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|slot| slot.id != self.id);
    }
}

/// Keeps an extra read-ahead claim alive for the life of one response.
///
/// Dropping it aborts the task, which drops the read it was holding, which is
/// what removes the claim from librqbit's priority set. There is no other
/// unregister step — the claim *is* the open read.
pub struct ReadaheadHandle(tokio::task::JoinHandle<()>);

impl Drop for ReadaheadHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Whether this filename names a container whose player is likely to read the
/// tail before it will play the head — see `TAIL_INDEXED_EXTENSIONS` for why
/// that is a claim about observed player behaviour, not about the format.
fn has_tail_index(name: &str) -> bool {
    name.rsplit('.')
        .next()
        .is_some_and(|ext| TAIL_INDEXED_EXTENSIONS.contains(&ext.to_ascii_lowercase().as_str()))
}

/// Where a byte offset sits relative to the piece that contains it.
///
/// The piece is the unit BitTorrent actually transfers, so this is the shape
/// of the floor under every seek: nothing at `offset` can be read until the
/// whole piece holding it has arrived and passed its hash check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PieceGeometry {
    pub piece_len: u64,
    /// Bytes from the requested offset to the end of its piece, clamped to the
    /// end of the file.
    pub remainder: u64,
}

pub struct TorrentEngine {
    api: Api,
    /// How an identifier turns into a magnet link. Only `MagnetSource` today;
    /// swapping in a different provider (e.g. resolving a `.torrent` file URL)
    /// only means constructing `TorrentEngine` with a different `Box` here.
    source: Box<dyn TorrentSource>,
    max_concurrent: Arc<Semaphore>,
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
    /// How many unknown torrents a single browse may start fetching metadata
    /// for. See `warm_for_browse`.
    browse_prefetch: usize,
    /// How many torrents may download at once. See `download_slots`.
    max_active_downloads: usize,
    /// One lock per info hash being started, so the same torrent is never
    /// added twice at once. See `start_lock`.
    starts: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    /// Metadata from torrents already resolved, so starting one does not
    /// re-fetch what a resolve just pulled. See `MetadataCache`.
    resolved_metadata: Mutex<MetadataCache>,
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
    /// Files whose trailing index has already been warmed, so the warmer runs
    /// once per file rather than once per seek.
    tail_warmed: StdMutex<HashSet<(String, usize)>>,
    seek_supersede: bool,
    mp4_tail_warm: bool,
    /// Extra read-ahead to claim past librqbit's own window, in bytes. 0 off.
    readahead_extra: u64,
    readahead_settle: Duration,
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
/// Three exemptions, and they are not stylistic:
///
/// * an **open stream** means some response body is still reading those bytes
///   (a second device, or this player's own header/index reads). Deleting
///   underneath it truncates a video someone is watching — see `StreamGuard`.
/// * **recent activity** means someone was reading it moments ago and has not
///   had time to come back. An open connection is not the same question as
///   "is anyone watching this": range requests are stateless, so there is no
///   connection at all between a seek and the next read, and none while a
///   player that just timed out prepares its retry. `StreamActivity` says this
///   outright, and the idle reaper already honours it — this path did not, so
///   the *irreversible* action was judging liveness more loosely than the
///   reversible one. That is what made a failed play destructive: a player
///   giving up on a cold torrent closed its connection, the next stream it
///   tried took focus, and the torrent it had just spent 15 seconds warming
///   was deleted along with every byte and every peer it had found. Each
///   attempt therefore started colder than the last, which is what a viewer
///   sees as the player cycling through every source and playing none.
/// * a **finished** torrent is a complete file. It costs no download
///   bandwidth, and throwing away a fully-downloaded movie because the viewer
///   started the next episode is destructive in a way nobody asks for. The
///   cache janitor already reclaims those, oldest-first, once the cap forces
///   it.
fn action_for_abandoned(
    has_open_stream: bool,
    recently_active: bool,
    finished: bool,
) -> UnfocusedAction {
    if has_open_stream || recently_active || finished {
        return UnfocusedAction::Leave;
    }
    UnfocusedAction::Discard
}

/// Picks the torrents allowed to download, given the unfinished ones in
/// admission order (oldest first). Pure, so the priority rules can be tested
/// without a session; see [`TorrentEngine::download_slots`] for what each rule
/// is for.
///
/// The cap is a target, not a ceiling: readers and the focused title are
/// admitted first and can push the set past it. Both are cases where pausing
/// the torrent is either impossible without freezing a live reader or
/// obviously wrong, so reporting a smaller set would be a lie about what is
/// using the line rather than a restriction on it.
fn choose_download_slots(
    unfinished: &[(usize, String)],
    focused: Option<&str>,
    has_open_stream: impl Fn(&str) -> bool,
    cap: usize,
) -> HashSet<String> {
    let mut slots: HashSet<String> = HashSet::new();

    for (_, hash) in unfinished {
        if has_open_stream(hash) {
            slots.insert(hash.clone());
        }
    }
    if let Some(focused) = focused {
        // Only if it is genuinely unfinished. A finished focus needs no slot,
        // and holding one for it would shrink the queue by one for as long as
        // the viewer stayed on that episode.
        if unfinished.iter().any(|(_, hash)| hash == focused) {
            slots.insert(focused.to_string());
        }
    }
    for (_, hash) in unfinished {
        if slots.len() >= cap {
            break;
        }
        slots.insert(hash.clone());
    }

    slots
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
        let session = match Session::new_with_opts(config.downloads_dir(), make_opts(config.peer_port))
            .await
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
        let remembered_advertised = Self::load_advertised(&advertised_path).await;
        if !remembered_advertised.is_empty() {
            info!(
                count = remembered_advertised.len(),
                "restored advertised info hashes, so links already in a client keep working"
            );
        }

        let engine = Arc::new(Self {
            api,
            source: Box::new(MagnetSource),
            max_concurrent: Arc::new(Semaphore::new(config.max_concurrent_torrents.max(1))),
            // At least one, or nothing could ever download -- including the
            // title on screen, which would look like the gateway hanging.
            max_active_downloads: config.max_active_downloads.max(1),
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
            browse_prefetch: config.browse_prefetch_count,
            starts: Mutex::new(HashMap::new()),
            resolved_metadata: Mutex::new(MetadataCache::default()),
            metadata_archive: MetadataArchive::new(config.session_state_dir().join("metadata")),
            failed_starts: Arc::new(StdMutex::new(HashMap::new())),
            searching: Arc::new(StdMutex::new(HashSet::new())),
            held: StdMutex::new(HashSet::new()),
            audit: StdMutex::new(crate::audit::AuditLog::disabled()),
            readers: StdMutex::new(Vec::new()),
            next_reader_id: AtomicU64::new(0),
            tail_warmed: StdMutex::new(HashSet::new()),
            seek_supersede: config.seek_supersede,
            mp4_tail_warm: config.mp4_tail_warm,
            readahead_extra: config.readahead_extra_mb.saturating_mul(1024 * 1024),
            readahead_settle: Duration::from_secs(config.readahead_settle_secs),
            cold_start_peer_limit: (config.cold_start_peer_limit > 0)
                .then_some(config.cold_start_peer_limit),
        });

        // Bank what librqbit just restored. These blobs are already in memory
        // and are about to become unreachable: librqbit deletes its own copy
        // when the janitor removes the torrent, and that is exactly the title
        // whose replay next month would otherwise need the swarm. Doing it
        // here rather than only on play makes the archive cover the library
        // that already exists, not just what gets watched from now on.
        engine.archive_session_metadata().await;

        Ok(engine)
    }

    /// Copies every torrent currently in the session into the metadata
    /// archive. Best effort, and cheap: the bytes are already resident.
    async fn archive_session_metadata(&self) {
        let known: Vec<String> = self
            .api
            .session()
            .with_torrents(|iter| iter.map(|(_, h)| h.info_hash().as_string()).collect());
        let mut archived = 0;
        for info_hash in known {
            if self.metadata_archive.load(&info_hash).await.is_some() {
                continue; // already banked by an earlier run
            }
            self.archive_metadata(&info_hash).await;
            archived += 1;
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
        *self
            .audit
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = log;
    }

    pub fn audit(&self) -> crate::audit::AuditLog {
        self.audit
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Fetches torrent metadata only (fast, no data downloaded) and lists its files.
    ///
    /// Goes through `Session::add_torrent` rather than the `Api` wrapper for
    /// one reason: the wrapper discards the assembled `.torrent` bytes, and
    /// those are exactly what lets the subsequent start skip a second
    /// metadata fetch. See `MetadataCache`.
    pub async fn resolve(&self, input: &str) -> Result<ResolvedTorrent> {
        let magnet = self.source.to_magnet(input)?;
        let info_hash = info_hash_from_magnet(&magnet)?;

        let opts = AddTorrentOptions {
            list_only: true,
            ..Default::default()
        };
        let response = self
            .api
            .session()
            .add_torrent(AddTorrent::from_url(magnet.clone()), Some(opts))
            .await
            .context(
                "failed to fetch torrent metadata (no peers found yet, or invalid torrent?)",
            )?;

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
        self.metadata_archive
            .store(&info_hash, &listed.torrent_bytes)
            .await;
        self.resolved_metadata
            .lock()
            .await
            .remember(info_hash.clone(), listed.torrent_bytes);

        Ok(ResolvedTorrent {
            info_hash,
            name,
            magnet,
            files,
            suggested_file_idx,
            seen_peers: listed.seen_peers,
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
        let start_lock = self.start_lock(&info_hash).await;
        let _start_guard = start_lock.lock().await;

        // Playing a title is the clearest possible statement that its hand
        // pause is over, and clearing it here means the resume below is not
        // undone by the download queue a second later.
        self.forget_hold(&info_hash);

        // Whoever held the lock may have finished the job while we waited.
        let idx = TorrentIdOrHash::parse(&info_hash)
            .map_err(|e| anyhow::anyhow!("invalid info hash {info_hash}: {e}"))?;
        if let Some(handle) = self.api.session().get(idx) {
            if handle.is_paused() {
                let _ = self.api.api_torrent_action_start(idx).await;
            }
            self.wait_until_streamable(&info_hash, INITIALIZE_TIMEOUT)
                .await?;
            return Ok(info_hash);
        }

        // Metadata this gateway already pulled for exactly this hash. Adding
        // from it resolves nothing over the network, so the slowest step of a
        // cold start disappears entirely -- which is the whole point of
        // keeping it. Memory first, then the on-disk archive, which is what
        // covers a title whose files the janitor reclaimed days ago. See
        // `MetadataCache` and `MetadataArchive`.
        let cached_metadata = match self.resolved_metadata.lock().await.get(&info_hash) {
            Some(bytes) => Some(bytes),
            None => self.metadata_archive.load(&info_hash).await,
        };

        // Past the warm-path return above, so everything from here is a cold
        // start and worth a record however it ends.
        phases.was_cold = true;
        phases.from_cached_metadata = cached_metadata.is_some();

        // A search already running for this hash is joined, not duplicated:
        // the answer it is about to produce is the same answer this request
        // wants, and a second concurrent search only splits the same peers
        // between two lookups. Reported as "still looking", because that is
        // what is true -- see `search_in_flight`.
        if self.search_in_flight(&info_hash) {
            anyhow::bail!(
                "still searching the swarm for this release -- it keeps looking in the \
                 background, so try again in a moment"
            );
        }

        // A hash whose search just came back empty is not asked again for a
        // short while, so a client auto-retrying a dead stream (a resumed
        // "continue watching", or its own error recovery) gets an instant
        // answer rather than queueing another full search behind the last
        // one. Skipped when we already hold the metadata, since that cooldown
        // exists to avoid re-running a search this add does not need to run.
        // See `START_FAILURE_COOLDOWN`.
        if let Some(remaining) = self
            .recent_start_failure(&info_hash)
            .filter(|_| cached_metadata.is_none())
        {
            anyhow::bail!(
                "found no peers {}s ago; waiting {}s before searching again -- if this \
                 release stays dead, pick another source for the same episode",
                (START_FAILURE_COOLDOWN - remaining).as_secs(),
                remaining.as_secs()
            );
        }

        // Throttles how many torrents can be *added* concurrently (each add
        // does a burst of tracker/DHT/peer-handshake work) -- not how many
        // can stream at once, so the permit is released as soon as the add
        // returns, background or not.
        let permit = Arc::clone(&self.max_concurrent)
            .acquire_owned()
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
            // Overrides the session-wide cap for this torrent only, and only
            // when asked for. `None` keeps the session default. librqbit reads
            // this once, here, so it cannot be walked back down after the
            // start -- see the field docs on `cold_start_peer_limit`.
            peer_limit: self.cold_start_peer_limit,
            ..Default::default()
        };

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
            // Still going. Deliberately no `record_start_failure`: nothing has
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

    /// Whether a background metadata search is already running for this hash.
    fn search_in_flight(&self, info_hash: &str) -> bool {
        self.searching
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(info_hash)
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
    fn spawn_search(
        &self,
        info_hash: String,
        source: AddTorrent<'static>,
        opts: AddTorrentOptions,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> tokio::task::JoinHandle<Result<()>> {
        self.searching
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(info_hash.clone());

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
            searching
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&info_hash);

            let failed = |reason: &str| {
                failed_starts
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert(info_hash.clone(), Instant::now());
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
            failed_starts
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&info_hash);

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

    /// Copies a live torrent's assembled `.torrent` blob into the archive.
    ///
    /// Reads it off the session rather than off the add call because that is
    /// the one place it exists whatever route the torrent took in -- magnet,
    /// cached bytes, or restored from librqbit's own persistence.
    async fn archive_metadata(&self, info_hash: &str) {
        let Ok(idx) = TorrentIdOrHash::parse(info_hash) else {
            return;
        };
        let Some(handle) = self.api.session().get(idx) else {
            return;
        };
        let Ok(torrent_bytes) = handle.with_metadata(|m| m.torrent_bytes.clone()) else {
            return; // still resolving; the next start banks it instead
        };
        self.metadata_archive
            .store(info_hash, &torrent_bytes)
            .await;
        self.resolved_metadata
            .lock()
            .await
            .remember(info_hash.to_string(), torrent_bytes);
    }

    /// How much of `START_FAILURE_COOLDOWN` is left for this hash, or `None`
    /// if it never failed or the cooldown has already elapsed. Also prunes
    /// every expired entry it passes over, so this map cannot grow for the
    /// life of the process.
    fn recent_start_failure(&self, info_hash: &str) -> Option<Duration> {
        remaining_cooldown(
            &mut self
                .failed_starts
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            info_hash,
            START_FAILURE_COOLDOWN,
        )
    }

    // Recording and clearing a start failure now happens inside the background
    // search task (see `spawn_search`), which is the only thing that knows how
    // the search actually ended -- the request that kicked it off has usually
    // timed out and gone by then.

    /// The lock guarding starts of one particular torrent, creating it on
    /// first use.
    ///
    /// Entries whose only remaining owner is the map itself are dropped as we
    /// pass over them, so a long browsing session cannot grow this without
    /// bound while a lock currently being waited on is never removed.
    async fn start_lock(&self, info_hash: &str) -> Arc<Mutex<()>> {
        lock_for(&mut *self.starts.lock().await, info_hash)
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
            // Resume anything the idle reaper (or a prefetch that decided
            // this was not the pick) paused. Cheap to check, and without it a
            // torrent you come back to would serve only the bytes already on
            // disk and then stall forever.
            if handle.is_paused() {
                debug!(%info_hash, "resuming paused torrent for a new request");
                self.api
                    .api_torrent_action_start(idx)
                    .await
                    .map_err(|e| anyhow::anyhow!("failed to resume torrent: {e}"))?;
                // Deliberately does NOT wait for peers to reconnect here. An
                // earlier version did, reasoning that a resumed torrent has
                // zero peers and would stall the moment playback caught up to
                // what was on disk. But most resumes are of a torrent that
                // *has* data on disk -- a part-watched episode, or a finished
                // one -- and those start playing instantly from that data
                // while peers reconnect in the background. Blocking here made
                // every one of them wait seconds for bytes it already had.
                // A genuinely empty resume is handled where it belongs, by
                // the pre-buffer and the stall/re-open path.
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

        let superseded = {
            let mut readers = self
                .readers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            let superseded: Vec<Arc<ReaderCancel>> = if self.seek_supersede && !tail_probe {
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

    /// The name of one file inside a torrent, for deciding what container it
    /// is. `None` when the torrent or index is unknown.
    pub fn file_name(&self, info_hash: &str, file_idx: usize) -> Option<String> {
        let idx = TorrentIdOrHash::parse(info_hash).ok()?;
        self.api
            .api_torrent_details(idx)
            .ok()?
            .files?
            .get(file_idx)
            .map(|f| f.name.clone())
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
    pub fn warm_mp4_tail(self: &Arc<Self>, info_hash: &str, file_idx: usize, file_len: u64) {
        if !self.mp4_tail_warm || file_len <= TAIL_WARM_BYTES {
            return;
        }

        // Claimed before anything is inspected, and claimed even for files
        // that turn out to be ineligible. This runs on every range request, so
        // the eligibility check -- which costs an engine lookup -- must happen
        // once per file rather than once per seek. Claiming up front also
        // means a warm that failed is not retried on the next seek, which is
        // the behaviour this is meant to remove rather than add.
        {
            let mut warmed = self
                .tail_warmed
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !warmed.insert((info_hash.to_string(), file_idx)) {
                return;
            }
        }

        let Some(name) = self.file_name(info_hash, file_idx) else {
            return;
        };
        if !has_tail_index(&name) {
            return;
        }

        let engine = Arc::clone(self);
        let info_hash = info_hash.to_string();
        tokio::spawn(async move {
            let from = file_len - TAIL_WARM_BYTES;
            let Ok(mut reader) = engine.open_stream_at(&info_hash, file_idx, from).await else {
                debug!(%info_hash, "could not open a tail read to warm the mp4 index");
                return;
            };
            let mut sink = vec![0u8; 64 * 1024];
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
        if self.readahead_extra == 0 {
            return None;
        }

        let engine = Arc::clone(self);
        let info_hash = info_hash.to_string();
        let settle = self.readahead_settle;
        let extra = self.readahead_extra;

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

        let engine = Arc::clone(self);
        let focus = info_hash.to_string();
        tokio::spawn(async move {
            // Nothing was playing before means nothing has been abandoned --
            // this is what keeps the first play of a session from touching
            // the backlog. Candidates prefetched for this browse still get
            // parked below, since those were never watched at all.
            if let Some(previous) = previous {
                if engine.discard_abandoned(&previous).await {
                    info!(
                        focused = %focus,
                        abandoned = %previous,
                        "switched title: dropped the one left behind and freed its partial data"
                    );
                }
            }
            // The new focus takes a download slot, and the queue is rebuilt
            // around it: whatever no longer fits is parked, and anything that
            // now does is resumed.
            let (started, parked) = engine.enforce_download_slots().await;
            if started > 0 || parked > 0 {
                debug!(
                    focused = %focus,
                    started,
                    parked,
                    "rebuilt the download queue around the stream being watched"
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
        let matches: Vec<(TorrentIdOrHash, bool, f64)> =
            self.api.session().with_torrents(|iter| {
                iter.filter_map(|(_, handle)| {
                    if handle.info_hash().as_string() != info_hash {
                        return None;
                    }
                    let stats = handle.stats();
                    let finished =
                        stats.total_bytes > 0 && stats.progress_bytes >= stats.total_bytes;
                    // Captured here, while the torrent still exists, so the
                    // log can say how much was thrown away.
                    let progress = if stats.total_bytes > 0 {
                        stats.progress_bytes as f64 / stats.total_bytes as f64 * 100.0
                    } else {
                        0.0
                    };
                    Some((handle.info_hash().into(), finished, progress))
                })
                .collect()
            });

        let Some((idx, finished, progress)) = matches.into_iter().next() else {
            return false;
        };
        if action_for_abandoned(
            self.has_open_stream(info_hash),
            self.is_recently_active(info_hash, ABANDON_GRACE).await,
            finished,
        ) != UnfocusedAction::Discard
        {
            return false;
        }

        match self.api.api_torrent_action_delete(idx).await {
            Ok(_) => {
                info!(info_hash = %info_hash, "discarded abandoned torrent and its partial data");
                // Deleting a viewer's partial download is the most destructive
                // thing this process does on its own initiative, and it was
                // invisible until it was caught in the act. It gets a line.
                self.audit()
                    .record(crate::audit::Event::TorrentDiscarded {
                        info_hash: info_hash.to_string(),
                        progress_percent: progress,
                    });
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

    /// Deletes one torrent and its data because cache retention decided it is
    /// too old to keep. `Ok(true)` means deleted, `Ok(false)` means the
    /// session never heard of the hash (an orphaned directory the caller has
    /// to reclaim itself), and `Err` means the session owns it but the delete
    /// failed -- the caller must then leave the files alone rather than pull
    /// them out from under a live handle.
    ///
    /// Unlike [`discard_abandoned`](Self::discard_abandoned) this spares
    /// nothing for being finished: retention exists precisely to throw
    /// finished backlog away. The caller owns the "is anyone reading this"
    /// checks.
    pub async fn discard_cached(&self, info_hash: &str) -> Result<bool> {
        let matches: Vec<(TorrentIdOrHash, f64)> = self.api.session().with_torrents(|iter| {
            iter.filter_map(|(_, handle)| {
                if handle.info_hash().as_string() != info_hash {
                    return None;
                }
                let stats = handle.stats();
                let progress = if stats.total_bytes > 0 {
                    stats.progress_bytes as f64 / stats.total_bytes as f64 * 100.0
                } else {
                    0.0
                };
                Some((handle.info_hash().into(), progress))
            })
            .collect()
        });

        let Some((idx, progress)) = matches.into_iter().next() else {
            return Ok(false);
        };
        self.api
            .api_torrent_action_delete(idx)
            .await
            .map_err(|e| anyhow::anyhow!("retention could not delete torrent: {e}"))?;
        self.audit().record(crate::audit::Event::TorrentDiscarded {
            info_hash: info_hash.to_string(),
            progress_percent: progress,
        });
        self.forget_activity(info_hash).await;
        Ok(true)
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
        // A hand pause on a torrent that no longer exists would otherwise
        // silently apply to the next download of the same title.
        self.forget_hold(info_hash);
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

    /// One row of a stream list, as far as warming is concerned.
    ///
    /// Prepares the torrents behind a title's stream list while the viewer is
    /// still reading it, so that pressing play is not where the waiting
    /// happens.
    ///
    /// Two different jobs, because a candidate is in one of two states:
    ///
    /// * **already in the session but paused** -- resume it. Pausing drops
    ///   every peer connection and they have to be rediscovered over
    ///   DHT/trackers, which is most of what "it took 30 seconds to start"
    ///   actually is.
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
        let mut resumed = 0usize;
        let mut prefetch: Vec<BrowseCandidate> = Vec::new();

        for candidate in candidates {
            let Ok(idx) = TorrentIdOrHash::parse(&candidate.info_hash) else {
                continue;
            };
            match self.api.session().get(idx) {
                Some(handle) => {
                    if resumed >= MAX_WARM_ON_BROWSE || !handle.is_paused() {
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
                            debug!(info_hash = %candidate.info_hash, "pre-warming previously watched torrent");
                            resumed += 1;
                        }
                        Err(e) => {
                            debug!(info_hash = %candidate.info_hash, "could not pre-warm: {e}")
                        }
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

    /// Which torrents are allowed to download right now, in admission order.
    ///
    /// The gateway downloads up to `max_active_downloads` titles at a time
    /// rather than only the one on screen. That is what lets the next
    /// episodes of a series arrive while the current one is being watched,
    /// instead of each one paying a full cold start when it is tapped.
    ///
    /// Membership, in priority order:
    ///
    /// * **anything with a live reader.** Non-negotiable rather than a
    ///   preference: a reader parked inside librqbit is woken only by the
    ///   piece it waits for, so pausing its torrent freezes it permanently
    ///   with no timeout (see `StreamGuard`). These hold a slot whether the
    ///   cap likes it or not, which is also the honest accounting -- they are
    ///   using the line either way.
    /// * **the focused title**, so the video on screen never queues behind
    ///   the backlog.
    /// * **the oldest unfinished torrents**, until the cap is reached.
    ///
    /// Ordering is by librqbit's `TorrentId`, which is assigned incrementally
    /// as torrents are added -- so ascending id *is* the order they were
    /// asked for, with no second bookkeeping to drift out of sync, and a
    /// session restored from disk rebuilds it in the same order. That is what
    /// makes the queue run front-to-back: four episodes finish one after
    /// another rather than all four crawling at a quarter of the speed and
    /// none of them becoming watchable.
    ///
    /// Finished torrents are never members. They cost no download bandwidth,
    /// so a slot spent on one is a slot the queue cannot use.
    fn download_slots(&self) -> HashSet<String> {
        let focused = self
            .focused
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();

        let mut unfinished: Vec<(usize, String)> = self.api.session().with_torrents(|iter| {
            iter.filter_map(|(id, handle)| {
                let stats = handle.stats();
                if stats.total_bytes > 0 && stats.progress_bytes >= stats.total_bytes {
                    return None;
                }
                let hash = handle.info_hash().as_string();
                // A torrent the operator paused by hand is not a candidate at
                // all, rather than a candidate the queue keeps resuming. It
                // also means the hold hands its slot to the next title in
                // line, which is what someone pausing a download wants: the
                // bandwidth goes somewhere, not nowhere.
                if self.is_held(&hash) {
                    return None;
                }
                Some((id, hash))
            })
            .collect()
        });
        unfinished.sort_by_key(|(id, _)| *id);

        choose_download_slots(
            &unfinished,
            focused.as_deref(),
            |hash| self.has_open_stream(hash),
            self.max_active_downloads,
        )
    }

    /// Whether this torrent currently holds one of the download slots above.
    pub fn holds_download_slot(&self, info_hash: &str) -> bool {
        self.download_slots().contains(info_hash)
    }

    /// Whether the operator paused this torrent by hand.
    ///
    /// Distinct from librqbit's own paused flag, which the download queue and
    /// the idle reaper both set and clear on their own schedule. A hand pause
    /// has to outlive those: without a separate record the queue's next tick
    /// (a second later) simply resumes it, and the button looks broken.
    pub fn is_held(&self, info_hash: &str) -> bool {
        self.held
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(info_hash)
    }

    /// Pauses one torrent and holds it paused until something explicitly says
    /// otherwise -- a resume, a delete, or the viewer playing it again.
    pub async fn hold_paused(&self, info_hash: &str) -> Result<bool> {
        let Some(idx) = self.session_idx(info_hash) else {
            return Ok(false);
        };
        self.held
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(info_hash.to_string());
        // Errors here are almost always "already paused", which is the state
        // being asked for, so the hold above stands either way.
        if let Err(e) = self.api.api_torrent_action_pause(idx).await {
            debug!(info_hash = %info_hash, "pause returned an error, holding anyway: {e}");
        }
        info!(info_hash = %info_hash, "paused by hand");
        Ok(true)
    }

    /// Releases a hand pause and lets the download queue have the torrent
    /// back. Whether it actually runs is then the queue's decision, exactly as
    /// it is for every other torrent -- so resuming a fifth title while four
    /// are already downloading queues it rather than oversubscribing the link.
    pub async fn release_hold(&self, info_hash: &str) -> Result<bool> {
        let removed = self
            .held
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(info_hash);
        if self.session_idx(info_hash).is_none() {
            return Ok(false);
        }
        self.enforce_download_slots().await;
        if removed {
            info!(info_hash = %info_hash, "resumed by hand");
        }
        Ok(true)
    }

    /// Forgets any hand pause for this hash. Called wherever the torrent stops
    /// existing, or where playing it makes the hold moot.
    fn forget_hold(&self, info_hash: &str) {
        self.held
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(info_hash);
    }

    /// The session's handle for a hash, or `None` if it holds no such torrent.
    fn session_idx(&self, info_hash: &str) -> Option<TorrentIdOrHash> {
        let idx = TorrentIdOrHash::parse(info_hash).ok()?;
        self.api.session().get(idx).map(|_| idx)
    }

    /// Brings the session in line with `download_slots`: resumes every
    /// unfinished torrent inside the set, pauses every one outside it.
    /// Returns `(started, paused)`.
    ///
    /// Run both on a focus change and on a timer, because the two things that
    /// move the set are the viewer picking a different title and a download
    /// finishing -- and only the first of those is an event this process sees.
    /// Without the timer a torrent that completed would keep its slot until
    /// the next time somebody changed episode.
    pub async fn enforce_download_slots(&self) -> (usize, usize) {
        let slots = self.download_slots();

        let candidates: Vec<(TorrentIdOrHash, String, bool)> =
            self.api.session().with_torrents(|iter| {
                iter.filter_map(|(_, handle)| {
                    let stats = handle.stats();
                    // Finished: no download bandwidth being consumed, and
                    // pausing it would only stop us seeding it back.
                    if stats.total_bytes > 0 && stats.progress_bytes >= stats.total_bytes {
                        return None;
                    }
                    Some((
                        handle.info_hash().into(),
                        handle.info_hash().as_string(),
                        handle.is_paused(),
                    ))
                })
                .collect()
            });

        let (mut started, mut paused) = (0, 0);
        for (idx, hash, is_paused) in candidates {
            // A hand pause outranks the queue in both directions: never
            // resumed here, and already paused, so there is nothing to do.
            if self.is_held(&hash) {
                continue;
            }
            match (slots.contains(&hash), is_paused) {
                (true, true) => {
                    if self.api.api_torrent_action_start(idx).await.is_ok() {
                        debug!(info_hash = %hash, "download queue: took a free slot");
                        started += 1;
                    }
                }
                (false, false) => {
                    // Belt and braces: a torrent with a live reader is always
                    // in the set above, so this should never fire -- but
                    // pausing one freezes it permanently, and the cost of
                    // checking twice is a map lookup.
                    if self.has_open_stream(&hash) {
                        continue;
                    }
                    if self.api.api_torrent_action_pause(idx).await.is_ok() {
                        debug!(info_hash = %hash, "download queue: parked until a slot frees up");
                        paused += 1;
                    }
                }
                _ => {}
            }
        }
        (started, paused)
    }

    /// Runs `enforce_download_slots` on a timer for the lifetime of the
    /// process, so a finished download hands its slot to the next in line.
    pub fn spawn_download_queue(self: &Arc<Self>, interval: Duration) {
        let engine = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // The first tick fires immediately; skip it so a torrent started
            // moments before startup is not parked before anyone can play it.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let (started, paused) = engine.enforce_download_slots().await;
                if started > 0 || paused > 0 {
                    debug!(started, paused, "download queue advanced");
                }
            }
        });
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
                let info_hash = handle.info_hash().as_string();
                ActiveTorrentSummary {
                    held: self.is_held(&info_hash),
                    info_hash,
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
        // Holding a download slot is itself a reason to keep running: the
        // queue exists precisely to fetch titles nobody is watching yet, so
        // judging those by request recency would pause every one of them a
        // half-hour in and the queue would never finish anything.
        let slots = self.download_slots();

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
            if slots.contains(&hash) {
                continue;
            }
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

    fn slot(client: &str, served: u64, tail_probe: bool) -> ReaderSlot {
        ReaderSlot {
            id: 0,
            info_hash: "aaaa".to_string(),
            file_idx: 0,
            client: client.parse().unwrap(),
            tail_probe,
            served: Arc::new(AtomicU64::new(served)),
            cancel: Arc::new(ReaderCancel::new()),
        }
    }

    fn ip(value: &str) -> IpAddr {
        value.parse().unwrap()
    }

    /// A scrub burst is the case this exists for: each abandoned read keeps
    /// its share of librqbit's round-robin piece requests, so ten of them
    /// leave the position the viewer landed on getting a tenth of the
    /// download.
    #[test]
    fn a_read_that_showed_nobody_anything_is_superseded() {
        assert!(slot("192.168.1.5", 0, false).superseded_by("aaaa", 0, ip("192.168.1.5")));
    }

    /// The one rule that must never be got wrong: a read that has delivered
    /// bytes is a picture on someone's screen, and cancelling it stops
    /// playback dead.
    #[test]
    fn a_read_that_is_playing_is_never_superseded() {
        assert!(!slot("192.168.1.5", 1, false).superseded_by("aaaa", 0, ip("192.168.1.5")));
    }

    /// Two devices on the same title would otherwise cancel each other's cold
    /// start, and then each other's retry, and neither would ever play.
    #[test]
    fn a_second_device_does_not_supersede_the_first() {
        assert!(!slot("192.168.1.5", 0, false).superseded_by("aaaa", 0, ip("192.168.1.9")));
    }

    /// An mp4 index read is a second read the same player needs *at the same
    /// time* as the one at the head, not a superseded seek. Cancelling it
    /// breaks exactly the players the tail warmer exists to help.
    #[test]
    fn an_index_probe_survives_the_playback_request_that_follows_it() {
        assert!(!slot("192.168.1.5", 0, true).superseded_by("aaaa", 0, ip("192.168.1.5")));
    }

    /// One player legitimately reads two files of a multi-file torrent (an
    /// episode and its subtitles), and neither is a seek away from the other.
    #[test]
    fn a_read_of_a_different_file_is_left_alone() {
        assert!(!slot("192.168.1.5", 0, false).superseded_by("aaaa", 1, ip("192.168.1.5")));
        assert!(!slot("192.168.1.5", 0, false).superseded_by("bbbb", 0, ip("192.168.1.5")));
    }

    /// mkv is in the list on evidence, not on theory: Matroska's SeekHead is at
    /// the front, and players probe the tail for the Cues regardless. Leaving it
    /// out cost a measured 12.2 s on one start — see `TAIL_INDEXED_EXTENSIONS`.
    /// Pinned as a test because the structural argument for excluding it is
    /// genuinely persuasive and someone will make it again.
    #[test]
    fn only_tail_indexed_containers_are_warmed() {
        assert!(has_tail_index("The.Movie.2024.1080p.mp4"));
        assert!(has_tail_index("clip.MP4"), "extensions are not case-sensitive");
        assert!(has_tail_index("holiday.mov"));
        assert!(has_tail_index("tires.s03e08.1080p.web.h264-cakes.mkv"));
        assert!(has_tail_index("stream.webm"));
        assert!(!has_tail_index("no-extension"));
        assert!(!has_tail_index("subtitles.srt"));
    }

    /// Admission order, oldest first -- what `download_slots` builds from
    /// librqbit's incrementing `TorrentId`.
    fn queue(hashes: &[&str]) -> Vec<(usize, String)> {
        hashes
            .iter()
            .enumerate()
            .map(|(id, hash)| (id, (*hash).to_string()))
            .collect()
    }

    fn nothing_open(_: &str) -> bool {
        false
    }

    /// The feature: four titles download at once instead of one.
    #[test]
    fn the_queue_runs_four_titles_at_a_time() {
        let unfinished = queue(&["e1", "e2", "e3", "e4", "e5", "e6"]);
        let slots = choose_download_slots(&unfinished, Some("e1"), nothing_open, 4);
        assert_eq!(slots.len(), 4);
    }

    /// "In order" is the half that makes it useful. Taking an arbitrary four
    /// would leave every episode part-downloaded and none of them watchable;
    /// taking the oldest four finishes them front-to-back.
    #[test]
    fn the_queue_admits_in_the_order_titles_were_asked_for() {
        let unfinished = queue(&["e1", "e2", "e3", "e4", "e5", "e6"]);
        let slots = choose_download_slots(&unfinished, None, nothing_open, 3);
        assert!(slots.contains("e1") && slots.contains("e2") && slots.contains("e3"));
        assert!(
            !slots.contains("e4") && !slots.contains("e5") && !slots.contains("e6"),
            "later requests wait for a slot rather than diluting the first three"
        );
    }

    /// A viewer who skips ahead to the last episode must not queue behind the
    /// backlog that was already downloading.
    #[test]
    fn the_title_on_screen_always_holds_a_slot() {
        let unfinished = queue(&["e1", "e2", "e3", "e4", "e5", "e6"]);
        let slots = choose_download_slots(&unfinished, Some("e6"), nothing_open, 2);
        assert!(slots.contains("e6"), "the focused title is never queued");
        assert!(slots.contains("e1"), "and the queue still runs behind it");
    }

    /// Pausing a torrent whose reader is parked inside librqbit freezes it
    /// permanently -- the piece it waits for never arrives to wake it. So a
    /// read in progress is admitted even past the cap; the alternative is not
    /// "a smaller set", it is a frozen video. See `StreamGuard`.
    #[test]
    fn a_torrent_being_read_is_admitted_even_past_the_cap() {
        let unfinished = queue(&["e1", "e2", "e3"]);
        let slots = choose_download_slots(&unfinished, Some("e1"), |hash| hash == "e3", 1);
        assert!(slots.contains("e3"), "a live reader cannot be parked");
        assert!(slots.contains("e1"), "nor can the title on screen");
    }

    /// A finished torrent is not in the input at all, so a viewer re-watching
    /// something already downloaded does not spend a slot on it.
    #[test]
    fn a_finished_focus_does_not_hold_a_slot_open() {
        let unfinished = queue(&["e2", "e3"]);
        let slots = choose_download_slots(&unfinished, Some("e1-finished"), nothing_open, 2);
        assert_eq!(slots.len(), 2);
        assert!(slots.contains("e2") && slots.contains("e3"));
        assert!(!slots.contains("e1-finished"));
    }

    /// Setting the cap to 1 must reproduce the old behaviour exactly: only
    /// the title on screen downloads. That is the escape hatch for a link
    /// that is only just keeping up with playback.
    #[test]
    fn a_cap_of_one_downloads_only_what_is_being_watched() {
        let unfinished = queue(&["e1", "e2", "e3"]);
        let slots = choose_download_slots(&unfinished, Some("e2"), nothing_open, 1);
        assert_eq!(slots.len(), 1);
        assert!(slots.contains("e2"));
    }

    #[test]
    fn an_empty_session_needs_no_slots() {
        assert!(choose_download_slots(&[], None, nothing_open, 4).is_empty());
        assert!(choose_download_slots(&[], Some("gone"), nothing_open, 4).is_empty());
    }

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
    fn metadata_cache_hands_back_what_a_resolve_fetched() {
        let mut cache = MetadataCache::default();
        cache.remember("aaaa".into(), bytes::Bytes::from_static(b"d4:infoe"));

        assert_eq!(
            cache.get("aaaa").as_deref(),
            Some(&b"d4:infoe"[..]),
            "a start must be able to reuse the metadata its resolve paid for"
        );
        assert!(cache.get("bbbb").is_none());
    }

    /// One entry is a whole `.torrent` file, so an unbounded map here would
    /// grow with every title browsed for the life of the process.
    #[test]
    fn metadata_cache_evicts_oldest_beyond_capacity() {
        let mut cache = MetadataCache::default();
        for i in 0..RESOLVED_METADATA_CAPACITY + 1 {
            cache.remember(format!("hash{i}"), bytes::Bytes::from_static(b"x"));
        }

        assert!(cache.get("hash0").is_none(), "oldest entry must be evicted");
        assert!(cache
            .get(&format!("hash{RESOLVED_METADATA_CAPACITY}"))
            .is_some());
        assert_eq!(cache.order.len(), RESOLVED_METADATA_CAPACITY);
        assert_eq!(cache.bytes.len(), RESOLVED_METADATA_CAPACITY);
    }

    /// Re-resolving the same torrent must refresh it in place. Pushing a
    /// second `order` entry for one hash would let the cache evict the entry
    /// its own duplicate still points at, dropping metadata it still holds.
    #[test]
    fn metadata_cache_does_not_double_count_repeats() {
        let mut cache = MetadataCache::default();
        cache.remember("aaaa".into(), bytes::Bytes::from_static(b"first"));
        cache.remember("aaaa".into(), bytes::Bytes::from_static(b"second"));

        assert_eq!(cache.order.len(), 1);
        assert_eq!(cache.get("aaaa").as_deref(), Some(&b"second"[..]));
    }

    /// A torrent with no metadata bytes is not a cache entry, it is a miss --
    /// storing it would make `start_file` add an empty torrent file instead of
    /// falling back to the magnet.
    #[test]
    fn metadata_cache_ignores_empty_metadata() {
        let mut cache = MetadataCache::default();
        cache.remember("aaaa".into(), bytes::Bytes::new());

        assert!(cache.get("aaaa").is_none());
        assert!(cache.order.is_empty());
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
        assert_eq!(
            action_for_abandoned(false, false, false),
            UnfocusedAction::Discard
        );
    }

    #[test]
    fn a_torrent_another_reader_still_holds_is_never_deleted() {
        // A second device watching something else, or this player's own
        // header/index reads. Deleting underneath a live reader truncates a
        // video someone is watching -- see `StreamGuard`.
        assert_eq!(
            action_for_abandoned(true, false, false),
            UnfocusedAction::Leave
        );
    }

    #[test]
    fn a_torrent_read_moments_ago_survives_the_switch() {
        // The compounding-failure regression. A player that gives up on a cold
        // torrent has no open connection, so `has_open_stream` alone reads it
        // as abandoned -- and deleting it there throws away the data and the
        // warmed-up peers that the player's *own retry*, seconds later, is
        // about to need. Every attempt then starts colder than the last, which
        // is what a viewer sees as the player cycling through every source and
        // playing none. Range requests are stateless; recency, not an open
        // socket, is what "someone is watching this" means here.
        assert_eq!(
            action_for_abandoned(false, true, false),
            UnfocusedAction::Leave
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

    #[test]
    fn a_finished_download_survives_the_switch() {
        // A complete file costs no download bandwidth, and deleting a movie
        // that finished downloading because the viewer started the next
        // episode is destructive. The cache janitor reclaims it under
        // pressure, oldest-first.
        assert_eq!(
            action_for_abandoned(false, false, true),
            UnfocusedAction::Leave
        );
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

    fn archive_tempdir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "streaming-gateway-metadata-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A distinct, correctly-shaped info hash per index: 40 hex digits, which
    /// is exactly what the archive's guard insists on.
    fn hash_of(nth: usize) -> String {
        format!("{nth:040x}")
    }

    /// The regression this archive exists for.
    ///
    /// A title played once and reclaimed by the janitor has to be startable
    /// again without asking the swarm anything, because by then the swarm may
    /// have no seeders left to ask -- which is a 25-second timeout, a 500, and
    /// a gateway that appears not to have noticed the tap at all.
    #[tokio::test]
    async fn archived_metadata_survives_to_be_read_back() {
        let dir = archive_tempdir();
        let archive = MetadataArchive::new(dir.join("metadata"));
        let hash = hash_of(1);

        assert!(
            archive.load(&hash).await.is_none(),
            "nothing archived yet, so nothing to find"
        );
        archive
            .store(&hash, &bytes::Bytes::from_static(b"torrent-blob"))
            .await;
        assert_eq!(
            archive.load(&hash).await.as_deref(),
            Some(&b"torrent-blob"[..]),
            "a stored blob must come back byte for byte -- librqbit parses it"
        );

        tokio::fs::remove_dir_all(&dir).await.unwrap();
    }

    /// The archive turns a caller-supplied string into a filename, so the
    /// shape check is a path-traversal guard, not a tidiness rule. `/videos/`
    /// takes its info hash straight off the wire.
    #[tokio::test]
    async fn the_archive_refuses_anything_not_shaped_like_an_info_hash() {
        let dir = archive_tempdir();
        let archive = MetadataArchive::new(dir.join("metadata"));

        for bad in ["../../etc/passwd", "short", ""] {
            archive.store(bad, &bytes::Bytes::from_static(b"x")).await;
            assert!(
                archive.load(bad).await.is_none(),
                "{bad:?} must never reach the filesystem"
            );
        }

        tokio::fs::remove_dir_all(&dir).await.unwrap();
    }

    /// Bounded, or a long-lived install accumulates a `.torrent` per title it
    /// has ever been shown -- and the browse path resolves far more of those
    /// than anyone ever plays.
    #[tokio::test]
    async fn the_archive_prunes_itself_back_to_its_capacity() {
        let dir = archive_tempdir();
        let archive = MetadataArchive::new(dir.join("metadata"));

        for nth in 0..(METADATA_ARCHIVE_CAPACITY + 8) {
            archive
                .store(&hash_of(nth), &bytes::Bytes::from_static(b"blob"))
                .await;
        }

        let mut kept = 0;
        let mut entries = tokio::fs::read_dir(dir.join("metadata")).await.unwrap();
        while let Some(_entry) = entries.next_entry().await.unwrap() {
            kept += 1;
        }
        assert!(
            kept <= METADATA_ARCHIVE_CAPACITY,
            "archive grew to {kept}, past its {METADATA_ARCHIVE_CAPACITY} cap"
        );

        tokio::fs::remove_dir_all(&dir).await.unwrap();
    }
}
