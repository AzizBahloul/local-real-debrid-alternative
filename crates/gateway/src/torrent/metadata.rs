//! What the engine remembers about torrents beyond the session itself: their
//! `.torrent` metadata (in memory and on disk) and the set of info hashes this
//! gateway has offered to a client.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use librqbit::api::TorrentIdOrHash;
use librqbit::Api;
use tracing::debug;

use super::resolver::is_hex40;

/// How many resolved torrents keep their metadata in memory for a later start.
///
/// One entry is a whole `.torrent` file (chiefly the piece hashes: 20 bytes
/// per piece, so a few hundred KB for a large release), which is why this is
/// small rather than generous — in memory it only has to bridge the gap
/// between reading a stream list and tapping one of its rows. The on-disk
/// archive behind it (`METADATA_ARCHIVE_CAPACITY`) is what makes a title
/// playable weeks later. See `MetadataCache`.
pub(super) const RESOLVED_METADATA_CAPACITY: usize = 16;

/// How many `.torrent` blobs the on-disk archive keeps at most.
///
/// Sized against the advertised set rather than against memory: an advertised
/// hash is a link a client may still tap, and the whole point of the archive
/// is that tapping one never costs a swarm lookup. At a few hundred KB each
/// the worst case is well under a gigabyte, against a cache measured in tens
/// of them. See `MetadataArchive`.
pub(super) const METADATA_ARCHIVE_CAPACITY: usize = 512;

/// How many new blobs the archive takes between two prunes, and how far below
/// its capacity a prune trims it.
///
/// Pruning lists and stats the whole directory, and it used to run after
/// every single write -- including one per restored torrent at startup. Doing
/// it in batches, trimming far enough below the cap that the next batch still
/// fits under it, keeps the capacity a hard ceiling for a fraction of the I/O.
const ARCHIVE_PRUNE_BATCH: usize = 32;

/// How many recently-advertised info hashes stay startable. Generous relative
/// to how many streams a browse session shows (15 per title by default).
pub(super) const ADVERTISED_HASH_CAPACITY: usize = 512;

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
pub(super) struct MetadataCache {
    pub(super) order: VecDeque<String>,
    pub(super) bytes: HashMap<String, Bytes>,
}

impl MetadataCache {
    pub(super) fn remember(&mut self, info_hash: String, torrent_bytes: Bytes) {
        if torrent_bytes.is_empty() {
            return;
        }
        if self
            .bytes
            .insert(info_hash.clone(), torrent_bytes)
            .is_some()
        {
            return;
        }
        self.order.push_back(info_hash);
        while self.order.len() > RESOLVED_METADATA_CAPACITY {
            if let Some(oldest) = self.order.pop_front() {
                self.bytes.remove(&oldest);
            }
        }
    }

    pub(super) fn get(&self, info_hash: &str) -> Option<Bytes> {
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
pub(super) struct MetadataArchive {
    dir: PathBuf,
    /// New blobs written since the last prune; see `ARCHIVE_PRUNE_BATCH`.
    /// Starts one short of a batch, so the first write of a process prunes
    /// whatever earlier runs left and the cap holds from then on.
    stored_since_prune: Arc<AtomicUsize>,
}

impl MetadataArchive {
    pub(super) fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            stored_since_prune: Arc::new(AtomicUsize::new(ARCHIVE_PRUNE_BATCH - 1)),
        }
    }

    fn path_for(&self, info_hash: &str) -> PathBuf {
        self.dir.join(format!("{info_hash}.torrent"))
    }

    /// The archived blob for `info_hash`, if there is a usable one.
    ///
    /// Best effort throughout: a missing or unreadable archive only costs the
    /// swarm lookup that would have happened without it, so nothing here is
    /// worth failing a request over.
    ///
    /// A blob that does not parse, or parses to a different info hash, is
    /// deleted rather than returned. Handed to librqbit it would fail the add
    /// on every attempt -- and because holding metadata also skips the
    /// failed-search cooldown, the title would error instantly forever instead
    /// of falling back to the magnet. Deleting it lets the next successful
    /// start write a good one.
    pub(super) async fn load(&self, info_hash: &str) -> Option<Bytes> {
        if !is_hex40(info_hash) {
            return None;
        }
        let path = self.path_for(info_hash);
        let bytes = Bytes::from(tokio::fs::read(&path).await.ok()?);
        if !blob_is_torrent_for(&bytes, info_hash) {
            debug!(%info_hash, "discarding an archived torrent that does not match its hash");
            let _ = tokio::fs::remove_file(&path).await;
            return None;
        }
        // Re-stamp on use so pruning evicts by last *use*, not by when the
        // blob was first written -- a series you keep coming back to should
        // not age out behind titles you opened once. Detached: the caller is
        // a cold start, and the stamp is worth no part of its wait.
        touch_detached(path);
        Some(bytes)
    }

    /// Archives `torrent_bytes` unless a blob for this hash is already there.
    /// Returns whether a new blob was written.
    ///
    /// An existing blob is left alone rather than rewritten: it describes the
    /// same info dictionary, and rewriting it on every start and every search
    /// was pure disk churn.
    pub(super) async fn store(&self, info_hash: &str, torrent_bytes: &Bytes) -> bool {
        if torrent_bytes.is_empty() || !is_hex40(info_hash) {
            return false;
        }
        let path = self.path_for(info_hash);
        if tokio::fs::try_exists(&path).await.unwrap_or(false) {
            return false;
        }
        // Atomic, because a torn blob is not a missing blob: see `load`.
        if let Err(e) = crate::util::write_atomic(&path, torrent_bytes.clone(), false).await {
            debug!("could not archive torrent metadata: {e}");
            return false;
        }
        if self.stored_since_prune.fetch_add(1, Ordering::Relaxed) + 1 >= ARCHIVE_PRUNE_BATCH {
            self.stored_since_prune.store(0, Ordering::Relaxed);
            self.prune(METADATA_ARCHIVE_CAPACITY - ARCHIVE_PRUNE_BATCH)
                .await;
        }
        true
    }

    /// Drops the least recently used blobs until at most `keep` remain.
    async fn prune(&self, keep: usize) {
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
        if entries.len() <= keep {
            return;
        }
        entries.sort_by(|a, b| b.0.cmp(&a.0));
        for (_, path) in entries.split_off(keep) {
            let _ = tokio::fs::remove_file(&path).await;
        }
    }
}

/// Whether `bytes` is a `.torrent` file whose info dictionary hashes to
/// `info_hash`.
fn blob_is_torrent_for(bytes: &[u8], info_hash: &str) -> bool {
    !bytes.is_empty()
        && librqbit::torrent_from_bytes(bytes)
            .is_ok_and(|torrent| torrent.info_hash.as_string() == info_hash)
}

/// A live torrent's assembled `.torrent` blob, or `None` if the session does
/// not hold it or has not resolved it yet.
pub(super) fn session_metadata(api: &Api, info_hash: &str) -> Option<Bytes> {
    let idx = TorrentIdOrHash::parse(info_hash).ok()?;
    let handle = api.session().get(idx)?;
    handle.with_metadata(|m| m.torrent_bytes.clone()).ok()
}

/// Stamps a file as used just now, so `MetadataArchive::prune` evicts by last
/// use rather than by first write. Best effort: a failed stamp only means the
/// blob ages from when it was written, which is the behaviour without this.
fn touch_detached(path: PathBuf) {
    tokio::task::spawn_blocking(move || {
        let now = std::time::SystemTime::now();
        let _ = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .and_then(|f| f.set_times(std::fs::FileTimes::new().set_modified(now)));
    });
}

/// One advertised info hash and the trackers the index reported for exactly
/// that release. Persisted so both survive a restart.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(super) struct AdvertisedEntry {
    pub(super) hash: String,
    #[serde(default)]
    pub(super) trackers: Vec<String>,
}

/// Info hashes this gateway has itself offered to a client, least recently
/// offered first.
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
/// the oldest entry only costs a re-browse to make it playable again. Offering
/// a hash again moves it to the newest end: a release that is on screen right
/// now must not be the next one evicted merely because it was first offered
/// long ago, which would turn a link the viewer is looking at into a 404.
pub(super) struct AdvertisedHashes {
    pub(super) order: VecDeque<String>,
    pub(super) trackers: HashMap<String, Vec<String>>,
    cap: usize,
}

impl AdvertisedHashes {
    pub(super) fn new(cap: usize) -> Self {
        Self {
            order: VecDeque::new(),
            trackers: HashMap::new(),
            cap,
        }
    }

    /// Rebuilds the set from what was on disk, oldest first, honouring the cap.
    pub(super) fn seed(cap: usize, entries: Vec<AdvertisedEntry>) -> Self {
        let mut set = Self::new(cap);
        for entry in entries {
            set.remember(entry.hash, entry.trackers);
        }
        set
    }

    pub(super) fn snapshot(&self) -> Vec<AdvertisedEntry> {
        self.order
            .iter()
            .map(|hash| AdvertisedEntry {
                hash: hash.clone(),
                trackers: self.trackers.get(hash).cloned().unwrap_or_default(),
            })
            .collect()
    }

    /// Records one browse's worth of offered hashes, in the order offered.
    ///
    /// Returns whether anything a restart would notice changed, so the caller
    /// writes the file only when there is something new to write. A re-browse
    /// of the title already at the newest end changes nothing, and it is by
    /// far the most common browse.
    pub(super) fn remember_all(
        &mut self,
        batch: impl IntoIterator<Item = (String, Vec<String>)>,
    ) -> bool {
        let batch: Vec<(String, Vec<String>)> = batch.into_iter().collect();
        let newest_before: Vec<String> =
            self.order.iter().rev().take(batch.len()).cloned().collect();

        let mut content_changed = false;
        for (hash, trackers) in batch.iter().cloned() {
            content_changed |= self.remember(hash, trackers);
        }

        // Moving entries to the newest end only reorders the set. If the
        // newest few are exactly what they were, every move put an entry back
        // where it already stood, and the whole order is unchanged.
        let order_unchanged = self
            .order
            .iter()
            .rev()
            .take(batch.len())
            .eq(newest_before.iter());
        content_changed || !order_unchanged
    }

    /// Records one offered hash. Returns whether an entry was added or gained
    /// trackers; a pure move to the newest end returns `false` (see
    /// `remember_all`, which accounts for order).
    pub(super) fn remember(&mut self, hash: String, trackers: Vec<String>) -> bool {
        if let Some(known) = self.trackers.get_mut(&hash) {
            // Re-advertised: keep the entry, but adopt trackers if this
            // sighting knows some and the stored one does not (a legacy
            // entry, or a magnet-path advert that carried none).
            let mut upgraded = false;
            if known.is_empty() && !trackers.is_empty() {
                *known = trackers;
                upgraded = true;
            }
            if self.order.back() != Some(&hash) {
                if let Some(pos) = self.order.iter().position(|h| *h == hash) {
                    self.order.remove(pos);
                }
                self.order.push_back(hash);
            }
            return upgraded;
        }
        if self.order.len() >= self.cap {
            if let Some(oldest) = self.order.pop_front() {
                self.trackers.remove(&oldest);
            }
        }
        self.trackers.insert(hash.clone(), trackers);
        self.order.push_back(hash);
        true
    }

    pub(super) fn contains(&self, hash: &str) -> bool {
        self.trackers.contains_key(hash)
    }

    /// The trackers remembered for an advertised hash, or `None` when the
    /// hash was never advertised -- one lookup for both questions a lazy
    /// start asks.
    pub(super) fn trackers_if_advertised(&self, hash: &str) -> Option<&[String]> {
        self.trackers.get(hash).map(Vec::as_slice)
    }
}

/// Decodes a persisted advertised set, accepting both the current format
/// (entries with trackers) and the pre-0.9 one (a bare list of hashes).
/// A file written by the previous version must keep every already-installed
/// Stremio link startable, not silently reset the whole set.
pub(super) fn parse_advertised(bytes: &[u8]) -> Vec<AdvertisedEntry> {
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

/// Reads the advertised set back, or an empty list if it is absent or
/// unreadable — a corrupt file must not stop the gateway from starting.
pub(super) async fn load_advertised(path: &Path) -> Vec<AdvertisedEntry> {
    let Ok(bytes) = tokio::fs::read(path).await else {
        return Vec::new();
    };
    parse_advertised(&bytes)
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

    /// The link on screen is the one about to be tapped. Evicting it because
    /// it was *first* offered long ago turned a fresh stream list into 404s.
    #[test]
    fn offering_a_hash_again_makes_it_the_newest() {
        let mut set = AdvertisedHashes::new(2);
        set.remember("old".into(), Vec::new());
        set.remember("other".into(), Vec::new());
        set.remember("old".into(), Vec::new());
        set.remember("new".into(), Vec::new());

        assert!(
            set.contains("old"),
            "a re-offered hash must not be evicted next"
        );
        assert!(!set.contains("other"));
        assert!(set.contains("new"));
    }

    /// The file is rewritten only when something changed, and re-browsing the
    /// same title -- the most common browse -- changes nothing.
    #[test]
    fn a_repeat_browse_reports_no_change_and_a_new_one_does() {
        let mut set = AdvertisedHashes::new(8);
        let browse = || vec![("a".to_string(), Vec::new()), ("b".to_string(), Vec::new())];

        assert!(set.remember_all(browse()), "first sighting is new");
        assert!(!set.remember_all(browse()), "the same list again is not");

        assert!(
            set.remember_all(vec![("c".to_string(), Vec::new())]),
            "a new hash is a change"
        );
        assert!(
            set.remember_all(browse()),
            "moving an older browse back to the newest end is a change in order"
        );
        assert!(
            set.remember_all(vec![(
                "a".to_string(),
                vec!["udp://t:1/announce".to_string()]
            )]),
            "gaining trackers is a change"
        );
    }

    #[test]
    fn advertised_set_keeps_trackers_per_hash() {
        let mut set = AdvertisedHashes::new(4);
        set.remember("aaaa".into(), vec!["udp://a.example:1337/announce".into()]);
        set.remember("bbbb".into(), Vec::new());

        assert_eq!(
            set.trackers_if_advertised("aaaa"),
            Some(&["udp://a.example:1337/announce".to_string()][..]),
            "a lazy start must announce where this release's seeders are"
        );
        assert_eq!(set.trackers_if_advertised("bbbb"), Some(&[][..]));
        assert_eq!(set.trackers_if_advertised("unknown"), None);
    }

    #[test]
    fn re_advertising_upgrades_a_trackerless_entry_but_never_downgrades() {
        let mut set = AdvertisedHashes::new(4);
        set.remember("aaaa".into(), Vec::new());
        set.remember("aaaa".into(), vec!["udp://a.example:1337/announce".into()]);
        assert_eq!(
            set.trackers_if_advertised("aaaa").map(<[String]>::len),
            Some(1),
            "a later sighting that knows trackers must fill in an empty entry"
        );

        // A later sighting with none must not wipe what is known.
        set.remember("aaaa".into(), Vec::new());
        assert_eq!(
            set.trackers_if_advertised("aaaa").map(<[String]>::len),
            Some(1)
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
    fn metadata_cache_hands_back_what_a_resolve_fetched() {
        let mut cache = MetadataCache::default();
        cache.remember("aaaa".into(), Bytes::from_static(b"d4:infoe"));

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
            cache.remember(format!("hash{i}"), Bytes::from_static(b"x"));
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
        cache.remember("aaaa".into(), Bytes::from_static(b"first"));
        cache.remember("aaaa".into(), Bytes::from_static(b"second"));

        assert_eq!(cache.order.len(), 1);
        assert_eq!(cache.get("aaaa").as_deref(), Some(&b"second"[..]));
    }

    /// A torrent with no metadata bytes is not a cache entry, it is a miss --
    /// storing it would make `start_file` add an empty torrent file instead of
    /// falling back to the magnet.
    #[test]
    fn metadata_cache_ignores_empty_metadata() {
        let mut cache = MetadataCache::default();
        cache.remember("aaaa".into(), Bytes::new());

        assert!(cache.get("aaaa").is_none());
        assert!(cache.order.is_empty());
    }

    /// A distinct, correctly-shaped info hash per index: 40 hex digits, which
    /// is exactly what the archive's guard insists on.
    fn hash_of(nth: usize) -> String {
        format!("{nth:040x}")
    }

    /// A minimal but real single-file `.torrent`, and the info hash it
    /// actually has.
    fn real_torrent(name: &str) -> (Bytes, String) {
        let mut info = Vec::new();
        info.extend_from_slice(b"d6:lengthi1e4:name");
        info.extend_from_slice(format!("{}:{name}", name.len()).as_bytes());
        info.extend_from_slice(b"12:piece lengthi16384e6:pieces20:");
        info.extend_from_slice(&[7u8; 20]);
        info.extend_from_slice(b"e");
        let mut blob = b"d4:info".to_vec();
        blob.extend_from_slice(&info);
        blob.extend_from_slice(b"e");
        let hash = librqbit::torrent_from_bytes(&blob)
            .expect("test torrent parses")
            .info_hash
            .as_string();
        (Bytes::from(blob), hash)
    }

    /// The regression this archive exists for.
    ///
    /// A title played once and reclaimed by the janitor has to be startable
    /// again without asking the swarm anything, because by then the swarm may
    /// have no seeders left to ask -- which is a 25-second timeout, a 500, and
    /// a gateway that appears not to have noticed the tap at all.
    #[tokio::test]
    async fn archived_metadata_survives_to_be_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let archive = MetadataArchive::new(dir.path().join("metadata"));
        let (blob, hash) = real_torrent("movie.mkv");

        assert!(
            archive.load(&hash).await.is_none(),
            "nothing archived yet, so nothing to find"
        );
        assert!(archive.store(&hash, &blob).await, "a new blob is written");
        assert_eq!(
            archive.load(&hash).await.as_deref(),
            Some(blob.as_ref()),
            "a stored blob must come back byte for byte -- librqbit parses it"
        );
        assert!(
            !archive.store(&hash, &blob).await,
            "an archived blob is not rewritten on every start"
        );
    }

    /// A torn or mismatched blob would fail every add, instantly and forever,
    /// because holding metadata also skips the failed-search cooldown. It has
    /// to be treated as missing -- and removed, so a good one can replace it.
    #[tokio::test]
    async fn an_unusable_blob_is_discarded_rather_than_handed_to_librqbit() {
        let dir = tempfile::tempdir().unwrap();
        let archive = MetadataArchive::new(dir.path().join("metadata"));
        let (blob, _) = real_torrent("movie.mkv");
        let (_, other_hash) = real_torrent("other.mkv");
        let torn_hash = hash_of(1);

        archive
            .store(&torn_hash, &Bytes::from_static(b"d4:info"))
            .await;
        assert!(archive.load(&torn_hash).await.is_none());
        assert!(
            !archive.path_for(&torn_hash).exists(),
            "the torn blob is removed"
        );

        archive.store(&other_hash, &blob).await;
        assert!(
            archive.load(&other_hash).await.is_none(),
            "a blob filed under the wrong hash would start a different torrent"
        );
    }

    /// The archive turns a caller-supplied string into a filename, so the
    /// shape check is a path-traversal guard, not a tidiness rule. `/videos/`
    /// takes its info hash straight off the wire.
    #[tokio::test]
    async fn the_archive_refuses_anything_not_shaped_like_an_info_hash() {
        let dir = tempfile::tempdir().unwrap();
        let archive = MetadataArchive::new(dir.path().join("metadata"));

        for bad in ["../../etc/passwd", "short", ""] {
            assert!(!archive.store(bad, &Bytes::from_static(b"x")).await);
            assert!(
                archive.load(bad).await.is_none(),
                "{bad:?} must never reach the filesystem"
            );
        }
    }

    /// Bounded, or a long-lived install accumulates a `.torrent` per title it
    /// has ever been shown -- and the browse path resolves far more of those
    /// than anyone ever plays.
    #[tokio::test]
    async fn the_archive_prunes_itself_back_to_its_capacity() {
        let dir = tempfile::tempdir().unwrap();
        let archive = MetadataArchive::new(dir.path().join("metadata"));

        for nth in 0..(METADATA_ARCHIVE_CAPACITY + 8) {
            archive
                .store(&hash_of(nth), &Bytes::from_static(b"blob"))
                .await;
        }

        let kept = std::fs::read_dir(dir.path().join("metadata"))
            .unwrap()
            .count();
        assert!(
            kept <= METADATA_ARCHIVE_CAPACITY,
            "archive grew to {kept}, past its {METADATA_ARCHIVE_CAPACITY} cap"
        );
    }

    /// Whatever an earlier run left behind, the first write of this process
    /// brings the archive back under its cap rather than waiting a batch.
    #[tokio::test]
    async fn the_first_write_of_a_process_prunes_what_earlier_runs_left() {
        let dir = tempfile::tempdir().unwrap();
        let metadata = dir.path().join("metadata");
        std::fs::create_dir_all(&metadata).unwrap();
        for nth in 0..(METADATA_ARCHIVE_CAPACITY + 20) {
            std::fs::write(metadata.join(format!("{}.torrent", hash_of(nth))), b"x").unwrap();
        }

        let archive = MetadataArchive::new(metadata.clone());
        archive
            .store(&hash_of(100_000), &Bytes::from_static(b"blob"))
            .await;

        assert!(std::fs::read_dir(&metadata).unwrap().count() <= METADATA_ARCHIVE_CAPACITY);
    }

    /// A save interrupted halfway must never be what the next start reads.
    #[tokio::test]
    async fn the_advertised_file_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("advertised.json");
        let mut set = AdvertisedHashes::new(8);
        set.remember("aaaa".into(), vec!["udp://t:1/announce".into()]);
        set.remember("bbbb".into(), Vec::new());

        let bytes = serde_json::to_vec(&set.snapshot()).unwrap();
        crate::util::write_atomic(&path, bytes, false)
            .await
            .unwrap();

        let restored = AdvertisedHashes::seed(8, load_advertised(&path).await);
        assert_eq!(restored.order, set.order, "oldest-first order survives");
        assert_eq!(restored.trackers, set.trackers);
    }
}
