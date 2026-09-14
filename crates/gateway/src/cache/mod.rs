//! Cache management for downloaded torrent data.
//!
//! Partial-download resume and "reuse a completed file instead of
//! re-downloading it" are not reimplemented here -- they come for free from
//! librqbit's own on-disk piece storage plus session fastresume/persistence
//! (wired up in `TorrentEngine::new`). What this module owns is the one
//! thing librqbit intentionally does *not* do on its own: enforcing a size
//! cap by deleting old, unwatched torrents.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use tracing::{info, warn};

use crate::torrent::TorrentEngine;

/// A torrent never gets evicted while it has been read from within this
/// window, even if the cache is over its size cap -- a seek or the next
/// buffer refill could come from a viewer at any moment.
const ACTIVE_STREAM_GRACE_PERIOD: Duration = Duration::from_secs(300);

/// How long a measured cache size is reused before the disk is walked again.
///
/// `/health` is polled by the desktop app every couple of seconds and the
/// terminal monitor asks too, and each ask used to walk every file of every
/// cached torrent. A few seconds of staleness is invisible on a number that
/// moves at download speed, and anything that changes it all at once -- a
/// clear, a delete, a janitor pass -- refreshes it straight away.
const USAGE_TTL: Duration = Duration::from_secs(3);

pub struct CacheManager {
    downloads_dir: PathBuf,
    max_size_bytes: u64,
    /// Keep at most this many torrents on disk, newest first; 0 disables the
    /// count rule and leaves only the size cap.
    max_torrents: usize,
    auto_cleanup: bool,
    engine: Arc<TorrentEngine>,
    /// The last measured usage and when it was taken. A tokio mutex held
    /// across the walk, so concurrent askers share one walk rather than each
    /// starting their own.
    usage: tokio::sync::Mutex<Option<(Instant, u64)>>,
}

struct TorrentDirEntry {
    path: PathBuf,
    info_hash: String,
    size: u64,
    modified: SystemTime,
}

/// One pass over the downloads directory.
struct DiskScan {
    /// Every directory shaped like a torrent this gateway created.
    torrents: Vec<TorrentDirEntry>,
    /// Everything under the directory, torrent-shaped or not.
    total: u64,
}

impl CacheManager {
    pub fn new(
        downloads_dir: PathBuf,
        max_size_bytes: u64,
        max_torrents: usize,
        auto_cleanup: bool,
        engine: Arc<TorrentEngine>,
    ) -> Arc<Self> {
        Arc::new(Self {
            downloads_dir,
            max_size_bytes,
            max_torrents,
            auto_cleanup,
            engine,
            usage: tokio::sync::Mutex::new(None),
        })
    }

    /// Spawns the periodic background janitor. Fire-and-forget: cleanup
    /// failures are logged, never fatal to the gateway.
    pub fn spawn_janitor(self: &Arc<Self>, interval: Duration) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                if let Err(e) = this.run_once().await {
                    warn!("cache cleanup pass failed: {e:#}");
                }
            }
        });
    }

    /// Bytes the downloads directory occupies on disk, at most `USAGE_TTL`
    /// old.
    pub async fn usage_bytes(&self) -> u64 {
        let mut usage = self.usage.lock().await;
        if let Some((measured_at, bytes)) = *usage {
            if measured_at.elapsed() < USAGE_TTL {
                return bytes;
            }
        }
        let bytes = scan(&self.downloads_dir).await.total;
        *usage = Some((Instant::now(), bytes));
        bytes
    }

    /// Forgets the measured usage, so the next ask walks the disk. For
    /// whatever just changed it all at once.
    pub async fn invalidate_usage(&self) {
        *self.usage.lock().await = None;
    }

    pub fn max_size_bytes(&self) -> u64 {
        self.max_size_bytes
    }

    async fn run_once(&self) -> anyhow::Result<()> {
        if !self.auto_cleanup {
            return Ok(());
        }

        let DiskScan { torrents, total } = scan(&self.downloads_dir).await;
        // The walk just measured exactly what `usage_bytes` would.
        *self.usage.lock().await = Some((Instant::now(), total));

        // One look at the activity records for the whole pass.
        let recently_active = self.engine.recently_active(ACTIVE_STREAM_GRACE_PERIOD);
        let last_request = self.last_requests();

        let mut entries = self
            .enforce_retention_count(torrents, &recently_active, &last_request)
            .await;

        let mut total: u64 = entries.iter().map(|e| e.size).sum();
        if total <= self.max_size_bytes {
            return Ok(());
        }

        // Least recently used first, by the same measure retention uses: a
        // torrent nobody has touched in the longest time is the safest thing
        // to reclaim. Directory mtime alone misorders a re-watch of something
        // cached days ago, since writing pieces touches the files inside the
        // directory and not the directory itself.
        entries.sort_by_key(|e| last_used(e, &last_request));

        let mut evicted = false;
        for entry in entries {
            if total <= self.max_size_bytes {
                break;
            }
            // Two independent protections, because either alone has a hole:
            //
            // * `is_running` -- the torrent is unpaused in the session. This
            //   is the strong one. A player buffers minutes of video then goes
            //   quiet, so request-recency alone would call an actively watched
            //   movie idle and delete it mid-playback (observed in practice).
            //   The idle reaper pauses genuinely-unwatched torrents, so
            //   "unpaused" stays meaningful rather than matching everything.
            // * request recency -- covers the window between a client's first
            //   request and the torrent actually being registered/unpaused.
            if self.engine.has_open_stream(&entry.info_hash)
                || self.engine.is_running(&entry.info_hash)
                || recently_active.contains(&entry.info_hash)
            {
                continue;
            }
            match tokio::fs::remove_dir_all(&entry.path).await {
                Ok(()) => {
                    info!(
                        info_hash = %entry.info_hash,
                        bytes = entry.size,
                        "cache: evicted unused torrent to stay under size cap"
                    );
                    total = total.saturating_sub(entry.size);
                    evicted = true;
                }
                Err(e) => warn!(
                    info_hash = %entry.info_hash,
                    path = %entry.path.display(),
                    "cache: failed to evict: {e}"
                ),
            }
        }
        if evicted {
            self.invalidate_usage().await;
        }

        if total > self.max_size_bytes {
            // Everything left is in use or a single torrent is simply bigger
            // than the whole cap (a 4K remux against a 20 GB cap, say). Either
            // way the janitor will keep evicting on every pass and never get
            // under -- worth saying out loud, because the visible symptom is
            // "my downloads keep disappearing" with no obvious cause.
            warn!(
                usage_bytes = total,
                max_bytes = self.max_size_bytes,
                "cache: still over the size cap after evicting everything not in use -- \
                 raise MAX_CACHE_SIZE_GB or stream smaller releases"
            );
        }

        Ok(())
    }

    /// When each torrent last saw a request, as wall-clock time so it can be
    /// compared with directory timestamps.
    fn last_requests(&self) -> HashMap<String, SystemTime> {
        let now = SystemTime::now();
        self.engine
            .recent_streams()
            .into_iter()
            .map(|(hash, _, age_secs)| (hash, now - Duration::from_secs(age_secs)))
            .collect()
    }

    /// The count rule: keep the `max_torrents` most recently used torrents,
    /// purge the rest, and return the survivors for the size pass.
    ///
    /// This runs even when the cache is nowhere near its size cap -- it *is*
    /// the retention policy; the size cap is only the backstop for the case
    /// where the few survivors are themselves enormous.
    async fn enforce_retention_count(
        &self,
        mut entries: Vec<TorrentDirEntry>,
        recently_active: &HashSet<String>,
        last_request: &HashMap<String, SystemTime>,
    ) -> Vec<TorrentDirEntry> {
        if self.max_torrents == 0 || entries.len() <= self.max_torrents {
            return entries;
        }

        entries.sort_by_key(|e| std::cmp::Reverse(last_used(e, last_request)));

        let victims = entries.split_off(self.max_torrents);
        let mut purged = false;
        for entry in victims {
            // A victim is spared while someone is provably still reading it,
            // or while it is still being written.
            //
            // `is_running` is deliberately not consulted here, unlike the
            // size pass: a finished torrent sits unpaused ("live") forever,
            // and honouring that would exempt exactly the watched-and-done
            // backlog this rule exists to throw away. Whatever is actually
            // being watched holds an open reader or a recent request -- and is
            // the freshest entry anyway, so it is in the keep set above.
            //
            // `is_downloading` is the narrower question and must be asked,
            // because a queued download passes neither of the other two
            // tests: it has no reader and issues no HTTP requests, since
            // nobody is watching it yet. Without this the janitor deletes the
            // download queue's own output every 60 seconds -- the files are
            // half-written, so the size cap is nowhere near, and it happens
            // silently under the retention rule instead.
            if !may_purge(
                self.engine.has_open_stream(&entry.info_hash),
                self.engine.is_downloading(&entry.info_hash),
                recently_active.contains(&entry.info_hash),
            ) {
                entries.push(entry);
                continue;
            }

            match self.engine.discard_cached(&entry.info_hash).await {
                Ok(true) => {
                    info!(
                        info_hash = %entry.info_hash,
                        bytes = entry.size,
                        "cache: purged torrent beyond the retention count"
                    );
                    purged = true;
                }
                // The session never heard of it (left over from an earlier
                // run), so the directory is reclaimed directly.
                Ok(false) => match tokio::fs::remove_dir_all(&entry.path).await {
                    Ok(()) => {
                        info!(
                            info_hash = %entry.info_hash,
                            bytes = entry.size,
                            "cache: purged orphaned torrent dir beyond the retention count"
                        );
                        purged = true;
                    }
                    Err(e) => {
                        warn!(
                            info_hash = %entry.info_hash,
                            path = %entry.path.display(),
                            "cache: failed to purge orphaned torrent dir: {e}"
                        );
                        entries.push(entry);
                    }
                },
                Err(e) => {
                    warn!(info_hash = %entry.info_hash, "cache: {e:#}");
                    entries.push(entry);
                }
            }
        }
        if purged {
            self.invalidate_usage().await;
        }

        entries
    }
}

/// When a torrent was last used: its last request if the engine remembers
/// one, its directory's mtime otherwise, whichever is later.
fn last_used(entry: &TorrentDirEntry, last_request: &HashMap<String, SystemTime>) -> SystemTime {
    last_request
        .get(&entry.info_hash)
        .map_or(entry.modified, |requested| (*requested).max(entry.modified))
}

/// Whether a torrent that fell beyond the retention count may actually be
/// deleted. Pure, so the three exemptions can be tested without a live engine.
///
/// Each one covers a case the others miss:
///
/// * **an open stream** is a response body reading those bytes right now.
/// * **still downloading** is the queue's own output. This one has no reader
///   and no recent request -- nobody is watching it yet, which is the entire
///   point of fetching it ahead -- so without this test it looks exactly like
///   cold backlog and gets deleted mid-write, on a cache that is nowhere near
///   its size cap. Note this asks "downloading", not "running": a *finished*
///   torrent sits unpaused forever, and sparing those would exempt precisely
///   the watched-and-done backlog retention exists to reclaim.
/// * **recent activity** covers the gap between a client's last range request
///   and its next one, where there is no connection at all.
fn may_purge(has_open_stream: bool, is_downloading: bool, recently_active: bool) -> bool {
    !(has_open_stream || is_downloading || recently_active)
}

/// Walks the downloads directory once, on the blocking pool.
///
/// One `std::fs` walk on a blocking thread rather than an async walk: every
/// async directory operation is itself a hop to the blocking pool, so walking
/// a few thousand files that way cost a few thousand hops, and the size pass
/// and `usage_bytes` each did it separately.
async fn scan(root: &Path) -> DiskScan {
    let root = root.to_path_buf();
    tokio::task::spawn_blocking(move || scan_blocking(&root))
        .await
        .unwrap_or(DiskScan {
            torrents: Vec::new(),
            total: 0,
        })
}

/// Sizes every top-level entry of `root`, and lists the `<info_hash>/`
/// directories among them. Anything not shaped like a 40-char hex info hash
/// is counted but never listed -- it was not created by this gateway (see
/// `TorrentEngine::start_file`'s `sub_folder`), so the janitor has no
/// business deleting it.
fn scan_blocking(root: &Path) -> DiskScan {
    let mut scan = DiskScan {
        torrents: Vec::new(),
        total: 0,
    };
    let Ok(read_dir) = std::fs::read_dir(root) else {
        return scan;
    };

    for entry in read_dir.flatten() {
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        if !meta.is_dir() {
            scan.total += allocated_size(&meta);
            continue;
        }
        let path = entry.path();
        let size = dir_size_blocking(&path);
        scan.total += size;

        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !crate::torrent::resolver::is_hex40(name) {
            continue;
        }
        scan.torrents.push(TorrentDirEntry {
            info_hash: name.to_ascii_lowercase(),
            path,
            size,
            modified: meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
        });
    }
    scan
}

/// How many bytes a file actually occupies on disk.
///
/// Deliberately *not* `metadata.len()`. librqbit creates each file at its
/// full final length before a single byte has been downloaded, leaving a
/// sparse file whose apparent size is the whole movie and whose allocated
/// size is almost nothing. Measuring `len()` therefore reports a fresh 5 GB
/// episode as 5 GB of cache the instant playback starts -- which is what made
/// the cache jump straight to its cap on the first play after a clear, and,
/// worse, made the janitor evict torrents to get under a limit it was never
/// actually near.
///
/// Counting allocated blocks measures what the disk has really given up, so
/// the number tracks the download and the cap means what it says.
#[cfg(unix)]
fn allocated_size(meta: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    // `blocks()` is in 512-byte units by POSIX definition, regardless of the
    // filesystem's own block size.
    meta.blocks().saturating_mul(512)
}

/// Elsewhere there is no portable way to ask, so fall back to the apparent
/// size. It over-reports sparse files, which is the safe direction: the cache
/// stays under its cap rather than over it.
#[cfg(not(unix))]
fn allocated_size(meta: &std::fs::Metadata) -> u64 {
    meta.len()
}

/// Recursive directory size, in allocated bytes. Unreadable entries count as
/// empty: this feeds a cap and a status line, neither worth failing over.
fn dir_size_blocking(path: &Path) -> u64 {
    let Ok(read_dir) = std::fs::read_dir(path) else {
        return 0;
    };
    read_dir
        .flatten()
        .map(|entry| match entry.metadata() {
            Ok(meta) if meta.is_dir() => dir_size_blocking(&entry.path()),
            Ok(meta) => allocated_size(&meta),
            Err(_) => 0,
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dir_size_sums_nested_files() {
        let tmp = tempdir();
        tokio::fs::write(tmp.join("a.bin"), vec![0u8; 100])
            .await
            .unwrap();
        tokio::fs::create_dir(tmp.join("sub")).await.unwrap();
        tokio::fs::write(tmp.join("sub/b.bin"), vec![0u8; 250])
            .await
            .unwrap();

        // Both files are counted, including the nested one. The exact figure
        // is the space the filesystem allocated, which rounds each file up to
        // a block, so this asserts the data is accounted for rather than an
        // exact sum that would only hold on one filesystem.
        let size = dir_size_blocking(&tmp);
        assert!(size >= 350, "both files must be counted, got {size}");

        tokio::fs::remove_dir_all(&tmp).await.unwrap();
    }

    /// The regression this module exists to avoid re-introducing.
    ///
    /// librqbit lays every download out at its full final length up front, so
    /// a movie that has barely started is a mostly-empty sparse file. Sizing
    /// the cache by apparent length counts that emptiness as used space: the
    /// gateway reported a multi-gigabyte cache seconds after a clear, and the
    /// janitor deleted torrents to get under a cap it had not really reached.
    #[tokio::test]
    async fn dir_size_counts_allocated_bytes_not_a_sparse_file_s_apparent_length() {
        use tokio::io::AsyncWriteExt;

        let tmp = tempdir();
        let apparent_len = 512 * 1024 * 1024; // half a gigabyte of nothing
        let written = 64 * 1024;

        let mut file = tokio::fs::File::create(tmp.join("movie.mkv"))
            .await
            .unwrap();
        file.set_len(apparent_len).await.unwrap();
        file.write_all(&vec![7u8; written]).await.unwrap();
        file.sync_all().await.unwrap();
        drop(file);

        let size = dir_size_blocking(&tmp);

        assert!(
            size >= written as u64,
            "the bytes actually written must be counted, got {size}"
        );
        assert!(
            size < apparent_len / 4,
            "a sparse hole must not be counted as cache usage: reported {size} \
             for a file holding only {written} real bytes"
        );

        tokio::fs::remove_dir_all(&tmp).await.unwrap();
    }

    /// The regression the download queue would otherwise introduce.
    ///
    /// Retention orders by recency and purges everything past the count. A
    /// torrent the queue is still fetching is not recent by either of the
    /// other two measures -- no reader, no HTTP request, because nobody is
    /// watching it yet -- so it sorts to the bottom and gets deleted on the
    /// next janitor pass, sixty seconds after it started, with the cache
    /// nowhere near its size cap.
    #[test]
    fn retention_never_deletes_a_download_still_in_progress() {
        assert!(
            !may_purge(false, true, false),
            "a torrent still being written must survive the retention sweep"
        );
    }

    #[test]
    fn retention_reclaims_a_finished_torrent_nobody_is_reading() {
        // The whole point of the count rule: a watched-and-done episode is
        // exactly what it exists to throw away. A finished torrent is left
        // unpaused ("live") by librqbit forever, so this is the case that
        // breaks if the exemption above is ever widened from "downloading" to
        // "running".
        assert!(may_purge(false, false, false));
    }

    #[test]
    fn retention_spares_whatever_is_being_read_or_was_just_read() {
        assert!(!may_purge(true, false, false), "a live reader");
        assert!(!may_purge(false, false, true), "between range requests");
    }

    #[test]
    fn the_scan_lists_only_torrent_directories_but_counts_everything() {
        let tmp = tempdir();
        let hash = "0123456789abcdef0123456789abcdef01234567";
        std::fs::create_dir(tmp.join(hash)).unwrap();
        std::fs::write(tmp.join(hash).join("movie.mkv"), vec![0u8; 10]).unwrap();
        std::fs::create_dir(tmp.join("not-a-hash")).unwrap();
        std::fs::write(tmp.join("not-a-hash").join("stray.bin"), vec![0u8; 10]).unwrap();

        let scan = scan_blocking(&tmp);
        assert_eq!(scan.torrents.len(), 1);
        assert_eq!(scan.torrents[0].info_hash, hash);
        assert!(
            scan.torrents[0].size > 0,
            "the torrent's data must be measured"
        );
        assert!(
            scan.total > scan.torrents[0].size,
            "usage covers the whole directory, not only what the janitor may delete"
        );

        std::fs::remove_dir_all(&tmp).unwrap();
    }

    /// Both passes order by the same measure, so the size pass cannot evict
    /// the title retention just decided was the freshest.
    #[test]
    fn a_recent_request_outranks_an_old_directory_timestamp() {
        let long_ago = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let entry = |hash: &str| TorrentDirEntry {
            path: PathBuf::from(hash),
            info_hash: hash.to_string(),
            size: 0,
            modified: long_ago,
        };
        let rewatched = entry("rewatched");
        let untouched = entry("untouched");
        let now = SystemTime::now();
        let requests = HashMap::from([("rewatched".to_string(), now)]);

        assert_eq!(last_used(&rewatched, &requests), now);
        assert_eq!(last_used(&untouched, &requests), long_ago);
    }

    fn tempdir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "streaming-gateway-cache-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}
