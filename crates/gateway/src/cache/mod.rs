//! Cache management for downloaded torrent data.
//!
//! Partial-download resume and "reuse a completed file instead of
//! re-downloading it" are not reimplemented here -- they come for free from
//! librqbit's own on-disk piece storage plus session fastresume/persistence
//! (wired up in `TorrentEngine::new`). What this module owns is the one
//! thing librqbit intentionally does *not* do on its own: enforcing a size
//! cap by deleting old, unwatched torrents.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tracing::{info, warn};

use crate::torrent::TorrentEngine;

/// A torrent never gets evicted while it has been read from within this
/// window, even if the cache is over its size cap -- a seek or the next
/// buffer refill could come from a viewer at any moment.
const ACTIVE_STREAM_GRACE_PERIOD: Duration = Duration::from_secs(300);

pub struct CacheManager {
    downloads_dir: PathBuf,
    max_size_bytes: u64,
    auto_cleanup: bool,
    engine: Arc<TorrentEngine>,
}

struct TorrentDirEntry {
    path: PathBuf,
    info_hash: String,
    size: u64,
    modified: SystemTime,
}

impl CacheManager {
    pub fn new(
        downloads_dir: PathBuf,
        max_size_bytes: u64,
        auto_cleanup: bool,
        engine: Arc<TorrentEngine>,
    ) -> Arc<Self> {
        Arc::new(Self {
            downloads_dir,
            max_size_bytes,
            auto_cleanup,
            engine,
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

    pub async fn usage_bytes(&self) -> u64 {
        dir_size(&self.downloads_dir).await.unwrap_or(0)
    }

    pub fn max_size_bytes(&self) -> u64 {
        self.max_size_bytes
    }

    async fn run_once(&self) -> anyhow::Result<()> {
        if !self.auto_cleanup {
            return Ok(());
        }

        let mut entries = list_torrent_dirs(&self.downloads_dir).await?;
        let mut total: u64 = entries.iter().map(|e| e.size).sum();
        if total <= self.max_size_bytes {
            return Ok(());
        }

        // Oldest last-modified first: a torrent nobody has touched in the
        // longest time is the safest thing to reclaim.
        entries.sort_by_key(|e| e.modified);

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
                || self
                    .engine
                    .is_recently_active(&entry.info_hash, ACTIVE_STREAM_GRACE_PERIOD)
                    .await
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
                }
                Err(e) => warn!(
                    info_hash = %entry.info_hash,
                    path = %entry.path.display(),
                    "cache: failed to evict: {e}"
                ),
            }
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
}

/// Lists top-level `downloads_dir/<info_hash>/` directories. Anything not
/// shaped like a 40-char hex info hash is left untouched -- it was not
/// created by this gateway (see `TorrentEngine::start_file`'s `sub_folder`),
/// so the janitor has no business deleting it.
async fn list_torrent_dirs(root: &Path) -> anyhow::Result<Vec<TorrentDirEntry>> {
    let mut out = Vec::new();
    let mut read_dir = match tokio::fs::read_dir(root).await {
        Ok(rd) => rd,
        Err(_) => return Ok(out),
    };

    while let Some(entry) = read_dir.next_entry().await? {
        let path = entry.path();
        let Ok(meta) = entry.metadata().await else {
            continue;
        };
        if !meta.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name.len() != 40 || !name.bytes().all(|b| b.is_ascii_hexdigit()) {
            continue;
        }
        let info_hash = name.to_ascii_lowercase();

        let size = dir_size(&path).await.unwrap_or(0);
        let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        out.push(TorrentDirEntry {
            path,
            info_hash,
            size,
            modified,
        });
    }

    Ok(out)
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

/// Recursive directory size. Boxed because an `async fn` cannot recurse into
/// itself directly (its future would have infinite size).
fn dir_size(path: &Path) -> Pin<Box<dyn Future<Output = anyhow::Result<u64>> + Send + '_>> {
    Box::pin(async move {
        let mut total = 0u64;
        let mut read_dir = match tokio::fs::read_dir(path).await {
            Ok(rd) => rd,
            Err(_) => return Ok(0),
        };
        while let Some(entry) = read_dir.next_entry().await? {
            let Ok(meta) = entry.metadata().await else {
                continue;
            };
            if meta.is_dir() {
                total += dir_size(&entry.path()).await?;
            } else {
                total += allocated_size(&meta);
            }
        }
        Ok(total)
    })
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
        let size = dir_size(&tmp).await.unwrap();
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

        let mut file = tokio::fs::File::create(tmp.join("movie.mkv")).await.unwrap();
        file.set_len(apparent_len).await.unwrap();
        file.write_all(&vec![7u8; written]).await.unwrap();
        file.sync_all().await.unwrap();
        drop(file);

        let size = dir_size(&tmp).await.unwrap();

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

    #[tokio::test]
    async fn list_torrent_dirs_ignores_non_hash_directories() {
        let tmp = tempdir();
        let hash = "0123456789abcdef0123456789abcdef01234567";
        tokio::fs::create_dir(tmp.join(hash)).await.unwrap();
        tokio::fs::write(tmp.join(hash).join("movie.mkv"), vec![0u8; 10])
            .await
            .unwrap();
        tokio::fs::create_dir(tmp.join("not-a-hash")).await.unwrap();

        let entries = list_torrent_dirs(&tmp).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].info_hash, hash);
        assert!(entries[0].size > 0, "the torrent's data must be measured");

        tokio::fs::remove_dir_all(&tmp).await.unwrap();
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
