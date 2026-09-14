//! Torrent *discovery*: turning a content id (an IMDB id like `tt0111161`)
//! into a list of candidate torrents to stream.
//!
//! The rest of this gateway resolves and streams torrents it is handed; this
//! module is the one place that goes looking for them. It deliberately does
//! not scrape torrent sites itself -- it queries an existing Stremio-protocol
//! index (Torrentio by default) over HTTPS and normalizes the answer.
//!
//! Keeping this behind a trait matters: the index is the piece most likely to
//! be swapped (a self-hosted Jackett bridge, a different public index, or
//! several merged together), and none of that should touch the streaming path.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::torrent::resolver::is_hex40;
use crate::util::lock;

/// A torrent the index thinks matches the requested title.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexedTorrent {
    pub info_hash: String,
    /// Which file inside the torrent holds this episode/movie, when the index
    /// knows. `None` means "let the gateway pick the largest video file".
    pub file_idx: Option<usize>,
    /// Release name, e.g. `Movie.2024.1080p.BluRay.x264-GROUP`.
    pub title: String,
    /// Short quality/source tag the index assigned, e.g. `1080p`.
    pub quality: String,
    pub size_bytes: Option<u64>,
    pub seeders: Option<u32>,
    /// Tracker URLs the index reported for this exact release (Torrentio's
    /// `sources` field, `tracker:` entries). These are where this torrent's
    /// seeders actually announce, so starting with them beats starting from
    /// a bare hash and hoping DHT finds the swarm quickly.
    pub trackers: Vec<String>,
}

/// What a search hands back: a boxed future, so the trait can be used as
/// `dyn StreamIndexer` and an index can be wrapped (see `CachingIndexer`) or
/// swapped without every holder naming the concrete type.
pub type SearchFuture<'a> = Pin<Box<dyn Future<Output = Result<Vec<IndexedTorrent>>> + Send + 'a>>;

/// Anything that can turn a content id into candidate torrents.
pub trait StreamIndexer: Send + Sync {
    fn search<'a>(&'a self, content_type: &'a str, content_id: &'a str) -> SearchFuture<'a>;
}

// ---------------------------------------------------------------------------
// Caching
// ---------------------------------------------------------------------------

/// How long one title's search result is reused.
///
/// Stremio asks for the same stream list over and over while someone browses:
/// opening a title, backing out, opening it again, and preloading the next
/// episode. Each ask was a round trip to a public index -- the slowest part of
/// showing the list, and a load on someone else's service. Five minutes is
/// short enough that seeder counts are still meaningful for choosing a source.
const SEARCH_CACHE_TTL: Duration = Duration::from_secs(300);

/// How many distinct titles keep a cached result.
const SEARCH_CACHE_CAPACITY: usize = 64;

type CacheKey = (String, String);

/// Remembers successful searches for a few minutes.
///
/// Failures are never cached: a timeout or an index hiccup has to be retried
/// by the next ask, not replayed to it.
pub struct CachingIndexer {
    inner: Arc<dyn StreamIndexer>,
    ttl: Duration,
    capacity: usize,
    entries: Mutex<HashMap<CacheKey, (Instant, Vec<IndexedTorrent>)>>,
}

impl CachingIndexer {
    pub fn new(inner: Arc<dyn StreamIndexer>) -> Self {
        Self::with_limits(inner, SEARCH_CACHE_TTL, SEARCH_CACHE_CAPACITY)
    }

    fn with_limits(inner: Arc<dyn StreamIndexer>, ttl: Duration, capacity: usize) -> Self {
        Self {
            inner,
            ttl,
            capacity: capacity.max(1),
            entries: Mutex::new(HashMap::new()),
        }
    }

    fn fresh(&self, key: &CacheKey) -> Option<Vec<IndexedTorrent>> {
        lock(&self.entries)
            .get(key)
            .filter(|(stored_at, _)| stored_at.elapsed() < self.ttl)
            .map(|(_, found)| found.clone())
    }

    fn store(&self, key: CacheKey, found: Vec<IndexedTorrent>) {
        let mut entries = lock(&self.entries);
        if entries.len() >= self.capacity && !entries.contains_key(&key) {
            let ttl = self.ttl;
            entries.retain(|_, (stored_at, _)| stored_at.elapsed() < ttl);
            if entries.len() >= self.capacity {
                let oldest = entries
                    .iter()
                    .min_by_key(|(_, (stored_at, _))| *stored_at)
                    .map(|(key, _)| key.clone());
                if let Some(oldest) = oldest {
                    entries.remove(&oldest);
                }
            }
        }
        entries.insert(key, (Instant::now(), found));
    }
}

impl StreamIndexer for CachingIndexer {
    fn search<'a>(&'a self, content_type: &'a str, content_id: &'a str) -> SearchFuture<'a> {
        Box::pin(async move {
            let key = (content_type.to_string(), content_id.to_string());
            if let Some(found) = self.fresh(&key) {
                return Ok(found);
            }
            let found = self.inner.search(content_type, content_id).await?;
            self.store(key, found.clone());
            Ok(found)
        })
    }
}

// ---------------------------------------------------------------------------
// Torrentio
// ---------------------------------------------------------------------------

/// Queries a Torrentio-compatible endpoint (`/stream/{type}/{id}.json`).
///
/// Torrentio speaks the same Stremio addon protocol this gateway serves, which
/// is convenient but *not* a coincidence worth relying on structurally: we map
/// its response into our own `IndexedTorrent` rather than forwarding it, so a
/// future non-Stremio index plugs in without changing callers.
pub struct TorrentioIndexer {
    client: reqwest::Client,
    base_url: String,
    max_results: usize,
}

impl TorrentioIndexer {
    pub fn new(base_url: String, max_results: usize, timeout: std::time::Duration) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            // A dead or unreachable index should fail at the connect, not
            // after the whole request budget.
            .connect_timeout(timeout.min(Duration::from_secs(10)))
            // Browsing asks in bursts; keep the TLS connection between them
            // rather than paying a fresh handshake for each title.
            .pool_idle_timeout(Duration::from_secs(90))
            // A default UA gets refused by some public indexes.
            .user_agent(concat!(
                "streaming-gateway/",
                env!("CARGO_PKG_VERSION"),
                " (+https://github.com/AzizBahloul/local-real-debrid-alternative)"
            ))
            .build()
            .context("failed to build indexer HTTP client")?;

        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            max_results,
        })
    }
}

#[derive(Debug, Deserialize)]
struct TorrentioResponse {
    #[serde(default)]
    streams: Vec<TorrentioStream>,
}

#[derive(Debug, Deserialize)]
struct TorrentioStream {
    #[serde(rename = "infoHash")]
    info_hash: Option<String>,
    #[serde(rename = "fileIdx")]
    file_idx: Option<usize>,
    /// Quality/source label, often multi-line (`"Torrentio\n1080p"`).
    #[serde(default)]
    name: String,
    /// Release name plus a stats line (`"Name\n👤 42 💾 2.1 GB ⚙️ Source"`).
    #[serde(default)]
    title: String,
    /// Peer sources, e.g. `["tracker:udp://x:1337/announce", "dht:<hash>"]`.
    #[serde(default)]
    sources: Vec<String>,
}

/// Extracts the tracker URLs out of Torrentio's `sources` list. The `dht:`
/// entries carry nothing a client does not already know (the info hash), so
/// only `tracker:` entries matter.
fn tracker_sources(sources: &[String]) -> Vec<String> {
    sources
        .iter()
        .filter_map(|s| s.strip_prefix("tracker:"))
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
        .collect()
}

impl StreamIndexer for TorrentioIndexer {
    fn search<'a>(&'a self, content_type: &'a str, content_id: &'a str) -> SearchFuture<'a> {
        Box::pin(self.fetch(content_type, content_id))
    }
}

impl TorrentioIndexer {
    async fn fetch(&self, content_type: &str, content_id: &str) -> Result<Vec<IndexedTorrent>> {
        // Both segments are validated by the caller before we get here, but
        // percent-encode anyway so a stray character can never reshape the URL.
        let url = format!(
            "{}/stream/{}/{}.json",
            self.base_url,
            urlencoding::encode(content_type),
            urlencoding::encode(content_id)
        );

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("indexer request to {url} failed"))?;

        let status = response.status();
        if !status.is_success() {
            anyhow::bail!("indexer returned HTTP {status}");
        }

        let body: TorrentioResponse = response
            .json()
            .await
            .context("indexer returned a body that is not the expected JSON shape")?;

        let mut out: Vec<IndexedTorrent> = body
            .streams
            .into_iter()
            .filter_map(|s| {
                // No info hash means nothing we can stream (e.g. a debrid
                // link). Skipping is correct, not an error.
                let info_hash = s.info_hash?;
                if !is_hex40(&info_hash) {
                    return None;
                }

                let (title, stats) = split_once_newline(&s.title);
                Some(IndexedTorrent {
                    info_hash: info_hash.to_ascii_lowercase(),
                    file_idx: s.file_idx,
                    title: if title.is_empty() {
                        info_hash.clone()
                    } else {
                        title
                    },
                    quality: clean_quality(&s.name),
                    size_bytes: parse_size(&stats),
                    seeders: parse_seeders(&stats),
                    trackers: tracker_sources(&s.sources),
                })
            })
            .collect();

        // Seeders are the single best predictor of whether a stream will
        // actually start, so lead with them; size breaks ties downward
        // (a 2 GB 1080p rip starts far sooner than a 50 GB remux).
        out.sort_by(|a, b| {
            b.seeders
                .unwrap_or(0)
                .cmp(&a.seeders.unwrap_or(0))
                .then_with(|| {
                    a.size_bytes
                        .unwrap_or(u64::MAX)
                        .cmp(&b.size_bytes.unwrap_or(u64::MAX))
                })
        });
        out.truncate(self.max_results);
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Parsing helpers for Torrentio's human-formatted stats line
// ---------------------------------------------------------------------------

/// Splits `"Release.Name\n👤 42 💾 2.1 GB"` into its two halves. Torrentio
/// sometimes omits the stats line entirely, hence the empty-string fallback.
fn split_once_newline(value: &str) -> (String, String) {
    match value.split_once('\n') {
        Some((head, tail)) => (head.trim().to_string(), tail.trim().to_string()),
        None => (value.trim().to_string(), String::new()),
    }
}

/// Turns `"Torrentio\n4k HDR"` into `"4k HDR"`; the provider name is noise
/// once the stream is listed under our own addon.
fn clean_quality(name: &str) -> String {
    name.split('\n')
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.eq_ignore_ascii_case("torrentio"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Reads the seeder count that follows the 👤 marker.
fn parse_seeders(stats: &str) -> Option<u32> {
    field_after(stats, '\u{1F464}')?.parse().ok()
}

/// Reads the size that follows the 💾 marker (`"2.1 GB"`) as bytes.
fn parse_size(stats: &str) -> Option<u64> {
    let idx = stats.find('\u{1F4BE}')?;
    let rest = &stats[idx + '\u{1F4BE}'.len_utf8()..];
    let mut parts = rest.split_whitespace();
    let value: f64 = parts.next()?.replace(',', ".").parse().ok()?;
    let multiplier = match parts.next()?.to_ascii_uppercase().as_str() {
        "B" => 1.0,
        "KB" | "KIB" => 1024.0,
        "MB" | "MIB" => 1024.0 * 1024.0,
        "GB" | "GIB" => 1024.0 * 1024.0 * 1024.0,
        "TB" | "TIB" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => return None,
    };
    Some((value * multiplier) as u64)
}

/// First whitespace-delimited token after `marker`.
fn field_after(haystack: &str, marker: char) -> Option<&str> {
    let idx = haystack.find(marker)?;
    haystack[idx + marker.len_utf8()..]
        .split_whitespace()
        .next()
}

#[cfg(test)]
mod tests {
    use super::*;

    const STATS: &str = "\u{1F464} 101 \u{1F4BE} 54.33 GB \u{2699}\u{FE0F} TorrentGalaxy";

    #[test]
    fn parses_seeders_from_stats_line() {
        assert_eq!(parse_seeders(STATS), Some(101));
    }

    #[test]
    fn parses_size_from_stats_line() {
        // 54.33 GiB
        let expected = (54.33 * 1024.0 * 1024.0 * 1024.0) as u64;
        assert_eq!(parse_size(STATS), Some(expected));
    }

    #[test]
    fn parses_size_units_case_insensitively() {
        assert_eq!(parse_size("\u{1F4BE} 700 mb"), Some(700 * 1024 * 1024));
        assert_eq!(parse_size("\u{1F4BE} 1.5 TiB"), Some(1_649_267_441_664));
    }

    #[test]
    fn missing_stats_parse_to_none_rather_than_zero() {
        // Zero would silently sort a healthy torrent to the bottom; None is
        // the honest answer and is handled explicitly at the sort site.
        assert_eq!(parse_seeders("no markers here"), None);
        assert_eq!(parse_size("no markers here"), None);
    }

    #[test]
    fn strips_provider_name_from_quality_label() {
        assert_eq!(clean_quality("Torrentio\n4k HDR"), "4k HDR");
        assert_eq!(clean_quality("Torrentio"), "");
        assert_eq!(clean_quality("1080p"), "1080p");
    }

    #[test]
    fn splits_release_name_from_stats() {
        let (name, stats) = split_once_newline("Movie.2024.1080p\n\u{1F464} 12 \u{1F4BE} 2 GB");
        assert_eq!(name, "Movie.2024.1080p");
        assert!(stats.starts_with('\u{1F464}'));
    }

    #[test]
    fn title_without_stats_line_still_parses() {
        let (name, stats) = split_once_newline("Just.A.Name");
        assert_eq!(name, "Just.A.Name");
        assert_eq!(stats, "");
    }

    #[test]
    fn extracts_only_tracker_entries_from_sources() {
        let sources = vec![
            "tracker:udp://tracker.example:1337/announce".to_string(),
            "tracker: udp://spaced.example:80/announce ".to_string(),
            "dht:45fa4233ef87c58f5f8b4817e4d50c9f5363caef".to_string(),
            "tracker:".to_string(),
        ];
        assert_eq!(
            tracker_sources(&sources),
            vec![
                "udp://tracker.example:1337/announce".to_string(),
                "udp://spaced.example:80/announce".to_string(),
            ],
            "dht/empty entries carry nothing a client does not already know"
        );
        assert!(tracker_sources(&[]).is_empty());
    }

    #[test]
    fn rejects_non_hex40_info_hashes() {
        assert!(is_hex40("45fa4233ef87c58f5f8b4817e4d50c9f5363caef"));
        assert!(!is_hex40("tooshort"));
        assert!(!is_hex40("../../etc/passwd"));
        assert!(!is_hex40(""));
    }

    /// Answers with one torrent per call and counts the calls; fails while
    /// `failing` is set.
    #[derive(Default)]
    struct FakeIndex {
        calls: std::sync::atomic::AtomicUsize,
        failing: std::sync::atomic::AtomicBool,
    }

    impl StreamIndexer for FakeIndex {
        fn search<'a>(&'a self, _: &'a str, content_id: &'a str) -> SearchFuture<'a> {
            use std::sync::atomic::Ordering;
            Box::pin(async move {
                let call = self.calls.fetch_add(1, Ordering::SeqCst);
                if self.failing.load(Ordering::SeqCst) {
                    anyhow::bail!("index unavailable");
                }
                Ok(vec![IndexedTorrent {
                    info_hash: format!("{call:040}"),
                    file_idx: None,
                    title: content_id.to_string(),
                    quality: String::new(),
                    size_bytes: None,
                    seeders: None,
                    trackers: Vec::new(),
                }])
            })
        }
    }

    fn calls(fake: &FakeIndex) -> usize {
        fake.calls.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[tokio::test]
    async fn browsing_the_same_title_again_does_not_ask_the_index_again() {
        let fake = Arc::new(FakeIndex::default());
        let cached = CachingIndexer::new(fake.clone());

        let first = cached.search("movie", "tt1").await.unwrap();
        let second = cached.search("movie", "tt1").await.unwrap();
        assert_eq!(first, second);
        assert_eq!(calls(&fake), 1);

        cached.search("series", "tt1").await.unwrap();
        cached.search("movie", "tt2").await.unwrap();
        assert_eq!(
            calls(&fake),
            3,
            "a different type or title is a different search"
        );
    }

    #[tokio::test]
    async fn a_failed_search_is_retried_rather_than_replayed() {
        let fake = Arc::new(FakeIndex::default());
        let cached = CachingIndexer::new(fake.clone());

        fake.failing
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(cached.search("movie", "tt1").await.is_err());
        fake.failing
            .store(false, std::sync::atomic::Ordering::SeqCst);
        assert!(cached.search("movie", "tt1").await.is_ok());
        assert_eq!(calls(&fake), 2);
    }

    #[tokio::test]
    async fn a_cached_result_expires() {
        let fake = Arc::new(FakeIndex::default());
        let cached = CachingIndexer::with_limits(fake.clone(), Duration::from_millis(20), 8);

        cached.search("movie", "tt1").await.unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
        cached.search("movie", "tt1").await.unwrap();
        assert_eq!(calls(&fake), 2);
    }

    #[tokio::test]
    async fn the_cache_stays_within_its_capacity() {
        let fake = Arc::new(FakeIndex::default());
        let cached = CachingIndexer::with_limits(fake.clone(), Duration::from_secs(60), 2);

        for id in ["tt1", "tt2", "tt3"] {
            cached.search("movie", id).await.unwrap();
        }
        assert_eq!(lock(&cached.entries).len(), 2);
        // The oldest went; the newest two are still served from memory.
        cached.search("movie", "tt3").await.unwrap();
        assert_eq!(calls(&fake), 3);
    }
}
