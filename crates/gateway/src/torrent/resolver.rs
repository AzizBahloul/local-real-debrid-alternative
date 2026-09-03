//! Turns user-supplied input (a magnet URI or a bare info hash) into a normalized
//! magnet link, and picks the file inside a torrent that is worth streaming.
//!
//! This is deliberately the *only* place that knows how to go from "raw input"
//! to "a magnet link". Search/indexing (looking up a magnet for an IMDB id,
//! querying a torrent index, etc.) is out of scope on purpose: this gateway
//! resolves torrents it is already given, it does not find them. Swapping in
//! a different provider later (e.g. resolving a `.torrent` file URL) only
//! means adding another `TorrentSource` impl.

use anyhow::{bail, Context, Result};

/// Extensions recognized as directly playable video containers.
const VIDEO_EXTENSIONS: &[&str] = &[
    "mkv", "mp4", "avi", "mov", "webm", "m4v", "ts", "wmv", "flv", "m2ts", "vob",
];

/// Trackers attached to any magnet we build ourselves from a bare info hash.
/// An index hands us only the hash; with no trackers the torrent has nothing
/// but DHT to find peers on, which is much slower to start and on some
/// networks never starts at all.
pub const DEFAULT_TRACKERS: &[&str] = &[
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.demonii.com:1337/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://tracker.openbittorrent.com:6969/announce",
];

/// Builds a magnet URI from a bare info hash, with `DEFAULT_TRACKERS` attached.
pub fn magnet_with_trackers(info_hash: &str, display_name: Option<&str>) -> String {
    let mut magnet = format!("magnet:?xt=urn:btih:{info_hash}");
    if let Some(name) = display_name {
        magnet.push_str(&format!("&dn={}", urlencoding::encode(name)));
    }
    for tracker in DEFAULT_TRACKERS {
        magnet.push_str(&format!("&tr={}", urlencoding::encode(tracker)));
    }
    magnet
}

/// A single file inside a resolved torrent.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TorrentFile {
    pub index: usize,
    pub name: String,
    pub length: u64,
    pub is_video: bool,
}

/// The result of fetching a torrent's metadata (no data downloaded yet).
#[derive(Debug, Clone, serde::Serialize)]
pub struct ResolvedTorrent {
    pub info_hash: String,
    pub name: Option<String>,
    pub magnet: String,
    pub files: Vec<TorrentFile>,
    /// Best-guess file index to play, when the torrent contains any video file.
    pub suggested_file_idx: Option<usize>,
}

impl ResolvedTorrent {
    pub fn file(&self, idx: usize) -> Option<&TorrentFile> {
        self.files.iter().find(|f| f.index == idx)
    }
}

/// Abstraction over "something that can turn an identifier into a magnet link".
/// Today only raw magnet/info-hash input is supported; a future provider
/// (e.g. resolving a `.torrent` file URL) plugs in here without touching
/// callers.
pub trait TorrentSource: Send + Sync {
    fn to_magnet(&self, input: &str) -> Result<String>;
}

pub struct MagnetSource;

impl TorrentSource for MagnetSource {
    fn to_magnet(&self, input: &str) -> Result<String> {
        normalize_to_magnet(input)
    }
}

/// Accepts either a full `magnet:?xt=urn:btih:...` URI or a bare 40-char hex
/// (or 32-char base32) BitTorrent info hash, and returns a magnet URI.
pub fn normalize_to_magnet(input: &str) -> Result<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        bail!("empty magnet/info-hash");
    }

    if let Some(magnet) = trimmed.strip_prefix("magnet:?") {
        // Sanity-check it actually carries a btih exact topic, so obviously
        // malformed input fails fast with a clear error instead of a confusing
        // failure three network hops later inside the torrent engine.
        if !magnet.contains("xt=urn:btih:") {
            bail!("magnet link is missing an 'xt=urn:btih:' info hash");
        }
        return Ok(trimmed.to_string());
    }

    let is_hex40 = trimmed.len() == 40 && trimmed.bytes().all(|b| b.is_ascii_hexdigit());
    let is_base32_32 = trimmed.len() == 32
        && trimmed
            .bytes()
            .all(|b| b.is_ascii_uppercase() || (b'2'..=b'7').contains(&b));

    if is_hex40 || is_base32_32 {
        return Ok(format!("magnet:?xt=urn:btih:{trimmed}"));
    }

    bail!(
        "'{trimmed}' is not a magnet link or a valid info hash (expected magnet:?xt=urn:btih:... \
         or a 40-char hex / 32-char base32 hash)"
    )
}

/// Extracts the display info-hash straight out of a magnet URI, for cases
/// where we already normalized but need the bare hash (e.g. building our own
/// canonical stream URLs).
pub fn info_hash_from_magnet(magnet: &str) -> Result<String> {
    let query = magnet.split_once('?').map_or(magnet, |(_, q)| q);
    let btih = query
        .split('&')
        .find_map(|part| part.strip_prefix("xt=urn:btih:"))
        .context("magnet link has no btih info hash")?;
    Ok(btih.to_ascii_lowercase())
}

pub fn is_video_file(name: &str) -> bool {
    name.rsplit('.')
        .next()
        .map(|ext| VIDEO_EXTENSIONS.contains(&ext.to_ascii_lowercase().as_str()))
        .unwrap_or(false)
}

/// Picks the largest video file, on the assumption that samples/extras are
/// small and the main feature is the largest file in the torrent.
pub fn suggest_video_file(files: &[TorrentFile]) -> Option<usize> {
    files
        .iter()
        .filter(|f| f.is_video)
        .max_by_key(|f| f.length)
        .map(|f| f.index)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_bare_hex_hash() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let magnet = normalize_to_magnet(hash).unwrap();
        assert_eq!(magnet, format!("magnet:?xt=urn:btih:{hash}"));
    }

    #[test]
    fn passes_through_valid_magnet() {
        let magnet = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=Example";
        assert_eq!(normalize_to_magnet(magnet).unwrap(), magnet);
    }

    #[test]
    fn rejects_magnet_without_btih() {
        let magnet = "magnet:?dn=Example";
        assert!(normalize_to_magnet(magnet).is_err());
    }

    #[test]
    fn rejects_garbage_input() {
        assert!(normalize_to_magnet("not a magnet or hash").is_err());
        assert!(normalize_to_magnet("").is_err());
        // path-traversal-looking input must never be treated as a hash
        assert!(normalize_to_magnet("../../etc/passwd").is_err());
    }

    #[test]
    fn magnet_with_trackers_carries_hash_name_and_trackers() {
        let magnet = magnet_with_trackers(
            "45fa4233ef87c58f5f8b4817e4d50c9f5363caef",
            Some("Movie 2024"),
        );
        assert!(magnet.starts_with("magnet:?xt=urn:btih:45fa4233ef87c58f5f8b4817e4d50c9f5363caef"));
        assert!(magnet.contains("&dn=Movie%202024"));
        assert!(magnet.contains("&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337%2Fannounce"));
        // Must round-trip through our own parser.
        assert_eq!(
            info_hash_from_magnet(&magnet).unwrap(),
            "45fa4233ef87c58f5f8b4817e4d50c9f5363caef"
        );
    }

    #[test]
    fn magnet_with_trackers_omits_dn_when_no_name_is_known() {
        let magnet = magnet_with_trackers("45fa4233ef87c58f5f8b4817e4d50c9f5363caef", None);
        assert!(!magnet.contains("&dn="));
        assert!(normalize_to_magnet(&magnet).is_ok());
    }

    #[test]
    fn extracts_info_hash_from_magnet() {
        let magnet = "magnet:?xt=urn:btih:ABCDEF0123456789abcdef0123456789abcdef01&dn=x";
        assert_eq!(
            info_hash_from_magnet(magnet).unwrap(),
            "abcdef0123456789abcdef0123456789abcdef01"
        );
    }

    #[test]
    fn detects_video_extensions_case_insensitively() {
        assert!(is_video_file("Movie.MKV"));
        assert!(is_video_file("clip.mp4"));
        assert!(!is_video_file("readme.txt"));
        assert!(!is_video_file("sub.srt"));
    }

    #[test]
    fn suggests_largest_video_file_ignoring_extras() {
        let files = vec![
            TorrentFile {
                index: 0,
                name: "sample.mp4".into(),
                length: 5_000_000,
                is_video: true,
            },
            TorrentFile {
                index: 1,
                name: "Movie.mkv".into(),
                length: 1_500_000_000,
                is_video: true,
            },
            TorrentFile {
                index: 2,
                name: "readme.txt".into(),
                length: 200,
                is_video: false,
            },
        ];
        assert_eq!(suggest_video_file(&files), Some(1));
    }
}
