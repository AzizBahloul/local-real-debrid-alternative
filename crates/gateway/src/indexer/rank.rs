//! Deciding which torrents a title lists first.
//!
//! Stremio has no stream filters: it shows an addon's list in the order the
//! addon sent it, and "auto-play the next episode" follows the first row. So
//! the order *is* the filter, and this is the one place that sets it.
//!
//! The order used to be seeders alone. That buries exactly the release people
//! are waiting for: a fresh 4K WEB-DL with 40 seeders sat under a 720p re-encode
//! with 1,600, and cutting the list to `INDEXER_MAX_RESULTS` afterwards threw
//! away all but one of the 4K rows (measured 2026-10-09 on a new episode: 11 of
//! 12 kept rows were 1080p/720p while Torrentio itself had listed the 4K ones
//! first). Seeders still matter -- a swarm of two will not hold a stream -- but
//! as a *floor*, not as the sort key.

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};

use super::IndexedTorrent;

/// Seeders from which a swarm is trusted to start and hold a stream. Below it
/// a release stays in the list but ranks under every release at or above it,
/// whatever its quality.
const HEALTHY_SEEDERS: u32 = 10;

/// What to hide and what to cap. The defaults hide only what is not worth
/// showing (dead swarms, camera rips) and cap nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RankPrefs {
    /// A release the index reports with fewer seeders than this is dropped.
    /// Unknown seeder counts are never dropped.
    pub min_seeders: u32,
    /// Hide anything above this vertical resolution (`1080`, `2160`). 0 = no cap.
    pub max_resolution: u32,
    /// Hide anything larger than this. 0 = no cap.
    pub max_size_bytes: u64,
}

impl Default for RankPrefs {
    fn default() -> Self {
        Self {
            min_seeders: 2,
            max_resolution: 0,
            max_size_bytes: 0,
        }
    }
}

impl RankPrefs {
    fn allows(&self, torrent: &IndexedTorrent) -> bool {
        if torrent.seeders.is_some_and(|s| s < self.min_seeders) {
            return false;
        }
        if is_screener(torrent) {
            return false;
        }
        if self.max_resolution != 0 && resolution(torrent) > self.max_resolution {
            return false;
        }
        if self.max_size_bytes != 0 && torrent.size_bytes.is_some_and(|b| b > self.max_size_bytes) {
            return false;
        }
        true
    }
}

/// Filters, then orders: swarm healthy enough to stream, then resolution, then
/// source quality, then seeders, then the smaller file (a 2 GB rip starts far
/// sooner than a 50 GB remux).
///
/// If the preferences would hide *everything*, the unfiltered list is ranked
/// instead -- a cap that leaves an empty list is worse than a release above it.
pub fn rank(found: Vec<IndexedTorrent>, prefs: &RankPrefs) -> Vec<IndexedTorrent> {
    // The same file listed twice (two providers, one hash) would use up two
    // of the few rows we show.
    let mut seen = HashSet::new();
    let found = found
        .into_iter()
        .filter(|t| seen.insert((t.info_hash.to_ascii_lowercase(), t.file_idx.unwrap_or(0))));

    let (kept, hidden): (Vec<_>, Vec<_>) = found.partition(|t| prefs.allows(t));
    let mut list = if kept.is_empty() { hidden } else { kept };
    list.sort_by_key(|t| (Reverse(score(t)), t.size_bytes.unwrap_or(u64::MAX)));
    list
}

/// Cuts a ranked list to `limit` rows without letting one resolution fill it.
///
/// A big release has dozens of 4K rows, and cutting by rank alone left a list
/// with no 1080p at all (measured 2026-10-09 on a new blockbuster: 15 rows, all
/// 4K, mostly 25 GB, while 1080p WEB-DLs with 2,000 seeders at 9 GB existed).
/// Someone on a phone or a slow link needs that choice to be on screen. So no
/// resolution gets more than half the rows while another has candidates; the
/// rest fill in rank order. The kept rows stay in rank order.
pub fn shortlist(ranked: Vec<IndexedTorrent>, limit: usize) -> Vec<IndexedTorrent> {
    if ranked.len() <= limit {
        return ranked;
    }
    let per_resolution = limit.div_ceil(2);
    let mut keep = vec![false; ranked.len()];
    let mut taken = 0;
    let mut by_resolution: HashMap<u32, usize> = HashMap::new();

    for (i, torrent) in ranked.iter().enumerate() {
        if taken == limit {
            break;
        }
        let count = by_resolution.entry(resolution(torrent)).or_default();
        if *count < per_resolution {
            *count += 1;
            keep[i] = true;
            taken += 1;
        }
    }
    for slot in keep.iter_mut().filter(|slot| !**slot) {
        if taken == limit {
            break;
        }
        *slot = true;
        taken += 1;
    }

    ranked
        .into_iter()
        .zip(keep)
        .filter_map(|(torrent, keep)| keep.then_some(torrent))
        .collect()
}

fn score(torrent: &IndexedTorrent) -> (bool, u32, u8, u32) {
    let seeders = torrent.seeders.unwrap_or(0);
    (
        seeders >= HEALTHY_SEEDERS,
        resolution(torrent),
        source_tier(torrent),
        seeders,
    )
}

/// Lowercased alphanumeric words of the quality label and release name:
/// `"4k DV | HDR"` -> `["4k", "dv", "hdr"]`, `"WEB-DL"` -> `["web", "dl"]`.
fn words(text: &str) -> impl Iterator<Item = String> + '_ {
    text.split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(str::to_ascii_lowercase)
}

fn tagged(torrent: &IndexedTorrent) -> Vec<String> {
    words(&torrent.quality)
        .chain(words(&torrent.title))
        .collect()
}

/// Vertical resolution, or 0 when neither the label nor the name says. The
/// index's own label is trusted first: it is what Torrentio sorted by.
fn resolution(torrent: &IndexedTorrent) -> u32 {
    let of = |text: &str| {
        words(text)
            .filter_map(|w| match w.as_str() {
                "2160p" | "4k" | "uhd" => Some(2160),
                "1440p" | "2k" => Some(1440),
                "1080p" | "1080i" | "fhd" => Some(1080),
                "720p" => Some(720),
                "576p" | "480p" | "360p" => Some(480),
                _ => None,
            })
            .max()
    };
    of(&torrent.quality)
        .or_else(|| of(&torrent.title))
        .unwrap_or(0)
}

/// 3 for a clean source (disc or web download), 2 for a re-encode of one, 1 for
/// anything else, including "does not say".
fn source_tier(torrent: &IndexedTorrent) -> u8 {
    let tags = tagged(torrent);
    let has = |names: &[&str]| tags.iter().any(|t| names.contains(&t.as_str()));
    if has(&["webrip", "bdrip", "brrip"]) {
        2
    } else if has(&["bluray", "blu", "remux", "web", "webdl"]) {
        3
    } else {
        1
    }
}

/// Camera, telesync and screener rips: a new release's loudest, least
/// watchable early copies, and exactly what a quality-first order would
/// otherwise have to be careful around.
fn is_screener(torrent: &IndexedTorrent) -> bool {
    tagged(torrent).iter().any(|t| {
        matches!(
            t.as_str(),
            "cam"
                | "hdcam"
                | "camrip"
                | "ts"
                | "hdts"
                | "telesync"
                | "tc"
                | "hdtc"
                | "telecine"
                | "scr"
                | "screener"
                | "dvdscr"
                | "workprint"
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(title: &str, quality: &str, seeders: Option<u32>, gb: f64) -> IndexedTorrent {
        IndexedTorrent {
            info_hash: format!(
                "{:040x}",
                title.len() as u64 * 7919 + seeders.unwrap_or(0) as u64
            ),
            file_idx: None,
            title: title.to_string(),
            quality: quality.to_string(),
            size_bytes: Some((gb * 1024.0 * 1024.0 * 1024.0) as u64),
            seeders,
            trackers: Vec::new(),
        }
    }

    fn titles(list: &[IndexedTorrent]) -> Vec<&str> {
        list.iter().map(|t| t.title.as_str()).collect()
    }

    #[test]
    fn a_new_better_release_outranks_an_older_crowded_one() {
        let ranked = rank(
            vec![
                t("Show.S05E08.720p.WEB.H264-OLD", "720p", Some(1600), 1.0),
                t(
                    "Show.S05E08.2160p.AMZN.WEB-DL",
                    "4k DV | HDR",
                    Some(37),
                    7.0,
                ),
                t("Show.S05E08.1080p.WEB-DL", "1080p", Some(300), 2.0),
            ],
            &RankPrefs::default(),
        );
        assert_eq!(
            titles(&ranked),
            [
                "Show.S05E08.2160p.AMZN.WEB-DL",
                "Show.S05E08.1080p.WEB-DL",
                "Show.S05E08.720p.WEB.H264-OLD"
            ]
        );
    }

    #[test]
    fn a_thin_swarm_ranks_under_every_healthy_one() {
        let ranked = rank(
            vec![
                t("4k.thin", "4k", Some(3), 7.0),
                t("720p.healthy", "720p", Some(12), 1.0),
            ],
            &RankPrefs::default(),
        );
        assert_eq!(titles(&ranked), ["720p.healthy", "4k.thin"]);
    }

    #[test]
    fn dead_swarms_are_dropped_but_unknown_counts_are_kept() {
        let ranked = rank(
            vec![
                t("dead", "1080p", Some(1), 2.0),
                t("unknown", "1080p", None, 2.0),
                t("alive", "1080p", Some(40), 2.0),
            ],
            &RankPrefs::default(),
        );
        assert_eq!(titles(&ranked), ["alive", "unknown"]);
    }

    #[test]
    fn camera_and_screener_rips_never_make_the_list() {
        let ranked = rank(
            vec![
                t("Movie.2026.HDCAM.x264", "", Some(900), 1.0),
                t("Movie.2026.1080p.WEB-DL", "1080p", Some(50), 3.0),
                t("Movie.2026.TS.XviD", "", Some(500), 1.0),
                t("Movie.2026.Tsunami.1080p.WEBRip", "1080p", Some(20), 2.0),
            ],
            &RankPrefs::default(),
        );
        assert_eq!(
            titles(&ranked),
            ["Movie.2026.1080p.WEB-DL", "Movie.2026.Tsunami.1080p.WEBRip"],
            "`Tsunami` contains `ts` but is not the word TS"
        );
    }

    #[test]
    fn the_source_breaks_a_resolution_tie() {
        let ranked = rank(
            vec![
                t("Show.1080p.HDTV", "1080p", Some(500), 1.0),
                t("Show.1080p.WEBRip", "1080p", Some(400), 1.0),
                t("Show.1080p.WEB-DL", "1080p", Some(30), 2.0),
                t("Show.1080p.BluRay", "1080p", Some(20), 9.0),
            ],
            &RankPrefs::default(),
        );
        // WEB-DL (30) and BluRay (20) share the top tier, so seeders order them.
        assert_eq!(
            titles(&ranked),
            [
                "Show.1080p.WEB-DL",
                "Show.1080p.BluRay",
                "Show.1080p.WEBRip",
                "Show.1080p.HDTV"
            ]
        );
    }

    #[test]
    fn a_resolution_cap_hides_what_is_above_it() {
        let prefs = RankPrefs {
            max_resolution: 1080,
            ..RankPrefs::default()
        };
        let ranked = rank(
            vec![
                t("uhd", "4k HDR", Some(100), 8.0),
                t("fhd", "1080p", Some(50), 2.0),
                t("untagged", "", Some(30), 2.0),
            ],
            &prefs,
        );
        assert_eq!(titles(&ranked), ["fhd", "untagged"]);
    }

    #[test]
    fn a_size_cap_hides_what_is_above_it() {
        let prefs = RankPrefs {
            max_size_bytes: 10 * 1024 * 1024 * 1024,
            ..RankPrefs::default()
        };
        let ranked = rank(
            vec![
                t("remux", "4k", Some(100), 45.0),
                t("encode", "1080p", Some(50), 3.0),
            ],
            &prefs,
        );
        assert_eq!(titles(&ranked), ["encode"]);
    }

    #[test]
    fn preferences_that_hide_everything_do_not_empty_the_list() {
        let prefs = RankPrefs {
            max_resolution: 720,
            ..RankPrefs::default()
        };
        let ranked = rank(vec![t("only.4k", "4k", Some(100), 8.0)], &prefs);
        assert_eq!(titles(&ranked), ["only.4k"]);
    }

    #[test]
    fn equal_scores_prefer_the_smaller_file() {
        let ranked = rank(
            vec![
                t("big", "1080p", Some(50), 9.0),
                t("small", "1080p", Some(50), 2.0),
            ],
            &RankPrefs::default(),
        );
        assert_eq!(titles(&ranked), ["small", "big"]);
    }

    #[test]
    fn one_file_listed_twice_takes_one_row() {
        let mut twin = t("same.file", "1080p", Some(50), 2.0);
        let original = twin.clone();
        twin.info_hash = twin.info_hash.to_ascii_uppercase();
        let ranked = rank(vec![original, twin], &RankPrefs::default());
        assert_eq!(ranked.len(), 1);
    }

    #[test]
    fn a_crowd_of_4k_rows_does_not_push_out_every_1080p() {
        let mut ranked = Vec::new();
        for i in 0..12 {
            ranked.push(t(&format!("uhd.{i:02}"), "4k", Some(500 - i), 25.0));
        }
        for i in 0..6 {
            ranked.push(t(&format!("fhd.{i:02}"), "1080p", Some(100 - i), 9.0));
        }
        let ranked = rank(ranked, &RankPrefs::default());
        let shown = shortlist(ranked, 10);

        assert_eq!(shown.len(), 10);
        let uhd = shown.iter().filter(|t| t.quality == "4k").count();
        assert_eq!(uhd, 5, "half the rows, not all of them");
        assert_eq!(shown[0].title, "uhd.00", "best still leads");
        assert!(
            shown.iter().position(|t| t.quality == "1080p").unwrap() > 4,
            "rank order is kept: the 1080p rows follow the first 4K ones"
        );
    }

    #[test]
    fn a_single_resolution_still_fills_the_list() {
        let ranked = rank(
            (0..12)
                .map(|i| t(&format!("fhd.{i:02}"), "1080p", Some(100 - i), 2.0))
                .collect(),
            &RankPrefs::default(),
        );
        assert_eq!(shortlist(ranked, 10).len(), 10);
    }

    #[test]
    fn a_short_list_is_left_alone() {
        let ranked = rank(vec![t("a", "4k", Some(50), 5.0)], &RankPrefs::default());
        assert_eq!(shortlist(ranked, 10).len(), 1);
    }

    #[test]
    fn resolution_comes_from_the_label_then_the_name() {
        assert_eq!(resolution(&t("x", "4k DV | HDR", None, 1.0)), 2160);
        assert_eq!(resolution(&t("Show.S01E01.720p.HDTV", "", None, 1.0)), 720);
        assert_eq!(resolution(&t("Show.S01E01", "WEB-DL", None, 1.0)), 0);
    }
}
