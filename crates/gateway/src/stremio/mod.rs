//! Stremio addon protocol: `/manifest.json` and `/stream/{type}/{id}.json`.
//!
//! Two kinds of id arrive here:
//!
//! * an id that already carries a magnet link (`magnet:?xt=...`) -- resolved
//!   directly, no lookup needed;
//! * a catalog id from Stremio's own metadata addons (`tt0111161`, and the
//!   `tt…:season:episode` form for series) -- handed to the `indexer` module,
//!   which asks a torrent index what matches it.
//!
//! ## Why stream URLs point at the LAN, not at whoever served this JSON
//!
//! The Stremio Android app refuses to fetch an addon manifest from a plain
//! `http://` LAN address, so the manifest usually has to be reached through
//! an https tunnel. Its *video player* has no such restriction (verified on
//! device: the player fetched `http://192.168.1.67:8081/…` happily while the
//! addon fetcher would not).
//!
//! So the two halves deliberately travel different paths: this JSON goes
//! through the tunnel (kilobytes), while `url` below points straight at the
//! LAN so the actual video -- gigabytes of it -- never leaves the local
//! network. That is what makes the tunnel's bandwidth cap irrelevant and
//! keeps playback at wifi speed instead of upload speed.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::Json;
use serde_json::{json, Value};
use tracing::{debug, warn};

use crate::indexer::{IndexedTorrent, StreamIndexer};
use crate::torrent::BrowseCandidate;
use crate::AppState;

pub const ADDON_ID: &str = "com.localgateway.streaminggateway";

pub async fn manifest(State(state): State<AppState>) -> Json<Value> {
    // Only advertise catalog id prefixes when discovery is actually on --
    // otherwise Stremio would query us for every title and always get an
    // empty list, which just looks broken.
    let id_prefixes: Vec<&str> = if state.indexer.is_some() {
        vec!["tt", "kitsu:", "magnet:"]
    } else {
        vec!["magnet:"]
    };

    let description = if state.indexer.is_some() {
        "Streams movies and series through your own local torrent engine: finds \
         candidate torrents for a title, downloads only the file you are watching \
         (in playback order), and serves it over HTTP with seeking."
    } else {
        "Plays a magnet link or info hash you already have through your own local \
         torrent engine. Discovery is disabled -- for browsing, use \
         GET /play?magnet=<magnet-link> directly (works in VLC, browsers, and TVs too)."
    };

    Json(json!({
        "id": ADDON_ID,
        "version": env!("CARGO_PKG_VERSION"),
        "name": "Local Streaming Gateway",
        "description": description,
        "logo": "https://raw.githubusercontent.com/Stremio/stremio-brand/master/logos/icon.png",
        "resources": [
            { "name": "stream", "types": ["movie", "series", "other"], "idPrefixes": id_prefixes }
        ],
        "types": ["movie", "series", "other"],
        "catalogs": [],
        "behaviorHints": { "configurable": false, "p2p": true }
    }))
}

/// `GET /stream/{type}/{id}.json`
pub async fn stream(
    State(state): State<AppState>,
    Path((content_type, raw_id)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
) -> Json<Value> {
    let id = raw_id.strip_suffix(".json").unwrap_or(&raw_id);
    let decoded = urlencoding::decode(id)
        .map(|c| c.into_owned())
        .unwrap_or_else(|_| id.to_string());

    // Whichever host this request actually arrived on -- used to offer a
    // fallback URL that is reachable from the client's current network.
    // `x-forwarded-host` wins because a tunnel rewrites `host` to its own
    // backend target (localhost), which would be useless to advertise.
    let forwarded_host = headers
        .get("x-forwarded-host")
        .or_else(|| headers.get(axum::http::header::HOST))
        .and_then(|v| v.to_str().ok())
        .map(sanitize_host);

    if decoded.starts_with("magnet:") {
        return Json(json!({ "streams": magnet_streams(&state, &decoded).await }));
    }

    if is_catalog_id(&decoded) {
        return Json(json!({
            "streams": indexed_streams(
                &state, &content_type, &decoded, forwarded_host.as_deref()
            ).await
        }));
    }

    // Not an id we recognize. An empty list is the correct response, not an
    // error: Stremio fans this same request out to every installed addon.
    Json(json!({ "streams": [] }))
}

/// Keeps only a plain `host[:port]` from a client-supplied header.
///
/// This value ends up inside a URL we hand to a media player, and it comes
/// from a request header, which any caller can set to anything. Restricting
/// it to the characters a hostname may actually contain means a crafted
/// `Host` cannot bolt a path, query, or second URL onto what we emit.
/// Anything unexpected yields `None`, i.e. simply no fallback entry.
fn sanitize_host(raw: &str) -> String {
    raw.trim()
        .split(',') // x-forwarded-host may be a list; the first hop is ours
        .next()
        .unwrap_or("")
        .trim()
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | ':'))
        .collect()
}

/// Catalog ids we are willing to look up. Deliberately strict: this string is
/// about to be put into an outbound URL, so anything shaped unexpectedly is
/// rejected rather than forwarded.
///
/// Accepts `tt0111161`, `tt0944947:1:1` (series episode) and `kitsu:12345`.
fn is_catalog_id(id: &str) -> bool {
    fn is_imdb_form(id: &str) -> bool {
        let mut parts = id.split(':');
        let Some(head) = parts.next() else {
            return false;
        };
        let Some(digits) = head.strip_prefix("tt") else {
            return false;
        };
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return false;
        }
        // Optional :season:episode, digits only.
        parts.all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
    }

    fn is_kitsu_form(id: &str) -> bool {
        id.strip_prefix("kitsu:").is_some_and(|rest| {
            !rest.is_empty()
                && rest
                    .split(':')
                    .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
        })
    }

    is_imdb_form(id) || is_kitsu_form(id)
}

/// Handles an id that already carries the magnet -- no index involved.
async fn magnet_streams(state: &AppState, magnet: &str) -> Vec<Value> {
    let Ok(resolved) = state.engine.resolve(magnet).await else {
        return Vec::new();
    };
    let Some(file_idx) = resolved.suggested_file_idx else {
        return Vec::new();
    };
    let Some(file) = resolved.file(file_idx) else {
        return Vec::new();
    };

    // A raw magnet may carry its own trackers -- remember them so a later
    // lazy start (after a restart, say) announces to the same places.
    let trackers: Vec<String> = resolved
        .magnet
        .split('&')
        .filter_map(|part| part.strip_prefix("tr="))
        .filter_map(|t| urlencoding::decode(t).ok().map(|d| d.into_owned()))
        .collect();
    state
        .engine
        .remember_advertised(&resolved.info_hash, &trackers)
        .await;

    vec![json!({
        "name": stream_label(""),
        "title": format!(
            "{}\n{} \u{2022} {}",
            resolved.name.clone().unwrap_or_else(|| file.name.clone()),
            file.name,
            human_size(file.length)
        ),
        "url": video_url(state, &resolved.info_hash, file_idx),
        "behaviorHints": {
            "notWebReady": false,
            "bingeGroup": "local-streaming-gateway"
        }
    })]
}

/// Handles a catalog id by asking the index what torrents match it.
///
/// Note this does *not* resolve each torrent's metadata: doing so would mean
/// a DHT/tracker round-trip per candidate while the user is still just
/// browsing a list. Resolution happens when they actually pick one, inside
/// `/play`.
async fn indexed_streams(
    state: &AppState,
    content_type: &str,
    id: &str,
    forwarded_host: Option<&str>,
) -> Vec<Value> {
    let Some(indexer) = &state.indexer else {
        return Vec::new();
    };

    let found = match indexer.search(content_type, id).await {
        Ok(found) => found,
        Err(e) => {
            // A dead index must not take the whole addon down -- Stremio just
            // shows no results from us, and other addons still work.
            warn!(%content_type, %id, "torrent index lookup failed: {e:#}");
            return Vec::new();
        }
    };

    debug!(%content_type, %id, count = found.len(), "index returned candidates");

    // Anything we list here must become startable when the user picks it --
    // `/videos/...` refuses hashes the gateway never offered. The trackers
    // ride along so that pick starts against this release's own swarm
    // instead of a bare info hash (see `AdvertisedHashes`).
    for torrent in &found {
        state
            .engine
            .remember_advertised(&torrent.info_hash, &torrent.trackers)
            .await;
    }

    // Reaching this list is the earliest reliable sign that someone is about
    // to play one of these, and it is the last moment before the tap when
    // waiting costs nobody anything. So the top candidates start fetching
    // metadata and connecting to peers now, while the viewer is still
    // reading the list, rather than inside the request their player makes
    // afterwards -- that fetch is most of what a slow start actually is.
    // Detached because the stream list must not wait on it.
    // `found` is sorted best-first (seeders, then size), and that first row
    // is what Stremio puts at the top of the list -- so it is the one worth
    // spending bytes on, not just metadata.
    let candidates: Vec<BrowseCandidate> = found
        .iter()
        .enumerate()
        .map(|(rank, t)| BrowseCandidate {
            info_hash: t.info_hash.clone(),
            file_idx: t.file_idx.unwrap_or(0),
            trackers: t.trackers.clone(),
            fetch_head: rank == 0,
        })
        .collect();
    let engine = Arc::clone(&state.engine);
    tokio::spawn(async move { engine.warm_for_browse(&candidates).await });

    found
        .iter()
        .flat_map(|torrent| {
            // The index does not always know which file holds the episode. 0
            // is the right default for the overwhelmingly common single-file
            // release; multi-file packs without a fileIdx are the rare case
            // and still play if the wanted file happens to be first.
            let file_idx = torrent.file_idx.unwrap_or(0);
            let binge = format!("local-streaming-gateway|{}", torrent.quality);

            let mut entries = vec![json!({
                "name": stream_label(&torrent.quality),
                "title": format!("{}\n\u{2705} same wifi as the PC \u{2022} full speed", describe(torrent)),
                "url": video_url(state, &torrent.info_hash, file_idx),
                "behaviorHints": { "notWebReady": false, "bingeGroup": binge },
            })];

            // Reachable-from-anywhere fallback, listed second so the direct
            // LAN route stays the default pick.
            if let Some(url) = remote_video_url(state, forwarded_host, &torrent.info_hash, file_idx)
            {
                entries.push(json!({
                    "name": remote_stream_label(&torrent.quality),
                    "title": format!(
                        "{}\n\u{26A0} slow \u{2022} only when away from home",
                        describe(torrent)
                    ),
                    "url": url,
                    // Deliberately a *different* binge group from the direct
                    // entry: Stremio auto-plays the next episode from the same
                    // group, so sharing one would silently keep a whole series
                    // on the slow route after a single tap on this row.
                    "behaviorHints": { "notWebReady": false, "bingeGroup": format!("{binge}|away") },
                }));
            }
            entries
        })
        .collect()
}

/// The URL handed to Stremio's player: a short, plain path with no query
/// string at all.
///
/// This shape is deliberate. Stremio on Android often hands the URL to an
/// *external* player (VLC) through an intent, and a long
/// `/play?magnet=magnet%3A%3Fxt%3D...` URL is easy to mangle in that handoff
/// (and unreadable in a player's UI). `/videos/<hash>/<idx>` survives it,
/// needs no redirect, and starts the torrent on first request.
///
/// The host is always `stream_base_url` (the LAN address) -- see this
/// module's header for why that is deliberate.
fn video_url(state: &AppState, info_hash: &str, file_idx: usize) -> String {
    format!("{}/videos/{info_hash}/{file_idx}", state.stream_base_url)
}

/// The same video, addressed through whatever host the *addon request* came
/// in on, when that differs from our LAN address.
///
/// This is what makes playback work from a wifi extender, a guest network, or
/// mobile data. The LAN URL is only reachable if the phone sits on the same
/// subnet as this machine -- plug the phone into an extender that hands out
/// its own subnet and `192.168.1.x` becomes unroutable, so the fast path
/// silently dies with no useful error.
///
/// But Stremio reached this addon *somehow* (through an https tunnel, say),
/// and that route works from wherever the phone currently is. Reusing its
/// `Host` gives a guaranteed-reachable fallback for free, with no extra
/// configuration and nothing to keep in sync.
///
/// Offered *alongside* the LAN entry, never instead of it: this path carries
/// the video through the tunnel, so it is slower and counts against any
/// bandwidth quota. Direct-first, fallback-second.
fn remote_video_url(
    state: &AppState,
    forwarded_host: Option<&str>,
    info_hash: &str,
    file_idx: usize,
) -> Option<String> {
    let host = forwarded_host?;
    if host.is_empty() || state.stream_base_url.contains(host) || !is_public_host(host) {
        // Reached directly on this network -- the fast URL already works, and
        // a second entry built from a private address would be a duplicate at
        // best and a broken `https://192.168.x.x` link at worst.
        return None;
    }
    // Our own https hostname is a public *name* that resolves to a private
    // address, so it passes every check above while still being this machine
    // on this LAN. Offering a "you must be away from home" entry for it would
    // label the local route as the slow one.
    if state
        .tls_host
        .as_deref()
        .is_some_and(|tls_host| host_matches(host, tls_host))
    {
        return None;
    }
    // Anything reaching us through a public tunnel arrived over https.
    Some(format!("https://{host}/videos/{info_hash}/{file_idx}"))
}

/// Compares a `Host` header against a bare hostname, ignoring any port and
/// case. `Host` carries `name:port` whenever the port is non-default, so a
/// plain string equality would miss the very requests this needs to catch.
fn host_matches(host: &str, name: &str) -> bool {
    host.split(':')
        .next()
        .unwrap_or(host)
        .eq_ignore_ascii_case(name)
}

/// Whether a host is plausibly a public address reachable from other networks,
/// as opposed to a LAN/loopback name that only works from here.
///
/// The fallback URL is only worth offering for the former: it exists precisely
/// to cover clients that *cannot* reach our private address, so echoing a
/// private one back would produce a link that fails in exactly the situation
/// it was meant to rescue.
fn is_public_host(host: &str) -> bool {
    let name = host.split(':').next().unwrap_or(host);

    if name.eq_ignore_ascii_case("localhost") {
        return false;
    }

    // Private/loopback IPv4 ranges (RFC 1918 + loopback + link-local).
    let octets: Vec<&str> = name.split('.').collect();
    if octets.len() == 4 && octets.iter().all(|o| o.parse::<u8>().is_ok()) {
        let n: Vec<u8> = octets.iter().map(|o| o.parse().unwrap()).collect();
        return !matches!(
            (n[0], n[1]),
            (127, _) | (10, _) | (192, 168) | (169, 254) | (172, 16..=31)
        );
    }

    // A real tunnel host is a dotted domain name.
    name.contains('.')
}

fn stream_label(quality: &str) -> String {
    if quality.is_empty() {
        "\u{26A1} Direct".to_string()
    } else {
        format!("\u{26A1} Direct\n{quality}")
    }
}

/// Label for the tunnelled copy of the same file.
///
/// Deliberately unappealing next to the direct entry. Both rows play the
/// identical file, but the tunnelled one carries every byte out to a relay
/// on the public internet and back, which measured ~2 MB/s against local
/// disk speed over the LAN. The old wording ("works on any network") read
/// like the safer, more capable choice, so it got picked on the home wifi
/// where it is strictly the worse one -- and the result looked like the
/// gateway buffering rather than a route that was never meant to carry video.
fn remote_stream_label(quality: &str) -> String {
    if quality.is_empty() {
        "\u{1F30D} Away".to_string()
    } else {
        format!("\u{1F30D} Away\n{quality}")
    }
}

/// Human-facing second line: release name, then whatever stats we actually
/// know. Unknown values are omitted rather than shown as zero -- "0 seeders"
/// and "unknown seeders" mean very different things to someone choosing.
fn describe(torrent: &IndexedTorrent) -> String {
    let mut line = String::new();
    if let Some(seeders) = torrent.seeders {
        line.push_str(&format!("\u{1F464} {seeders}"));
    }
    if let Some(size) = torrent.size_bytes {
        if !line.is_empty() {
            line.push_str("  ");
        }
        line.push_str(&format!("\u{1F4BE} {}", human_size(size)));
    }

    if line.is_empty() {
        torrent.title.clone()
    } else {
        format!("{}\n{line}", torrent.title)
    }
}

fn human_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn torrent() -> IndexedTorrent {
        IndexedTorrent {
            info_hash: "45fa4233ef87c58f5f8b4817e4d50c9f5363caef".into(),
            file_idx: Some(0),
            title: "Movie.2024.1080p.BluRay".into(),
            quality: "1080p".into(),
            size_bytes: Some(2 * 1024 * 1024 * 1024),
            seeders: Some(42),
            trackers: vec!["udp://tracker.example:1337/announce".into()],
        }
    }

    #[test]
    fn accepts_imdb_movie_and_episode_ids() {
        assert!(is_catalog_id("tt0111161"));
        assert!(is_catalog_id("tt0944947:1:1"));
        assert!(is_catalog_id("kitsu:12345"));
        assert!(is_catalog_id("kitsu:12345:2"));
    }

    #[test]
    fn rejects_ids_that_could_reshape_an_outbound_url() {
        assert!(!is_catalog_id("tt0111161/../../admin"));
        assert!(!is_catalog_id("tt"));
        assert!(!is_catalog_id("ttabc"));
        assert!(!is_catalog_id("tt0111161:"));
        assert!(!is_catalog_id("kitsu:"));
        assert!(!is_catalog_id("kitsu:abc"));
        assert!(!is_catalog_id(""));
        assert!(!is_catalog_id("http://evil.example/"));
    }

    #[test]
    fn video_url_has_no_query_string_to_survive_an_external_player_handoff() {
        // Stremio hands this to VLC via an Android intent; a bare path
        // survives that, a percent-encoded magnet query string may not.
        let state_base = "http://192.168.1.67:8080";
        let url = format!("{state_base}/videos/{}/{}", torrent().info_hash, 0);
        assert_eq!(
            url,
            "http://192.168.1.67:8080/videos/45fa4233ef87c58f5f8b4817e4d50c9f5363caef/0"
        );
        assert!(!url.contains('?'));
        assert!(!url.contains('%'));
    }

    #[test]
    fn describe_omits_unknown_stats_instead_of_showing_zero() {
        let mut t = torrent();
        t.seeders = None;
        t.size_bytes = None;
        assert_eq!(describe(&t), "Movie.2024.1080p.BluRay");

        t.seeders = Some(7);
        assert!(describe(&t).contains("\u{1F464} 7"));
        assert!(!describe(&t).contains("\u{1F4BE}"));
    }

    #[test]
    fn human_size_formats_expected_units() {
        assert_eq!(human_size(512), "512 B");
        assert_eq!(human_size(2 * 1024 * 1024 * 1024), "2.00 GB");
    }

    #[test]
    fn sanitize_host_keeps_plain_hosts_and_ports() {
        assert_eq!(
            sanitize_host("abc123.ngrok-free.app"),
            "abc123.ngrok-free.app"
        );
        assert_eq!(sanitize_host("192.168.1.67:8080"), "192.168.1.67:8080");
        assert_eq!(sanitize_host("  example.com  "), "example.com");
        // x-forwarded-host can be a comma-separated chain; take the first hop.
        assert_eq!(
            sanitize_host("first.example, second.example"),
            "first.example"
        );
    }

    #[test]
    fn sanitize_host_strips_anything_that_could_extend_the_url() {
        // The result is interpolated into a URL given to a media player, so a
        // crafted Host must not be able to append a path, query or new host.
        assert_eq!(sanitize_host("evil.com/../../admin"), "evil.com");
        assert_eq!(sanitize_host("evil.com?x=1"), "evil.com");
        assert_eq!(sanitize_host("evil.com#frag"), "evil.com");
        assert_eq!(sanitize_host("evil.com evil2.com"), "evil.com");
        assert_eq!(sanitize_host("evil.com@other.com"), "evil.com");
        assert_eq!(sanitize_host("/leading-slash"), "");
    }

    #[test]
    fn private_and_loopback_hosts_never_get_a_remote_entry() {
        // Echoing a private address back as an https fallback would produce a
        // link that is broken (wrong scheme) and useless in the one case the
        // fallback exists for: a client that cannot reach our LAN.
        for host in [
            "127.0.0.1:8080",
            "localhost:11470",
            "192.168.1.67:8080",
            "10.0.0.5",
            "172.16.3.4",
            "172.31.255.1",
            "169.254.1.1",
        ] {
            assert!(
                !is_public_host(host),
                "{host} must not be treated as public"
            );
        }
    }

    #[test]
    fn public_tunnel_hosts_do_get_a_remote_entry() {
        for host in [
            "abc123.ngrok-free.app",
            "example.trycloudflare.com",
            "gateway.example.com:443",
            "8.8.8.8",
            "172.15.0.1", // just outside the private 172.16-31 range
            "172.32.0.1",
        ] {
            assert!(is_public_host(host), "{host} should be treated as public");
        }
    }

    #[test]
    fn quality_label_falls_back_when_index_gave_no_tag() {
        assert_eq!(stream_label(""), "\u{26A1} Direct");
        assert_eq!(stream_label("1080p"), "\u{26A1} Direct\n1080p");
        assert_eq!(remote_stream_label(""), "\u{1F30D} Away");
        assert_eq!(remote_stream_label("1080p"), "\u{1F30D} Away\n1080p");
    }

    /// Both rows play the same file, but only one of them should look like
    /// the obvious pick. Picking the tunnelled row on the home wifi routes
    /// gigabytes through a public relay at a fraction of LAN speed, and the
    /// symptom is indistinguishable from the gateway being slow.
    #[test]
    fn the_tunnelled_row_never_reads_as_the_better_choice() {
        let direct = stream_label("1080p");
        let away = remote_stream_label("1080p");
        assert_ne!(direct, away, "the two routes must be tellable apart");
        assert!(
            !away.contains("Direct"),
            "the tunnelled row must not borrow the direct row's wording"
        );
    }
}
