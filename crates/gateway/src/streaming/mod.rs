//! HTTP streaming server: HTTP Range support (RFC 7233) over a torrent file
//! that may still be downloading. Piece prioritization and the "block until
//! the needed piece has arrived" logic live inside `librqbit`'s `FileStream`
//! (see `TorrentEngine::api`); this module owns the public URL surface,
//! input validation, and translating byte ranges into HTTP status/headers.
//!
//! * `range` -- which bytes a request gets, and with what status.
//! * `prebuffer` -- holding the response until real bytes exist.
//! * `body` -- the response body that heals itself when the torrent stalls.

mod body;
mod prebuffer;
mod range;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, Method};
use axum::response::{IntoResponse, Redirect, Response};
use librqbit::api::TorrentIdOrHash;
use serde::Deserialize;
use tracing::{debug, warn};

use crate::error::ApiErrorResponse;
use crate::torrent::{resolver, TorrentEngine};
use body::{healing_body, BodyContext};
use prebuffer::{piece_aware_floor, prebuffer};
use range::{plan_range, Unsatisfiable};

/// A range starting within this distance of the end of the file is treated as
/// an index probe rather than playback.
///
/// Players fetch a non-faststart mp4's trailing `moov` atom before they can
/// decode anything, and that read wants entirely different handling from
/// playback: a few hundred kilobytes, once, with no interest in filling a
/// buffer. Sized above the largest realistic piece so a probe and a genuine
/// seek to the final minutes are never confused.
const TAIL_PROBE_ZONE: u64 = 24 * 1024 * 1024;

/// The pre-buffer ceiling for a tail probe. The player wants an index, not a
/// head start, and topping up here spends the viewer's time on bytes nothing
/// will read.
const TAIL_PROBE_PREBUFFER: usize = 256 * 1024;

/// Below this, a pre-buffer that got its floor is recorded as served from disk
/// rather than from the swarm — see the `warm` field where it is used.
///
/// Two orders of magnitude above a disk read of this size and two below the
/// smallest wait a swarm round-trip can produce, so the classification does
/// not hinge on where exactly in that gap the threshold sits.
const WARM_PREBUFFER_MS: u64 = 50;

#[derive(Deserialize)]
pub struct PlayQuery {
    pub magnet: String,
    #[serde(rename = "fileIdx")]
    pub file_idx: Option<usize>,
}

/// `GET /play?magnet=<magnet-link>[&fileIdx=n]`
///
/// The classic "paste a magnet, get a stream" entry point. Resolves the
/// torrent, starts (or resumes) downloading the requested file restricted to
/// just that file, and redirects to the stable `/videos/{info_hash}/{file_idx}`
/// URL that actually serves range requests. Players re-issue range requests
/// directly against the redirect target for every seek, so this only runs
/// the (slower) metadata-resolution step once per magnet.
pub async fn play(
    State(engine): State<Arc<TorrentEngine>>,
    Query(query): Query<PlayQuery>,
) -> Result<Response, ApiErrorResponse> {
    const MAX_MAGNET_LEN: usize = 8192;
    if query.magnet.len() > MAX_MAGNET_LEN {
        return Err(ApiErrorResponse::bad_request("magnet link is too long"));
    }

    let resolved = engine
        .resolve(&query.magnet)
        .await
        .map_err(|e| ApiErrorResponse::bad_request(format!("could not resolve torrent: {e:#}")))?;

    let file_idx = query
        .file_idx
        .or(resolved.suggested_file_idx)
        .ok_or_else(|| {
            ApiErrorResponse::bad_request(
                "torrent has no playable video file; pass ?fileIdx= explicitly",
            )
        })?;

    if resolved.file(file_idx).is_none() {
        return Err(ApiErrorResponse::bad_request(
            "fileIdx out of range for this torrent",
        ));
    }

    // The redirect target refuses any hash this gateway never offered, and
    // "offered" is what we are doing right here. Without this the URL works
    // only while the torrent stays in the session: the moment it leaves --
    // a restart, or switching to another title, which discards the one left
    // behind -- the link a player is still holding starts 404ing with
    // "this gateway has not offered that info hash". The magnet's own
    // trackers ride along so that later start announces where this release's
    // seeders actually are. See `AdvertisedHashes`.
    engine
        .remember_advertised(
            &resolved.info_hash,
            &resolver::trackers_from_magnet(&resolved.magnet),
        )
        .await;

    // The resolve above just talked to the swarm; hand those peers straight
    // to the download so it connects immediately instead of re-discovering
    // the same swarm over DHT/trackers a second time. The metadata it fetched
    // is reused too, so this start does no swarm lookup of its own at all.
    let info_hash = engine
        .start_file(&query.magnet, file_idx, resolved.seen_peers.clone())
        .await
        .map_err(|e| ApiErrorResponse::internal(format!("failed to start streaming: {e:#}")))?;

    Ok(Redirect::temporary(&format!("/videos/{info_hash}/{file_idx}")).into_response())
}

#[derive(Deserialize)]
pub struct VideoPathParams {
    info_hash: String,
    file_idx: usize,
}

/// `GET /videos/{info_hash}/{file_idx}` -- the actual range-serving endpoint.
///
/// `info_hash` is validated as a bare 40-char hex string before it ever
/// reaches a lookup, so this endpoint has no filesystem-path input at all:
/// the client can only ever select "which already-known torrent" and "which
/// file index inside it", both resolved through librqbit's own metadata
/// table, never through a client-supplied path.
///
/// `HEAD` is answered from the same headers and returns before any read is
/// opened. A player probing the length is not a viewer: without this it
/// registered a reader (retiring the player's own real read), took the focus
/// (discarding the title before it), and sat through the full pre-buffer for
/// a body axum then threw away.
pub async fn stream_video(
    State(engine): State<Arc<TorrentEngine>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    method: Method,
    Path(VideoPathParams {
        info_hash,
        file_idx,
    }): Path<VideoPathParams>,
    headers: HeaderMap,
) -> Result<Response, ApiErrorResponse> {
    validate_info_hash(&info_hash).map_err(ApiErrorResponse::bad_request)?;
    // Every map in the engine is keyed by the lowercase form; a client that
    // happened to send uppercase must not get its own separate bookkeeping.
    let info_hash = info_hash.to_ascii_lowercase();

    let idx = TorrentIdOrHash::parse(&info_hash)
        .map_err(|e| ApiErrorResponse::bad_request(format!("invalid info hash: {e}")))?;

    // Before the start below, which can take most of a minute on a cold
    // title: the janitor must see this title as in use for all of it. Ignored
    // for a hash the gateway never offered, so a stranger cannot grow the
    // activity map with made-up hashes.
    engine.touch_stream(&info_hash, addr.ip());

    // Start the torrent if this is the first request for it. Without this the
    // URL would only work after a prior /play, which rules out handing a bare
    // /videos/... link to an external player -- the main way Stremio plays on
    // Android. Idempotent: a torrent already in the session is left alone,
    // and a hash this gateway never advertised is refused outright rather
    // than joined (see `AdvertisedHashes`).
    let started = engine
        .ensure_started(&info_hash, file_idx)
        .await
        .map_err(|e| ApiErrorResponse::internal(format!("failed to start torrent: {e:#}")))?;
    if !started {
        return Err(ApiErrorResponse::not_found(
            "unknown torrent: this gateway has not offered that info hash",
        ));
    }

    let file = engine
        .stream_file(&info_hash, file_idx)
        .map_err(|e| ApiErrorResponse::not_found(format!("stream unavailable: {e:#}")))?;
    let file_len = file.len;

    let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    let plan = plan_range(range_header, file_len).map_err(|Unsatisfiable| {
        ApiErrorResponse::range_not_satisfiable(
            format!("range {range_header:?} starts past the end of a file of length {file_len}"),
            file_len,
        )
    })?;
    let (start, end) = (plan.start, plan.end);

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Ok(mime) = engine.api().torrent_file_mime_type(idx, file_idx) {
        resp_headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(mime));
    }
    if let Some(content_range) = &plan.content_range {
        resp_headers.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(content_range).expect("ascii digits only"),
        );
    }
    resp_headers.insert(header::CONTENT_LENGTH, HeaderValue::from(plan.len()));

    if method == Method::HEAD {
        return Ok((plan.status, resp_headers).into_response());
    }

    // An index probe, not playback: see `TAIL_PROBE_ZONE`. It gets its own
    // small budget, and it is exempt from being superseded, because it is a
    // read the same player needs *alongside* the one at the head rather than
    // instead of it.
    let tail_probe = start >= file_len.saturating_sub(TAIL_PROBE_ZONE);

    // Registered before the read is opened, so a scrub that arrives while an
    // earlier one is still blocked in the pre-buffer retires it immediately
    // rather than after it finally gives up. See `register_reader`.
    let ticket = engine.register_reader(&info_hash, file_idx, addr.ip(), tail_probe);
    let cancel = ticket.cancel_handle();

    // `ensure_started` above has just made sure the torrent is present and
    // running, so this opens the read directly rather than checking again.
    let reader = engine
        .open_started_stream_at(&info_hash, file_idx, start)
        .await
        .map_err(|e| ApiErrorResponse::not_found(format!("stream unavailable: {e:#}")))?;

    // Held for the entire response body: this is what keeps the idle reaper
    // from pausing the torrent out from under a viewer. See `StreamGuard`.
    let guard = engine.open_stream_guard(&info_hash);

    // Starting a different title pauses the previous one immediately, rather
    // than leaving it to compete for the line until the idle reaper gets to
    // it minutes later. No-op when this is the same torrent (a seek).
    engine.focus_stream(&info_hash);

    // Fetch the file's trailing index in parallel with its opening frames, so
    // the player's own tail request lands on data that is already here rather
    // than paying a second cold start for it. Detached and best-effort; a
    // playback read must never wait on it. See `warm_mp4_tail`.
    if !tail_probe {
        engine.warm_mp4_tail(&info_hash, file_idx, file_len, &file.name);
    }

    // Read before the wait, because a piece that arrives *during* it would
    // make the request look like it never had anything to wait for.
    let geometry = engine.piece_geometry(&info_hash, file_idx, start);
    let piece_remainder = geometry.map_or(0, |g| g.remainder);

    // Hold the response until some real data is in hand -- see `prebuffer`.
    let wanted = usize::try_from(end - start).unwrap_or(usize::MAX);
    let prebuffer_target = if tail_probe {
        TAIL_PROBE_PREBUFFER.min(wanted)
    } else {
        engine.tuning().prebuffer_bytes.min(wanted)
    };

    // How much to actually *block* for. Capped at what remains of the piece
    // this offset lands in: past that boundary the next byte lives in a
    // second piece, and waiting for it doubles the wait for no benefit the
    // player can use. Floored so the response can never be a set of headers
    // over a few kilobytes, which is the failure the pre-buffer exists to
    // prevent.
    let prebuffer_floor = piece_aware_floor(piece_remainder, prebuffer_target);

    let prebuffer_began = Instant::now();
    let (head, reader) = prebuffer(
        reader,
        prebuffer_floor,
        prebuffer_target,
        engine.tuning().prebuffer_timeout,
        &cancel,
    )
    .await;
    let prebuffer_ms = prebuffer_began.elapsed().as_millis() as u64;

    // Superseded while it waited: the same player has already asked for a
    // different offset, so nothing will ever read this response.
    let superseded = cancel.is_cancelled();

    // Judged against the floor, which is the only part of the pre-buffer that
    // waits. Above it the top-up takes whatever is already on disk and stops,
    // so a read that got its floor but not the full ceiling did exactly what
    // it was designed to do -- recording that as a timeout filled the log
    // with faults that were really "the next piece has not arrived yet".
    //
    // A superseded read is short because it was told to stop, not because the
    // swarm was slow. Calling that a timeout would make the log report a
    // fault where there was a deliberate decision.
    let timed_out = !superseded && head.len() < prebuffer_floor;

    // The last phase of the wait a player is timing out on, and the one the
    // cold-start record cannot see (it happens after the engine hands back).
    //
    // Recorded before the superseded check below, not after: a burst of
    // retired scrubs is precisely what someone is looking for when they ask
    // why a seek took so long, and returning early without a line would leave
    // the log showing one slow start and no sign of the nine reads that were
    // competing with it.
    //
    // `warm` is inferred from the wait rather than by asking which pieces are
    // present, and that inference is sound in one direction: librqbit's reader
    // cannot return a byte that is not already on disk, so filling the floor
    // in under `WARM_PREBUFFER_MS` could only have come from data that was
    // already there. The reverse is not claimed -- a warm read that happened to
    // be slow is recorded as cold, which errs toward flagging a wait rather
    // than explaining it away.
    engine.audit().record(crate::audit::Event::Prebuffer {
        info_hash: info_hash.clone(),
        bytes: head.len(),
        wanted: prebuffer_target,
        ms: prebuffer_ms,
        timed_out,
        warm: !timed_out && !superseded && prebuffer_ms < WARM_PREBUFFER_MS,
        piece_remainder,
        superseded,
    });

    // Answering fast and empty-handed is the point: it releases the
    // piece-priority claim to the request the viewer is actually waiting on.
    if superseded {
        debug!(%info_hash, start, "dropping a read this client seeked away from");
        return Err(ApiErrorResponse::superseded(
            "superseded by a newer range request from the same client",
        ));
    }

    // Nothing arrived at all. Sending the 206 anyway is what the timeout path
    // used to do, and it is the one outcome the pre-buffer was built to make
    // impossible: a player handed headers over an empty body treats the stream
    // as broken and stops, rather than waiting the way it does for a response
    // that is merely slow. Measured on 2026-09-07 -- a seek to 83% of a file
    // 40% downloaded returned `206` with `bytes: 0` after 15 s and playback
    // ended. A retryable status keeps the player asking instead.
    //
    // `timed_out` already excludes the pre-buffer-disabled case (a floor of
    // 0 cannot be missed), so this cannot fire when the operator has
    // deliberately turned the wait off.
    if timed_out && head.is_empty() {
        warn!(
            %info_hash, start, piece_remainder,
            "no bytes after {}s; asking the player to retry rather than sending an empty body",
            prebuffer_ms / 1000
        );
        return Err(ApiErrorResponse::not_ready(
            "no data for this offset yet; retry shortly",
        ));
    }

    let body = healing_body(BodyContext {
        position: start + head.len() as u64,
        end,
        engine: Arc::clone(&engine),
        info_hash,
        file_idx,
        file_len,
        client: addr.ip(),
        head,
        reader,
        guard,
        ticket,
        cancel,
    });

    Ok((plan.status, resp_headers, body).into_response())
}

/// Only ever accepts a bare 40-char hex BitTorrent info hash. Anything else
/// (including `..`, `/`, or any other filesystem-path-shaped input) is
/// rejected before it is used for anything.
pub fn validate_info_hash(value: &str) -> Result<(), String> {
    if resolver::is_hex40(value) {
        Ok(())
    } else {
        Err("info hash must be exactly 40 hex characters".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_info_hash() {
        assert!(validate_info_hash("0123456789abcdef0123456789abcdef01234567").is_ok());
        assert!(validate_info_hash("0123456789ABCDEF0123456789ABCDEF01234567").is_ok());
    }

    #[test]
    fn rejects_path_traversal_and_malformed_hashes() {
        assert!(validate_info_hash("../../etc/passwd").is_err());
        assert!(validate_info_hash("short").is_err());
        assert!(validate_info_hash("0123456789abcdef0123456789abcdef0123456z").is_err());
        assert!(validate_info_hash("").is_err());
        // 41 chars
        assert!(validate_info_hash("0123456789abcdef0123456789abcdef012345678").is_err());
    }

    /// Empty-handed and superseded are both "no body", and they must not get
    /// the same status. One asks the player to come back; the other tells it
    /// not to bother, because it has already asked for somewhere else.
    #[test]
    fn an_empty_handed_read_is_retryable_and_a_superseded_one_is_not() {
        assert_eq!(
            ApiErrorResponse::not_ready("x").status,
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            ApiErrorResponse::superseded("x").status,
            axum::http::StatusCode::CONFLICT
        );
        assert!(
            !ApiErrorResponse::not_ready("x").status.is_success(),
            "a 2xx here is the empty-206 bug returning: the player would treat \
             headers-with-no-body as a broken stream and stop"
        );
    }
}
