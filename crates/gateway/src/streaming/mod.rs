//! HTTP streaming server: HTTP Range support (RFC 7233) over a torrent file
//! that may still be downloading. Piece prioritization and the "block until
//! the needed piece has arrived" logic live inside `librqbit`'s `FileStream`
//! (see `TorrentEngine::api`); this module owns the public URL surface,
//! input validation, and translating byte ranges into HTTP status/headers.

use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes};
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use librqbit::api::TorrentIdOrHash;
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::{debug, warn};

use crate::error::ApiErrorResponse;
use crate::torrent::{
    resolver, BoxedReader, ReaderCancel, ReaderTicket, StreamGuard, TorrentEngine,
};

/// Bytes the pre-buffer will actually *wait* for. Everything above this is
/// only taken if it is already downloaded -- see `prebuffer`. Deliberately
/// small: it is a floor that prevents an empty response, not a playback
/// buffer, and the player does its own buffering on top.
///
/// It is a *latency* figure, not a throughput one: whatever this is set to,
/// the viewer waits for it before a single byte of the response goes out. At
/// the trickle a torrent runs at in its first seconds, 512 KB was on its own
/// worth more than ten seconds of staring at a spinner, for no benefit -- a
/// player needs enough bytes to start parsing the container, not half a
/// megabyte of it.
const PREBUFFER_MIN_BYTES: usize = 128 * 1024;

/// Never block for less than this, whatever the piece arithmetic says.
///
/// `piece_aware_floor` can shrink the blocking wait, and shrinking it too far
/// reintroduces the bug the pre-buffer exists to prevent: a response with
/// headers and almost no body reads to a player as a broken stream, not a slow
/// one. Small enough to cost nothing, large enough to be a body.
const PREBUFFER_FLOOR_BYTES: usize = 64 * 1024;

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

/// Below this, a full pre-buffer is recorded as served from disk rather than
/// from the swarm — see the `warm` field where it is used.
///
/// Two orders of magnitude above a disk read of this size and two below the
/// smallest wait a swarm round-trip can produce, so the classification does
/// not hinge on where exactly in that gap the threshold sits.
const WARM_PREBUFFER_MS: u64 = 50;

/// How long "is more data already available?" is allowed to take before the
/// pre-buffer stops topping up and sends what it has.
const READY_DATA_POLL: Duration = Duration::from_millis(50);

/// Size of each chunk handed to the HTTP layer while streaming.
const STREAM_CHUNK_BYTES: usize = 64 * 1024;

/// How many times in a row a stalled stream may be re-opened before the body
/// gives up. Past this the swarm is genuinely dead, and ending the response
/// lets the player surface a real error instead of spinning forever.
const MAX_CONSECUTIVE_REOPENS: u32 = 5;

/// How often a long-running response refreshes its "still being watched"
/// timestamp, so the cache janitor's own grace period stays accurate during
/// playback that issues no further HTTP requests.
const TOUCH_INTERVAL: Duration = Duration::from_secs(20);

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
pub async fn stream_video(
    State(engine): State<Arc<TorrentEngine>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Path(VideoPathParams {
        info_hash,
        file_idx,
    }): Path<VideoPathParams>,
    headers: HeaderMap,
) -> Result<Response, ApiErrorResponse> {
    validate_info_hash(&info_hash).map_err(ApiErrorResponse::bad_request)?;

    let idx = TorrentIdOrHash::parse(&info_hash)
        .map_err(|e| ApiErrorResponse::bad_request(format!("invalid info hash: {e}")))?;

    engine.touch_stream(&info_hash, addr.ip()).await;

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

    let file_len = engine
        .file_length(&info_hash, file_idx)
        .map_err(|e| ApiErrorResponse::not_found(format!("stream unavailable: {e:#}")))?;

    let mut status = StatusCode::OK;
    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Ok(mime) = engine.api().torrent_file_mime_type(idx, file_idx) {
        resp_headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(mime));
    }

    let range = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| parse_range_header(v, file_len));

    let (start, end) = match range {
        Some((start, end)) => {
            if start >= file_len || end.is_some_and(|e| e <= start || e > file_len) {
                return Err(ApiErrorResponse::range_not_satisfiable(format!(
                    "range {start}-{end:?} outside file of length {file_len}"
                )));
            }
            let end = end.unwrap_or(file_len);
            status = StatusCode::PARTIAL_CONTENT;
            resp_headers.insert(
                header::CONTENT_RANGE,
                HeaderValue::from_str(&format!(
                    "bytes {start}-{}/{file_len}",
                    end.saturating_sub(1)
                ))
                .expect("ascii digits only"),
            );
            (start, end)
        }
        None => (0, file_len),
    };

    insert_len_header(&mut resp_headers, end - start);

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

    let reader = engine
        .open_stream_at(&info_hash, file_idx, start)
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
        engine.warm_mp4_tail(&info_hash, file_idx, file_len);
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
        engine.prebuffer_bytes().min(wanted)
    };

    // How much to actually *block* for. Capped at what remains of the piece
    // this offset lands in: past that boundary the next byte lives in a
    // second piece, and waiting for it doubles the wait for no benefit the
    // player can use. Floored so the response can never be a set of headers
    // over a few kilobytes, which is the failure the pre-buffer exists to
    // prevent.
    let prebuffer_floor = piece_aware_floor(piece_remainder, prebuffer_target);

    let prebuffer_began = std::time::Instant::now();
    let (head, reader) = prebuffer(
        reader,
        prebuffer_floor,
        prebuffer_target,
        engine.prebuffer_timeout(),
        &cancel,
    )
    .await;
    let prebuffer_ms = prebuffer_began.elapsed().as_millis() as u64;

    // Superseded while it waited: the same player has already asked for a
    // different offset, so nothing will ever read this response.
    let superseded = cancel.is_cancelled();

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
    // cannot return a byte that is not already on disk, so a full-size fill in
    // under `WARM_PREBUFFER_MS` could only have come from data that was
    // already there. The reverse is not claimed -- a warm read that happened to
    // be slow is recorded as cold, which errs toward flagging a wait rather
    // than explaining it away.
    // A superseded read is short of its target because it was told to stop, not
    // because the swarm was slow. Calling that a timeout would make the log
    // report a fault where there was a deliberate decision.
    let timed_out = !superseded && head.len() < prebuffer_target;

    engine.audit().record(crate::audit::Event::Prebuffer {
        info_hash: info_hash.clone(),
        bytes: head.len(),
        wanted: prebuffer_target,
        ms: prebuffer_ms,
        timed_out,
        warm: head.len() >= prebuffer_target && prebuffer_ms < WARM_PREBUFFER_MS,
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
    // `timed_out` already excludes the pre-buffer-disabled case (target 0), so
    // this cannot fire when the operator has deliberately turned the wait off.
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

    Ok((status, resp_headers, body).into_response())
}

/// How many bytes the pre-buffer may *block* for at an offset with
/// `piece_remainder` bytes left in its piece.
///
/// Nothing at that offset can be read until its whole piece has arrived, so
/// blocking for anything within the remainder is free — it is data that comes
/// with the piece already being waited for. Asking for one byte past it means
/// waiting for a *second* piece, and on a release with 8 MB pieces that is the
/// difference between one piece-fetch and two.
///
/// `PREBUFFER_FLOOR_BYTES` is the safety rail: an offset a few kilobytes short
/// of a piece boundary would otherwise return a response with headers and
/// almost no body, which players read as a broken stream rather than a slow
/// one. A remainder of 0 means the geometry was unavailable (metadata not
/// loaded yet), where the old fixed behaviour is the right fallback.
fn piece_aware_floor(piece_remainder: u64, target: usize) -> usize {
    let uncapped = PREBUFFER_MIN_BYTES.min(target);
    if piece_remainder == 0 {
        return uncapped;
    }
    let remainder = usize::try_from(piece_remainder).unwrap_or(usize::MAX);
    uncapped
        .min(remainder.max(PREBUFFER_FLOOR_BYTES))
        .min(target)
}

/// Everything the streaming body needs to keep going, and to rebuild its own
/// reader if the torrent stops feeding it.
struct BodyContext {
    engine: Arc<TorrentEngine>,
    info_hash: String,
    file_idx: usize,
    file_len: u64,
    client: IpAddr,
    /// Absolute file offset of the next byte to send.
    position: u64,
    /// Absolute file offset one past the last byte to send.
    end: u64,
    /// Already-read bytes from the pre-buffer, sent before anything else.
    head: Vec<u8>,
    reader: BoxedReader,
    guard: StreamGuard,
    /// Keeps this response in the reader registry, and reports the bytes it
    /// has delivered — which is what makes it immune to being superseded.
    ticket: ReaderTicket,
    cancel: Arc<ReaderCancel>,
}

/// Builds the response body, re-opening the underlying torrent read whenever
/// it stops producing bytes.
///
/// A torrent read parks waiting for one specific piece, is woken only by that
/// piece arriving, and has no timeout -- so if the swarm goes quiet, or the
/// torrent is paused, or its internal stream registry is replaced (which
/// happens on resume), the read hangs forever and playback freezes with no
/// error anywhere. Re-opening is the cure rather than merely a retry: it
/// re-runs peer discovery for this file and re-registers the current playback
/// position in the piece-priority set. It is the same recovery a viewer
/// performs by hand when they skip forward to unstick a frozen video, done
/// automatically and without interrupting the response.
fn healing_body(ctx: BodyContext) -> Body {
    let BodyContext {
        engine,
        info_hash,
        file_idx,
        file_len,
        client,
        mut position,
        end,
        head,
        mut reader,
        guard,
        ticket,
        cancel,
    } = ctx;

    let stall_timeout = engine.stall_timeout();

    // Published for the read-ahead claim to follow. An atomic rather than a
    // message because the claim only ever wants the latest value and must
    // never be able to hold the body up.
    let live_position = Arc::new(AtomicU64::new(position));

    Body::from_stream(async_stream::stream! {
        // Moved in so it lives exactly as long as the body: dropping the
        // response releases the torrent back to the idle reaper.
        let _guard = guard;
        // Likewise: dropping this deregisters the response, so a reader only
        // competes for piece priority while it actually exists.
        let ticket = ticket;
        // Claims priority past librqbit's fixed window once playback settles.
        // `None` unless `READAHEAD_EXTRA_MB` is set; dropping it drops the
        // claim. See `spawn_readahead`.
        let _readahead = engine.spawn_readahead(
            &info_hash,
            file_idx,
            Arc::clone(&live_position),
            file_len,
        );
        let mut stalls = 0u32;
        let mut last_touch = Instant::now();

        // Records how this response ended, whatever ends it.
        //
        // It has to be a drop guard rather than a line after the loop: the
        // most common ending by far is the player hanging up, which drops this
        // generator mid-`yield`, and code placed after the loop would simply
        // never run for it. That would leave the log recording only the
        // tidy endings -- exactly backwards, since an abandoned stream is the
        // interesting one.
        let mut closer = CloseRecorder {
            audit: engine.audit(),
            info_hash: info_hash.clone(),
            client,
            opened_at: Instant::now(),
            start_offset: position,
            position,
            reason: "client disconnected",
        };

        if !head.is_empty() {
            // Counted before it is sent: from here on this response has shown
            // the viewer something, and must never be superseded by a later
            // request. See `register_reader`.
            ticket.record_served(head.len() as u64);
            yield Ok::<Bytes, std::io::Error>(Bytes::from(head));
        }

        let mut chunk = vec![0u8; STREAM_CHUNK_BYTES];
        while position < end {
            let want = usize::try_from(end - position)
                .unwrap_or(usize::MAX)
                .min(chunk.len());

            // `Ok(None)` means "produced nothing in time", which is the
            // signal to rebuild the reader rather than an error.
            //
            // The cancellation arm only ever fires for a response that has
            // delivered nothing at all -- a read the player seeked away from
            // while it was still waiting for its first piece. Ending it here
            // hands its share of the piece requests to the position the viewer
            // actually landed on, which is the entire point (a torrent read
            // parks until its piece arrives, so without this the abandoned
            // read keeps its claim for the full stall timeout).
            let outcome = if stall_timeout.is_zero() {
                tokio::select! {
                    result = reader.read(&mut chunk[..want]) => result.map(Some),
                    _ = cancel.cancelled() => {
                        closer.reason = "superseded by a newer request";
                        break;
                    }
                }
            } else {
                tokio::select! {
                    result = tokio::time::timeout(
                        stall_timeout,
                        reader.read(&mut chunk[..want]),
                    ) => match result {
                        Ok(result) => result.map(Some),
                        Err(_) => Ok(None),
                    },
                    _ = cancel.cancelled() => {
                        closer.reason = "superseded by a newer request";
                        break;
                    }
                }
            };

            match outcome {
                Ok(Some(0)) => {
                    closer.reason = "end of file";
                    break;
                }
                Ok(Some(n)) => {
                    position += n as u64;
                    closer.position = position;
                    ticket.record_served(n as u64);
                    live_position.store(position, std::sync::atomic::Ordering::Relaxed);
                    stalls = 0;
                    if last_touch.elapsed() >= TOUCH_INTERVAL {
                        engine.touch_stream(&info_hash, client).await;
                        last_touch = Instant::now();
                    }
                    yield Ok(Bytes::copy_from_slice(&chunk[..n]));
                }
                Ok(None) => {
                    stalls += 1;
                    if stalls > MAX_CONSECUTIVE_REOPENS {
                        warn!(
                            %info_hash, position,
                            "stream stalled and did not recover after {MAX_CONSECUTIVE_REOPENS} \
                             re-opens; ending the response so the player can retry"
                        );
                        closer.reason = "stalled, gave up re-opening";
                        break;
                    }
                    warn!(
                        %info_hash, position, attempt = stalls,
                        "no data for {}s; re-opening the torrent stream",
                        stall_timeout.as_secs()
                    );
                    engine.touch_stream(&info_hash, client).await;
                    last_touch = Instant::now();
                    match engine.open_stream_at(&info_hash, file_idx, position).await {
                        Ok(fresh) => reader = fresh,
                        Err(e) => {
                            warn!(%info_hash, "could not re-open stalled stream: {e:#}");
                            closer.reason = "could not re-open stalled stream";
                            break;
                        }
                    }
                }
                Err(e) => {
                    warn!(%info_hash, position, "torrent read failed: {e}");
                    closer.reason = "torrent read failed";
                    break;
                }
            }
        }
    })
}

/// Emits a `StreamClose` when the response body ends, however it ends.
///
/// See the comment where it is constructed for why this is a drop guard and
/// not a line at the end of the loop.
struct CloseRecorder {
    audit: crate::audit::AuditLog,
    info_hash: String,
    client: IpAddr,
    opened_at: Instant,
    /// File offset this response began at, so `bytes_served` measures what
    /// this request delivered rather than how far into the file it reached --
    /// a seek to the last minute of a film is not a 2 GB read.
    start_offset: u64,
    position: u64,
    reason: &'static str,
}

impl Drop for CloseRecorder {
    fn drop(&mut self) {
        self.audit.record(crate::audit::Event::StreamClose {
            info_hash: self.info_hash.clone(),
            client: self.client.to_string(),
            bytes_served: self.position.saturating_sub(self.start_offset),
            duration_ms: self.opened_at.elapsed().as_millis() as u64,
            reason: self.reason.to_string(),
        });
    }
}

/// Reads up to `max_bytes` from `reader` before the response is sent, and
/// returns those bytes alongside the reader for the remainder.
///
/// This exists because of how players react to a *cold* torrent. Serving
/// headers immediately and letting the body trickle looks like a broken
/// stream: Stremio cycles through players, reports a 00:00 duration, or gives
/// up entirely. Blocking here instead turns that into an ordinary slow
/// request, which players wait on happily.
///
/// It is nearly free in the common case -- once a torrent has data on disk
/// this read completes at memory speed -- so the wait only ever happens when
/// there genuinely is nothing to send yet. A timeout bounds the worst case:
/// on expiry the response goes out with whatever arrived rather than hanging.
///
/// The consumed bytes are returned rather than chained back onto the reader
/// because the body has to know the exact file offset it has reached, so it
/// can re-open at that offset if the stream stalls.
///
/// `min_bytes` is what this will actually *block* for; `max_bytes` is only
/// ever reached from data already on disk. The caller sizes the former against
/// the piece the offset lands in (see `piece_aware_floor`), because waiting
/// past that boundary means waiting for a whole second piece.
///
/// `cancel` ends the wait early when the same client has already seeked
/// elsewhere. That matters more here than anywhere: this is where an
/// abandoned scrub would otherwise sit for the full timeout holding a piece
/// priority claim for a position nobody is going to watch.
async fn prebuffer(
    mut reader: BoxedReader,
    min_bytes: usize,
    max_bytes: usize,
    timeout: Duration,
    cancel: &ReaderCancel,
) -> (Vec<u8>, BoxedReader) {
    if max_bytes == 0 {
        return (Vec::new(), reader);
    }

    let min_bytes = min_bytes.min(max_bytes);
    let mut buf = Vec::with_capacity(max_bytes.min(1024 * 1024));

    // Phase 1 -- block until there is enough to be worth sending. This is the
    // wait that stops a player seeing an empty, apparently-broken stream.
    //
    // The cancellation arm cannot simply return the reader: the other arm
    // holds a mutable borrow of it for as long as the `select!` does. So it
    // sets a flag and the return happens once both futures are dropped.
    let mut superseded = false;
    let filled = tokio::select! {
        filled = tokio::time::timeout(timeout, fill(&mut reader, &mut buf, min_bytes)) => filled,
        _ = cancel.cancelled() => {
            superseded = true;
            Ok(())
        }
    };
    if superseded {
        return (buf, reader);
    }
    if filled.is_err() {
        debug!(
            got = buf.len(),
            want = min_bytes,
            "pre-buffer timed out; responding with what arrived"
        );
    }

    // Phase 2 -- top up towards `max_bytes`, but only with data that is
    // *already* downloaded. Blocking here would be pure added latency: the
    // torrent fetches whole pieces (4-16 MB), so insisting on the full buffer
    // can mean waiting for another entire piece when the client could already
    // be playing. This is what keeps a seek fast -- it returns as soon as the
    // piece it landed in is available, instead of waiting for the next one.
    if buf.len() < max_bytes {
        // A timeout here is the expected, healthy outcome: it means the next
        // byte has not been downloaded yet, so stop and let the client start.
        let _ = tokio::time::timeout(READY_DATA_POLL, fill(&mut reader, &mut buf, max_bytes)).await;
    }

    (buf, reader)
}

/// Reads until `buf` holds `target` bytes or the stream ends. A read error is
/// treated as end-of-input: the partial buffer is still valid to send, and
/// the error will resurface on the response body if it is real.
async fn fill(reader: &mut (impl AsyncRead + Unpin), buf: &mut Vec<u8>, target: usize) {
    let mut chunk = vec![0u8; 64 * 1024];
    while buf.len() < target {
        let want = (target - buf.len()).min(chunk.len());
        match reader.read(&mut chunk[..want]).await {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
}

fn insert_len_header(headers: &mut HeaderMap, len: u64) {
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&len.to_string()).expect("ascii digits only"),
    );
}

/// Only ever accepts a bare 40-char hex BitTorrent info hash. Anything else
/// (including `..`, `/`, or any other filesystem-path-shaped input) is
/// rejected before it is used for anything.
pub fn validate_info_hash(value: &str) -> Result<(), String> {
    if value.len() == 40 && value.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err("info hash must be exactly 40 hex characters".to_string())
    }
}

/// Parses a single-range `Range: bytes=start-end` header. Multi-range
/// requests (`bytes=0-10,20-30`) are not supported (no player used in
/// practice sends them for video) and fall back to a full-content response,
/// same as the header being absent.
///
/// Handles the RFC 7233 *suffix* form `bytes=-N` ("the last N bytes"), which
/// needs `file_len` to resolve into an absolute offset. That form is not
/// exotic: an MKV stores its Cues/SeekHead index at the *end* of the file, so
/// Android players routinely ask for the tail first to learn the duration
/// before playing a frame. Failing to parse it here did not produce an error
/// -- it looked identical to a request with no `Range` header at all, so the
/// gateway answered `200` and began streaming the whole file from byte 0. The
/// player then waited forever for an index that would only arrive gigabytes
/// later, which is exactly the "spins on 00:00 / 00:00 and never plays"
/// symptom. `TAIL_PROBE_ZONE` downstream exists to serve this read cheaply and
/// was unreachable while this returned `None`.
fn parse_range_header(value: &str, file_len: u64) -> Option<(u64, Option<u64>)> {
    let spec = value.strip_prefix("bytes=")?.trim();
    let (start, end) = spec.split_once('-')?;
    let (start, end) = (start.trim(), end.trim());

    if start.is_empty() {
        // Suffix form: `bytes=-N` is the last N bytes, always running to EOF.
        // A zero-length suffix is unsatisfiable rather than "the whole file",
        // and N larger than the file legitimately means the entire file.
        let n = end.parse::<u64>().ok()?;
        if n == 0 {
            return None;
        }
        return Some((file_len.saturating_sub(n), None));
    }

    let start = start.parse::<u64>().ok()?;
    let end = if end.is_empty() {
        None
    } else {
        Some(end.parse::<u64>().ok()?.saturating_add(1))
    };
    Some((start, end))
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
        assert!(validate_info_hash("0123456789abcdef0123456789abcdef012345678").is_err());
        // 41 chars
    }

    const TEST_LEN: u64 = 1_000_000;

    #[test]
    fn parses_open_ended_range() {
        assert_eq!(parse_range_header("bytes=100-", TEST_LEN), Some((100, None)));
    }

    #[test]
    fn parses_closed_range_as_exclusive_end() {
        assert_eq!(
            parse_range_header("bytes=0-99", TEST_LEN),
            Some((0, Some(100)))
        );
    }

    #[test]
    fn rejects_malformed_range_headers() {
        assert_eq!(parse_range_header("nonsense", TEST_LEN), None);
        assert_eq!(parse_range_header("bytes=abc-def", TEST_LEN), None);
    }

    /// The regression that made Android sit on `00:00 / 00:00` forever.
    ///
    /// An MKV keeps its Cues index at the end, so a player asks for the tail
    /// before it can report a duration. This form used to fail to parse, which
    /// was indistinguishable from "no Range header" -- the gateway answered
    /// `200` with the whole file from byte 0 and the player waited on an index
    /// that was gigabytes away. It must resolve to an absolute offset running
    /// to EOF, so the response is a `206` covering only the tail.
    #[test]
    fn parses_suffix_range_as_the_last_n_bytes() {
        assert_eq!(
            parse_range_header("bytes=-65536", TEST_LEN),
            Some((TEST_LEN - 65536, None))
        );
    }

    /// A suffix larger than the file is not an error -- RFC 7233 says it means
    /// the whole file. Clamping to 0 rather than underflowing is what keeps it
    /// from becoming a wild offset near u64::MAX.
    #[test]
    fn a_suffix_longer_than_the_file_starts_at_zero() {
        assert_eq!(
            parse_range_header("bytes=-99999999", TEST_LEN),
            Some((0, None))
        );
    }

    /// `bytes=-0` requests the last zero bytes, which is unsatisfiable. It must
    /// not fall through to "start at the end of the file", nor be mistaken for
    /// a request for the entire file.
    #[test]
    fn a_zero_length_suffix_is_rejected() {
        assert_eq!(parse_range_header("bytes=-0", TEST_LEN), None);
    }

    /// A reader that yields `chunk` once and then blocks forever -- stands in
    /// for a cold torrent that has the first pieces but not the rest yet.
    struct StallsAfter {
        chunk: Vec<u8>,
        sent: bool,
    }

    impl AsyncRead for StallsAfter {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            if self.sent {
                return std::task::Poll::Pending; // never resolves
            }
            let n = self.chunk.len().min(buf.remaining());
            buf.put_slice(&self.chunk[..n]);
            self.sent = true;
            std::task::Poll::Ready(Ok(()))
        }
    }

    /// A cancel signal that is never fired, for the tests that are not about
    /// superseding.
    fn never_cancelled() -> Arc<ReaderCancel> {
        Arc::new(ReaderCancel::never_cancelled())
    }

    #[tokio::test]
    async fn prebuffer_returns_all_requested_bytes_when_data_is_available() {
        let data = vec![7u8; 8192];
        let reader = Box::new(std::io::Cursor::new(data.clone()));
        let (head, mut rest) =
            prebuffer(reader, 4096, 4096, Duration::from_secs(5), &never_cancelled()).await;

        // Every byte must survive the split, in order: the body sends `head`
        // first and then continues from `rest`.
        assert_eq!(head.len(), 4096, "should have taken the full pre-buffer");
        let mut got = head;
        rest.read_to_end(&mut got).await.unwrap();
        assert_eq!(got, data, "pre-buffering must not drop or reorder bytes");
    }

    #[tokio::test]
    async fn prebuffer_gives_up_on_timeout_and_keeps_what_it_read() {
        // The whole point: a torrent that stalls must still produce a
        // response, not hang until the client gives up.
        let reader = Box::new(StallsAfter {
            chunk: vec![1u8; 512],
            sent: false,
        });
        let started = std::time::Instant::now();
        let (head, _rest) = prebuffer(
            reader,
            PREBUFFER_MIN_BYTES,
            4 * 1024 * 1024,
            Duration::from_millis(200),
            &never_cancelled(),
        )
        .await;

        assert!(
            started.elapsed() < Duration::from_secs(2),
            "must return promptly on timeout, not block"
        );
        assert_eq!(head, vec![1u8; 512], "partial data must still be served");
    }

    /// The precondition behind the `not_ready` branch in `stream_video`: a
    /// swarm that sends nothing at all leaves the pre-buffer empty, not merely
    /// short. That is the case where returning a 206 hands the player headers
    /// over an empty body, and the player stops — measured on 2026-09-07, a
    /// seek to 83% of a file 40% downloaded, `bytes: 0` after 15 s.
    #[tokio::test]
    async fn a_prebuffer_that_gets_nothing_comes_back_empty() {
        let reader = Box::new(StallsAfter {
            chunk: Vec::new(),
            sent: true,
        });
        let (head, _rest) = prebuffer(
            reader,
            PREBUFFER_MIN_BYTES,
            1024 * 1024,
            Duration::from_millis(200),
            &never_cancelled(),
        )
        .await;

        assert!(head.is_empty(), "nothing arrived, so nothing may be served");
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

    #[tokio::test]
    async fn prebuffer_disabled_takes_nothing_and_hands_the_reader_back() {
        let reader = Box::new(std::io::Cursor::new(vec![9u8; 100]));
        let (head, mut rest) =
            prebuffer(reader, 0, 0, Duration::from_secs(5), &never_cancelled()).await;
        assert!(
            head.is_empty(),
            "disabled pre-buffer must not consume bytes"
        );

        let mut got = Vec::new();
        rest.read_to_end(&mut got).await.unwrap();
        assert_eq!(got, vec![9u8; 100]);
    }

    /// A superseded read must abandon its wait immediately, not sit out the
    /// full pre-buffer timeout.
    ///
    /// The timeout is what it costs to get this wrong: until the wait ends,
    /// the abandoned read keeps its claim in librqbit's piece-priority set, so
    /// the position the viewer actually seeked to is still sharing the
    /// download with a position nobody is watching. That is the whole defect
    /// superseding exists to fix, so "it returns eventually" is not enough.
    #[tokio::test]
    async fn a_superseded_prebuffer_stops_waiting_at_once() {
        let reader = Box::new(StallsAfter {
            chunk: vec![1u8; 512],
            sent: false,
        });
        let cancel = never_cancelled();

        let waiter = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            waiter.cancel_for_test();
        });

        let started = std::time::Instant::now();
        // A 60s timeout it must not wait out, and a floor it can never reach
        // from a reader that produces 512 bytes and then stops forever.
        let (head, _rest) = prebuffer(
            reader,
            PREBUFFER_MIN_BYTES,
            4 * 1024 * 1024,
            Duration::from_secs(60),
            &cancel,
        )
        .await;

        assert!(
            started.elapsed() < Duration::from_secs(5),
            "a superseded read must not hold its priority claim for the full timeout"
        );
        assert_eq!(
            head,
            vec![1u8; 512],
            "whatever did arrive is still handed back, so nothing is lost if the \
             caller decides to serve it anyway"
        );
    }

    /// Blocking past the end of the piece the offset lands in means waiting
    /// for a whole second piece -- several seconds on a large-piece release,
    /// for bytes the player has not asked for yet.
    #[test]
    fn the_prebuffer_never_blocks_past_its_own_piece() {
        // The common case: a piece boundary far away, so nothing changes.
        assert_eq!(
            piece_aware_floor(4 * 1024 * 1024, 1024 * 1024),
            PREBUFFER_MIN_BYTES
        );

        // Landing 100 KB before a boundary: block for the 100 KB rather than
        // the usual 128 KB, and a second piece is never involved.
        assert_eq!(piece_aware_floor(100 * 1024, 1024 * 1024), 100 * 1024);

        // Landing almost exactly on a boundary. Crossing it is unavoidable
        // here, and returning a near-empty body is the worse failure -- so the
        // floor wins. See `PREBUFFER_FLOOR_BYTES`.
        assert_eq!(
            piece_aware_floor(200, 1024 * 1024),
            PREBUFFER_FLOOR_BYTES,
            "a handful of bytes is not a response a player will accept"
        );

        // Never more than was asked for: a small range request must not be
        // made to wait for bytes outside it.
        assert_eq!(piece_aware_floor(4 * 1024 * 1024, 8 * 1024), 8 * 1024);

        // No metadata yet means no geometry to be clever with.
        assert_eq!(piece_aware_floor(0, 1024 * 1024), PREBUFFER_MIN_BYTES);
    }

    /// The offset the body will re-open at is `range start + prebuffered
    /// bytes`. If that arithmetic is wrong the stream silently resumes at the
    /// wrong place after a stall, which corrupts playback rather than fixing
    /// it -- so pin the exact relationship the handler relies on.
    #[tokio::test]
    async fn prebuffered_length_is_the_offset_the_body_resumes_from() {
        let range_start = 1_000_000u64;
        let reader = Box::new(std::io::Cursor::new(vec![3u8; 8192]));
        let (head, _rest) =
            prebuffer(reader, 2048, 2048, Duration::from_secs(5), &never_cancelled()).await;

        assert_eq!(head.len(), 2048);
        assert_eq!(
            range_start + head.len() as u64,
            1_002_048,
            "body must resume exactly where the pre-buffer stopped"
        );
    }
}
