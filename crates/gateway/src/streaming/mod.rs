//! HTTP streaming server: HTTP Range support (RFC 7233) over a torrent file
//! that may still be downloading. Piece prioritization and the "block until
//! the needed piece has arrived" logic live inside `librqbit`'s `FileStream`
//! (see `TorrentEngine::api`); this module owns the public URL surface,
//! input validation, and translating byte ranges into HTTP status/headers.

use std::net::{IpAddr, SocketAddr};
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
use crate::torrent::{BoxedReader, StreamGuard, TorrentEngine};

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

    // The resolve above just talked to the swarm; hand those peers straight
    // to the download so it connects immediately instead of re-discovering
    // the same swarm over DHT/trackers a second time.
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
        .and_then(parse_range_header);

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

    // Hold the response until some real data is in hand -- see `prebuffer`.
    let wanted = usize::try_from(end - start).unwrap_or(usize::MAX);
    let (head, reader) = prebuffer(
        reader,
        engine.prebuffer_bytes().min(wanted),
        engine.prebuffer_timeout(),
    )
    .await;

    let body = healing_body(BodyContext {
        position: start + head.len() as u64,
        end,
        engine: Arc::clone(&engine),
        info_hash,
        file_idx,
        client: addr.ip(),
        head,
        reader,
        guard,
    });

    Ok((status, resp_headers, body).into_response())
}

/// Everything the streaming body needs to keep going, and to rebuild its own
/// reader if the torrent stops feeding it.
struct BodyContext {
    engine: Arc<TorrentEngine>,
    info_hash: String,
    file_idx: usize,
    client: IpAddr,
    /// Absolute file offset of the next byte to send.
    position: u64,
    /// Absolute file offset one past the last byte to send.
    end: u64,
    /// Already-read bytes from the pre-buffer, sent before anything else.
    head: Vec<u8>,
    reader: BoxedReader,
    guard: StreamGuard,
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
        client,
        mut position,
        end,
        head,
        mut reader,
        guard,
    } = ctx;

    let stall_timeout = engine.stall_timeout();

    Body::from_stream(async_stream::stream! {
        // Moved in so it lives exactly as long as the body: dropping the
        // response releases the torrent back to the idle reaper.
        let _guard = guard;
        let mut stalls = 0u32;
        let mut last_touch = Instant::now();

        if !head.is_empty() {
            yield Ok::<Bytes, std::io::Error>(Bytes::from(head));
        }

        let mut chunk = vec![0u8; STREAM_CHUNK_BYTES];
        while position < end {
            let want = usize::try_from(end - position)
                .unwrap_or(usize::MAX)
                .min(chunk.len());

            // `Ok(None)` means "produced nothing in time", which is the
            // signal to rebuild the reader rather than an error.
            let outcome = if stall_timeout.is_zero() {
                reader.read(&mut chunk[..want]).await.map(Some)
            } else {
                match tokio::time::timeout(stall_timeout, reader.read(&mut chunk[..want])).await {
                    Ok(result) => result.map(Some),
                    Err(_) => Ok(None),
                }
            };

            match outcome {
                Ok(Some(0)) => break, // end of file
                Ok(Some(n)) => {
                    position += n as u64;
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
                            break;
                        }
                    }
                }
                Err(e) => {
                    warn!(%info_hash, position, "torrent read failed: {e}");
                    break;
                }
            }
        }
    })
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
async fn prebuffer(
    mut reader: BoxedReader,
    max_bytes: usize,
    timeout: Duration,
) -> (Vec<u8>, BoxedReader) {
    if max_bytes == 0 {
        return (Vec::new(), reader);
    }

    let min_bytes = PREBUFFER_MIN_BYTES.min(max_bytes);
    let mut buf = Vec::with_capacity(max_bytes.min(1024 * 1024));

    // Phase 1 -- block until there is enough to be worth sending. This is the
    // wait that stops a player seeing an empty, apparently-broken stream.
    let filled = tokio::time::timeout(timeout, fill(&mut reader, &mut buf, min_bytes)).await;
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
fn parse_range_header(value: &str) -> Option<(u64, Option<u64>)> {
    let spec = value.strip_prefix("bytes=")?;
    let (start, end) = spec.split_once('-')?;
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

    #[test]
    fn parses_open_ended_range() {
        assert_eq!(parse_range_header("bytes=100-"), Some((100, None)));
    }

    #[test]
    fn parses_closed_range_as_exclusive_end() {
        assert_eq!(parse_range_header("bytes=0-99"), Some((0, Some(100))));
    }

    #[test]
    fn rejects_malformed_range_headers() {
        assert_eq!(parse_range_header("nonsense"), None);
        assert_eq!(parse_range_header("bytes=abc-def"), None);
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

    #[tokio::test]
    async fn prebuffer_returns_all_requested_bytes_when_data_is_available() {
        let data = vec![7u8; 8192];
        let reader = Box::new(std::io::Cursor::new(data.clone()));
        let (head, mut rest) = prebuffer(reader, 4096, Duration::from_secs(5)).await;

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
        let (head, _rest) = prebuffer(reader, 4 * 1024 * 1024, Duration::from_millis(200)).await;

        assert!(
            started.elapsed() < Duration::from_secs(2),
            "must return promptly on timeout, not block"
        );
        assert_eq!(head, vec![1u8; 512], "partial data must still be served");
    }

    #[tokio::test]
    async fn prebuffer_disabled_takes_nothing_and_hands_the_reader_back() {
        let reader = Box::new(std::io::Cursor::new(vec![9u8; 100]));
        let (head, mut rest) = prebuffer(reader, 0, Duration::from_secs(5)).await;
        assert!(
            head.is_empty(),
            "disabled pre-buffer must not consume bytes"
        );

        let mut got = Vec::new();
        rest.read_to_end(&mut got).await.unwrap();
        assert_eq!(got, vec![9u8; 100]);
    }

    /// The offset the body will re-open at is `range start + prebuffered
    /// bytes`. If that arithmetic is wrong the stream silently resumes at the
    /// wrong place after a stall, which corrupts playback rather than fixing
    /// it -- so pin the exact relationship the handler relies on.
    #[tokio::test]
    async fn prebuffered_length_is_the_offset_the_body_resumes_from() {
        let range_start = 1_000_000u64;
        let reader = Box::new(std::io::Cursor::new(vec![3u8; 8192]));
        let (head, _rest) = prebuffer(reader, 2048, Duration::from_secs(5)).await;

        assert_eq!(head.len(), 2048);
        assert_eq!(
            range_start + head.len() as u64,
            1_002_048,
            "body must resume exactly where the pre-buffer stopped"
        );
    }
}
