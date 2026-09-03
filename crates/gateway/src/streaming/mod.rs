//! HTTP streaming server: HTTP Range support (RFC 7233) over a torrent file
//! that may still be downloading. Piece prioritization and the "block until
//! the needed piece has arrived" logic live inside `librqbit`'s `FileStream`
//! (see `TorrentEngine::api`); this module owns the public URL surface,
//! input validation, and translating byte ranges into HTTP status/headers.

use std::io::SeekFrom;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use librqbit::api::TorrentIdOrHash;
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt};
use tracing::debug;

use crate::error::ApiErrorResponse;
use crate::torrent::TorrentEngine;

/// Bytes the pre-buffer will actually *wait* for. Everything above this is
/// only taken if it is already downloaded -- see `prebuffer`. Deliberately
/// small: it is a floor that prevents an empty response, not a playback
/// buffer, and the player does its own buffering on top.
const PREBUFFER_MIN_BYTES: usize = 512 * 1024;

/// How long "is more data already available?" is allowed to take before the
/// pre-buffer stops topping up and sends what it has.
const READY_DATA_POLL: Duration = Duration::from_millis(50);

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

    let info_hash = engine
        .start_file(&query.magnet, file_idx)
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

    let mut file_stream = engine
        .api()
        .api_stream(idx, file_idx)
        .await
        .map_err(|e| ApiErrorResponse::not_found(format!("stream unavailable: {e}")))?;

    let file_len = file_stream.len();

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

    let boxed: Box<dyn AsyncRead + Send + Unpin> = if let Some((start, end)) = range {
        if start >= file_len || end.is_some_and(|e| e <= start || e > file_len) {
            return Err(ApiErrorResponse::range_not_satisfiable(format!(
                "range {start}-{end:?} outside file of length {file_len}"
            )));
        }
        status = StatusCode::PARTIAL_CONTENT;
        let end = end.unwrap_or(file_len);

        file_stream
            .seek(SeekFrom::Start(start))
            .await
            .map_err(|e| ApiErrorResponse::internal(format!("seek failed: {e}")))?;

        let to_take = end - start;
        insert_len_header(&mut resp_headers, to_take);
        resp_headers.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!(
                "bytes {start}-{}/{file_len}",
                end.saturating_sub(1)
            ))
            .expect("ascii digits only"),
        );
        Box::new(file_stream.take(to_take))
    } else {
        insert_len_header(&mut resp_headers, file_len);
        Box::new(file_stream)
    };

    // Hold the response until some real data is in hand -- see `prebuffer`.
    let boxed = prebuffer(boxed, engine.prebuffer_bytes(), engine.prebuffer_timeout()).await;

    let byte_stream = tokio_util::io::ReaderStream::with_capacity(boxed, 64 * 1024);
    Ok((status, resp_headers, Body::from_stream(byte_stream)).into_response())
}

/// Reads up to `max_bytes` from `reader` before the response is sent, then
/// returns a reader that replays those bytes followed by the rest.
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
async fn prebuffer(
    mut reader: Box<dyn AsyncRead + Send + Unpin>,
    max_bytes: usize,
    timeout: Duration,
) -> Box<dyn AsyncRead + Send + Unpin> {
    if max_bytes == 0 {
        return reader;
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

    // Cursor replays the bytes we consumed, then hands over to the live
    // stream -- the client sees one continuous body either way.
    Box::new(std::io::Cursor::new(buf).chain(reader))
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
        let mut out = prebuffer(reader, 4096, Duration::from_secs(5)).await;

        // Every byte must survive the buffer/replay round trip, in order.
        let mut got = Vec::new();
        out.read_to_end(&mut got).await.unwrap();
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
        let mut out = prebuffer(reader, 4 * 1024 * 1024, Duration::from_millis(200)).await;

        assert!(
            started.elapsed() < Duration::from_secs(2),
            "must return promptly on timeout, not block"
        );

        let mut got = vec![0u8; 512];
        out.read_exact(&mut got).await.unwrap();
        assert_eq!(got, vec![1u8; 512], "partial data must still be served");
    }

    #[tokio::test]
    async fn prebuffer_disabled_passes_the_reader_through_untouched() {
        let reader = Box::new(std::io::Cursor::new(vec![9u8; 100]));
        let mut out = prebuffer(reader, 0, Duration::from_secs(5)).await;
        let mut got = Vec::new();
        out.read_to_end(&mut got).await.unwrap();
        assert_eq!(got, vec![9u8; 100]);
    }
}
