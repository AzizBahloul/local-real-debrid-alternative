//! HTTP streaming server: HTTP Range support (RFC 7233) over a torrent file
//! that may still be downloading. Piece prioritization and the "block until
//! the needed piece has arrived" logic live inside `librqbit`'s `FileStream`
//! (see `TorrentEngine::api`); this module owns the public URL surface,
//! input validation, and translating byte ranges into HTTP status/headers.

use std::io::SeekFrom;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use librqbit::api::TorrentIdOrHash;
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt};

use crate::error::ApiErrorResponse;
use crate::torrent::TorrentEngine;

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

    let byte_stream = tokio_util::io::ReaderStream::with_capacity(boxed, 64 * 1024);
    Ok((status, resp_headers, Body::from_stream(byte_stream)).into_response())
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
}
