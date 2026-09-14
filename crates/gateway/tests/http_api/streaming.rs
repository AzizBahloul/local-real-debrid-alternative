//! Serving bytes: a torrent built on the spot and seeded onto disk (see
//! `seed_torrent`), read back through `/videos` and `/play`.

use axum::http::{header, Method, StatusCode};

use crate::{request, seed_torrent, send, Gateway, SeededTorrent};

fn videos(seeded: &SeededTorrent) -> String {
    format!("/videos/{}/0", seeded.info_hash)
}

#[tokio::test]
async fn a_range_request_is_answered_with_exactly_those_bytes() {
    let gateway = Gateway::start().await;
    let seeded = seed_torrent(&gateway).await;
    let len = seeded.content.len();

    // Crosses a piece boundary (pieces are 32 KiB).
    let reply = send(
        gateway.router(),
        request(Method::GET, &videos(&seeded)).header(header::RANGE, "bytes=32000-33999"),
    )
    .await;

    assert_eq!(reply.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        reply.header(header::CONTENT_RANGE),
        Some(format!("bytes 32000-33999/{len}").as_str())
    );
    assert_eq!(reply.header(header::CONTENT_LENGTH), Some("2000"));
    assert_eq!(reply.header(header::ACCEPT_RANGES), Some("bytes"));
    assert_eq!(&reply.body[..], &seeded.content[32000..34000]);
}

#[tokio::test]
async fn a_whole_file_request_is_byte_exact() {
    let gateway = Gateway::start().await;
    let seeded = seed_torrent(&gateway).await;

    let reply = send(gateway.router(), request(Method::GET, &videos(&seeded))).await;

    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(
        reply.header(header::CONTENT_LENGTH),
        Some(seeded.content.len().to_string().as_str())
    );
    assert!(reply.header(header::CONTENT_RANGE).is_none());
    assert!(
        reply.body[..] == seeded.content[..],
        "the file must come back unchanged"
    );
}

/// RFC 7233: a last byte past the end means "to the end". Players ask for a
/// fixed-size window without knowing the length, and refusing those left
/// them unable to read the final stretch of a file.
#[tokio::test]
async fn a_range_running_past_the_end_is_clamped_to_it() {
    let gateway = Gateway::start().await;
    let seeded = seed_torrent(&gateway).await;
    let len = seeded.content.len();

    let reply = send(
        gateway.router(),
        request(Method::GET, &videos(&seeded))
            .header(header::RANGE, format!("bytes={}-{}", len - 10, len + 5000)),
    )
    .await;

    assert_eq!(reply.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        reply.header(header::CONTENT_RANGE),
        Some(format!("bytes {}-{}/{len}", len - 10, len - 1).as_str())
    );
    assert_eq!(&reply.body[..], &seeded.content[len - 10..]);
}

#[tokio::test]
async fn a_range_starting_past_the_end_is_416_with_the_real_length() {
    let gateway = Gateway::start().await;
    let seeded = seed_torrent(&gateway).await;
    let len = seeded.content.len();

    let reply = send(
        gateway.router(),
        request(Method::GET, &videos(&seeded)).header(header::RANGE, format!("bytes={len}-")),
    )
    .await;

    assert_eq!(reply.status, StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(
        reply.header(header::CONTENT_RANGE),
        Some(format!("bytes */{len}").as_str())
    );
}

/// A player probing the length is not a viewer, and must not be treated as
/// one: no reader, no body, no wait.
#[tokio::test]
async fn head_answers_with_headers_and_opens_no_stream() {
    let gateway = Gateway::start().await;
    let seeded = seed_torrent(&gateway).await;

    let reply = send(
        gateway.router(),
        request(Method::HEAD, &videos(&seeded)).header(header::RANGE, "bytes=0-99"),
    )
    .await;

    assert_eq!(reply.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(reply.header(header::CONTENT_LENGTH), Some("100"));
    assert!(reply.header(header::CONTENT_TYPE).is_some());
    assert!(reply.body.is_empty());
    assert_eq!(gateway.state.engine.open_stream_count(), 0);
}

/// The replay case the archive exists for: the title's files and its
/// session entry are gone, and `/play` still starts it without a swarm.
#[tokio::test]
async fn play_resolves_from_the_archive_and_redirects_to_the_stream() {
    let gateway = Gateway::start().await;
    let seeded = seed_torrent(&gateway).await;
    let magnet = format!("magnet:?xt=urn:btih:{}", seeded.info_hash);

    let reply = send(
        gateway.router(),
        request(
            Method::GET,
            &format!("/play?magnet={}", urlencoding::encode(&magnet)),
        ),
    )
    .await;

    assert_eq!(
        reply.status,
        StatusCode::TEMPORARY_REDIRECT,
        "{:?}",
        reply.json()
    );
    assert_eq!(
        reply.header(header::LOCATION),
        Some(videos(&seeded).as_str())
    );
}

/// A magnet may carry its hash in base32. The redirect must still name the
/// hex form, which is the only one `/videos` accepts.
#[tokio::test]
async fn a_base32_magnet_redirects_to_the_hex_stream_url() {
    let gateway = Gateway::start().await;
    let seeded = seed_torrent(&gateway).await;
    let magnet = format!("magnet:?xt=urn:btih:{}", base32(&seeded.info_hash));

    let reply = send(
        gateway.router(),
        request(
            Method::GET,
            &format!("/play?magnet={}", urlencoding::encode(&magnet)),
        ),
    )
    .await;

    assert_eq!(
        reply.status,
        StatusCode::TEMPORARY_REDIRECT,
        "{:?}",
        reply.json()
    );
    assert_eq!(
        reply.header(header::LOCATION),
        Some(videos(&seeded).as_str())
    );
}

#[tokio::test]
async fn health_lists_a_started_torrent() {
    let gateway = Gateway::start().await;
    let seeded = seed_torrent(&gateway).await;

    let reply = send(
        gateway.router(),
        request(Method::GET, &videos(&seeded)).header(header::RANGE, "bytes=0-0"),
    )
    .await;
    assert_eq!(reply.status, StatusCode::PARTIAL_CONTENT);

    let health = send(gateway.router(), request(Method::GET, "/health"))
        .await
        .json();
    let torrents = health["active_torrents"].as_array().unwrap();
    assert_eq!(torrents.len(), 1);
    assert_eq!(torrents[0]["info_hash"], seeded.info_hash.as_str());
    assert_eq!(torrents[0]["name"], seeded.name.as_str());
}

/// RFC 4648 base32 of a hex string, for building the magnet above.
fn base32(hex: &str) -> String {
    const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let bytes: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect();
    let mut out = String::new();
    let (mut buffer, mut bits) = (0u32, 0u32);
    for byte in bytes {
        buffer = (buffer << 8) | u32::from(byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(ALPHABET[((buffer >> bits) & 31) as usize] as char);
        }
    }
    out
}
