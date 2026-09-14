//! Holding a video response until real bytes are in hand -- see `prebuffer`.

use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::debug;

use super::body::STREAM_CHUNK_BYTES;
use crate::torrent::{BoxedReader, ReaderCancel};

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
pub const PREBUFFER_MIN_BYTES: usize = 128 * 1024;

/// Never block for less than this, whatever the piece arithmetic says.
///
/// `piece_aware_floor` can shrink the blocking wait, and shrinking it too far
/// reintroduces the bug the pre-buffer exists to prevent: a response with
/// headers and almost no body reads to a player as a broken stream, not a slow
/// one. Small enough to cost nothing, large enough to be a body.
const PREBUFFER_FLOOR_BYTES: usize = 64 * 1024;

/// How long "is more data already available?" is allowed to take before the
/// pre-buffer stops topping up and sends what it has.
const READY_DATA_POLL: Duration = Duration::from_millis(50);

/// The most the pre-buffer allocates up front. The ceiling itself is
/// configurable, and a large one should grow into its buffer rather than
/// reserve it for a cold read that will only ever fill the floor.
const MAX_UPFRONT_CAPACITY: usize = 1024 * 1024;

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
pub fn piece_aware_floor(piece_remainder: u64, target: usize) -> usize {
    let uncapped = PREBUFFER_MIN_BYTES.min(target);
    if piece_remainder == 0 {
        return uncapped;
    }
    let remainder = usize::try_from(piece_remainder).unwrap_or(usize::MAX);
    uncapped
        .min(remainder.max(PREBUFFER_FLOOR_BYTES))
        .min(target)
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
pub async fn prebuffer(
    mut reader: BoxedReader,
    min_bytes: usize,
    max_bytes: usize,
    timeout: Duration,
    cancel: &ReaderCancel,
) -> (Bytes, BoxedReader) {
    if max_bytes == 0 {
        return (Bytes::new(), reader);
    }

    let min_bytes = min_bytes.min(max_bytes);
    // A `BytesMut` rather than a `Vec` so handing the result to the body is a
    // `freeze`, never a copy.
    let mut buf = BytesMut::with_capacity(max_bytes.min(MAX_UPFRONT_CAPACITY));

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
        return (buf.freeze(), reader);
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

    (buf.freeze(), reader)
}

/// Reads until `buf` holds `target` bytes or the stream ends. A read error is
/// treated as end-of-input: the partial buffer is still valid to send, and
/// the error will resurface on the response body if it is real.
///
/// Reads straight into `buf`'s spare capacity, so the bytes are copied once
/// (by the reader) rather than into a scratch chunk and then again into the
/// buffer.
async fn fill(reader: &mut (impl AsyncRead + Unpin), buf: &mut BytesMut, target: usize) {
    while buf.len() < target {
        let want = (target - buf.len()).min(STREAM_CHUNK_BYTES);
        buf.reserve(want);
        match reader.read_buf(&mut (&mut *buf).limit(want)).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

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
        let (head, mut rest) = prebuffer(
            reader,
            4096,
            4096,
            Duration::from_secs(5),
            &never_cancelled(),
        )
        .await;

        // Every byte must survive the split, in order: the body sends `head`
        // first and then continues from `rest`.
        assert_eq!(head.len(), 4096, "should have taken the full pre-buffer");
        let mut got = head.to_vec();
        rest.read_to_end(&mut got).await.unwrap();
        assert_eq!(got, data, "pre-buffering must not drop or reorder bytes");
    }

    /// Bytes arriving in reads smaller than a chunk, and a target that is not
    /// a multiple of them, must still come back whole and in order.
    #[tokio::test]
    async fn prebuffer_assembles_many_small_reads_exactly() {
        let data: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
        let reader = Box::new(tokio::io::BufReader::with_capacity(
            1000,
            std::io::Cursor::new(data.clone()),
        ));
        let (head, mut rest) = prebuffer(
            reader,
            200_001,
            250_003,
            Duration::from_secs(5),
            &never_cancelled(),
        )
        .await;

        assert_eq!(head.len(), 250_003);
        let mut got = head.to_vec();
        rest.read_to_end(&mut got).await.unwrap();
        assert_eq!(got, data);
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
        let (head, _rest) = prebuffer(
            reader,
            2048,
            2048,
            Duration::from_secs(5),
            &never_cancelled(),
        )
        .await;

        assert_eq!(head.len(), 2048);
        assert_eq!(
            range_start + head.len() as u64,
            1_002_048,
            "body must resume exactly where the pre-buffer stopped"
        );
    }
}
