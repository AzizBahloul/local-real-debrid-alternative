//! The streaming response body: sends the pre-buffered head, then keeps
//! reading the torrent, re-opening the read whenever it stalls.

use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes};
use bytes::{BufMut, BytesMut};
use tokio::io::AsyncReadExt;
use tracing::warn;

use crate::torrent::{BoxedReader, ReaderCancel, ReaderTicket, StreamGuard, TorrentEngine};

/// Size of each chunk handed to the HTTP layer while streaming.
pub const STREAM_CHUNK_BYTES: usize = 64 * 1024;

/// How many times in a row a stalled stream may be re-opened before the body
/// gives up. Past this the swarm is genuinely dead, and ending the response
/// lets the player surface a real error instead of spinning forever.
const MAX_CONSECUTIVE_REOPENS: u32 = 5;

/// How often a long-running response refreshes its "still being watched"
/// timestamp, so the cache janitor's own grace period stays accurate during
/// playback that issues no further HTTP requests.
const TOUCH_INTERVAL: Duration = Duration::from_secs(20);

/// Everything the streaming body needs to keep going, and to rebuild its own
/// reader if the torrent stops feeding it.
pub struct BodyContext {
    pub engine: Arc<TorrentEngine>,
    pub info_hash: String,
    pub file_idx: usize,
    pub file_len: u64,
    pub client: IpAddr,
    /// Absolute file offset of the next byte to send.
    pub position: u64,
    /// Absolute file offset one past the last byte to send.
    pub end: u64,
    /// Already-read bytes from the pre-buffer, sent before anything else.
    pub head: Bytes,
    pub reader: BoxedReader,
    pub guard: StreamGuard,
    /// Keeps this response in the reader registry, and reports the bytes it
    /// has delivered — which is what makes it immune to being superseded.
    pub ticket: ReaderTicket,
    pub cancel: Arc<ReaderCancel>,
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
pub fn healing_body(ctx: BodyContext) -> Body {
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

    let stall_timeout = engine.tuning().stall_timeout;

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
            yield Ok::<Bytes, std::io::Error>(head);
        }

        // Filled by the reader in place and split off as a `Bytes` for each
        // yield, so a chunk reaches the socket without being copied again. The
        // spare capacity left over from a short read is reused by the next.
        let mut chunk = BytesMut::new();
        while position < end {
            let want = usize::try_from(end - position)
                .unwrap_or(usize::MAX)
                .min(STREAM_CHUNK_BYTES);
            chunk.reserve(want);

            // `None` means "produced nothing in time", which is the signal to
            // rebuild the reader rather than an error. A zero stall timeout
            // turns that recovery off and waits on the read indefinitely.
            let read = async {
                let mut limited = (&mut chunk).limit(want);
                let read = reader.read_buf(&mut limited);
                if stall_timeout.is_zero() {
                    Some(read.await)
                } else {
                    tokio::time::timeout(stall_timeout, read).await.ok()
                }
            };

            // The cancellation arm only ever fires for a response that has
            // delivered nothing at all -- a read the player seeked away from
            // while it was still waiting for its first piece. Ending it here
            // hands its share of the piece requests to the position the viewer
            // actually landed on, which is the entire point (a torrent read
            // parks until its piece arrives, so without this the abandoned
            // read keeps its claim for the full stall timeout).
            let outcome = tokio::select! {
                outcome = read => outcome,
                _ = cancel.cancelled() => {
                    closer.reason = "superseded by a newer request";
                    break;
                }
            };

            match outcome {
                Some(Ok(0)) => {
                    closer.reason = "end of file";
                    break;
                }
                Some(Ok(n)) => {
                    position += n as u64;
                    closer.position = position;
                    ticket.record_served(n as u64);
                    live_position.store(position, Ordering::Relaxed);
                    stalls = 0;
                    if last_touch.elapsed() >= TOUCH_INTERVAL {
                        engine.touch_stream(&info_hash, client);
                        last_touch = Instant::now();
                    }
                    yield Ok(chunk.split().freeze());
                }
                None => {
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
                    engine.touch_stream(&info_hash, client);
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
                Some(Err(e)) => {
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
