//! Bookkeeping for the video responses reading a torrent right now: who is
//! reading what, which reads have been overtaken by a newer one, and the
//! extra reads the engine holds open on a viewer's behalf.

use std::collections::HashMap;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::TorrentEngine;
use crate::util::lock;

/// The most torrent reads the gateway holds open at once, across every
/// torrent and every client.
///
/// This bound exists because of a librqbit detail that fails very badly:
/// every open `FileStream` holds one permit of the session's blocking
/// semaphore for as long as it exists. Writing each downloaded piece to disk,
/// and reading each chunk it uploads, needs a permit from that same
/// semaphore. librqbit's default is 8 permits. So once 8 reads are open,
/// nothing can be written, no piece completes, and no read ever returns.
/// Every title freezes at once, and any new read waits forever for a permit.
///
/// Reproduced 2026-09-15: 12 readers of one title, from 12 addresses, froze
/// its download at 3 MB and 0 MiB/s for 40 s. It recovered only when curl
/// timeouts closed some of the readers. A household does not need 12 readers
/// to get there: each player holds its playback read, an index probe and the
/// read it is seeking away from, plus the tail warmer, so two devices can
/// already reach 8.
///
/// The session is given `SESSION_BLOCKING_PERMITS`, and the gateway never
/// opens more reads than this, so disk writes always keep permits of their
/// own. A read past the cap is refused as retryable, which leaves the
/// session running.
pub(super) const MAX_OPEN_READS: usize = 64;

/// Permits left for librqbit's own disk writes and uploads. This is its
/// default pool size, which is what it was tuned with.
const DISK_IO_PERMITS: usize = 8;

/// The size of librqbit's blocking semaphore. It is set through
/// `SessionOptions::runtime_worker_threads`, which despite its name sizes only
/// that semaphore (librqbit 9.0.1).
///
/// Tokio's default blocking-thread limit is 512, so this many concurrent
/// `block_in_place` calls still fit.
pub(super) const SESSION_BLOCKING_PERMITS: usize = MAX_OPEN_READS + DISK_IO_PERMITS;
const _: () = assert!(
    SESSION_BLOCKING_PERMITS <= 512,
    "each permit can become a block_in_place thread, and tokio stops at 512"
);

/// A read was refused because `MAX_OPEN_READS` are already open.
#[derive(Debug)]
pub struct TooManyOpenReads;

impl std::fmt::Display for TooManyOpenReads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{MAX_OPEN_READS} torrent reads are already open; refusing another \
             so the downloads they are waiting on can keep writing"
        )
    }
}

impl std::error::Error for TooManyOpenReads {}

/// A torrent read that holds one of the gateway's read slots for as long as it
/// exists. See `MAX_OPEN_READS`.
pub(super) struct SlottedRead<R> {
    pub(super) read: R,
    pub(super) _slot: OwnedSemaphorePermit,
}

impl<R: AsyncRead + Unpin> AsyncRead for SlottedRead<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.read).poll_read(cx, buf)
    }
}

/// librqbit's per-stream look-ahead window, from
/// `torrent_state::streaming::PER_STREAM_BUF_DEFAULT`.
///
/// Not configurable from outside the library, which is the whole reason
/// `spawn_readahead` exists: the only way to prioritise further ahead than
/// this is to hold a second read positioned there.
pub(super) const LIBRQBIT_STREAM_WINDOW: u64 = 32 * 1024 * 1024;

/// How much of a file's tail the warmer pulls: the last 64 KB, read to the
/// end of the file.
///
/// These are the bytes every tail read touches. mpv reads the last few KB of
/// an mkv before it plays. Stremio's server reads the last 64 KB, from the
/// same address, to hash the file for subtitle matching. An mp4's `moov` ends
/// at the end of the file. That is one piece, or two when the last piece is
/// shorter than 64 KB.
///
/// This used to be 24 MB, read once. That fetched the single piece 24 MB
/// before the end, and nobody reads that piece first. Measured 2026-09-15 on
/// a cold Stremio start of a 1.18 GB mkv: both tail reads then waited 2.9 s
/// for the last piece. They queued behind the head read's requests, which
/// librqbit pipelines 2 MB deep per peer.
pub(super) const TAIL_WARM_BYTES: u64 = 64 * 1024;

/// How long the tail warmer waits for those bytes before giving up. Generous
/// because it costs nothing to wait -- it is a background read nobody is
/// watching, and once its pieces arrive it stops asking for anything.
pub(super) const TAIL_WARM_TIMEOUT: Duration = Duration::from_secs(120);

/// Containers whose players read the tail of the file before they will play
/// the head.
///
/// For mp4/m4v/mov this is the `moov` atom, and it is a certainty on a
/// non-faststart file. mkv/webm are here for a measured reason rather than a
/// structural one: Matroska puts its SeekHead at the front, so the theory says
/// it needs none of this — but muxers routinely write the Cues at the end, and
/// players probe for them anyway. On 2026-09-07 a 1.16 GB mkv start spent
/// **12.2 s of its 20 s** on exactly one such request (`bytes=1162008830-`,
/// the last 21 KB), serialised after the head read instead of beside it. The
/// warmer costs one piece when it guesses wrong, and that trade is not close.
const TAIL_INDEXED_EXTENSIONS: [&str; 5] = ["mp4", "m4v", "mov", "mkv", "webm"];

/// How often the read-ahead claim is re-pointed at the current position.
pub(super) const READAHEAD_REFRESH: Duration = Duration::from_secs(5);

/// How far the playback position must move before the read-ahead claim is
/// re-opened. Re-opening is a real API call, so it is not done per refresh.
pub(super) const READAHEAD_ADVANCE_BYTES: u64 = 8 * 1024 * 1024;

/// Marks a torrent as "being read right now", for exactly as long as the HTTP
/// response body that owns it is alive.
///
/// The idle reaper must never pause a torrent that has a live reader.
/// librqbit's reader parks on `Poll::Pending` waiting for one specific piece
/// and is only ever woken by *that piece completing*; pausing does not wake
/// it, and there is no timeout. So pausing under a live reader freezes it
/// permanently -- the video stops dead and only a fresh HTTP request (what a
/// viewer does by hand when they skip forward) recovers it.
///
/// Request recency cannot stand in for this. A player fills its buffer, then
/// reads nothing for minutes while it plays what it already holds, which is
/// indistinguishable from an abandoned torrent by that measure.
pub struct StreamGuard {
    pub(super) engine: Arc<TorrentEngine>,
    pub(super) info_hash: String,
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        lock(&self.engine.open_streams).release(&self.info_hash);
    }
}

/// How many response bodies are reading each torrent right now.
///
/// Reference-counted rather than a flag because one player legitimately holds
/// several reads at once -- a container header, an index at the end of the
/// file, and the playback position -- and the torrent must stay protected
/// until the last of them is done, not the first.
#[derive(Default)]
pub(super) struct OpenStreamCounts(pub(super) HashMap<String, usize>);

impl OpenStreamCounts {
    pub(super) fn acquire(&mut self, info_hash: &str) {
        match self.0.get_mut(info_hash) {
            Some(count) => *count += 1,
            None => {
                self.0.insert(info_hash.to_string(), 1);
            }
        }
    }

    pub(super) fn release(&mut self, info_hash: &str) {
        if let Some(count) = self.0.get_mut(info_hash) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                // Removed rather than left at zero so the map tracks live
                // readers only, and cannot grow for the life of the process.
                self.0.remove(info_hash);
            }
        }
    }

    pub(super) fn is_open(&self, info_hash: &str) -> bool {
        self.0.get(info_hash).is_some_and(|count| *count > 0)
    }

    /// How many distinct torrents have a live reader. Torrents rather than
    /// readers because one player routinely holds several reads on the same
    /// file at once (a container header, an index near the end, the playback
    /// position), and reporting "3 active streams" for one viewer watching one
    /// episode describes the implementation rather than what is happening.
    pub(super) fn torrents_open(&self) -> usize {
        self.0.values().filter(|count| **count > 0).count()
    }
}

/// A one-way "stop what you are doing" signal for a single response.
///
/// Built on a closed semaphore rather than a flag plus a `Notify` because the
/// obvious version of that has a race: check the flag, find it clear, await
/// the notification, and miss the one that fired in between. A semaphore with
/// no permits can only ever resolve by being closed, and closing is
/// idempotent, observable, and cannot be missed by a late waiter.
pub struct ReaderCancel(Semaphore);

impl ReaderCancel {
    pub(super) fn new() -> Self {
        Self(Semaphore::new(0))
    }

    /// A signal that will never fire, for callers with nothing to supersede
    /// them (tests, and any future non-HTTP reader).
    pub fn never_cancelled() -> Self {
        Self::new()
    }

    /// Fires the signal by hand. Only for tests -- in the gateway itself the
    /// decision to supersede belongs to `register_reader`, which is the one
    /// place that can see every reader and apply the same rules to all of them.
    #[cfg(test)]
    pub fn cancel_for_test(&self) {
        self.cancel();
    }

    pub(super) fn cancel(&self) {
        self.0.close();
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.is_closed()
    }

    /// Resolves when, and only when, this reader has been superseded.
    pub async fn cancelled(&self) {
        let _ = self.0.acquire().await;
    }
}

/// One in-flight video response, as far as deciding who still deserves piece
/// priority is concerned.
pub(super) struct ReaderSlot {
    pub(super) id: u64,
    pub(super) info_hash: String,
    pub(super) file_idx: usize,
    /// Superseding is scoped to one client, so a second device starting the
    /// same title cannot cancel the first device's cold start (and be
    /// cancelled in turn by its retry, forever).
    pub(super) client: IpAddr,
    /// An index probe, or any other range with an end short of the file's.
    /// Exempt, because the same client runs these alongside its playback
    /// read, not instead of it. See `register_reader`.
    pub(super) side_read: bool,
    pub(super) served: Arc<AtomicU64>,
    pub(super) cancel: Arc<ReaderCancel>,
}

impl ReaderSlot {
    /// Whether a new read of `(info_hash, file_idx)` from `client` makes this
    /// one redundant.
    ///
    /// Every clause is load-bearing; see `register_reader` for why each one is
    /// there and what breaks without it.
    pub(super) fn superseded_by(&self, info_hash: &str, file_idx: usize, client: IpAddr) -> bool {
        self.info_hash == info_hash
            && self.file_idx == file_idx
            && self.client == client
            && !self.side_read
            && self.served.load(Ordering::Relaxed) == 0
    }
}

/// Registration of a live video response. Held for as long as the response is,
/// and removed from the registry when dropped.
pub struct ReaderTicket {
    pub(super) engine: Arc<TorrentEngine>,
    pub(super) id: u64,
    pub(super) served: Arc<AtomicU64>,
    pub(super) cancel: Arc<ReaderCancel>,
}

impl ReaderTicket {
    /// The signal to stop. Selected on by the pre-buffer and by the response
    /// body, which are the two places a superseded read would otherwise sit
    /// holding piece priority it is never going to use.
    pub fn cancel_handle(&self) -> Arc<ReaderCancel> {
        Arc::clone(&self.cancel)
    }

    /// Reports progress. A reader that has delivered even one byte is someone
    /// watching something and is never superseded.
    pub fn record_served(&self, bytes: u64) {
        self.served.fetch_add(bytes, Ordering::Relaxed);
    }
}

impl Drop for ReaderTicket {
    fn drop(&mut self) {
        lock(&self.engine.readers).retain(|slot| slot.id != self.id);
    }
}

/// Keeps an extra read-ahead claim alive for the life of one response.
///
/// Dropping it aborts the task, which drops the read it was holding, which is
/// what removes the claim from librqbit's priority set. There is no other
/// unregister step — the claim *is* the open read.
pub struct ReadaheadHandle(pub(super) tokio::task::JoinHandle<()>);

impl Drop for ReadaheadHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Whether this filename names a container whose player is likely to read the
/// tail before it will play the head — see `TAIL_INDEXED_EXTENSIONS` for why
/// that is a claim about observed player behaviour, not about the format.
pub(super) fn has_tail_index(name: &str) -> bool {
    name.rsplit('.').next().is_some_and(|ext| {
        TAIL_INDEXED_EXTENSIONS
            .iter()
            .any(|known| ext.eq_ignore_ascii_case(known))
    })
}

/// Where a byte offset sits relative to the piece that contains it.
///
/// The piece is the unit BitTorrent actually transfers, so this is the shape
/// of the floor under every seek: nothing at `offset` can be read until the
/// whole piece holding it has arrived and passed its hash check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PieceGeometry {
    pub piece_len: u64,
    /// Bytes from the requested offset to the end of its piece, clamped to the
    /// end of the file.
    pub remainder: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot(client: &str, served: u64, side_read: bool) -> ReaderSlot {
        ReaderSlot {
            id: 0,
            info_hash: "aaaa".to_string(),
            file_idx: 0,
            client: client.parse().unwrap(),
            side_read,
            served: Arc::new(AtomicU64::new(served)),
            cancel: Arc::new(ReaderCancel::new()),
        }
    }

    fn ip(value: &str) -> IpAddr {
        value.parse().unwrap()
    }

    /// A scrub burst is the case this exists for: each abandoned read keeps
    /// its share of librqbit's round-robin piece requests, so ten of them
    /// leave the position the viewer landed on getting a tenth of the
    /// download.
    #[test]
    fn a_read_that_showed_nobody_anything_is_superseded() {
        assert!(slot("192.168.1.5", 0, false).superseded_by("aaaa", 0, ip("192.168.1.5")));
    }

    /// The one rule that must never be got wrong: a read that has delivered
    /// bytes is a picture on someone's screen, and cancelling it stops
    /// playback dead.
    #[test]
    fn a_read_that_is_playing_is_never_superseded() {
        assert!(!slot("192.168.1.5", 1, false).superseded_by("aaaa", 0, ip("192.168.1.5")));
    }

    /// Two devices on the same title would otherwise cancel each other's cold
    /// start, and then each other's retry, and neither would ever play.
    #[test]
    fn a_second_device_does_not_supersede_the_first() {
        assert!(!slot("192.168.1.5", 0, false).superseded_by("aaaa", 0, ip("192.168.1.9")));
    }

    /// An mp4 index read is a second read the same player needs *at the same
    /// time* as the one at the head, not a superseded seek. Cancelling it
    /// breaks exactly the players the tail warmer exists to help.
    #[test]
    fn an_index_probe_survives_the_playback_request_that_follows_it() {
        assert!(!slot("192.168.1.5", 0, true).superseded_by("aaaa", 0, ip("192.168.1.5")));
    }

    /// Stremio's server hashes the first and last 64 KB of the file from the
    /// player's own address while the player starts. If a seek cancelled that
    /// read, subtitle matching would fail for no reason.
    #[test]
    fn a_bounded_read_survives_the_playback_request_that_follows_it() {
        let hash_read = slot("192.168.1.128", 0, true);
        assert!(!hash_read.superseded_by("aaaa", 0, ip("192.168.1.128")));
    }

    /// One player legitimately reads two files of a multi-file torrent (an
    /// episode and its subtitles), and neither is a seek away from the other.
    #[test]
    fn a_read_of_a_different_file_is_left_alone() {
        assert!(!slot("192.168.1.5", 0, false).superseded_by("aaaa", 1, ip("192.168.1.5")));
        assert!(!slot("192.168.1.5", 0, false).superseded_by("bbbb", 0, ip("192.168.1.5")));
    }

    /// mkv is in the list on evidence, not on theory: Matroska's SeekHead is at
    /// the front, and players probe the tail for the Cues regardless. Leaving it
    /// out cost a measured 12.2 s on one start — see `TAIL_INDEXED_EXTENSIONS`.
    /// Pinned as a test because the structural argument for excluding it is
    /// genuinely persuasive and someone will make it again.
    #[test]
    fn only_tail_indexed_containers_are_warmed() {
        assert!(has_tail_index("The.Movie.2024.1080p.mp4"));
        assert!(
            has_tail_index("clip.MP4"),
            "extensions are not case-sensitive"
        );
        assert!(has_tail_index("holiday.mov"));
        assert!(has_tail_index("tires.s03e08.1080p.web.h264-cakes.mkv"));
        assert!(has_tail_index("stream.webm"));
        assert!(!has_tail_index("no-extension"));
        assert!(!has_tail_index("subtitles.srt"));
    }

    #[test]
    fn open_stream_counts_track_a_single_reader() {
        let mut counts = OpenStreamCounts::default();
        assert!(!counts.is_open("aaaa"));

        counts.acquire("aaaa");
        assert!(
            counts.is_open("aaaa"),
            "a live reader must protect its torrent"
        );

        counts.release("aaaa");
        assert!(!counts.is_open("aaaa"));
        assert!(counts.0.is_empty(), "released entries must not linger");
    }

    #[test]
    fn open_stream_counts_protect_until_the_last_reader_finishes() {
        // A player opens several reads at once (header, index, playback
        // position). Releasing one must not expose the torrent to the reaper
        // while the others are still going -- that is the freeze this whole
        // mechanism exists to prevent.
        let mut counts = OpenStreamCounts::default();
        counts.acquire("aaaa");
        counts.acquire("aaaa");
        counts.acquire("aaaa");

        counts.release("aaaa");
        assert!(counts.is_open("aaaa"));
        counts.release("aaaa");
        assert!(counts.is_open("aaaa"));

        counts.release("aaaa");
        assert!(!counts.is_open("aaaa"));
    }

    #[test]
    fn open_stream_counts_ignore_unbalanced_releases() {
        // Must not underflow into a huge count, which would pin a torrent
        // as "in use" forever and defeat both the reaper and the janitor.
        let mut counts = OpenStreamCounts::default();
        counts.release("never-acquired");
        assert!(!counts.is_open("never-acquired"));

        counts.acquire("aaaa");
        counts.release("aaaa");
        counts.release("aaaa");
        assert!(!counts.is_open("aaaa"));
    }

    #[test]
    fn open_stream_counts_keep_torrents_independent() {
        let mut counts = OpenStreamCounts::default();
        counts.acquire("aaaa");
        counts.acquire("bbbb");
        counts.release("aaaa");

        assert!(!counts.is_open("aaaa"));
        assert!(
            counts.is_open("bbbb"),
            "one torrent's reader must not release another's"
        );
        assert_eq!(counts.torrents_open(), 1);
    }

    /// The slot has to go when the read does. If it leaked, the cap would
    /// fill up over a day of seeking and then refuse every read.
    #[tokio::test]
    async fn a_read_gives_its_slot_back_when_dropped() {
        use tokio::io::AsyncReadExt;

        let slots = Arc::new(Semaphore::new(1));
        let mut read = SlottedRead {
            read: &b"abc"[..],
            _slot: Arc::clone(&slots).try_acquire_owned().unwrap(),
        };
        assert!(Arc::clone(&slots).try_acquire_owned().is_err());

        let mut out = String::new();
        read.read_to_string(&mut out).await.unwrap();
        assert_eq!(out, "abc");

        drop(read);
        assert!(Arc::clone(&slots).try_acquire_owned().is_ok());
    }
}
