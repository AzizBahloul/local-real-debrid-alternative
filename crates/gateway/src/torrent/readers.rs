//! Bookkeeping for the video responses reading a torrent right now: who is
//! reading what, which reads have been overtaken by a newer one, and the
//! extra reads the engine holds open on a viewer's behalf.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use super::TorrentEngine;
use crate::util::lock;

/// librqbit's per-stream look-ahead window, from
/// `torrent_state::streaming::PER_STREAM_BUF_DEFAULT`.
///
/// Not configurable from outside the library, which is the whole reason
/// `spawn_readahead` exists: the only way to prioritise further ahead than
/// this is to hold a second read positioned there.
pub(super) const LIBRQBIT_STREAM_WINDOW: u64 = 32 * 1024 * 1024;

/// How much of an mp4's tail the warmer pulls. One piece is enough to make
/// the range request the player is about to issue land on data already here;
/// this is comfortably over the largest piece size in practice.
pub(super) const TAIL_WARM_BYTES: u64 = 24 * 1024 * 1024;

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
    /// An index/tail probe. Exempt, because it is a second read the same
    /// player genuinely needs at the same time, not a superseded seek.
    pub(super) tail_probe: bool,
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
            && !self.tail_probe
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

    fn slot(client: &str, served: u64, tail_probe: bool) -> ReaderSlot {
        ReaderSlot {
            id: 0,
            info_hash: "aaaa".to_string(),
            file_idx: 0,
            client: client.parse().unwrap(),
            tail_probe,
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
}
