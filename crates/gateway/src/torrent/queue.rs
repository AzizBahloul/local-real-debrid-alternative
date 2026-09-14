//! Which torrents run: the download queue, the idle reaper, hand pauses, and
//! what happens to a title the viewer walks away from.
//!
//! Every pass here starts from one `QueueEntry` snapshot of the session, taken
//! with no engine lock held, and makes its decisions from that. See the lock
//! notes on `TorrentEngine`.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use librqbit::api::TorrentIdOrHash;
use tracing::{debug, info, warn};

use super::{Progress, TorrentEngine, ABANDON_GRACE};
use crate::util::lock;

/// What to do with a torrent that is not the one being watched.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum UnfocusedAction {
    /// Leave it exactly as it is.
    Leave,
    /// Stop downloading and delete the partial file.
    Discard,
}

/// What should happen to the title the viewer just switched away from.
///
/// Starting a different title means that one has been abandoned — nobody
/// changes episode intending to come back to a half-downloaded file — so its
/// partial data is dead weight in a cache that sits permanently at its cap.
/// It is deleted rather than paused, which is the only thing here that
/// actually frees space.
///
/// This applies to *the previous focus only*, never to the whole session. An
/// earlier version swept every unfinished torrent on each switch, which wiped
/// eight queued titles the first time anything was played — the viewer's
/// backlog is not the same thing as the title they just left.
///
/// Three exemptions, and they are not stylistic:
///
/// * an **open stream** means some response body is still reading those bytes
///   (a second device, or this player's own header/index reads). Deleting
///   underneath it truncates a video someone is watching — see `StreamGuard`.
/// * **recent activity** means someone was reading it moments ago and has not
///   had time to come back. An open connection is not the same question as
///   "is anyone watching this": range requests are stateless, so there is no
///   connection at all between a seek and the next read, and none while a
///   player that just timed out prepares its retry. `StreamActivity` says this
///   outright, and the idle reaper already honours it — this path did not, so
///   the *irreversible* action was judging liveness more loosely than the
///   reversible one. That is what made a failed play destructive: a player
///   giving up on a cold torrent closed its connection, the next stream it
///   tried took focus, and the torrent it had just spent 15 seconds warming
///   was deleted along with every byte and every peer it had found. Each
///   attempt therefore started colder than the last, which is what a viewer
///   sees as the player cycling through every source and playing none.
/// * a **finished** torrent is a complete file. It costs no download
///   bandwidth, and throwing away a fully-downloaded movie because the viewer
///   started the next episode is destructive in a way nobody asks for. The
///   cache janitor already reclaims those, oldest-first, once the cap forces
///   it.
pub(super) fn action_for_abandoned(
    has_open_stream: bool,
    recently_active: bool,
    finished: bool,
) -> UnfocusedAction {
    if has_open_stream || recently_active || finished {
        return UnfocusedAction::Leave;
    }
    UnfocusedAction::Discard
}

/// Picks the torrents allowed to download, given the unfinished ones in
/// admission order (oldest first). Pure, so the priority rules can be tested
/// without a session; see [`TorrentEngine::download_slots`] for what each rule
/// is for.
///
/// The cap is a target, not a ceiling: readers and the focused title are
/// admitted first and can push the set past it. Both are cases where pausing
/// the torrent is either impossible without freezing a live reader or
/// obviously wrong, so reporting a smaller set would be a lie about what is
/// using the line rather than a restriction on it.
pub(super) fn choose_download_slots<'a>(
    unfinished: &[&'a str],
    focused: Option<&str>,
    has_open_stream: impl Fn(&str) -> bool,
    cap: usize,
) -> HashSet<&'a str> {
    let mut slots: HashSet<&'a str> = HashSet::new();

    for hash in unfinished {
        if has_open_stream(hash) {
            slots.insert(hash);
        }
    }
    if let Some(focused) = focused {
        // Only if it is genuinely unfinished. A finished focus needs no slot,
        // and holding one for it would shrink the queue by one for as long as
        // the viewer stayed on that episode.
        if let Some(hash) = unfinished.iter().find(|hash| **hash == focused) {
            slots.insert(hash);
        }
    }
    for hash in unfinished {
        if slots.len() >= cap {
            break;
        }
        slots.insert(hash);
    }
    slots
}

/// One torrent as the queue sees it, captured in a single pass over the
/// session.
pub(super) struct QueueEntry {
    /// librqbit's incrementing id, i.e. admission order.
    id: usize,
    idx: TorrentIdOrHash,
    hash: String,
    paused: bool,
    finished: bool,
}

impl TorrentEngine {
    /// Every torrent in the session, in admission order.
    ///
    /// One pass, and nothing but librqbit calls inside it: the session holds
    /// its own lock for the length of the closure, so taking an engine lock
    /// in there is how two threads end up waiting on each other.
    pub(super) fn queue_snapshot(&self) -> Vec<QueueEntry> {
        let mut entries: Vec<QueueEntry> = self.api.session().with_torrents(|iter| {
            iter.map(|(id, handle)| {
                let stats = handle.stats();
                QueueEntry {
                    id,
                    idx: handle.info_hash().into(),
                    hash: handle.info_hash().as_string(),
                    paused: handle.is_paused(),
                    finished: Progress::of(stats.progress_bytes, stats.total_bytes).finished,
                }
            })
            .collect()
        });
        entries.sort_by_key(|entry| entry.id);
        entries
    }

    /// The hand-paused hashes, copied out so no lock is held while they are
    /// consulted.
    pub(super) fn held_snapshot(&self) -> HashSet<String> {
        lock(&self.held).clone()
    }

    /// Which torrents are allowed to download right now, in admission order.
    ///
    /// The gateway downloads up to `max_active_downloads` titles at a time
    /// rather than only the one on screen. That is what lets the next
    /// episodes of a series arrive while the current one is being watched,
    /// instead of each one paying a full cold start when it is tapped.
    ///
    /// Membership, in priority order:
    ///
    /// * **anything with a live reader.** Non-negotiable rather than a
    ///   preference: a reader parked inside librqbit is woken only by the
    ///   piece it waits for, so pausing its torrent freezes it permanently
    ///   with no timeout (see `StreamGuard`). These hold a slot whether the
    ///   cap likes it or not, which is also the honest accounting -- they are
    ///   using the line either way.
    /// * **the focused title**, so the video on screen never queues behind
    ///   the backlog.
    /// * **the oldest unfinished torrents**, until the cap is reached.
    ///
    /// Ordering is by librqbit's `TorrentId`, which is assigned incrementally
    /// as torrents are added -- so ascending id *is* the order they were
    /// asked for, with no second bookkeeping to drift out of sync, and a
    /// session restored from disk rebuilds it in the same order. That is what
    /// makes the queue run front-to-back: four episodes finish one after
    /// another rather than all four crawling at a quarter of the speed and
    /// none of them becoming watchable.
    ///
    /// Finished torrents are never members. They cost no download bandwidth,
    /// so a slot spent on one is a slot the queue cannot use. Nor is a
    /// torrent the operator paused by hand: it is not a candidate at all,
    /// rather than a candidate the queue keeps resuming, which also hands its
    /// slot to the next title in line -- what someone pausing a download
    /// wants: the bandwidth goes somewhere, not nowhere.
    pub(super) fn download_slots<'a>(
        &self,
        entries: &'a [QueueEntry],
        held: &HashSet<String>,
    ) -> HashSet<&'a str> {
        let focused = lock(&self.focused).clone();
        let unfinished: Vec<&str> = entries
            .iter()
            .filter(|entry| !entry.finished && !held.contains(&entry.hash))
            .map(|entry| entry.hash.as_str())
            .collect();
        choose_download_slots(
            &unfinished,
            focused.as_deref(),
            |hash| self.has_open_stream(hash),
            self.max_active_downloads,
        )
    }

    /// Declares `info_hash` the thing being watched, discarding what was
    /// being watched before it.
    ///
    /// Called on every stream open, but only does work when the focus
    /// actually moves — a seek re-opens the reader on the same torrent dozens
    /// of times and must not re-sweep each time.
    ///
    /// The sweep runs detached: it makes one API call per torrent and the
    /// viewer's first byte should not wait on any of them.
    pub fn focus_stream(self: &Arc<Self>, info_hash: &str) {
        let previous = {
            let mut focused = lock(&self.focused);
            if focused.as_deref() == Some(info_hash) {
                return;
            }
            focused.replace(info_hash.to_string())
        };

        let engine = Arc::clone(self);
        let focus = info_hash.to_string();
        tokio::spawn(async move {
            // Nothing was playing before means nothing has been abandoned --
            // this is what keeps the first play of a session from touching
            // the backlog. Candidates prefetched for this browse still get
            // parked below, since those were never watched at all.
            if let Some(previous) = previous {
                if engine.discard_abandoned(&previous).await {
                    info!(
                        focused = %focus,
                        abandoned = %previous,
                        "switched title: dropped the one left behind and freed its partial data"
                    );
                }
            }
            // The new focus takes a download slot, and the queue is rebuilt
            // around it: whatever no longer fits is parked, and anything that
            // now does is resumed.
            let (started, parked) = engine.enforce_download_slots().await;
            if started > 0 || parked > 0 {
                debug!(
                    focused = %focus,
                    started,
                    parked,
                    "rebuilt the download queue around the stream being watched"
                );
            }
        });
    }

    /// Deletes the abandoned torrent and its partial data. Returns whether it
    /// actually went.
    ///
    /// Uses librqbit's delete (torrent *and* files) rather than forget: a
    /// forgotten torrent leaves its partial file on disk with nothing
    /// tracking it, which is exactly the orphaned bulk the cache cap is
    /// already fighting.
    pub async fn discard_abandoned(&self, info_hash: &str) -> bool {
        let Some((idx, progress)) = self.session_progress(info_hash) else {
            return false;
        };
        if action_for_abandoned(
            self.has_open_stream(info_hash),
            self.is_recently_active(info_hash, ABANDON_GRACE),
            progress.finished,
        ) != UnfocusedAction::Discard
        {
            return false;
        }

        match self.api.api_torrent_action_delete(idx).await {
            Ok(_) => {
                info!(info_hash = %info_hash, "discarded abandoned torrent and its partial data");
                // Deleting a viewer's partial download is the most destructive
                // thing this process does on its own initiative, and it was
                // invisible until it was caught in the act. It gets a line.
                self.record_discard(info_hash, progress);
                true
            }
            // Racing a state change here is normal and harmless.
            Err(e) => {
                debug!(info_hash = %info_hash, "could not discard abandoned torrent: {e}");
                false
            }
        }
    }

    /// Deletes one torrent and its data because cache retention decided it is
    /// too old to keep. `Ok(true)` means deleted, `Ok(false)` means the
    /// session never heard of the hash (an orphaned directory the caller has
    /// to reclaim itself), and `Err` means the session owns it but the delete
    /// failed -- the caller must then leave the files alone rather than pull
    /// them out from under a live handle.
    ///
    /// Unlike [`discard_abandoned`](Self::discard_abandoned) this spares
    /// nothing for being finished: retention exists precisely to throw
    /// finished backlog away. The caller owns the "is anyone reading this"
    /// checks.
    pub async fn discard_cached(&self, info_hash: &str) -> Result<bool> {
        let Some((idx, progress)) = self.session_progress(info_hash) else {
            return Ok(false);
        };
        self.api
            .api_torrent_action_delete(idx)
            .await
            .map_err(|e| anyhow::anyhow!("retention could not delete torrent: {e}"))?;
        self.record_discard(info_hash, progress);
        Ok(true)
    }

    /// The bookkeeping every deletion shares: the audit line, and forgetting
    /// what the engine knew about a torrent that no longer exists.
    fn record_discard(&self, info_hash: &str, progress: Progress) {
        self.audit().record(crate::audit::Event::TorrentDiscarded {
            info_hash: info_hash.to_string(),
            progress_percent: progress.percent,
        });
        self.forget_activity(info_hash);
    }

    /// Deletes every torrent and all of its data. Returns how many went.
    ///
    /// Unlike the automatic paths this spares nothing — not a finished
    /// download, not one being streamed right now. It only ever runs from an
    /// explicit "clear everything" click, where second-guessing the operator
    /// would be the surprising behaviour. A stream in flight dies with it,
    /// which is the honest consequence of wiping the file underneath it.
    pub async fn discard_everything(&self) -> usize {
        let mut deleted = 0;
        for entry in self.queue_snapshot() {
            match self.api.api_torrent_action_delete(entry.idx).await {
                Ok(_) => {
                    self.forget_activity(&entry.hash);
                    deleted += 1;
                }
                Err(e) => warn!(info_hash = %entry.hash, "could not delete torrent: {e}"),
            }
        }

        // Nothing is playing any more, so the next stream is a first play and
        // must not be treated as a switch away from a torrent that is gone.
        *lock(&self.focused) = None;
        // A clear is a fresh start, and a hash that failed its search a moment
        // ago deserves a fresh search rather than the rest of its cooldown.
        // Metadata is kept on purpose: it is what makes replaying any of
        // these titles instant, and it is not cache data in the sense the
        // operator asked to clear.
        lock(&self.failed_starts).clear();
        lock(&self.tail_warmed).clear();

        info!(deleted, "cleared every torrent and its data on request");
        deleted
    }

    /// Whether the operator paused this torrent by hand.
    ///
    /// Distinct from librqbit's own paused flag, which the download queue and
    /// the idle reaper both set and clear on their own schedule. A hand pause
    /// has to outlive those: without a separate record the queue's next tick
    /// (a second later) simply resumes it, and the button looks broken.
    pub fn is_held(&self, info_hash: &str) -> bool {
        lock(&self.held).contains(info_hash)
    }

    /// Pauses one torrent and holds it paused until something explicitly says
    /// otherwise -- a resume, a delete, or the viewer playing it again.
    pub async fn hold_paused(&self, info_hash: &str) -> Result<bool> {
        let Some(idx) = self.session_idx(info_hash) else {
            return Ok(false);
        };
        lock(&self.held).insert(info_hash.to_string());
        // Errors here are almost always "already paused", which is the state
        // being asked for, so the hold above stands either way.
        if let Err(e) = self.api.api_torrent_action_pause(idx).await {
            debug!(info_hash = %info_hash, "pause returned an error, holding anyway: {e}");
        }
        info!(info_hash = %info_hash, "paused by hand");
        Ok(true)
    }

    /// Releases a hand pause and lets the download queue have the torrent
    /// back. Whether it actually runs is then the queue's decision, exactly as
    /// it is for every other torrent -- so resuming a fifth title while four
    /// are already downloading queues it rather than oversubscribing the link.
    pub async fn release_hold(&self, info_hash: &str) -> Result<bool> {
        let removed = lock(&self.held).remove(info_hash);
        if self.session_idx(info_hash).is_none() {
            return Ok(false);
        }
        self.enforce_download_slots().await;
        if removed {
            info!(info_hash = %info_hash, "resumed by hand");
        }
        Ok(true)
    }

    /// Forgets any hand pause for this hash. Called wherever the torrent stops
    /// existing, or where playing it makes the hold moot.
    pub(super) fn forget_hold(&self, info_hash: &str) {
        lock(&self.held).remove(info_hash);
    }

    /// Brings the session in line with `download_slots`: resumes every
    /// unfinished torrent inside the set, pauses every one outside it.
    /// Returns `(started, paused)`.
    ///
    /// Run both on a focus change and on a timer, because the two things that
    /// move the set are the viewer picking a different title and a download
    /// finishing -- and only the first of those is an event this process sees.
    /// Without the timer a torrent that completed would keep its slot until
    /// the next time somebody changed episode.
    pub async fn enforce_download_slots(&self) -> (usize, usize) {
        let entries = self.queue_snapshot();
        let held = self.held_snapshot();
        let slots = self.download_slots(&entries, &held);

        let (mut started, mut paused) = (0, 0);
        // Finished torrents are skipped: no download bandwidth being consumed,
        // and pausing one would only stop us seeding it back. A hand pause
        // outranks the queue in both directions: never resumed here, and
        // already paused, so there is nothing to do.
        for entry in entries
            .iter()
            .filter(|entry| !entry.finished && !held.contains(&entry.hash))
        {
            match (slots.contains(entry.hash.as_str()), entry.paused) {
                (true, true) => {
                    if self.api.api_torrent_action_start(entry.idx).await.is_ok() {
                        debug!(info_hash = %entry.hash, "download queue: took a free slot");
                        started += 1;
                    }
                }
                (false, false) => {
                    // Belt and braces: a torrent with a live reader is always
                    // in the set above, so this should never fire -- but
                    // pausing one freezes it permanently, and the cost of
                    // checking twice is a map lookup.
                    if self.has_open_stream(&entry.hash) {
                        continue;
                    }
                    if self.api.api_torrent_action_pause(entry.idx).await.is_ok() {
                        debug!(info_hash = %entry.hash, "download queue: parked until a slot frees up");
                        paused += 1;
                    }
                }
                _ => {}
            }
        }
        (started, paused)
    }

    /// Runs `enforce_download_slots` on a timer for the lifetime of the
    /// process, so a finished download hands its slot to the next in line.
    pub fn spawn_download_queue(self: &Arc<Self>, interval: Duration) {
        let engine = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // The first tick fires immediately; skip it so a torrent started
            // moments before startup is not parked before anyone can play it.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let (started, paused) = engine.enforce_download_slots().await;
                if started > 0 || paused > 0 {
                    debug!(started, paused, "download queue advanced");
                }
            }
        });
    }

    /// Pauses torrents nobody has streamed from in `idle_after`.
    ///
    /// Every running torrent competes for the same finite upstream bandwidth.
    /// A 23 GB 4K release someone opened once and abandoned will happily eat
    /// most of it, starving the movie actually being watched -- which shows up
    /// as buffering that looks like a network problem but is really
    /// self-inflicted. Pausing idle torrents hands that bandwidth back.
    ///
    /// Paused torrents keep their data and resume instantly on the next
    /// request (see `ensure_started`), so this is invisible in normal use.
    /// Completed torrents are left alone: they cost no download bandwidth and
    /// seeding them back is good manners.
    pub async fn pause_idle_torrents(&self, idle_after: Duration) -> usize {
        let entries = self.queue_snapshot();
        let held = self.held_snapshot();
        // Holding a download slot is itself a reason to keep running: the
        // queue exists precisely to fetch titles nobody is watching yet, so
        // judging those by request recency would pause every one of them a
        // half-hour in and the queue would never finish anything.
        let slots = self.download_slots(&entries, &held);
        // One look at the activity map for the whole pass, not one per torrent.
        let recently_active = self.recently_active(idle_after);

        let mut paused = 0;
        for entry in entries
            .iter()
            .filter(|entry| !entry.paused && !entry.finished)
        {
            if slots.contains(entry.hash.as_str()) {
                continue;
            }
            // Two independent checks, because request recency alone is not
            // enough. A live reader is parked inside librqbit waiting for a
            // piece and issues no HTTP requests at all; pausing it there
            // freezes it permanently with no timeout. See `StreamGuard`.
            if self.has_open_stream(&entry.hash) || recently_active.contains(&entry.hash) {
                continue;
            }
            match self.api.api_torrent_action_pause(entry.idx).await {
                Ok(_) => {
                    info!(info_hash = %entry.hash, "paused idle torrent to free bandwidth");
                    paused += 1;
                }
                // Racing a state change here is normal and harmless.
                Err(e) => debug!(info_hash = %entry.hash, "could not pause idle torrent: {e}"),
            }
        }
        paused
    }

    /// Runs `pause_idle_torrents` on a timer for the lifetime of the process.
    pub fn spawn_idle_reaper(self: &Arc<Self>, interval: Duration, idle_after: Duration) {
        let engine = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // The first tick fires immediately; skip it so a torrent started
            // moments before startup is not paused before anyone can play it.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                engine.pause_idle_torrents(idle_after).await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nothing_open(_: &str) -> bool {
        false
    }

    /// The feature: four titles download at once instead of one.
    #[test]
    fn the_queue_runs_four_titles_at_a_time() {
        let unfinished = ["e1", "e2", "e3", "e4", "e5", "e6"];
        let slots = choose_download_slots(&unfinished, Some("e1"), nothing_open, 4);
        assert_eq!(slots.len(), 4);
    }

    /// "In order" is the half that makes it useful. Taking an arbitrary four
    /// would leave every episode part-downloaded and none of them watchable;
    /// taking the oldest four finishes them front-to-back.
    #[test]
    fn the_queue_admits_in_the_order_titles_were_asked_for() {
        let unfinished = ["e1", "e2", "e3", "e4", "e5", "e6"];
        let slots = choose_download_slots(&unfinished, None, nothing_open, 3);
        assert!(slots.contains("e1") && slots.contains("e2") && slots.contains("e3"));
        assert!(
            !slots.contains("e4") && !slots.contains("e5") && !slots.contains("e6"),
            "later requests wait for a slot rather than diluting the first three"
        );
    }

    /// A viewer who skips ahead to the last episode must not queue behind the
    /// backlog that was already downloading.
    #[test]
    fn the_title_on_screen_always_holds_a_slot() {
        let unfinished = ["e1", "e2", "e3", "e4", "e5", "e6"];
        let slots = choose_download_slots(&unfinished, Some("e6"), nothing_open, 2);
        assert!(slots.contains("e6"), "the focused title is never queued");
        assert!(slots.contains("e1"), "and the queue still runs behind it");
    }

    /// Pausing a torrent whose reader is parked inside librqbit freezes it
    /// permanently -- the piece it waits for never arrives to wake it. So a
    /// read in progress is admitted even past the cap; the alternative is not
    /// "a smaller set", it is a frozen video. See `StreamGuard`.
    #[test]
    fn a_torrent_being_read_is_admitted_even_past_the_cap() {
        let unfinished = ["e1", "e2", "e3"];
        let slots = choose_download_slots(&unfinished, Some("e1"), |hash| hash == "e3", 1);
        assert!(slots.contains("e3"), "a live reader cannot be parked");
        assert!(slots.contains("e1"), "nor can the title on screen");
    }

    /// A finished torrent is not in the input at all, so a viewer re-watching
    /// something already downloaded does not spend a slot on it.
    #[test]
    fn a_finished_focus_does_not_hold_a_slot_open() {
        let unfinished = ["e2", "e3"];
        let slots = choose_download_slots(&unfinished, Some("e1-finished"), nothing_open, 2);
        assert_eq!(slots.len(), 2);
        assert!(slots.contains("e2") && slots.contains("e3"));
        assert!(!slots.contains("e1-finished"));
    }

    /// Setting the cap to 1 must reproduce the old behaviour exactly: only
    /// the title on screen downloads. That is the escape hatch for a link
    /// that is only just keeping up with playback.
    #[test]
    fn a_cap_of_one_downloads_only_what_is_being_watched() {
        let unfinished = ["e1", "e2", "e3"];
        let slots = choose_download_slots(&unfinished, Some("e2"), nothing_open, 1);
        assert_eq!(slots.len(), 1);
        assert!(slots.contains("e2"));
    }

    #[test]
    fn an_empty_session_needs_no_slots() {
        assert!(choose_download_slots(&[], None, nothing_open, 4).is_empty());
        assert!(choose_download_slots(&[], Some("gone"), nothing_open, 4).is_empty());
    }

    #[test]
    fn switching_title_discards_the_one_the_viewer_left() {
        // The whole point: starting a new episode abandons the old one, so
        // its half-downloaded file is deleted rather than kept forever in a
        // cache that is permanently at its cap.
        assert_eq!(
            action_for_abandoned(false, false, false),
            UnfocusedAction::Discard
        );
    }

    #[test]
    fn a_torrent_another_reader_still_holds_is_never_deleted() {
        // A second device watching something else, or this player's own
        // header/index reads. Deleting underneath a live reader truncates a
        // video someone is watching -- see `StreamGuard`.
        assert_eq!(
            action_for_abandoned(true, false, false),
            UnfocusedAction::Leave
        );
    }

    #[test]
    fn a_torrent_read_moments_ago_survives_the_switch() {
        // The compounding-failure regression. A player that gives up on a cold
        // torrent has no open connection, so `has_open_stream` alone reads it
        // as abandoned -- and deleting it there throws away the data and the
        // warmed-up peers that the player's *own retry*, seconds later, is
        // about to need. Every attempt then starts colder than the last, which
        // is what a viewer sees as the player cycling through every source and
        // playing none. Range requests are stateless; recency, not an open
        // socket, is what "someone is watching this" means here.
        assert_eq!(
            action_for_abandoned(false, true, false),
            UnfocusedAction::Leave
        );
    }

    #[test]
    fn a_finished_download_survives_the_switch() {
        // A complete file costs no download bandwidth, and deleting a movie
        // that finished downloading because the viewer started the next
        // episode is destructive. The cache janitor reclaims it under
        // pressure, oldest-first.
        assert_eq!(
            action_for_abandoned(false, false, true),
            UnfocusedAction::Leave
        );
    }
}
