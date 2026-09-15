//! Getting a torrent's peers back after the network drops out from under it.
//!
//! librqbit retries a peer whose connection failed on a steep schedule: 10
//! seconds, then a minute, then six, then thirty-six (`backoff()` in its peer
//! stats, factor 6 with jitter, so each wait is up to twice that). A tracker
//! or DHT reply naming the same address again does not bring it forward,
//! because an address the torrent already knows is ignored
//! (`PeerStates::add_if_not_seen`). Only the peer connecting in can, and a
//! seeder behind NAT never does.
//!
//! So when the line goes down -- a router rebooting, Wi-Fi dropping out, the
//! ISP blinking -- every peer times out within ten seconds (librqbit's read
//! timeout), and if the line stays down for more than about two minutes each
//! one burns its first two retries against it. The third comes 7 to 14
//! minutes after the drop, however soon the line returns. Measured
//! 2026-09-15 against a lab swarm whose seeders never connect out: a 200 s
//! outage left the download stopped for 475 s after the seeders were back.
//! The player spins all that time, and mpv eventually reports the stall as
//! the end of the file.
//!
//! Pausing and restarting the torrent is the way out that librqbit's public
//! API allows: the restart builds a fresh peer table, with every backoff gone,
//! and announces to the trackers and the DHT straight away. Open reads come
//! through it untouched, because librqbit hands their wakers from the live
//! state to the paused one and back (`TorrentStatePaused::streams`), and a
//! paused torrent keeps its pieces, so nothing is checked again. With it, the
//! same outage had the download moving again 15 s after the seeders returned,
//! and in Stremio, where mpv had already given up, pressing play 16 s after
//! they returned was playing again 3 s later.
//!
//! What triggers it is bytes stopping, not the peer count reaching zero. On
//! the public DHT a torrent is visited all the time by peers that complete a
//! handshake, send nothing and leave, so a count of connected peers is
//! regularly above zero on a torrent that is getting nowhere.

use std::sync::Arc;
use std::time::{Duration, Instant};

use librqbit::api::TorrentIdOrHash;
use tracing::{debug, info, warn};

use super::{Progress, TorrentEngine};
use crate::util::lock;

/// How long a torrent that was downloading may receive nothing before its
/// peer table is rebuilt. Twice librqbit's peer read timeout, so the peers a
/// dead line took are gone by then and a live one that merely paused between
/// pieces has had its chance, and well inside the 60 seconds mpv waits on a
/// silent connection before giving up.
const REVIVE_AFTER: Duration = Duration::from_secs(20);

/// The wait before the second rebuild when the first brought nothing, doubled
/// after each one that also brings nothing, up to [`REVIVE_EVERY_MAX`]. A line
/// that is down for a router reboot of a minute or two gets a rebuild within
/// about that long of coming back, and a swarm that has really died is
/// re-announced every few minutes rather than hammered.
const REVIVE_EVERY_MIN: Duration = Duration::from_secs(30);
const REVIVE_EVERY_MAX: Duration = Duration::from_secs(300);

/// The shortest time between two rebuilds when a player's request brings one
/// forward. A player whose stream stalled asks again every few seconds, and
/// each rebuild starts every announce over, so a rebuild is given this long to
/// hear back before another replaces it.
const REQUESTED_REVIVE_GAP: Duration = Duration::from_secs(10);

/// How often the session is looked at.
const SWARM_CHECK_INTERVAL: Duration = Duration::from_secs(5);

/// How long one running torrent has gone without receiving anything, as far
/// as rebuilding its peer table goes.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct Silence {
    /// Bytes have arrived at some point. Until they have, a torrent receiving
    /// nothing is still looking for its swarm, and rebuilding the peer table
    /// would only start that search over.
    had_data: bool,
    /// librqbit's count of bytes received at the last look. It starts from
    /// zero whenever the torrent starts, rebuilds included.
    fetched: u64,
    /// When the bytes stopped.
    since: Option<Instant>,
    /// The last rebuild, and how long to wait after it before the next.
    last_revive: Option<(Instant, Duration)>,
}

impl Silence {
    /// Takes one look at a running torrent's received-bytes count, and says
    /// whether to rebuild its peer table now.
    pub(super) fn observe(&mut self, fetched: u64, now: Instant) -> bool {
        // Smaller only after a restart, which is not data arriving.
        let arrived = fetched > self.fetched;
        self.fetched = fetched;
        if arrived {
            *self = Silence {
                had_data: true,
                fetched,
                ..Silence::default()
            };
            return false;
        }
        if !self.had_data {
            return false;
        }
        let since = *self.since.get_or_insert(now);
        if now.duration_since(since) < REVIVE_AFTER {
            return false;
        }
        let wait = match self.last_revive {
            None => REVIVE_EVERY_MIN,
            Some((at, wait)) if now.duration_since(at) < wait => return false,
            Some((_, wait)) => (wait * 2).min(REVIVE_EVERY_MAX),
        };
        self.last_revive = Some((now, wait));
        true
    }

    /// A player asked for this torrent, whose received-bytes count is now
    /// `fetched`. Says whether to rebuild its peer table now rather than on
    /// schedule. A player asking again is the best sign there is that the line
    /// is back, and on a long outage the scheduled rebuild can be minutes
    /// away. A rebuild here also starts the schedule over from its shortest
    /// wait.
    pub(super) fn requested(&mut self, fetched: u64, now: Instant) -> bool {
        let silent = self.had_data
            && fetched <= self.fetched
            && self
                .since
                .is_some_and(|since| now.duration_since(since) >= REVIVE_AFTER);
        let too_soon = self
            .last_revive
            .is_some_and(|(at, _)| now.duration_since(at) < REQUESTED_REVIVE_GAP);
        if !silent || too_soon {
            return false;
        }
        self.last_revive = Some((now, REVIVE_EVERY_MIN));
        true
    }

    /// A torrent that is paused, finished or held receives nothing on purpose,
    /// so that time is not silence. Whether data had arrived is kept, so that
    /// when it runs again and nothing comes, it counts.
    pub(super) fn rest(&mut self) {
        *self = Silence {
            had_data: self.had_data,
            ..Silence::default()
        };
    }
}

/// One torrent as the swarm watch sees it.
struct Observed {
    hash: String,
    idx: TorrentIdOrHash,
    /// Bytes received since it last started, or `None` when it is not trying
    /// to receive any: paused, still starting, or complete.
    fetched: Option<u64>,
}

impl TorrentEngine {
    /// Rebuilds the peer table of every running torrent whose bytes stopped
    /// arriving and have not started again. Returns how many were rebuilt.
    pub async fn revive_lost_swarms(&self) -> usize {
        let now = Instant::now();
        // Copied out first: see the lock notes on `TorrentEngine`.
        let held = self.held_snapshot();
        let observed: Vec<Observed> = self.api.session().with_torrents(|iter| {
            iter.map(|(_, handle)| {
                let stats = handle.stats();
                let running = !handle.is_paused()
                    && !Progress::of(stats.progress_bytes, stats.total_bytes).finished;
                Observed {
                    hash: handle.info_hash().as_string(),
                    idx: handle.info_hash().into(),
                    fetched: stats
                        .live
                        .as_ref()
                        .filter(|_| running)
                        .map(|live| live.snapshot.fetched_bytes),
                }
            })
            .collect()
        });

        let due: Vec<Observed> = {
            let mut silences = lock(&self.download_silence);
            silences.retain(|hash, _| observed.iter().any(|torrent| &torrent.hash == hash));
            observed
                .into_iter()
                .filter(|torrent| {
                    let silence = silences.entry(torrent.hash.clone()).or_default();
                    match torrent.fetched {
                        Some(fetched) if !held.contains(&torrent.hash) => {
                            silence.observe(fetched, now)
                        }
                        _ => {
                            silence.rest();
                            false
                        }
                    }
                })
                .collect()
        };

        let mut revived = 0;
        for torrent in due {
            if self
                .rebuild_peers(&torrent.hash, torrent.idx, "on schedule")
                .await
            {
                revived += 1;
            }
        }
        revived
    }

    /// Rebuilds the peer table at once if a player just asked for a torrent
    /// whose bytes stopped arriving. See [`Silence::requested`]. Detached, so
    /// the request never waits on it; the reads it opens wake when the new
    /// peers deliver.
    pub fn revive_if_silent(self: &Arc<Self>, info_hash: &str) {
        let Some(idx) = self.session_idx(info_hash) else {
            return;
        };
        let Some(fetched) = self
            .api
            .session()
            .get(idx)
            .and_then(|handle| handle.stats().live.map(|live| live.snapshot.fetched_bytes))
        else {
            return;
        };
        let due = lock(&self.download_silence)
            .get_mut(info_hash)
            .is_some_and(|silence| silence.requested(fetched, Instant::now()));
        if due {
            let engine = Arc::clone(self);
            let hash = info_hash.to_string();
            tokio::spawn(async move {
                engine
                    .rebuild_peers(&hash, idx, "a player asked for it")
                    .await
            });
        }
    }

    /// Pauses the torrent and starts it again straight away. See the module
    /// docs for why, and CLAUDE.md for why nothing may ever sit between the
    /// two calls. Returns whether it is running again.
    async fn rebuild_peers(&self, info_hash: &str, idx: TorrentIdOrHash, why: &str) -> bool {
        if let Err(e) = self.api.api_torrent_action_pause(idx).await {
            // Paused or removed since it was looked at, which is not a
            // torrent that needs this any more.
            debug!(info_hash = %info_hash, "could not pause to rebuild peers: {e}");
            return false;
        }
        let started = self.api.api_torrent_action_start(idx).await;
        // The download queue may have resumed it in the moment it sat paused,
        // which rebuilds the peer list just the same.
        if started.is_ok() || self.is_running(info_hash) {
            info!(
                info_hash = %info_hash,
                why,
                "nothing downloaded for a while; rebuilt the peer list and announced again"
            );
            return true;
        }
        if let Err(e) = started {
            // The download queue resumes a torrent that holds a slot on its
            // next tick, and one with a reader always holds a slot. Said
            // loudly anyway, because until then its readers wait.
            warn!(
                info_hash = %info_hash,
                "paused to rebuild peers but could not start again: {e}"
            );
        }
        false
    }

    /// Runs `revive_lost_swarms` every few seconds for the lifetime of the
    /// process.
    pub fn spawn_swarm_watch(self: &Arc<Self>) {
        let engine = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(SWARM_CHECK_INTERVAL);
            loop {
                ticker.tick().await;
                engine.revive_lost_swarms().await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TICK: Duration = SWARM_CHECK_INTERVAL;

    /// Feeds `silence` one look per tick for `ticks` ticks from `from`, with
    /// the byte count stuck at `fetched`, and returns the offsets (in seconds)
    /// at which it asked to rebuild.
    fn run(silence: &mut Silence, from: Instant, ticks: u32, fetched: u64) -> Vec<u64> {
        (0..ticks)
            .map(|n| (n, from + TICK * n))
            .filter(|(_, at)| silence.observe(fetched, *at))
            .map(|(n, _)| (TICK * n).as_secs())
            .collect()
    }

    /// A downloading torrent, `fetched` bytes in, seen at `at`.
    fn downloading(fetched: u64, at: Instant) -> Silence {
        let mut silence = Silence::default();
        assert!(!silence.observe(fetched, at));
        silence
    }

    #[test]
    fn a_torrent_still_looking_for_its_first_bytes_is_left_to_look() {
        let mut silence = Silence::default();
        assert!(run(&mut silence, Instant::now(), 200, 0).is_empty());
    }

    #[test]
    fn a_torrent_receiving_bytes_is_left_alone() {
        let start = Instant::now();
        let mut silence = Silence::default();
        let rebuilt = (0..200u32).any(|n| silence.observe(u64::from(n) * 16_384, start + TICK * n));
        assert!(!rebuilt);
    }

    /// The case this exists for: the line goes down for a few minutes. The
    /// first rebuild comes soon after the bytes stop, and more keep coming
    /// while it stays down, further apart each time.
    #[test]
    fn bytes_stopping_rebuilds_soon_then_less_and_less_often() {
        let start = Instant::now();
        let mut silence = downloading(50_000_000, start);

        let rebuilds = run(&mut silence, start + TICK, 240, 50_000_000);
        let gaps: Vec<u64> = rebuilds.windows(2).map(|w| w[1] - w[0]).collect();

        assert_eq!(rebuilds[0], REVIVE_AFTER.as_secs());
        assert_eq!(gaps[0], REVIVE_EVERY_MIN.as_secs());
        assert!(
            gaps.windows(2).all(|w| w[1] >= w[0]),
            "gaps shrank: {gaps:?}"
        );
        assert_eq!(*gaps.last().unwrap(), REVIVE_EVERY_MAX.as_secs());
    }

    /// Peers that handshake and send nothing -- the public DHT is full of
    /// them -- must not keep the schedule at its shortest wait. Only bytes
    /// count, and a rebuild restarting librqbit's count from zero is not
    /// bytes either.
    #[test]
    fn a_restart_counting_from_zero_is_not_data_arriving() {
        let start = Instant::now();
        let mut silence = downloading(50_000_000, start);
        let first = run(&mut silence, start + TICK, 5, 50_000_000);
        assert_eq!(first, vec![REVIVE_AFTER.as_secs()]);

        // After the rebuild, librqbit's counter reads zero.
        let after = start + TICK * 6;
        let second = run(&mut silence, after, 12, 0);
        assert_eq!(
            second.len(),
            1,
            "one rebuild on the doubled wait: {second:?}"
        );
    }

    /// Bytes arriving again, even briefly, start the whole sequence over, so
    /// the next outage is answered as quickly as the first.
    #[test]
    fn bytes_arriving_again_reset_the_wait() {
        let start = Instant::now();
        let mut silence = downloading(1_000, start);
        assert!(run(&mut silence, start, 60, 1_000).len() > 2);

        let later = start + TICK * 60;
        assert!(!silence.observe(2_000, later));
        let again = run(&mut silence, later + TICK, 10, 2_000);
        assert_eq!(again.first().copied(), Some(REVIVE_AFTER.as_secs()));
    }

    /// After a long outage the scheduled rebuild can be five minutes off. A
    /// viewer pressing play again once the line is back must not wait for it.
    #[test]
    fn a_request_after_a_long_outage_rebuilds_without_waiting_for_the_schedule() {
        let start = Instant::now();
        let mut silence = downloading(9_000, start);
        let rebuilds = run(&mut silence, start + TICK, 100, 9_000);
        let last = start + TICK + Duration::from_secs(*rebuilds.last().unwrap());

        let pressed_play = last + Duration::from_secs(60);
        assert!(
            !silence.observe(9_000, pressed_play),
            "the schedule is still waiting"
        );
        assert!(silence.requested(9_000, pressed_play));

        // It started the schedule over rather than pushing it further out.
        assert!(!silence.observe(9_000, pressed_play + REVIVE_EVERY_MIN - TICK));
        assert!(silence.observe(9_000, pressed_play + REVIVE_EVERY_MIN));
    }

    /// A stalled player retries every few seconds. Each of those must not
    /// throw away the announces the last rebuild has only just sent.
    #[test]
    fn requests_close_together_rebuild_once() {
        let start = Instant::now();
        let mut silence = downloading(7, start);
        run(&mut silence, start, 4, 7);
        let silent = start + REVIVE_AFTER + TICK;

        assert!(silence.requested(7, silent));
        assert!(!silence.requested(7, silent + Duration::from_secs(3)));
        assert!(silence.requested(7, silent + REQUESTED_REVIVE_GAP));
    }

    /// A request is not a reason to rebuild a torrent that is receiving
    /// bytes, one that never has, or one whose bytes stopped a moment ago.
    #[test]
    fn a_request_rebuilds_only_a_torrent_that_has_gone_silent() {
        let now = Instant::now();
        assert!(
            !Silence::default().requested(0, now + Duration::from_secs(600)),
            "still searching"
        );

        let mut silence = downloading(100, now);
        assert!(!silence.requested(200, now + TICK), "bytes are arriving");
        silence.observe(200, now + TICK);
        silence.observe(200, now + TICK * 2);
        assert!(
            !silence.requested(200, now + TICK * 3),
            "only just went quiet"
        );
    }

    /// Paused by the queue or by hand, the torrent receives nothing because
    /// nobody wants it to. That time must not count, or it would be rebuilt
    /// the moment it was resumed, before it had a chance to reconnect.
    #[test]
    fn time_spent_paused_is_not_silence() {
        let start = Instant::now();
        let mut silence = downloading(4_000, start);
        silence.observe(4_000, start + TICK);
        silence.rest();

        let resumed = start + Duration::from_secs(3600);
        assert!(!silence.observe(0, resumed));
        assert!(!silence.observe(0, resumed + REVIVE_AFTER - TICK));
        assert!(
            silence.observe(0, resumed + REVIVE_AFTER),
            "a resumed torrent that still receives nothing has lost its swarm"
        );
    }
}
