//! The two small state machines every panel of this window was hand-rolling:
//! background work the UI thread waits on without blocking ([`Job`]), and the
//! press-twice guard on destructive buttons ([`Confirm`]).

use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

/// One kind of background work, at most one of it in flight at a time.
///
/// Holds the receiving end of whatever is doing the work and, for work that
/// repeats, when it is next due. The UI thread calls [`Job::poll`] every
/// frame; nothing here ever blocks.
pub struct Job<T> {
    rx: Option<Receiver<T>>,
    next_due: Instant,
}

impl<T> Default for Job<T> {
    fn default() -> Self {
        Self {
            rx: None,
            next_due: Instant::now(),
        }
    }
}

impl<T> Job<T> {
    pub fn in_flight(&self) -> bool {
        self.rx.is_some()
    }

    /// True when nothing is in flight and the schedule says to go again.
    pub fn due(&self, now: Instant) -> bool {
        self.rx.is_none() && now >= self.next_due
    }

    /// Pushes the next run out to `interval` from now.
    pub fn schedule_in(&mut self, interval: Duration) {
        self.next_due = Instant::now() + interval;
    }

    /// Makes the next [`Job::due`] true at once, for when something just
    /// changed and the regular interval would only delay saying so.
    pub fn expedite(&mut self) {
        self.next_due = Instant::now();
    }

    /// Starts the work `launch` hands back a receiver for.
    ///
    /// Refuses -- returning false, and without calling `launch` at all --
    /// when this job is already in flight. The caller has to handle that
    /// answer: the bug this type replaced was an action silently dropped on
    /// exactly this path, with the window going on as though it had run.
    #[must_use = "a refused start means the work did not run"]
    pub fn start(&mut self, launch: impl FnOnce() -> Receiver<T>) -> bool {
        if self.rx.is_some() {
            return false;
        }
        self.rx = Some(launch());
        true
    }

    /// The finished result, once. `None` while in flight or idle.
    ///
    /// A worker that died without answering (a panic on its thread) also
    /// clears the slot, so one crash cannot wedge a job "in flight" forever.
    pub fn poll(&mut self) -> Option<T> {
        let rx = self.rx.as_ref()?;
        match rx.try_recv() {
            Ok(value) => {
                self.rx = None;
                Some(value)
            }
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => {
                self.rx = None;
                None
            }
        }
    }
}

impl<T: Send + 'static> Job<T> {
    /// Runs `work` on a thread of its own. For the blocking subprocess work
    /// (`systemctl`, a child's graceful stop) that must stay off the shared
    /// HTTP worker, where it would hold up the health poll for seconds.
    #[must_use = "a refused start means the work did not run"]
    pub fn spawn(&mut self, work: impl FnOnce() -> T + Send + 'static) -> bool {
        self.start(|| {
            let (tx, rx) = mpsc::channel();
            thread::spawn(move || {
                let _ = tx.send(work());
            });
            rx
        })
    }
}

/// A destructive button that has to be pressed twice.
///
/// `K` says *which* thing is armed -- `()` for the one cache purge, an info
/// hash for a swarm row -- so arming one row never lets a second click on a
/// different row go straight through.
pub struct Confirm<K> {
    armed: Option<K>,
}

impl<K> Default for Confirm<K> {
    fn default() -> Self {
        Self { armed: None }
    }
}

impl<K: PartialEq> Confirm<K> {
    /// The first press on `key` arms it and returns false; a second press on
    /// the same key fires, disarms, and returns true. A press on a different
    /// key re-arms onto that one.
    pub fn arm_or_fire(&mut self, key: K) -> bool {
        if self.armed.as_ref() == Some(&key) {
            self.armed = None;
            true
        } else {
            self.armed = Some(key);
            false
        }
    }

    pub fn is_armed(&self, key: &K) -> bool {
        self.armed.as_ref() == Some(key)
    }

    pub fn armed(&self) -> Option<&K> {
        self.armed.as_ref()
    }

    pub fn disarm(&mut self) {
        self.armed = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wait<T>(job: &mut Job<T>) -> Option<T> {
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if let Some(value) = job.poll() {
                return Some(value);
            }
            if !job.in_flight() {
                return None;
            }
            thread::sleep(Duration::from_millis(5));
        }
        None
    }

    #[test]
    fn a_second_start_is_refused_while_the_first_is_in_flight() {
        let mut job = Job::default();
        let (tx, rx) = mpsc::channel();
        assert!(job.start(|| rx));
        let mut launched = false;
        assert!(
            !job.start(|| {
                launched = true;
                mpsc::channel().1
            }),
            "an in-flight job must refuse"
        );
        assert!(!launched, "a refused start must not launch its work");

        tx.send(7).unwrap();
        assert_eq!(job.poll(), Some(7));
        assert!(!job.in_flight());
        assert_eq!(job.poll(), None, "a result is delivered once");
    }

    #[test]
    fn spawned_work_reports_back() {
        let mut job = Job::default();
        assert!(job.spawn(|| "done"));
        assert_eq!(wait(&mut job), Some("done"));
    }

    /// A panicking worker must not leave the job looking busy forever --
    /// that would grey out a button for the rest of the session.
    #[test]
    fn a_worker_that_dies_frees_the_job() {
        let mut job: Job<()> = Job::default();
        assert!(job.start(|| mpsc::channel().1));
        assert_eq!(job.poll(), None);
        assert!(!job.in_flight());
    }

    #[test]
    fn due_follows_the_schedule_and_never_overlaps() {
        let mut job: Job<()> = Job::default();
        assert!(job.due(Instant::now()));
        job.schedule_in(Duration::from_secs(60));
        assert!(!job.due(Instant::now()));
        job.expedite();
        assert!(job.due(Instant::now()));
        let (_tx, rx) = mpsc::channel();
        assert!(job.start(|| rx));
        assert!(!job.due(Instant::now()), "in flight is never due");
    }

    #[test]
    fn a_confirmation_fires_only_on_a_second_press_of_the_same_key() {
        let mut confirm = Confirm::default();
        assert!(!confirm.arm_or_fire("a"));
        assert!(confirm.is_armed(&"a"));
        // A different row re-arms rather than firing.
        assert!(!confirm.arm_or_fire("b"));
        assert!(!confirm.is_armed(&"a"));
        assert!(confirm.arm_or_fire("b"));
        assert_eq!(confirm.armed(), None, "firing disarms");

        let mut purge = Confirm::default();
        assert!(!purge.arm_or_fire(()));
        purge.disarm();
        assert!(!purge.arm_or_fire(()), "an aborted arm starts over");
        assert!(purge.arm_or_fire(()));
    }
}
