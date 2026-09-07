//! Persistent audit log: who connected, how long it took, and what broke.
//!
//! Until this existed the only record of a session was the GUI's in-memory Log
//! panel, which is capped at a few hundred lines and dies with the window. Two
//! separate investigations ended at "there are no logs", so this writes to
//! disk, survives a restart, and is exportable from the GUI in one click.
//!
//! # Why a thread and not a task
//!
//! Events are handed to a **bounded** channel drained by a dedicated OS
//! thread. Both halves of that are deliberate:
//!
//! * *Bounded, with `try_send`* — a full channel drops the event and bumps a
//!   counter rather than waiting. A log that can apply back-pressure to a
//!   video stream is worse than a log with a hole in it, and the hole is
//!   recorded (see `dropped`), so the file never quietly lies about being
//!   complete.
//! * *An OS thread, not a `tokio::spawn`* — the writer does blocking file I/O.
//!   On the async runtime that would occupy a worker thread mid-write, on the
//!   same runtime that is servicing video range requests. Off it, a slow disk
//!   cannot touch playback at all.
//!
//! # Format
//!
//! One JSON object per line (JSONL): greppable with plain `grep`, parseable
//! with `jq`, and appendable without ever rewriting what came before — which
//! matters because the file is read (for export) while it is being written.

use std::collections::VecDeque;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};

use serde::Serialize;

/// How many events may be queued before new ones are dropped.
///
/// Generous enough that a burst of range requests (a player seeking) is never
/// lost, small enough to bound memory if the disk stops accepting writes.
const CHANNEL_CAPACITY: usize = 4096;

/// Rotate once the live file passes this size.
const MAX_FILE_BYTES: u64 = 8 * 1024 * 1024;

/// How many recent start phases `/health` reports.
///
/// Enough to cover one viewing session's worth of starts and seeks, which is
/// the window in which someone says "that took ages" and wants to know which
/// phase it was. The file is the long-term record; this is the one you can
/// read without leaving the browser tab you are already in.
const RECENT_STARTS_CAPACITY: usize = 24;

/// The live file plus one rotated generation, so worst-case disk use is
/// `2 * MAX_FILE_BYTES` and an export still reaches back beyond the rotation.
const ROTATED_SUFFIX: &str = ".1";

/// A logged event. `event` is the discriminator every line carries, so
/// `grep '"event":"cold_start"'` is a complete query.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    /// Process came up. The first line of every session, and what makes a
    /// crash visible: two `server_start` lines with no `server_stop` between
    /// them means the process died without unwinding.
    ServerStart {
        version: String,
        http_port: u16,
        https_port: Option<u16>,
        cache_dir: String,
    },
    /// Clean shutdown (Ctrl-C, SIGTERM, or the GUI's stop button).
    ServerStop { uptime_secs: u64 },
    /// A panic, captured by the hook installed in `install_panic_hook`.
    /// Written synchronously — the process may be seconds from gone.
    Crash { message: String, location: String },
    /// One HTTP request, logged when its headers are sent.
    ///
    /// `latency_ms` is **time to first byte**, which for a video request is
    /// the number that decides whether a player waits or gives up. It includes
    /// the whole cold start when there was one.
    Request {
        client: String,
        path: String,
        status: u16,
        latency_ms: u64,
        range: Option<String>,
    },
    /// Starting a torrent that was not already running, broken into the phases
    /// that make up the wait. This is the breakdown behind a slow `Request`:
    /// which step actually cost the time, rather than just "it was slow".
    ColdStart {
        info_hash: String,
        file_idx: usize,
        /// Metadata fetch (DHT/trackers), skipped when already cached.
        metadata_ms: u64,
        /// Leaving `Initializing` — hash-checking data already on disk.
        initialize_ms: u64,
        total_ms: u64,
        /// Whether the slow metadata step was skipped by the cache.
        from_cached_metadata: bool,
        outcome: String,
    },
    /// Waiting for the first real bytes before answering. A `timed_out` here
    /// with `bytes: 0` is a stream that was handed to the player empty.
    Prebuffer {
        info_hash: String,
        bytes: usize,
        wanted: usize,
        ms: u64,
        timed_out: bool,
        /// The data was already on disk, so this was a seek into a region
        /// already downloaded rather than a wait on the swarm. The single most
        /// useful field here: a slow *warm* pre-buffer is a gateway problem,
        /// while a slow cold one is the piece-size arithmetic and no amount of
        /// code will move it.
        warm: bool,
        /// Bytes between the requested offset and the end of the piece it
        /// lands in — the floor this request could not have beaten.
        piece_remainder: u64,
        /// The client seeked away before this read produced anything, so it
        /// was retired to give its piece priority back. A run of these is what
        /// a scrub burst looks like in the log, and it is the explanation for
        /// the one slow start sitting among them.
        superseded: bool,
    },
    /// A response body finished, by any route: played out, player disconnected,
    /// or the torrent stopped feeding it.
    StreamClose {
        info_hash: String,
        client: String,
        bytes_served: u64,
        duration_ms: u64,
        reason: String,
    },
    /// A torrent and its partial data were deleted.
    TorrentDiscarded {
        info_hash: String,
        progress_percent: f64,
    },
    /// Something went wrong that did not stop the process.
    Problem { context: String, message: String },
}

#[derive(Serialize)]
struct Line<'a> {
    /// Local time — this is a desktop app and the person reading the file is
    /// sitting in front of the machine that wrote it.
    ts: String,
    #[serde(flatten)]
    event: &'a Event,
}

/// One measured phase of one start, as `/health` reports it.
///
/// The same numbers go to the log file, but a JSONL file on a machine you are
/// not sitting at is not a thing anyone checks mid-complaint. This is the
/// "which phase was slow?" answer available from the same `GET /health` that
/// already answers "is it running?".
#[derive(Debug, Clone, Serialize)]
pub struct StartSample {
    pub at: String,
    /// `cold_start` or `prebuffer` — the two waits a viewer actually feels.
    pub phase: &'static str,
    pub info_hash: String,
    /// Wall time this phase took.
    pub ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initialize_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<usize>,
    /// For a pre-buffer: whether the bytes were already on disk. This is the
    /// warm/cold split, and it is what tells a slow seek (the swarm) apart
    /// from a slow gateway (us).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warm: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
}

impl Event {
    /// The `/health` summary of this event, for the events that describe a
    /// wait somebody sat through. Everything else returns `None`.
    fn as_start_sample(&self) -> Option<StartSample> {
        match self {
            Event::ColdStart {
                info_hash,
                metadata_ms,
                initialize_ms,
                total_ms,
                outcome,
                ..
            } => Some(StartSample {
                at: now_local(),
                phase: "cold_start",
                info_hash: info_hash.clone(),
                ms: *total_ms,
                metadata_ms: Some(*metadata_ms),
                initialize_ms: Some(*initialize_ms),
                bytes: None,
                warm: None,
                outcome: Some(outcome.clone()),
            }),
            Event::Prebuffer {
                info_hash,
                bytes,
                ms,
                warm,
                timed_out,
                superseded,
                ..
            } => Some(StartSample {
                at: now_local(),
                phase: "prebuffer",
                info_hash: info_hash.clone(),
                ms: *ms,
                metadata_ms: None,
                initialize_ms: None,
                bytes: Some(*bytes),
                warm: Some(*warm),
                // Named apart from a timeout on purpose: one is the swarm
                // being slow, the other is this gateway deciding the read no
                // longer had a viewer. Collapsing them would turn the fix into
                // something that looks like the fault.
                outcome: if *superseded {
                    Some("superseded".to_string())
                } else if *timed_out {
                    Some("timed out".to_string())
                } else {
                    None
                },
            }),
            _ => None,
        }
    }
}

/// Handle used to record events. Cloning is cheap and every clone writes to
/// the same file.
#[derive(Clone)]
pub struct AuditLog {
    tx: Option<SyncSender<Event>>,
    dropped: Arc<AtomicU64>,
    path: Option<PathBuf>,
    /// The last few measured waits, for `/health`. Kept separately from the
    /// file so it works even when logging is disabled -- a read-only home
    /// directory must not also cost you the instrumentation.
    recent: Arc<Mutex<VecDeque<StartSample>>>,
}

impl std::fmt::Debug for AuditLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditLog")
            .field("path", &self.path)
            .field("dropped", &self.dropped.load(Ordering::Relaxed))
            .finish()
    }
}

impl AuditLog {
    /// Opens `dir/events.jsonl` and starts the writer thread.
    ///
    /// A failure here is reported and then swallowed: the gateway must still
    /// stream when the log cannot be written (read-only home directory, full
    /// disk). The returned log is inert in that case, which `is_enabled`
    /// exposes so the GUI can say so rather than showing an empty file.
    pub fn open(dir: &Path) -> Self {
        match Self::try_open(dir) {
            Ok(log) => log,
            Err(e) => {
                tracing::warn!(
                    dir = %dir.display(),
                    "could not open the audit log ({e:#}); continuing without it"
                );
                Self::disabled()
            }
        }
    }

    /// An `AuditLog` that discards everything. Used when opening the file
    /// failed, and by tests that do not care about the log.
    pub fn disabled() -> Self {
        Self {
            tx: None,
            dropped: Arc::new(AtomicU64::new(0)),
            path: None,
            recent: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    fn try_open(dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("events.jsonl");

        // Opened once here rather than per write so a failure surfaces at
        // startup, where it can be reported, instead of silently on the first
        // event hours later.
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        let (tx, rx) = sync_channel::<Event>(CHANNEL_CAPACITY);
        let writer_path = path.clone();
        std::thread::Builder::new()
            .name("audit-log".to_string())
            .spawn(move || {
                let mut writer = Writer {
                    file: BufWriter::new(file),
                    path: writer_path,
                };
                // Ends when every `AuditLog` clone has been dropped, which
                // happens at process shutdown.
                while let Ok(event) = rx.recv() {
                    writer.write(&event);
                    // Drain whatever else is queued before flushing, so a
                    // burst costs one flush rather than one per event.
                    while let Ok(next) = rx.try_recv() {
                        writer.write(&next);
                    }
                    let _ = writer.file.flush();
                }
                let _ = writer.file.flush();
            })?;

        Ok(Self {
            tx: Some(tx),
            dropped: Arc::new(AtomicU64::new(0)),
            path: Some(path),
            recent: Arc::new(Mutex::new(VecDeque::new())),
        })
    }

    /// Records an event. Never blocks and never fails: a full queue increments
    /// `dropped` instead of waiting, because this is called from the request
    /// path.
    pub fn record(&self, event: Event) {
        // Before the file, and outside the `tx` check: the in-memory timeline
        // is what `/health` serves, and it has to keep working when the log
        // could not be opened at all.
        if let Some(sample) = event.as_start_sample() {
            let mut recent = self
                .recent
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if recent.len() == RECENT_STARTS_CAPACITY {
                recent.pop_front();
            }
            recent.push_back(sample);
        }

        let Some(tx) = &self.tx else { return };
        if let Err(TrySendError::Full(_)) = tx.try_send(event) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// The last few measured waits, oldest first. Served by `/health`.
    pub fn recent_starts(&self) -> Vec<StartSample> {
        self.recent
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .cloned()
            .collect()
    }

    /// Where the log is being written, if it is.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    pub fn is_enabled(&self) -> bool {
        self.tx.is_some()
    }

    /// How many events were dropped because the queue was full. Reported in
    /// the export header so a gap in the file is never mistaken for a quiet
    /// period.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// The whole log, oldest first: the rotated generation followed by the
    /// live file.
    ///
    /// Reads what is on disk, so events still queued in the channel are not
    /// included. That is a deliberate trade against making the export path
    /// able to block on the writer.
    pub fn read_all(&self) -> String {
        let Some(path) = &self.path else {
            return String::new();
        };
        let rotated = rotated_path(path);
        let mut out = String::new();
        for candidate in [&rotated, path] {
            if let Ok(text) = std::fs::read_to_string(candidate) {
                out.push_str(&text);
            }
        }
        out
    }
}

struct Writer {
    file: BufWriter<std::fs::File>,
    path: PathBuf,
}

impl Writer {
    fn write(&mut self, event: &Event) {
        let line = Line {
            ts: now_local(),
            event,
        };
        // A serialization failure must not take down the writer thread and
        // with it every later event, so it degrades to a plain-text line.
        match serde_json::to_string(&line) {
            Ok(json) => {
                let _ = writeln!(self.file, "{json}");
            }
            Err(e) => {
                let _ = writeln!(self.file, "{{\"event\":\"log_error\",\"message\":\"{e}\"}}");
            }
        }
        self.rotate_if_needed();
    }

    /// Rotation is checked after writing rather than before, so a single
    /// oversized line still lands in one piece.
    fn rotate_if_needed(&mut self) {
        let too_big = self
            .file
            .get_ref()
            .metadata()
            .map(|m| m.len() >= MAX_FILE_BYTES)
            .unwrap_or(false);
        if !too_big {
            return;
        }
        let _ = self.file.flush();
        if std::fs::rename(&self.path, rotated_path(&self.path)).is_err() {
            return;
        }
        if let Ok(fresh) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
        {
            self.file = BufWriter::new(fresh);
        }
    }
}

fn rotated_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(ROTATED_SUFFIX);
    PathBuf::from(name)
}

fn now_local() -> String {
    chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, false)
}

/// Where the log lives when the user has not chosen a directory.
///
/// An absolute path under `XDG_STATE_HOME`, **never** relative to the working
/// directory. The cache directory is relative, and the installed `.deb`
/// binaries run from wherever the desktop launcher happened to start them, so
/// a relative default scatters logs across `$HOME`, `/`, and anywhere else the
/// app was ever launched from — which is exactly the failure that made the
/// cache directory confusing enough to warrant its own warning in CLAUDE.md.
pub fn default_log_dir() -> PathBuf {
    if let Some(state) = std::env::var_os("XDG_STATE_HOME").filter(|s| !s.is_empty()) {
        return PathBuf::from(state).join("novastream");
    }
    if let Some(home) = std::env::var_os("HOME").filter(|s| !s.is_empty()) {
        return PathBuf::from(home).join(".local/state/novastream");
    }
    PathBuf::from("novastream-logs")
}

/// Routes panics into the log before the process goes.
///
/// A panic in a worker task is otherwise invisible once the GUI window is
/// closed: it goes to stderr, which is piped into a panel that no longer
/// exists. Chains to the previous hook so the normal backtrace still prints.
pub fn install_panic_hook(log: AuditLog) {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let message = info
            .payload()
            .downcast_ref::<&str>()
            .map(|s| (*s).to_string())
            .or_else(|| info.payload().downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "panic with a non-string payload".to_string());
        let location = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "unknown".to_string());
        log.record(Event::Crash { message, location });
        // The writer thread is asynchronous and this process may be about to
        // abort, so give the record a moment to reach the file. Bounded, and
        // only ever paid on a path that is already fatal.
        std::thread::sleep(std::time::Duration::from_millis(150));
        previous(info);
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Blocks until the writer thread has drained, which it signals by
    /// dropping its end once every sender is gone.
    fn flush(log: AuditLog) {
        drop(log);
        std::thread::sleep(std::time::Duration::from_millis(250));
    }

    #[test]
    fn records_land_on_disk_as_one_json_object_per_line() {
        let dir = tempfile::tempdir().unwrap();
        let log = AuditLog::open(dir.path());
        log.record(Event::ServerStop { uptime_secs: 7 });
        log.record(Event::Problem {
            context: "indexer".to_string(),
            message: "timed out".to_string(),
        });

        let path = log.path().unwrap().to_path_buf();
        flush(log);

        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2, "one line per event: {text}");
        for line in lines {
            let parsed: serde_json::Value = serde_json::from_str(line).expect("valid JSON");
            assert!(parsed.get("ts").is_some(), "every line is timestamped");
            assert!(parsed.get("event").is_some(), "every line is tagged");
        }
    }

    /// The tag is the whole query interface -- `grep '"event":"cold_start"'`
    /// has to work, so the discriminator must be a plain snake_case string
    /// and not a nested object or a numeric index.
    #[test]
    fn events_are_tagged_with_a_greppable_name() {
        let dir = tempfile::tempdir().unwrap();
        let log = AuditLog::open(dir.path());
        log.record(Event::ColdStart {
            info_hash: "abc".to_string(),
            file_idx: 0,
            metadata_ms: 10,
            initialize_ms: 20,
            total_ms: 30,
            from_cached_metadata: false,
            outcome: "ok".to_string(),
        });
        let path = log.path().unwrap().to_path_buf();
        flush(log);

        let text = std::fs::read_to_string(&path).unwrap();
        assert!(
            text.contains(r#""event":"cold_start""#),
            "cold starts must be greppable by that exact string: {text}"
        );
    }

    /// The gateway streams video whether or not it can write a log. Pointing
    /// the log at a path that cannot be created must not panic or fail.
    #[test]
    fn an_unwritable_directory_degrades_instead_of_failing() {
        let dir = tempfile::tempdir().unwrap();
        // A *file* where the log directory should be: `create_dir_all` cannot
        // succeed here.
        let blocked = dir.path().join("blocked");
        std::fs::write(&blocked, b"not a directory").unwrap();

        let log = AuditLog::open(&blocked);
        assert!(!log.is_enabled(), "a log that cannot open must be inert");
        log.record(Event::ServerStop { uptime_secs: 1 });
        assert_eq!(log.read_all(), "");
    }

    #[test]
    fn a_full_queue_drops_events_instead_of_blocking() {
        // No writer draining it: the channel is the only thing holding events,
        // so this fills deterministically.
        let (tx, rx) = sync_channel::<Event>(2);
        let log = AuditLog {
            tx: Some(tx),
            dropped: Arc::new(AtomicU64::new(0)),
            path: None,
            recent: Arc::new(Mutex::new(VecDeque::new())),
        };
        for _ in 0..10 {
            // The assertion is that this returns at all -- a blocking send
            // here would hang the test, and in production would stall a video
            // response on the log writer.
            log.record(Event::ServerStop { uptime_secs: 0 });
        }
        assert_eq!(log.dropped(), 8, "8 of 10 did not fit in a queue of 2");
        drop(rx);
    }

    #[test]
    fn the_default_log_directory_is_absolute() {
        // Relative would follow the process's working directory, which for the
        // installed binaries is wherever the desktop launcher started them.
        assert!(
            default_log_dir().is_absolute()
                || std::env::var_os("HOME").is_none_or(|h| h.is_empty()),
            "the log directory must not depend on the working directory"
        );
    }

    #[test]
    fn rotation_keeps_the_previous_generation_and_export_reads_both() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        std::fs::write(&path, "live\n").unwrap();
        std::fs::write(rotated_path(&path), "older\n").unwrap();

        let log = AuditLog {
            tx: None,
            dropped: Arc::new(AtomicU64::new(0)),
            path: Some(path),
            recent: Arc::new(Mutex::new(VecDeque::new())),
        };
        assert_eq!(
            log.read_all(),
            "older\nlive\n",
            "the export must read the rotated generation first, oldest first"
        );
    }
}
