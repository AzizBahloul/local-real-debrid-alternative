//! The window's state, and the rules that turn that state into what the
//! screen says.
//!
//! Split by what the code does to the state rather than by panel:
//! [`actions`] changes the world (start, stop, install, purge), [`poll`] reads
//! it back without blocking the frame, and [`ui`] draws it. The two decisions
//! that used to be buried in those -- what status to show, and what always-on
//! should do on open -- are pure functions here, so they can be table-tested.

mod actions;
mod poll;
mod ui;

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::{Duration, Instant};

use crate::health::{HealthView, Traffic};
use crate::http::HttpWorker;
use crate::job::{Confirm, Job};
use crate::server_process::{find_server_binary, LogTail, ServerProcess};
use crate::theme::{AMBER, PHOSPHOR, TEXT_DIM};
use crate::{addon_address, fx, service, widgets};

/// The gateway's own `--port` default. Pinned against the server's config by
/// `the_default_port_is_the_one_the_server_uses`: a window polling a port the
/// server does not bind reports a healthy gateway as down.
pub const DEFAULT_PORT: u16 = 8080;
/// What the settings grid offers before anything was saved. Relative on
/// purpose; `service::absolute_cache_dir` anchors it for the service.
pub const DEFAULT_CACHE_DIR: &str = "./cache";

const MAX_LOG_LINES: usize = 500;
/// One second rather than the two it used to be: this is now the sample rate
/// of the traffic graph, and a 0.5 Hz graph of a live download looks like a
/// staircase. It is one loopback request against our own process.
const HEALTH_POLL_INTERVAL: Duration = Duration::from_secs(1);
const STOP_GRACE_PERIOD: Duration = Duration::from_secs(5);
/// ~30 fps. Enough for the rain and the sweep to move smoothly, and half the
/// wake-ups of a 60 fps repaint on a window that stays open for whole films.
pub const FRAME_INTERVAL: Duration = Duration::from_millis(33);
/// The rate while the window is not focused -- sitting behind a player for
/// the length of a film. Nobody is watching the rain; the numbers still move.
pub const UNFOCUSED_FRAME_INTERVAL: Duration = Duration::from_millis(200);
/// How often the systemd state is re-read. Slower than the health poll: this
/// one costs a `systemctl` process, and enabled/lingering only change when
/// somebody presses a button in this window.
const SERVICE_POLL_INTERVAL: Duration = Duration::from_secs(3);
/// Journal lines pulled in when attaching to a running service. Enough to
/// reach back past a busy hour to the startup banner, which is the only place
/// the addon URL is ever printed.
const JOURNAL_BACKLOG: usize = 2000;
/// How long to wait before trying the journal again after a failed attach, so
/// a missing `journalctl` cannot spin the log panel.
const JOURNAL_RETRY_DELAY: Duration = Duration::from_secs(10);
/// How often the child server and the journal follower are checked for having
/// exited. A `try_wait` per frame answered the same question 30 times a
/// second; a crash noticed within a second is noticed in time.
const LIVENESS_CHECK_INTERVAL: Duration = Duration::from_secs(1);
/// Most log lines taken off the pipe in one frame, so a burst is spread over
/// a few frames rather than landing in one long one.
const LOG_DRAIN_PER_FRAME: usize = 2048;

const BINARY_MISSING: &str = "streaming-gateway binary not found next to this app or on PATH";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Stopped,
    Starting,
    Running,
    Stopping,
}

impl Status {
    /// One colour per state, used by the lamp, the primary button and the
    /// header trim at once -- the whole window shifts hue together, which is
    /// what makes the state readable peripherally.
    pub fn color(self) -> egui::Color32 {
        match self {
            Status::Stopped => TEXT_DIM,
            Status::Starting | Status::Stopping => AMBER,
            Status::Running => PHOSPHOR,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Status::Stopped => "OFFLINE",
            Status::Starting => "HANDSHAKE",
            Status::Running => "ONLINE",
            Status::Stopping => "SHUTDOWN",
        }
    }
}

/// The `systemctl` work in flight, if any. Typed rather than a bare "busy"
/// flag because what the window should say depends on *which* action it is:
/// an install restarts an already-running unit, and reading "busy + active"
/// as stopping is how an install used to announce itself as a shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pending {
    None,
    Start,
    Stop,
    Install,
}

/// Everything [`derive_status`] reads, as plain values.
#[derive(Debug, Clone, Copy)]
pub struct StatusInputs {
    /// The server belongs to systemd (`service.installed`).
    pub service_mode: bool,
    pub pending: Pending,
    pub service_active: bool,
    /// The child this window started is being stopped.
    pub child_stopping: bool,
    pub child_running: bool,
    /// The server's startup banner has been seen.
    pub url_known: bool,
    /// A `/health` reply is in hand.
    pub health_known: bool,
}

/// What the lamp, the header and the power button say.
///
/// A pending service action speaks first, whichever owner the server has:
/// the first install of all happens while the unit does not exist yet, so the
/// window is still in child mode when it starts, and must not offer
/// `--start` -- a child started then takes the port the service is about to
/// bind.
pub fn derive_status(i: StatusInputs) -> Status {
    match i.pending {
        Pending::Start | Pending::Install => return Status::Starting,
        Pending::Stop => return Status::Stopping,
        Pending::None => {}
    }
    if i.service_mode {
        // The service's own state is authoritative here -- this window may
        // have been opened long after the gateway started.
        return if !i.service_active {
            Status::Stopped
        } else if !i.url_known && !i.health_known {
            Status::Starting
        } else {
            Status::Running
        };
    }
    if i.child_stopping {
        Status::Stopping
    } else if i.child_running && !i.url_known {
        Status::Starting
    } else if i.child_running {
        Status::Running
    } else {
        Status::Stopped
    }
}

/// What opening the window does to always-on mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlwaysOnStep {
    Nothing,
    /// Install the unit (which also starts it) and the login tray entry.
    Install,
    /// Only put the login tray entry back. The gateway is already running and
    /// is left strictly alone.
    WriteAutostart,
    /// Start the installed, stopped unit.
    Start,
    WriteAutostartAndStart,
}

/// Brings always-on mode up to date the moment the window opens.
///
/// Only a missing unit is a reason to install: installing is a
/// `systemctl restart`, and routing a merely missing autostart file through
/// it restarted a gateway that could be mid-stream to a phone -- for the sake
/// of a tray icon at the next login.
pub fn always_on_step(installed: bool, autostart: bool, active: bool) -> AlwaysOnStep {
    match (installed, autostart, active) {
        (false, _, _) => AlwaysOnStep::Install,
        (true, false, true) => AlwaysOnStep::WriteAutostart,
        (true, false, false) => AlwaysOnStep::WriteAutostartAndStart,
        (true, true, false) => AlwaysOnStep::Start,
        (true, true, true) => AlwaysOnStep::Nothing,
    }
}

/// The environment facts that cost a process or a filesystem walk to learn,
/// gathered off the UI thread once the first frame is up.
struct Probe {
    binary: Option<PathBuf>,
    systemd: bool,
    service: service::Status,
    autostart: bool,
}

impl Probe {
    fn run() -> Self {
        Self {
            binary: find_server_binary(),
            systemd: service::available(),
            // Read even without a systemd session to talk to: an installed
            // unit still means the server is not this window's to start, and
            // treating it as absent would put a child on the service's port.
            service: service::status(),
            autostart: service::autostart_enabled(),
        }
    }
}

/// How a `systemctl` action ended, with the autostart state read afterwards
/// on the same worker thread.
struct ServiceOutcome {
    error: Option<String>,
    autostart: bool,
}

/// One line in the TTY panel, coloured once when it arrived.
struct LogEntry {
    text: String,
    color: egui::Color32,
}

/// The TTY panel's lines and where they come from, and the two URLs scraped
/// out of them.
struct LogPanel {
    lines: VecDeque<LogEntry>,
    detected_url: Option<String>,
    /// The full https manifest URL, which arrives a few seconds after
    /// `detected_url` because the certificate is fetched in the background.
    addon_manifest: Option<String>,
    /// `journalctl -f` on the unit -- the source when the server belongs to
    /// systemd rather than to this window.
    journal: Option<LogTail>,
    journal_attach: Job<Result<LogTail, String>>,
    next_journal_attempt: Instant,
}

impl LogPanel {
    fn new() -> Self {
        Self {
            lines: VecDeque::with_capacity(MAX_LOG_LINES),
            detected_url: None,
            addon_manifest: None,
            journal: None,
            journal_attach: Job::default(),
            next_journal_attempt: Instant::now(),
        }
    }

    /// Appends one line. `text` must already have been through
    /// [`crate::text::strip_ansi`]: this is where a line is coloured, once,
    /// and the colour is read out of the text.
    fn push(&mut self, text: String) {
        if self.lines.len() >= MAX_LOG_LINES {
            self.lines.pop_front();
        }
        let color = widgets::log_color(&text);
        self.lines.push_back(LogEntry { text, color });
    }

    fn forget_urls(&mut self) {
        self.detected_url = None;
        self.addon_manifest = None;
    }

    /// Empties the panel for a new source: the old lines belong to a
    /// different process, and URLs scraped from them may name a port the new
    /// one is not using.
    fn reset(&mut self) {
        self.lines.clear();
        self.forget_urls();
    }
}

/// Screen furniture: effects and transient flourishes with no bearing on the
/// gateway.
struct Chrome {
    rain: fx::MatrixRain,
    boot: fx::BootSequence,
    crt: fx::CrtOverlay,
    /// Wall-clock of the next wordmark glitch and when the current one ends,
    /// so the tear is an occasional event rather than a permanent wobble.
    next_glitch: f64,
    glitch_until: f64,
    /// What was last copied, and when, for the transient confirmation.
    copied: Option<(String, f64)>,
}

pub struct GatewayApp {
    // Environment, probed off the UI thread on the first frame.
    probe: Job<Probe>,
    /// False until the probe lands. Nothing that depends on who owns the
    /// server -- always-on, the power button -- acts before then.
    service_known: bool,
    binary: Option<PathBuf>,
    /// This executable, used to spawn the tray sibling process.
    exe: Option<PathBuf>,
    /// Whether there is a systemd user session to install a service into.
    systemd: bool,

    // Settings, as the grid edits them.
    port: String,
    cache_dir: String,

    // Owner one: a child process of this window.
    process: ServerProcess,
    running: bool,
    child_stop: Job<()>,
    start_error: Option<String>,

    // Owner two: the systemd user service.
    service: service::Status,
    /// `service.summary()`, rendered when the status changes rather than on
    /// every frame.
    service_summary: String,
    service_poll: Job<service::Status>,
    /// In-flight enable/start/stop of the service. `systemctl` waits for the
    /// unit to finish changing state, which is far too long to spend on the
    /// UI thread.
    service_action: Job<ServiceOutcome>,
    pending: Pending,
    service_error: Option<String>,
    /// Whether the tray icon is set to come back at the next login.
    autostart: bool,
    /// Whether `ensure_always_on` has had its one go this session, so a
    /// failure is reported once rather than retried on every frame.
    always_on_attempted: bool,
    next_liveness_check: Instant,

    // The gateway's HTTP API.
    http: HttpWorker,
    health_poll: Job<Option<crate::health::HealthInfo>>,
    health: Option<HealthView>,
    traffic: Traffic,
    clear: Job<Result<usize, String>>,
    export: Job<Result<String, String>>,
    export_result: Option<String>,
    /// Failures of the fire-and-forget swarm-row calls, reported by the
    /// worker that made them.
    action_errors: (Sender<String>, Receiver<String>),
    /// The line under the controls: a purge result, or a row action that
    /// failed.
    notice: Option<String>,

    // Press-twice guards. Deleting is not undoable, so it does not hang off a
    // single mis-tap next to the stop button.
    clear_confirm: Confirm<()>,
    /// Keyed by info hash. See `act_on_torrent`.
    delete_confirm: Confirm<String>,

    log: LogPanel,
    /// Set by the explicit quit action, so the window closing does not leave
    /// a tray icon behind.
    quitting: bool,
    chrome: Chrome,

    // The addon address, against the one installed addons were given. See
    // `addon_address`.
    /// The address remembered from an earlier run.
    addon_remembered: Option<String>,
    /// The address the log last showed, once it has been compared.
    addon_checked: Option<String>,
    /// Set while the shown address differs from the remembered one: the old
    /// address, which the addon installed in Stremio still uses.
    addon_moved_from: Option<String>,
}

impl Default for GatewayApp {
    fn default() -> Self {
        // An installed service's own settings win over this app's defaults:
        // a service moved to another port is otherwise invisible here, because
        // the health poll would go to 8080 and find nothing. One small file
        // read -- everything that costs a process waits for the probe.
        let (port, cache_dir) = service::read_settings()
            .unwrap_or_else(|| (DEFAULT_PORT.to_string(), DEFAULT_CACHE_DIR.to_string()));
        let service = service::Status::default();
        Self {
            probe: Job::default(),
            service_known: false,
            binary: None,
            exe: std::env::current_exe().ok(),
            systemd: false,
            port,
            cache_dir,
            process: ServerProcess::new(),
            running: false,
            child_stop: Job::default(),
            start_error: None,
            service_summary: service.summary(),
            service,
            service_poll: Job::default(),
            service_action: Job::default(),
            pending: Pending::None,
            service_error: None,
            autostart: false,
            always_on_attempted: false,
            next_liveness_check: Instant::now(),
            http: HttpWorker::default(),
            health_poll: Job::default(),
            health: None,
            traffic: Traffic::default(),
            clear: Job::default(),
            export: Job::default(),
            export_result: None,
            action_errors: mpsc::channel(),
            notice: None,
            clear_confirm: Confirm::default(),
            delete_confirm: Confirm::default(),
            log: LogPanel::new(),
            quitting: false,
            chrome: Chrome {
                rain: fx::MatrixRain::new(0x5EED_1234_ABCD_0001),
                boot: fx::BootSequence::new(),
                crt: fx::CrtOverlay::default(),
                next_glitch: 3.0,
                glitch_until: 0.0,
                copied: None,
            },
            // One small file read, like `read_settings` above.
            addon_remembered: addon_address::load(),
            addon_checked: None,
            addon_moved_from: None,
        }
    }
}

impl GatewayApp {
    pub fn status(&self) -> Status {
        derive_status(StatusInputs {
            service_mode: self.service_mode(),
            pending: self.pending,
            service_active: self.service.active,
            child_stopping: self.child_stop.in_flight(),
            child_running: self.running,
            url_known: self.log.detected_url.is_some(),
            health_known: self.health.is_some(),
        })
    }

    /// True when the gateway belongs to systemd rather than to this window.
    ///
    /// Everything downstream keys off this: who gets started, whose logs the
    /// TTY panel shows, and -- the point of the whole feature -- whether
    /// closing the window has any effect on the stream at all.
    fn service_mode(&self) -> bool {
        self.service.installed
    }

    fn set_service(&mut self, status: service::Status) {
        if status != self.service {
            self.service_summary = status.summary();
        }
        self.service = status;
    }

    pub fn quitting(&self) -> bool {
        self.quitting
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const IDLE: StatusInputs = StatusInputs {
        service_mode: false,
        pending: Pending::None,
        service_active: false,
        child_stopping: false,
        child_running: false,
        url_known: false,
        health_known: false,
    };

    #[test]
    fn a_service_owned_gateway_is_reported_from_the_units_own_state() {
        let service = StatusInputs {
            service_mode: true,
            ..IDLE
        };
        assert_eq!(derive_status(service), Status::Stopped);
        let active = StatusInputs {
            service_active: true,
            ..service
        };
        // Up, but no banner and no /health yet.
        assert_eq!(derive_status(active), Status::Starting);
        assert_eq!(
            derive_status(StatusInputs {
                health_known: true,
                ..active
            }),
            Status::Running
        );
        // A window opened long after the gateway started finds /health
        // before it finds the banner; that is running, not starting.
        assert_eq!(
            derive_status(StatusInputs {
                url_known: true,
                ..active
            }),
            Status::Running
        );
        // The child fields mean nothing in service mode.
        assert_eq!(
            derive_status(StatusInputs {
                child_running: true,
                child_stopping: true,
                ..service
            }),
            Status::Stopped
        );
    }

    #[test]
    fn a_child_owned_gateway_is_reported_from_the_child() {
        assert_eq!(derive_status(IDLE), Status::Stopped);
        let running = StatusInputs {
            child_running: true,
            ..IDLE
        };
        assert_eq!(derive_status(running), Status::Starting);
        assert_eq!(
            derive_status(StatusInputs {
                url_known: true,
                ..running
            }),
            Status::Running
        );
        assert_eq!(
            derive_status(StatusInputs {
                child_stopping: true,
                url_known: true,
                ..running
            }),
            Status::Stopping
        );
    }

    /// The bugs this table exists for: an install on an already-active unit
    /// read as "busy + active" and showed SHUTDOWN, and the very first
    /// install -- which runs in child mode, the unit not existing yet --
    /// showed OFFLINE with `--start` enabled next to it.
    #[test]
    fn a_pending_service_action_speaks_first_in_either_mode() {
        for service_mode in [false, true] {
            for service_active in [false, true] {
                let base = StatusInputs {
                    service_mode,
                    service_active,
                    url_known: true,
                    health_known: true,
                    ..IDLE
                };
                for (pending, expected) in [
                    (Pending::Install, Status::Starting),
                    (Pending::Start, Status::Starting),
                    (Pending::Stop, Status::Stopping),
                ] {
                    assert_eq!(
                        derive_status(StatusInputs { pending, ..base }),
                        expected,
                        "{pending:?} with service_mode={service_mode} active={service_active}"
                    );
                }
            }
        }
    }

    /// Every combination, spelled out. The one that matters most is the
    /// second row: a running gateway with a missing autostart file gets the
    /// file back and nothing else -- never a restart.
    #[test]
    fn opening_the_window_takes_exactly_the_always_on_step_needed() {
        use AlwaysOnStep::*;
        let table = [
            // installed, autostart, active
            ((true, false, true), WriteAutostart),
            ((true, false, false), WriteAutostartAndStart),
            ((true, true, false), Start),
            ((true, true, true), Nothing),
            ((false, false, false), Install),
            ((false, true, false), Install),
            ((false, false, true), Install),
            ((false, true, true), Install),
        ];
        for ((installed, autostart, active), expected) in table {
            assert_eq!(
                always_on_step(installed, autostart, active),
                expected,
                "installed={installed} autostart={autostart} active={active}"
            );
        }
    }

    /// The window polls `/health` on this port before it has read anything
    /// else, so it has to be the port the server binds by default. Read from
    /// the server's source as text, like `tests/packaging.rs` reads the
    /// manifests: this crate does not link the server.
    #[test]
    fn the_default_port_is_the_one_the_server_uses() {
        const SERVER_CONFIG: &str = include_str!("../../../gateway/src/config/mod.rs");
        let attribute = SERVER_CONFIG
            .lines()
            .map(str::trim)
            .find(|line| line.starts_with("#[arg(") && line.contains("env = \"GATEWAY_PORT\""))
            .expect("the server declares a GATEWAY_PORT argument");
        assert!(
            attribute.contains(&format!("default_value_t = {DEFAULT_PORT}")),
            "the server's port default is not {DEFAULT_PORT}: {attribute}"
        );
    }

    #[test]
    fn each_status_has_its_own_colour_and_label() {
        let all = [
            Status::Stopped,
            Status::Starting,
            Status::Running,
            Status::Stopping,
        ];
        assert_ne!(Status::Running.color(), Status::Stopped.color());
        assert_ne!(Status::Running.color(), Status::Starting.color());
        for status in all {
            assert!(status.label().is_ascii(), "non-ascii status label");
        }
    }

    #[test]
    fn the_log_panel_keeps_its_cap_and_colours_each_line_once() {
        let mut panel = LogPanel::new();
        for i in 0..(MAX_LOG_LINES + 10) {
            panel.push(format!("line {i}"));
        }
        panel.push("[stderr] failed to bind".to_string());
        assert_eq!(panel.lines.len(), MAX_LOG_LINES);
        let last = panel.lines.back().unwrap();
        assert_eq!(last.color, crate::theme::RED);
        assert_eq!(panel.lines.front().unwrap().text, "line 11");
    }
}
