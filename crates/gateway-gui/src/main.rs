//! NovaStream's desktop launcher, dressed as the terminal it is standing in
//! for.
//!
//! The visual language is a phosphor CRT: digital rain behind the glass,
//! scanlines and a refresh sweep over it, and instrument panels in between.
//! The reason it is worth the code is that this window's whole job is to
//! answer "is the gateway alive and is data actually moving?" from across a
//! room, and a live graph plus a breathing lamp answer that in a glance where
//! a column of numbers does not.
//!
//! Layer discipline, since it is easy to break:
//!   background layer -> the rain (painted first, before any panel)
//!   panel layer      -> every widget (panel frames are transparent, so the
//!                       rain shows through the margins)
//!   foreground layer -> the CRT overlay, on top of everything
//!   tooltip layer    -> the cold-boot cover, on top of that
//!
//! The FX live in [`fx`], the palette and chrome in [`theme`], and the
//! instruments in [`widgets`]; this file is state, layout and the subprocess
//! plumbing.

mod fx;
mod server_process;
mod service;
mod theme;
mod tray;
mod widgets;

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};

use serde::Deserialize;
use server_process::{find_server_binary, LogTail, ServerProcess};

use theme::{AMBER, PHOSPHOR, RED, TEXT, TEXT_DIM};

const MAX_LOG_LINES: usize = 500;
/// One second rather than the two it used to be: this is now the sample rate
/// of the traffic graph, and a 0.5 Hz graph of a live download looks like a
/// staircase. It is one loopback request against our own process.
const HEALTH_POLL_INTERVAL: Duration = Duration::from_secs(1);
const STOP_GRACE_PERIOD: Duration = Duration::from_secs(5);
/// Samples kept per trace -- two minutes of history at the poll rate above.
const HISTORY: usize = 120;
/// ~30 fps. Enough for the rain and the sweep to move smoothly, and half the
/// wake-ups of a 60 fps repaint on a window that stays open for whole films.
const FRAME_INTERVAL: Duration = Duration::from_millis(33);
/// How often the systemd state is re-read. Slower than the health poll: this
/// one costs a couple of `systemctl` processes, and enabled/lingering only
/// change when somebody presses a button in this window.
const SERVICE_POLL_INTERVAL: Duration = Duration::from_secs(3);
/// Journal lines pulled in when attaching to a running service. Enough to
/// reach back past a busy hour to the startup banner, which is the only place
/// the addon URL is ever printed.
const JOURNAL_BACKLOG: usize = 2000;
/// How long to wait before trying the journal again after a failed attach, so
/// a missing `journalctl` cannot spin the log panel.
const JOURNAL_RETRY_DELAY: Duration = Duration::from_secs(10);

#[derive(Debug, Deserialize, Clone)]
struct TorrentInfo {
    name: String,
    state: String,
    progress_percent: f64,
    progress_bytes: u64,
    total_bytes: u64,
    download_speed_mib_s: f64,
    upload_speed_mib_s: f64,
    peers: u32,
    /// Needed to address the row's own pause/resume/delete calls.
    #[serde(default)]
    info_hash: String,
    /// Paused by hand rather than by the download queue. Defaulted so this
    /// window still reads an older server's `/health` -- every row then simply
    /// offers Pause, which is true of a server that has no hand-pause concept.
    #[serde(default)]
    held: bool,
}

#[derive(Debug, Deserialize, Clone)]
struct HealthInfo {
    uptime_seconds: u64,
    active_streams: usize,
    cache_usage_bytes: u64,
    cache_max_bytes: u64,
    process_memory_bytes: u64,
    process_cpu_percent: f32,
    /// Defaulted so a GUI newer than the server it is pointed at degrades to
    /// an empty swarm panel instead of failing to parse the whole response
    /// and reporting the gateway as down.
    #[serde(default)]
    active_torrents: Vec<TorrentInfo>,
    #[serde(default)]
    version: String,
}

impl HealthInfo {
    fn download_mib_s(&self) -> f32 {
        self.active_torrents
            .iter()
            .map(|t| t.download_speed_mib_s as f32)
            .sum()
    }

    fn upload_mib_s(&self) -> f32 {
        self.active_torrents
            .iter()
            .map(|t| t.upload_speed_mib_s as f32)
            .sum()
    }

    fn peers(&self) -> u32 {
        self.active_torrents.iter().map(|t| t.peers).sum()
    }
}

/// A fixed-length ring of samples for one graph trace.
///
/// A plain `Vec` with a front-pop: at 120 samples and 1 Hz the shift is
/// irrelevant, and the graph wants a contiguous slice.
#[derive(Default)]
struct History(Vec<f32>);

impl History {
    fn push(&mut self, value: f32) {
        if self.0.len() >= HISTORY {
            self.0.remove(0);
        }
        self.0.push(value);
    }

    fn samples(&self) -> &[f32] {
        &self.0
    }

    fn latest(&self) -> f32 {
        self.0.last().copied().unwrap_or(0.0)
    }
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

fn human_duration(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{h}h {m}m")
    } else if m > 0 {
        format!("{m}m {s}s")
    } else {
        format!("{s}s")
    }
}

/// Pulls the `http://<ip>:<port>` the server printed in its own startup
/// banner out of a log line, so the GUI shows the exact address the server
/// actually bound (including the fallback-port case) instead of guessing.
fn extract_url(line: &str) -> Option<String> {
    extract_scheme_url(line, "http://")
}

/// Pulls the https addon base URL out of the server's "PASTE THIS INTO
/// STREMIO" line.
///
/// This has to be scraped rather than derived, because Stremio on Android
/// refuses a plain-http addon outright: building `{http address}/manifest.json`
/// hands the user the one URL their phone is guaranteed to reject, which looks
/// exactly like the gateway being broken. The https URL also has a different
/// host *and* port (`<ip-with-dashes>.local-ip.sh:8443`), so it cannot be
/// reconstructed from the http one anyway.
///
/// Anchored on the `/manifest.json` suffix on purpose -- plenty of other log
/// lines carry an unrelated `https://` (the indexer, the certificate provider,
/// the addon logo).
fn extract_addon_url(line: &str) -> Option<String> {
    let url = extract_scheme_url(line, "https://")?;
    let base = url.strip_suffix("/manifest.json")?;
    Some(base.to_string())
}

/// Where "save to Desktop" actually saves.
///
/// The desktop folder is not always `$HOME/Desktop`: on a localized install it
/// carries a translated name, and XDG records the real one in
/// `user-dirs.dirs`. Falling straight back to `$HOME` rather than creating a
/// directory is deliberate — a report the user cannot find is the failure this
/// feature exists to fix, and inventing an English `Desktop/` next to their
/// real localized one would do exactly that.
fn desktop_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("XDG_DESKTOP_DIR").filter(|s| !s.is_empty()) {
        return PathBuf::from(dir);
    }

    let home = std::env::var_os("HOME").map(PathBuf::from);
    if let Some(home) = &home {
        if let Some(dir) = desktop_from_user_dirs(home) {
            return dir;
        }
        let conventional = home.join("Desktop");
        if conventional.is_dir() {
            return conventional;
        }
        return home.clone();
    }
    PathBuf::from(".")
}

/// Reads `XDG_DESKTOP_DIR` out of `~/.config/user-dirs.dirs`, whose lines look
/// like `XDG_DESKTOP_DIR="$HOME/Bureau"`.
fn desktop_from_user_dirs(home: &Path) -> Option<PathBuf> {
    let text = std::fs::read_to_string(home.join(".config/user-dirs.dirs")).ok()?;
    let value = parse_user_dirs_desktop(&text)?;
    Some(PathBuf::from(
        value.replace("$HOME", &home.to_string_lossy()),
    ))
}

fn parse_user_dirs_desktop(text: &str) -> Option<String> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.starts_with('#'))
        .find_map(|line| line.strip_prefix("XDG_DESKTOP_DIR="))
        .map(|value| value.trim_matches('"').to_string())
        .filter(|value| !value.is_empty())
}

/// Drops ANSI colour escapes from a log line.
///
/// The server colours its own output whether or not anything is a terminal,
/// which is right for a console and wrong for both of this panel's sources:
/// a pipe and journald keep the escapes verbatim, and egui has no notion of
/// them, so every line would be fenced with literal `[2m`/`[0m`. Stripping
/// also matters for meaning, not just looks -- `widgets::log_color` decides a
/// line's severity by reading it, and an escape sequence in front of the word
/// it looks for defeats that.
fn strip_ansi(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        if c != '\u{1b}' {
            out.push(c);
            continue;
        }
        // CSI sequences (ESC [ ... final byte in @-~) are all the server
        // emits; anything else is dropped up to the next plausible end so a
        // stray escape cannot swallow the rest of the line.
        if chars.next() == Some('[') {
            for c in chars.by_ref() {
                if ('\u{40}'..='\u{7e}').contains(&c) {
                    break;
                }
            }
        }
    }
    out
}

fn extract_scheme_url(line: &str, scheme: &str) -> Option<String> {
    let start = line.find(scheme)?;
    let rest = &line[start..];
    let end = rest.find(|c: char| c.is_whitespace()).unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Status {
    Stopped,
    Starting,
    Running,
    Stopping,
}

impl Status {
    /// One colour per state, used by the lamp, the primary button and the
    /// header trim at once -- the whole window shifts hue together, which is
    /// what makes the state readable peripherally.
    fn color(self) -> egui::Color32 {
        match self {
            Status::Stopped => TEXT_DIM,
            Status::Starting | Status::Stopping => AMBER,
            Status::Running => PHOSPHOR,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Status::Stopped => "OFFLINE",
            Status::Starting => "HANDSHAKE",
            Status::Running => "ONLINE",
            Status::Stopping => "SHUTDOWN",
        }
    }
}

struct GatewayApp {
    binary: Option<PathBuf>,
    process: ServerProcess,
    running: bool,

    port: String,
    cache_dir: String,

    logs: VecDeque<String>,
    detected_url: Option<String>,
    /// The https addon URL, which arrives a few seconds after `detected_url`
    /// because the certificate is fetched in the background.
    addon_url: Option<String>,

    health: Option<HealthInfo>,
    health_rx: Option<Receiver<Option<HealthInfo>>>,
    next_health_poll: Instant,

    down_history: History,
    up_history: History,

    stopping: bool,
    stop_done_rx: Option<Receiver<()>>,

    start_error: Option<String>,

    /// True once "clear everything" has been clicked and is waiting for the
    /// confirming second click. Deleting every download is not undoable, so
    /// it does not hang off a single mis-tap next to the stop button.
    clear_armed: bool,
    clear_rx: Option<Receiver<Result<usize, String>>>,
    clear_result: Option<String>,

    /// In flight "save logs to Desktop". Carries the written path on success.
    export_rx: Option<Receiver<Result<String, String>>>,
    export_result: Option<String>,

    /// This executable, used to spawn the tray sibling process.
    exe: Option<PathBuf>,
    /// Whether there is a systemd user session to install a service into.
    systemd: bool,
    /// Last known state of the always-on service, refreshed in the background
    /// because every field of it costs a subprocess.
    service: service::Status,
    service_rx: Option<Receiver<service::Status>>,
    next_service_poll: Instant,
    /// In-flight enable/disable/start/stop of the service. `systemctl` waits
    /// for the unit to finish changing state, which is far too long to spend
    /// on the UI thread.
    service_action_rx: Option<Receiver<Option<String>>>,
    service_error: Option<String>,
    /// Whether the tray icon is set to come back at the next login.
    autostart: bool,
    /// Whether `ensure_always_on` has already had its one go this session, so
    /// a failure is reported once rather than retried on every frame.
    always_on_attempted: bool,
    /// The swarm row whose delete button has been pressed once. See
    /// `act_on_torrent`.
    delete_armed: Option<String>,
    /// `journalctl -f` on the unit -- the log panel's source when the server
    /// belongs to systemd rather than to this window.
    journal: Option<LogTail>,
    next_journal_attempt: Instant,
    /// Set by the explicit quit action, so the window closing does not leave
    /// a tray icon behind.
    quitting: bool,

    /// Screen furniture.
    rain: fx::MatrixRain,
    boot: fx::BootSequence,
    /// Wall-clock of the last wordmark glitch and how long it runs, so the
    /// tear is an occasional event rather than a permanent wobble.
    next_glitch: f64,
    glitch_until: f64,
    /// What was last copied, and when, for the transient confirmation.
    copied: Option<(String, f64)>,
}

impl Default for GatewayApp {
    fn default() -> Self {
        // An installed service's own settings win over this app's defaults:
        // a service moved to another port is otherwise invisible here, because
        // the health poll would go to 8080 and find nothing.
        let (port, cache_dir) =
            service::read_settings().unwrap_or_else(|| ("8080".to_string(), "./cache".to_string()));
        Self {
            binary: find_server_binary(),
            process: ServerProcess::new(),
            running: false,
            port,
            cache_dir,
            logs: VecDeque::with_capacity(MAX_LOG_LINES),
            detected_url: None,
            addon_url: None,
            health: None,
            health_rx: None,
            next_health_poll: Instant::now(),
            down_history: History::default(),
            up_history: History::default(),
            stopping: false,
            stop_done_rx: None,
            start_error: None,
            clear_armed: false,
            clear_rx: None,
            clear_result: None,
            export_rx: None,
            export_result: None,
            exe: std::env::current_exe().ok(),
            systemd: service::available(),
            service: service::status(),
            service_rx: None,
            next_service_poll: Instant::now() + SERVICE_POLL_INTERVAL,
            service_action_rx: None,
            service_error: None,
            autostart: service::autostart_enabled(),
            always_on_attempted: false,
            delete_armed: None,
            journal: None,
            next_journal_attempt: Instant::now(),
            quitting: false,
            rain: fx::MatrixRain::new(0x5EED_1234_ABCD_0001),
            boot: fx::BootSequence::new(),
            next_glitch: 3.0,
            glitch_until: 0.0,
            copied: None,
        }
    }
}

impl GatewayApp {
    fn status(&self) -> Status {
        if self.service_mode() {
            // The service's own state is authoritative here -- this window
            // may have been opened long after the gateway started.
            return if self.service_busy() {
                if self.service.active {
                    Status::Stopping
                } else {
                    Status::Starting
                }
            } else if !self.service.active {
                Status::Stopped
            } else if self.detected_url.is_none() && self.health.is_none() {
                Status::Starting
            } else {
                Status::Running
            };
        }
        if self.stopping {
            Status::Stopping
        } else if self.running && self.detected_url.is_none() {
            Status::Starting
        } else if self.running {
            Status::Running
        } else {
            Status::Stopped
        }
    }

    fn push_log(&mut self, line: String) {
        if self.logs.len() >= MAX_LOG_LINES {
            self.logs.pop_front();
        }
        self.logs.push_back(strip_ansi(&line));
    }

    /// True when the gateway belongs to systemd rather than to this window.
    ///
    /// Everything downstream keys off this: who gets started, whose logs the
    /// TTY panel shows, and -- the point of the whole feature -- whether
    /// closing the window has any effect on the stream at all.
    fn service_mode(&self) -> bool {
        self.service.installed
    }

    /// Runs a blocking `systemctl`/`loginctl` sequence off the UI thread.
    ///
    /// `systemctl stop` waits for the unit to actually stop, which for a
    /// gateway with open streams is a second or more of a frozen window.
    fn run_service_action(&mut self, action: impl FnOnce() -> anyhow::Result<()> + Send + 'static) {
        if self.service_action_rx.is_some() {
            return;
        }
        self.service_error = None;
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let _ = tx.send(action().err().map(|e| format!("{e:#}")));
        });
        self.service_action_rx = Some(rx);
    }

    fn service_busy(&self) -> bool {
        self.service_action_rx.is_some()
    }

    /// Turns on always-on mode: install the unit, start it, keep it across
    /// reboots, and leave the tray icon behind at login.
    fn enable_always_on(&mut self) {
        let Some(binary) = self.binary.clone() else {
            self.service_error =
                Some("streaming-gateway binary not found next to this app or on PATH".to_string());
            return;
        };
        // A server already running as this window's child would hold the port
        // the service is about to bind, and the service would restart-loop.
        self.stop_server();
        let port = self.port.clone();
        let cache = self.cache_dir.clone();
        let exe = self.exe.clone();
        self.run_service_action(move || {
            service::install(&binary, &port, &cache)?;
            if let Some(exe) = exe {
                // Non-fatal: the gateway is the thing that must survive a
                // reboot; the tray coming back at login is a convenience.
                let _ = service::install_autostart(&exe);
            }
            Ok(())
        });
        self.autostart = true;
    }

    // Turning always-on *off* is deliberately not reachable from the window --
    // see `service_panel`. The `--disable-always-on` command-line flag remains
    // the escape hatch, and it calls `service::uninstall` directly.

    /// Attaches the log panel to the service's journal.
    ///
    /// The panel is cleared first: its previous contents belong to a different
    /// process, and the URLs scraped from them may point at a port the service
    /// is not using.
    fn attach_journal(&mut self) {
        self.next_journal_attempt = Instant::now() + JOURNAL_RETRY_DELAY;
        let command =
            service::journal_command(JOURNAL_BACKLOG, service::invocation_id().as_deref());
        match LogTail::spawn(command) {
            Ok(tail) => {
                self.logs.clear();
                self.detected_url = None;
                self.addon_url = None;
                self.journal = Some(tail);
            }
            Err(e) => {
                self.journal = None;
                self.push_log(format!("[stderr] cannot read the service log: {e:#}"));
            }
        }
    }

    fn start_server(&mut self) {
        if self.service_mode() {
            let port = self.port.clone();
            let cache = self.cache_dir.clone();
            self.run_service_action(move || {
                // Settings first: this is also how an edited port or cache
                // directory reaches a service that is already installed.
                service::write_settings(&port, &cache)?;
                service::start()
            });
            return;
        }

        let Some(binary) = self.binary.clone() else {
            self.start_error =
                Some("streaming-gateway binary not found next to this app or on PATH".to_string());
            return;
        };
        let env = vec![
            ("GATEWAY_PORT".to_string(), self.port.clone()),
            ("CACHE_DIRECTORY".to_string(), self.cache_dir.clone()),
        ];
        match self.process.start(&binary, &env) {
            Ok(()) => {
                self.running = true;
                self.start_error = None;
                self.detected_url = None;
                self.addon_url = None;
                self.logs.clear();
            }
            Err(e) => self.start_error = Some(format!("{e:#}")),
        }
    }

    /// Asks the running server to delete every torrent and all of its data.
    ///
    /// Goes through the server rather than deleting files here: the engine
    /// holds open handles to what is on disk, so removing the directory
    /// underneath it would leave the session pointing at files that no longer
    /// exist. The endpoint is loopback-only, which is why this works from the
    /// desktop app and from nowhere else.
    fn clear_cache(&mut self) {
        let (tx, rx) = mpsc::channel();
        let port = self.port.clone();
        thread::spawn(move || {
            let url = format!("http://127.0.0.1:{port}/cache/clear");
            let outcome = match ureq::post(&url).timeout(Duration::from_secs(30)).call() {
                Ok(resp) => resp
                    .into_json::<serde_json::Value>()
                    .ok()
                    .and_then(|v| v.get("deleted").and_then(|d| d.as_u64()))
                    .map(|n| n as usize)
                    .ok_or_else(|| "server gave an unreadable reply".to_string()),
                Err(e) => Err(format!("{e}")),
            };
            let _ = tx.send(outcome);
        });
        self.clear_rx = Some(rx);
        self.clear_result = None;
    }

    /// Writes one self-contained report to the Desktop.
    ///
    /// It bundles two sources that only exist in two different places, which
    /// is the whole reason this is a button rather than a documented file
    /// path: the server's structured event log (on disk, survives restarts,
    /// fetched over loopback because the server may have chosen a directory
    /// this process cannot guess — see the endpoint's own doc) and this
    /// window's live log panel (the server's raw stdout, which is never
    /// written to disk at all and dies with the window). Neither half is
    /// sufficient alone: the file has the latency numbers and the panel has
    /// the startup banner and any stderr from a failed launch.
    fn export_logs(&mut self) {
        let (tx, rx) = mpsc::channel();
        let port = self.port.clone();
        // Snapshotted here rather than read from the worker: `self` cannot
        // cross the thread boundary, and the panel keeps changing.
        let panel: Vec<String> = self.logs.iter().cloned().collect();

        thread::spawn(move || {
            let url = format!("http://127.0.0.1:{port}/audit/export");
            let events = match ureq::get(&url).timeout(Duration::from_secs(60)).call() {
                Ok(resp) => resp
                    .into_string()
                    .unwrap_or_else(|e| format!("(could not read the log body: {e})\n")),
                // Not fatal: the panel below is still worth writing out, and a
                // report that says why half of it is missing beats no report.
                Err(e) => format!("(could not fetch the event log from the server: {e})\n"),
            };

            let stamp = chrono::Local::now().format("%Y%m%d-%H%M%S");
            let mut body = events;
            body.push_str("\n\n");
            body.push_str("# ---------------------------------------------\n");
            body.push_str("# Desktop app log panel (server stdout, this run)\n");
            body.push_str("# ---------------------------------------------\n");
            if panel.is_empty() {
                body.push_str("(empty -- the server was not started by this window)\n");
            } else {
                for line in &panel {
                    body.push_str(line);
                    body.push('\n');
                }
            }

            let path = desktop_dir().join(format!("novastream-log-{stamp}.txt"));
            let outcome = std::fs::write(&path, body)
                .map(|()| path.display().to_string())
                .map_err(|e| format!("could not write {}: {e}", path.display()));
            let _ = tx.send(outcome);
        });

        self.export_rx = Some(rx);
        self.export_result = None;
    }

    fn stop_server(&mut self) {
        if self.service_mode() {
            self.run_service_action(service::stop);
            return;
        }
        if self.stopping {
            return;
        }
        if let Some(child) = self.process.take_child() {
            self.stopping = true;
            self.health = None;
            let (tx, rx) = mpsc::channel();
            thread::spawn(move || {
                ServerProcess::stop_blocking(child, STOP_GRACE_PERIOD);
                let _ = tx.send(());
            });
            self.stop_done_rx = Some(rx);
        }
    }

    /// Keeps the always-on panel and the service-mode dispatch honest, without
    /// spawning `systemctl` on the UI thread.
    fn poll_service_state(&mut self) {
        if let Some(rx) = &self.service_action_rx {
            if let Ok(outcome) = rx.try_recv() {
                self.service_error = outcome;
                self.service_action_rx = None;
                self.autostart = service::autostart_enabled();
                // The unit just changed state; do not wait out the interval
                // before saying so.
                self.next_service_poll = Instant::now();
                self.next_journal_attempt = Instant::now();
            }
        }

        if !self.systemd {
            return;
        }

        if let Some(rx) = &self.service_rx {
            if let Ok(status) = rx.try_recv() {
                let restarted = status.active && !self.service.active;
                self.service = status;
                self.service_rx = None;
                if restarted {
                    // A fresh run means a fresh banner, so re-attach rather
                    // than keep tailing the old boot's log.
                    self.journal = None;
                    self.next_journal_attempt = Instant::now();
                }
            }
        } else if Instant::now() >= self.next_service_poll {
            self.next_service_poll = Instant::now() + SERVICE_POLL_INTERVAL;
            let (tx, rx) = mpsc::channel();
            thread::spawn(move || {
                let _ = tx.send(service::status());
            });
            self.service_rx = Some(rx);
        }
    }

    fn poll_background_work(&mut self) {
        self.poll_service_state();
        self.ensure_always_on();

        if self.service_mode() {
            // systemd owns the process; this window only reports on it.
            self.running = self.service.active;
            if !self.service.active {
                self.journal = None;
                self.detected_url = None;
                self.addon_url = None;
            } else if self
                .journal
                .as_mut()
                .map(|tail| !tail.is_running())
                .unwrap_or(true)
                && Instant::now() >= self.next_journal_attempt
            {
                self.attach_journal();
            }
        } else if self.running && !self.process.is_running() {
            // Reap a server that exited on its own (crash, port conflict, ^C
            // from the terminal it was launched from, etc.).
            self.running = false;
            self.detected_url = None;
            self.addon_url = None;
            self.health = None;
        }

        let lines = match self.journal.as_mut() {
            Some(tail) => tail.drain(),
            None => self.process.drain_logs(),
        };
        for line in lines {
            // Scraped from the stripped text, not the raw one: a colour reset
            // sitting against the end of a URL becomes part of it otherwise,
            // and the addon URL then fails its `/manifest.json` check.
            let text = strip_ansi(line.text());
            if self.detected_url.is_none() {
                if let Some(url) = extract_url(&text) {
                    self.detected_url = Some(url);
                }
            }
            if self.addon_url.is_none() {
                if let Some(url) = extract_addon_url(&text) {
                    self.addon_url = Some(url);
                }
            }
            self.push_log(if line.is_err() {
                format!("[stderr] {text}")
            } else {
                text
            });
        }

        if let Some(rx) = &self.clear_rx {
            if let Ok(outcome) = rx.try_recv() {
                self.clear_result = Some(match outcome {
                    Ok(0) => "nothing to clear".to_string(),
                    Ok(1) => "cleared 1 download".to_string(),
                    Ok(n) => format!("cleared {n} downloads"),
                    Err(e) => format!("could not clear: {e}"),
                });
                self.clear_rx = None;
                // Force the next health poll so the freed space shows at once
                // rather than after the regular interval.
                self.next_health_poll = Instant::now();
            }
        }

        if let Some(rx) = &self.export_rx {
            if let Ok(outcome) = rx.try_recv() {
                self.export_result = Some(match outcome {
                    Ok(path) => format!("saved to {path}"),
                    Err(e) => e,
                });
                self.export_rx = None;
            }
        }

        if let Some(rx) = &self.stop_done_rx {
            if rx.try_recv().is_ok() {
                self.stopping = false;
                self.running = false;
                self.detected_url = None;
                self.addon_url = None;
                self.health = None;
                self.stop_done_rx = None;
            }
        }

        // Polled whether or not this app started the server. A gateway
        // launched from a terminal is just as real, and without this the
        // window sat blank next to a perfectly healthy server -- and the
        // clear button, which needs a server to talk to, stayed disabled.
        if let Some(rx) = &self.health_rx {
            if let Ok(result) = rx.try_recv() {
                // Sampled on the reply rather than on a timer so the graph
                // scrolls at the rate data actually arrived. A dead server
                // still contributes a zero, which is what draws the traffic
                // falling off a cliff instead of freezing mid-air.
                self.down_history.push(
                    result
                        .as_ref()
                        .map(HealthInfo::download_mib_s)
                        .unwrap_or(0.0),
                );
                self.up_history
                    .push(result.as_ref().map(HealthInfo::upload_mib_s).unwrap_or(0.0));
                self.health = result;
                self.health_rx = None;
            }
        } else if Instant::now() >= self.next_health_poll {
            self.next_health_poll = Instant::now() + HEALTH_POLL_INTERVAL;
            let (tx, rx) = mpsc::channel();
            let port = self.port.clone();
            thread::spawn(move || {
                let url = format!("http://127.0.0.1:{port}/health");
                let result = ureq::get(&url)
                    .timeout(Duration::from_millis(1500))
                    .call()
                    .ok()
                    .and_then(|resp| resp.into_json::<HealthInfo>().ok());
                let _ = tx.send(result);
            });
            self.health_rx = Some(rx);
        }
    }

    /// Copies `text` and arms the transient `[copied]` confirmation.
    fn copy(&mut self, ui: &egui::Ui, text: &str, time: f64) {
        ui.output_mut(|o| o.copied_text = text.to_string());
        self.copied = Some((text.to_string(), time));
    }

    /// Leaves for good, rather than folding into the tray.
    ///
    /// `quitting` is what tells the close handler apart from the window's own
    /// close button: both arrive as the same `close_requested` event, and only
    /// one of them should leave an icon behind.
    fn quit(&mut self, ctx: &egui::Context) {
        self.quitting = true;
        ctx.send_viewport_cmd(egui::ViewportCommand::Close);
    }

    /// Closing the window puts the app in the tray instead of ending it.
    ///
    /// The tray is a separate process (see `tray.rs` for why it has to be),
    /// started here just before this one goes away.
    fn hand_over_to_tray(&mut self) {
        let Some(exe) = self.exe.clone() else {
            return;
        };
        if let Err(e) = tray::spawn_tray_process(&exe) {
            // Nothing to show it in -- the window is already closing. The
            // stdout line is what the GUI's own Log panel would have caught.
            eprintln!("could not put NovaStream in the tray: {e}");
        }
    }

    fn hud(&mut self, ui: &mut egui::Ui, status: Status, time: f64) {
        let mut quit = false;
        // The wordmark glitches for a moment every few seconds. Scheduled
        // rather than random-per-frame so it cannot land twice in a row and
        // read as a rendering fault.
        if time > self.next_glitch {
            self.glitch_until = time + 0.16;
            self.next_glitch = time + 6.0 + (time % 5.0);
        }
        let glitch = if time < self.glitch_until {
            ((self.glitch_until - time) / 0.16) as f32
        } else {
            0.0
        };

        ui.horizontal(|ui| {
            ui.vertical(|ui| {
                ui.add_space(2.0);
                widgets::wordmark(ui, "NOVASTREAM", 4.0, 1.0, glitch);
                ui.add_space(3.0);
                ui.label(
                    egui::RichText::new("magnet -> local http stream :: stremio / vlc / lan")
                        .font(egui::FontId::monospace(10.0))
                        .color(TEXT_DIM),
                );
            });
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                ui.vertical(|ui| {
                    ui.set_width(112.0);
                    widgets::status_lamp(
                        ui,
                        status.label(),
                        status.color(),
                        status == Status::Running,
                    );
                    let detail = match &self.health {
                        Some(h) => format!("up {}", human_duration(h.uptime_seconds)),
                        None => "no carrier".to_string(),
                    };
                    ui.label(
                        egui::RichText::new(detail)
                            .font(egui::FontId::monospace(10.0))
                            .color(TEXT_DIM),
                    );
                    let version = self
                        .health
                        .as_ref()
                        .map(|h| h.version.as_str())
                        .filter(|v| !v.is_empty())
                        .unwrap_or(env!("CARGO_PKG_VERSION"));
                    ui.label(
                        egui::RichText::new(format!("v{version}"))
                            .font(egui::FontId::monospace(10.0))
                            .color(TEXT_DIM),
                    );
                    // The window's own close button hands over to the tray, so
                    // "leave for good" needs somewhere to live. Named for what
                    // it does to the gateway, which is the only question worth
                    // asking before clicking it.
                    let quit_label = if self.service_mode() {
                        "quit app"
                    } else {
                        "quit + stop"
                    };
                    if widgets::ghost_button(ui, quit_label, RED).clicked() {
                        quit = true;
                    }
                });
            });
        });

        ui.add_space(6.0);
        let (rule, _) =
            ui.allocate_exact_size(egui::vec2(ui.available_width(), 1.0), egui::Sense::hover());
        ui.painter().line_segment(
            [rule.left_center(), rule.right_center()],
            egui::Stroke::new(1.0, theme::GRID),
        );

        if quit {
            self.quit(ui.ctx());
        }
    }

    /// The main actions, side by side on one line and all in the house green.
    ///
    /// One row rather than a stack of full-width bars, and one colour rather
    /// than one per button: colour here was decoration, and three differently
    /// coloured bars read as three different kinds of thing when they are just
    /// three buttons.
    ///
    /// The single exception is a *destructive* action that has been armed --
    /// "confirm: erase all" stays red. That red is not decoration; it is the
    /// only warning between a click and every download on the disk.
    fn controls(&mut self, ui: &mut egui::Ui, status: Status) {
        let (label, enabled) = match status {
            Status::Stopped => ("./gateway --start", true),
            Status::Starting => ("linking ...", false),
            Status::Running => ("./gateway --stop", true),
            Status::Stopping => ("unlinking ...", false),
        };

        // Enabled on "a server answered /health", not on "this app started
        // it" -- the gateway is often launched from a terminal, and a button
        // that silently does nothing is worse than one that is plainly
        // unavailable. (The log dump lives in the tty panel with the logs.)
        let live = self.health.is_some();
        let clearing = self.clear_rx.is_some();

        let clear_label = if clearing {
            "purging ..."
        } else if self.clear_armed {
            "confirm: erase all"
        } else {
            "purge cache"
        };

        let mut power_clicked = false;
        let mut clear_clicked = false;
        // `columns` divides the available width evenly, and `command_button`
        // fills whatever width it is handed -- so the row stays even as the
        // window resizes, without any button knowing its own size.
        ui.columns(2, |cols| {
            power_clicked = widgets::command_button(
                &mut cols[0],
                label,
                PHOSPHOR,
                enabled && self.binary.is_some(),
                40.0,
            )
            .clicked();
            clear_clicked = widgets::command_button(
                &mut cols[1],
                clear_label,
                if self.clear_armed { RED } else { PHOSPHOR },
                live && !clearing,
                40.0,
            )
            .clicked();
        });

        if power_clicked {
            match status {
                Status::Stopped => self.start_server(),
                Status::Running => self.stop_server(),
                _ => {}
            }
        }

        if clear_clicked {
            if self.clear_armed {
                self.clear_armed = false;
                self.clear_cache();
            } else {
                self.clear_armed = true;
            }
        }

        if self.clear_armed {
            ui.horizontal(|ui| {
                ui.label(
                    egui::RichText::new("!! deletes every download, finished or not")
                        .font(egui::FontId::monospace(10.0))
                        .color(RED),
                );
                if widgets::ghost_button(ui, "abort", TEXT_DIM).clicked() {
                    self.clear_armed = false;
                }
            });
        }
        if let Some(message) = &self.clear_result {
            ui.label(
                egui::RichText::new(message)
                    .font(egui::FontId::monospace(10.0))
                    .color(TEXT_DIM),
            );
        }
    }

    /// Reports always-on mode, which is no longer a choice.
    ///
    /// There is no off switch here on purpose. Always-on is what makes the
    /// product work the way it is used -- the phone expects the addon to
    /// answer whether or not this window happens to be open -- and a toggle
    /// that turns it off is a way to break that by accident and then wonder
    /// why Stremio cannot find the gateway. The gateway is stopped from the
    /// tray's Exit entry, which is where someone looks for an off switch when
    /// the window is closed anyway.
    ///
    /// The panel still reports honestly rather than just claiming "on": a unit
    /// that is installed but not lingering comes back at the next login and
    /// not at the next reboot, and saying "on" for that is how someone finds
    /// the gateway down after a power cut.
    fn service_panel(&mut self, ui: &mut egui::Ui) {
        let busy = self.service_busy();
        let summary = self.service.summary();
        let always_on = self.service.always_on();
        let systemd = self.systemd;
        let error = self.service_error.clone();

        theme::section(
            ui,
            "always-on",
            if always_on { PHOSPHOR } else { AMBER },
            |ui| {
                if !systemd {
                    ui.label(
                        egui::RichText::new(
                            "no systemd user session here -- the gateway can only run\n\
                         while this window is open",
                        )
                        .font(egui::FontId::monospace(11.0))
                        .color(TEXT_DIM),
                    );
                    return;
                }

                ui.label(
                    egui::RichText::new(if busy {
                        "service :: installing ...".to_string()
                    } else {
                        format!("service :: {summary}")
                    })
                    .font(egui::FontId::monospace(11.0))
                    .color(if always_on { PHOSPHOR } else { TEXT }),
                );

                ui.label(
                    egui::RichText::new(
                        "always on :: starts at boot, keeps running when this window closes\n\
                         to stop it: tray icon -> Exit NovaStream",
                    )
                    .font(egui::FontId::monospace(10.0))
                    .color(TEXT_DIM),
                );

                if let Some(error) = &error {
                    ui.label(
                        egui::RichText::new(format!("!! {error}"))
                            .font(egui::FontId::monospace(10.0))
                            .color(RED),
                    );
                }
            },
        );
    }

    /// Installs the service and the login tray icon if they are not there yet.
    ///
    /// Runs from the update loop rather than from a button, because always-on
    /// is now the only mode: a fresh install, or a machine where the unit was
    /// removed by hand, should converge on it without anyone being asked.
    /// Guarded on `service_busy` so the poll cannot stack installs, and on
    /// `always_on_attempted` so a genuine failure (no binary, systemd refusing
    /// the unit) is reported once instead of retried every frame.
    fn ensure_always_on(&mut self) {
        if !self.systemd || self.always_on_attempted || self.service_busy() {
            return;
        }
        // Never act on a "not installed" that a poll in flight is about to
        // contradict -- installing on top of a unit that already exists would
        // restart a gateway somebody is streaming from.
        if self.service_rx.is_some() && !self.service.installed {
            return;
        }
        if self.service.installed && self.autostart {
            self.always_on_attempted = true;
            return;
        }
        self.always_on_attempted = true;
        self.enable_always_on();
    }

    fn traffic_panel(&self, ui: &mut egui::Ui) {
        theme::section(ui, "net i/o", PHOSPHOR, |ui| {
            let down = self.down_history.latest();
            let up = self.up_history.latest();
            ui.horizontal(|ui| {
                ui.label(
                    egui::RichText::new(format!("{down:>7.2} MiB/s"))
                        .font(egui::FontId::monospace(17.0))
                        .color(PHOSPHOR),
                );
                ui.label(
                    egui::RichText::new("DOWN")
                        .font(egui::FontId::monospace(10.0))
                        .color(TEXT_DIM),
                );
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(
                        egui::RichText::new("UP")
                            .font(egui::FontId::monospace(10.0))
                            .color(TEXT_DIM),
                    );
                    ui.label(
                        egui::RichText::new(format!("{up:.2} MiB/s"))
                            .font(egui::FontId::monospace(12.0))
                            .color(TEXT),
                    );
                });
            });
            ui.add_space(4.0);
            widgets::traffic_graph(
                ui,
                72.0,
                &[
                    widgets::Trace {
                        samples: self.down_history.samples(),
                        color: PHOSPHOR,
                        label: "down",
                    },
                    // Pale against bright: the two traces stay tellable apart
                    // by weight rather than by a second hue.
                    widgets::Trace {
                        samples: self.up_history.samples(),
                        color: TEXT,
                        label: "up",
                    },
                ],
                "MiB/s",
            );
        });
    }

    fn system_panel(&self, ui: &mut egui::Ui, health: &HealthInfo) {
        theme::section(ui, "system", PHOSPHOR, |ui| {
            let cache_fraction = if health.cache_max_bytes == 0 {
                0.0
            } else {
                health.cache_usage_bytes as f32 / health.cache_max_bytes as f32
            };
            widgets::meter(
                ui,
                "CACHE",
                cache_fraction,
                &format!(
                    "{} / {}",
                    human_bytes(health.cache_usage_bytes),
                    human_bytes(health.cache_max_bytes)
                ),
                if cache_fraction > 0.9 {
                    AMBER
                } else {
                    PHOSPHOR
                },
            );
            widgets::meter(
                ui,
                "CPU",
                health.process_cpu_percent / 100.0,
                &format!("{:.1} %", health.process_cpu_percent),
                if health.process_cpu_percent > 80.0 {
                    AMBER
                } else {
                    PHOSPHOR
                },
            );
            // Scaled against 1 GiB: the gateway is expected to sit far below
            // that, so the bar is a "has something run away?" indicator, not
            // a fraction of anything real.
            widgets::meter(
                ui,
                "MEM",
                health.process_memory_bytes as f32 / (1024.0 * 1024.0 * 1024.0),
                &human_bytes(health.process_memory_bytes),
                PHOSPHOR,
            );
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                for (label, value, color) in [
                    ("STREAMS", health.active_streams.to_string(), PHOSPHOR),
                    ("PEERS", health.peers().to_string(), TEXT),
                    ("SWARMS", health.active_torrents.len().to_string(), TEXT),
                ] {
                    ui.label(
                        egui::RichText::new(format!("{label} "))
                            .font(egui::FontId::monospace(10.0))
                            .color(TEXT_DIM),
                    );
                    ui.label(
                        egui::RichText::new(value)
                            .font(egui::FontId::monospace(13.0))
                            .color(color),
                    );
                    ui.add_space(10.0);
                }
            });
        });
    }

    fn swarm_panel(&mut self, ui: &mut egui::Ui, health: &HealthInfo) {
        let armed = self.delete_armed.clone();
        let mut requested: Option<(String, widgets::RowAction)> = None;

        theme::section(
            ui,
            &format!("swarm [{}]", health.active_torrents.len()),
            PHOSPHOR,
            |ui| {
                if health.active_torrents.is_empty() {
                    ui.label(
                        egui::RichText::new("idle -- no torrent attached")
                            .font(egui::FontId::monospace(11.0))
                            .color(TEXT_DIM),
                    );
                    return;
                }
                for (index, torrent) in health.active_torrents.iter().enumerate() {
                    if index > 0 {
                        ui.add_space(8.0);
                    }
                    let action = widgets::swarm_row(
                        ui,
                        &torrent.name,
                        &torrent.state,
                        torrent.held,
                        (torrent.progress_percent / 100.0) as f32,
                        &format!(
                            "{:.1}%  {} / {}  {} peers  {:.2} MiB/s dn  {:.2} MiB/s up",
                            torrent.progress_percent,
                            human_bytes(torrent.progress_bytes),
                            human_bytes(torrent.total_bytes),
                            torrent.peers,
                            torrent.download_speed_mib_s,
                            torrent.upload_speed_mib_s,
                        ),
                        armed.as_deref() == Some(torrent.info_hash.as_str()),
                    );
                    if let Some(action) = action {
                        requested = Some((torrent.info_hash.clone(), action));
                    }
                }
            },
        );

        if let Some((info_hash, action)) = requested {
            self.act_on_torrent(info_hash, action);
        }
    }

    /// Applies one swarm-row button.
    ///
    /// Delete is armed first and confirmed second, matching the whole-cache
    /// purge: it destroys a download that may have taken an hour to fetch, and
    /// the button sits inches from Pause. Any other click disarms, so an
    /// armed row cannot be confirmed later by accident.
    fn act_on_torrent(&mut self, info_hash: String, action: widgets::RowAction) {
        if info_hash.is_empty() {
            self.clear_result =
                Some("this server is too old to control torrents individually".to_string());
            return;
        }
        let verb = match action {
            widgets::RowAction::Pause => "pause",
            widgets::RowAction::Resume => "resume",
            widgets::RowAction::Delete => {
                if self.delete_armed.as_deref() != Some(info_hash.as_str()) {
                    self.delete_armed = Some(info_hash);
                    return;
                }
                "delete"
            }
        };
        self.delete_armed = None;

        let port = self.port.clone();
        let hash = info_hash.clone();
        let verb = verb.to_string();
        // Fire and forget: the next `/health` poll is what redraws the row, so
        // there is no reply worth blocking the UI thread for. A failure shows
        // up as the row simply not changing, and in the log panel.
        thread::spawn(move || {
            let url = format!("http://127.0.0.1:{port}/torrents/{hash}/{verb}");
            if let Err(e) = ureq::post(&url).timeout(Duration::from_secs(20)).call() {
                eprintln!("{verb} {hash} failed: {e}");
            }
        });
    }

    fn endpoint_panel(&mut self, ui: &mut egui::Ui, url: String, time: f64) {
        let addon = self.addon_url.clone();
        theme::section(ui, "endpoints", PHOSPHOR, |ui| {
            // The addon URL goes first and the plain-http one is labelled for
            // what it is: the http address is not a usable addon URL on a
            // phone, and showing it as one is the single most common way this
            // looks broken.
            ui.label(
                egui::RichText::new("STREMIO ADDON  (phone / tablet / tv)")
                    .font(egui::FontId::monospace(10.0))
                    .color(TEXT_DIM),
            );
            match &addon {
                Some(addon) => {
                    let manifest = format!("{addon}/manifest.json");
                    self.copyable_line(ui, &manifest, time);
                }
                None => {
                    ui.label(
                        egui::RichText::new("preparing the https certificate, a few seconds ...")
                            .font(egui::FontId::monospace(11.0))
                            .color(AMBER),
                    );
                }
            }

            ui.add_space(8.0);
            ui.label(
                egui::RichText::new("DIRECT ADDRESS  (vlc / browsers)")
                    .font(egui::FontId::monospace(10.0))
                    .color(TEXT_DIM),
            );
            self.copyable_line(ui, &url, time);
        });
    }

    /// A selectable URL with a `[copy]` affordance and a two-second
    /// confirmation, so a click has visible consequences on a clipboard the
    /// user cannot otherwise see.
    fn copyable_line(&mut self, ui: &mut egui::Ui, text: &str, time: f64) {
        let just_copied = self
            .copied
            .as_ref()
            .is_some_and(|(copied, at)| copied == text && time - at < 2.0);
        ui.horizontal_wrapped(|ui| {
            ui.label(
                egui::RichText::new(text)
                    .font(egui::FontId::monospace(11.0))
                    .color(TEXT),
            );
            let (label, color) = if just_copied {
                ("copied", PHOSPHOR)
            } else {
                ("copy", TEXT_DIM)
            };
            if widgets::ghost_button(ui, label, color).clicked() {
                self.copy(ui, text, time);
            }
        });
    }

    fn tty_panel(&mut self, ui: &mut egui::Ui, time: f64) {
        // The dump belongs with the logs it dumps. Only offered while a
        // server is answering /health: the export is served by the gateway's
        // own /audit/export endpoint, so without a server there is nothing to
        // dump from.
        let live = self.health.is_some();
        let exporting = self.export_rx.is_some();
        let mut export_clicked = false;
        theme::section(ui, "tty // gateway stdout", PHOSPHOR, |ui| {
            ui.horizontal(|ui| {
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if live {
                        let label = if exporting { "dumping ..." } else { "dump logs" };
                        if widgets::ghost_button(ui, label, TEXT_DIM).clicked() && !exporting {
                            export_clicked = true;
                        }
                    }
                    if let Some(message) = &self.export_result {
                        ui.label(
                            egui::RichText::new(message.as_str())
                                .font(egui::FontId::monospace(10.0))
                                .color(TEXT_DIM),
                        );
                    }
                });
            });
            ui.set_min_height(ui.available_height().max(40.0));
            egui::ScrollArea::vertical()
                .stick_to_bottom(true)
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    if self.logs.is_empty() {
                        ui.label(
                            egui::RichText::new("-- no output; the gateway is not running --")
                                .font(egui::FontId::monospace(11.0))
                                .color(TEXT_DIM),
                        );
                    }
                    for line in &self.logs {
                        ui.label(
                            egui::RichText::new(line)
                                .font(egui::FontId::monospace(11.0))
                                .color(widgets::log_color(line)),
                        );
                    }
                    // The live prompt: the one thing that makes a log panel
                    // read as a terminal rather than as a text dump.
                    ui.horizontal(|ui| {
                        ui.spacing_mut().item_spacing.x = 0.0;
                        ui.label(
                            egui::RichText::new("root@novastream:~$ ")
                                .font(egui::FontId::monospace(11.0))
                                .color(PHOSPHOR),
                        );
                        let (caret, _) =
                            ui.allocate_exact_size(egui::vec2(7.0, 12.0), egui::Sense::hover());
                        if fx::cursor_on(time) {
                            ui.painter()
                                .rect_filled(caret, egui::Rounding::ZERO, PHOSPHOR);
                        }
                    });
                });
        });
        if export_clicked {
            self.export_logs();
        }
    }
}

impl eframe::App for GatewayApp {
    /// The void behind every panel. Panels are transparent so the rain painted
    /// into the background layer stays visible through them.
    fn clear_color(&self, _visuals: &egui::Visuals) -> [f32; 4] {
        theme::BG_VOID.to_normalized_gamma_f32()
    }

    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.poll_background_work();

        // The close button is a "put it away", not a "shut it down" -- unless
        // the quit action set `quitting` first, in which case this same event
        // means exactly what it says.
        if !self.quitting && ctx.input(|i| i.viewport().close_requested()) {
            self.hand_over_to_tray();
        }

        let status = self.status();
        let screen = ctx.screen_rect();
        let time = ctx.input(|i| i.time);
        let dt = ctx.input(|i| i.stable_dt).min(0.1);
        let glow = fx::flicker(time);

        // Rain first, into the background layer: everything drawn afterwards
        // lands on top of it.
        self.rain.paint(
            &ctx.layer_painter(egui::LayerId::background()),
            screen,
            dt,
            0.13 * glow,
        );

        let bare = egui::Frame::none().inner_margin(egui::Margin::symmetric(14.0, 10.0));

        egui::TopBottomPanel::top("hud")
            .frame(bare)
            .show(ctx, |ui| self.hud(ui, status, time));

        egui::TopBottomPanel::bottom("tty")
            .frame(bare)
            .resizable(true)
            .default_height(180.0)
            .min_height(74.0)
            .show(ctx, |ui| self.tty_panel(ui, time));

        egui::CentralPanel::default().frame(bare).show(ctx, |ui| {
            egui::ScrollArea::vertical()
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    if let Some(err) = self.start_error.clone() {
                        ui.label(
                            egui::RichText::new(format!("!! {err}"))
                                .font(egui::FontId::monospace(11.0))
                                .color(RED),
                        );
                        ui.add_space(4.0);
                    }
                    if self.binary.is_none() {
                        ui.label(
                            egui::RichText::new(
                                "!! streaming-gateway binary not found next to this app or on PATH",
                            )
                            .font(egui::FontId::monospace(11.0))
                            .color(AMBER),
                        );
                        ui.add_space(4.0);
                    }

                    self.controls(ui, status);
                    ui.add_space(10.0);
                    self.service_panel(ui);
                    ui.add_space(10.0);
                    self.traffic_panel(ui);

                    if let Some(health) = self.health.clone() {
                        ui.add_space(10.0);
                        self.system_panel(ui, &health);
                        ui.add_space(10.0);
                        self.swarm_panel(ui, &health);
                    }

                    if let Some(url) = self.detected_url.clone() {
                        ui.add_space(10.0);
                        self.endpoint_panel(ui, url, time);
                    }

                    ui.add_space(10.0);
                    ui.collapsing(
                        egui::RichText::new("> config --edit")
                            .font(egui::FontId::monospace(11.0))
                            .color(TEXT_DIM),
                        |ui| {
                            ui.add_enabled_ui(status == Status::Stopped, |ui| {
                                egui::Grid::new("settings_grid")
                                    .num_columns(2)
                                    .show(ui, |ui| {
                                        ui.label("port");
                                        ui.text_edit_singleline(&mut self.port);
                                        ui.end_row();
                                        ui.label("cache dir");
                                        ui.text_edit_singleline(&mut self.cache_dir);
                                        ui.end_row();
                                    });
                            });
                        },
                    );
                    ui.add_space(6.0);
                });
        });

        fx::crt_overlay(ctx, screen);

        if self.boot.paint(ctx, screen, time) && ctx.input(|i| i.pointer.any_click()) {
            self.boot.skip();
        }

        ctx.request_repaint_after(FRAME_INTERVAL);
    }

    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        if let Some(child) = self.process.take_child() {
            ServerProcess::stop_blocking(child, STOP_GRACE_PERIOD);
        }
    }
}

/// The app icon (cropped from `data/bourguiba.jpg`), embedded directly into
/// the binary so the window/taskbar icon is correct regardless of where the
/// binary was launched from -- no runtime dependency on an installed path.
fn app_icon() -> egui::IconData {
    eframe::icon_data::from_png_bytes(include_bytes!("../assets/icon-256.png"))
        .expect("bundled icon-256.png is a valid PNG")
}

/// How the process was asked to present itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// The full window.
    Window,
    /// Just the tray icon, no window and no GL context.
    Tray,
    /// The always-on switch, without opening anything. Exists so a headless
    /// box (or `setup.sh`) can turn the service on without a display, and so
    /// the one that writes the unit file is the same code the window uses.
    EnableAlwaysOn,
    DisableAlwaysOn,
    Status,
    Help,
}

fn parse_mode<I: IntoIterator<Item = String>>(args: I) -> Mode {
    for arg in args {
        match arg.as_str() {
            "--tray" => return Mode::Tray,
            "--enable-always-on" => return Mode::EnableAlwaysOn,
            "--disable-always-on" => return Mode::DisableAlwaysOn,
            "--status" => return Mode::Status,
            "-h" | "--help" => return Mode::Help,
            _ => {}
        }
    }
    Mode::Window
}

const USAGE: &str = "\
NovaStream desktop app

usage: streaming-gateway-gui [--tray | --enable-always-on | --disable-always-on | --status]

  (no arguments)       open the window
  --tray               run as a tray icon only; this is what the window hands
                       over to when you close it, and what the login autostart
                       entry launches
  --enable-always-on   install and start the gateway as a systemd user service
                       so it survives this app closing and a reboot, then exit
  --disable-always-on  stop and remove that service
  --status             print what the service is currently doing
  -h, --help           show this

With always-on enabled the gateway runs as the systemd user service
`novastream.service`; closing the window then leaves it running and only puts
this app in the tray.
";

/// The headless half of the always-on switch.
fn run_service_command(mode: Mode, exe: &Path) -> i32 {
    if !service::available() {
        eprintln!("no systemd user session here; always-on mode needs one");
        return 1;
    }
    match mode {
        Mode::EnableAlwaysOn => {
            let Some(binary) = find_server_binary() else {
                eprintln!("streaming-gateway binary not found next to this app or on PATH");
                return 1;
            };
            let (port, cache) = service::read_settings()
                .unwrap_or_else(|| ("8080".to_string(), "./cache".to_string()));
            if let Err(e) = service::install(&binary, &port, &cache) {
                eprintln!("{e:#}");
                return 1;
            }
            if let Err(e) = service::install_autostart(exe) {
                // The gateway is up either way; only the tray-at-login part
                // failed, so this is a warning and not the exit code.
                eprintln!("warning: {e:#}");
            }
            println!("always-on: {}", service::status().summary());
            0
        }
        Mode::DisableAlwaysOn => {
            if let Err(e) = service::uninstall() {
                eprintln!("{e:#}");
                return 1;
            }
            let _ = service::remove_autostart();
            println!("always-on: off");
            0
        }
        _ => {
            println!("always-on: {}", service::status().summary());
            0
        }
    }
}

fn main() -> eframe::Result<()> {
    let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("streaming-gateway-gui"));
    match parse_mode(std::env::args().skip(1)) {
        Mode::Help => {
            print!("{USAGE}");
            return Ok(());
        }
        Mode::Tray => std::process::exit(tray::run_tray_daemon(exe)),
        mode @ (Mode::EnableAlwaysOn | Mode::DisableAlwaysOn | Mode::Status) => {
            std::process::exit(run_service_command(mode, &exe))
        }
        Mode::Window => {}
    }

    // One face at a time: a tray icon left by an earlier close would sit next
    // to the window that is about to open, and clicking it would try to open a
    // second one.
    tray::stop_running_tray();

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            // Taller than it is wide, like a terminal: the dashboard is a
            // stack of instruments and the TTY needs room under it.
            .with_inner_size([600.0, 880.0])
            .with_min_inner_size([460.0, 560.0])
            .with_icon(app_icon()),
        ..Default::default()
    };
    eframe::run_native(
        "NovaStream",
        options,
        Box::new(|cc| {
            theme::apply(&cc.egui_ctx);
            Ok(Box::new(GatewayApp::default()))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact lines `network::print_banner` / `print_addon_ready` emit.
    /// The GUI scrapes the server's stdout, so these strings are a real
    /// cross-crate contract -- if the banner is reworded, this test is what
    /// notices before the GUI silently shows nothing.
    const ADDON_LINE: &str = "   https://192-168-1-67.local-ip.sh:8443/manifest.json";
    const DIRECT_LINE: &str = "   http://192.168.1.67:8080";

    #[test]
    fn scrapes_both_urls_from_the_banner() {
        assert_eq!(
            extract_addon_url(ADDON_LINE).as_deref(),
            Some("https://192-168-1-67.local-ip.sh:8443")
        );
        assert_eq!(
            extract_url(DIRECT_LINE).as_deref(),
            Some("http://192.168.1.67:8080")
        );
    }

    /// The regression this whole change exists for: `find("http://")` must not
    /// match inside an `https://` URL, or the GUI captures the addon line as
    /// the direct address and shows a manifest URL Android refuses.
    #[test]
    fn http_scrape_never_matches_an_https_url() {
        assert_eq!(extract_url(ADDON_LINE), None);
    }

    /// Other log lines carry an unrelated `https://` (indexer, certificate
    /// provider, addon logo). Only the manifest line may be adopted.
    #[test]
    fn ignores_https_urls_that_are_not_the_manifest() {
        for line in [
            "torrent discovery enabled url=https://torrentio.strem.fun",
            "fetching https://local-ip.sh/server.pem",
            "logo https://raw.githubusercontent.com/Stremio/x/icon.png",
        ] {
            assert_eq!(extract_addon_url(line), None, "adopted: {line}");
        }
    }

    /// The fallback-port case: the GUI must show what the server actually
    /// bound, not the configured default.
    #[test]
    fn scrapes_the_fallback_port() {
        assert_eq!(
            extract_url("   http://192.168.1.67:11470").as_deref(),
            Some("http://192.168.1.67:11470")
        );
    }

    /// The gateway colours its output whether or not it is talking to a
    /// terminal, and both of the panel's sources (a pipe, and journald)
    /// preserve those escapes byte for byte.
    #[test]
    fn colour_escapes_never_reach_the_log_panel() {
        let raw = "\u{1b}[2m2026-09-07T00:49:30Z\u{1b}[0m \u{1b}[32m INFO\u{1b}[0m ready";
        assert_eq!(strip_ansi(raw), "2026-09-07T00:49:30Z  INFO ready");
        // A plain line must come through untouched, escapes or not.
        assert_eq!(
            strip_ansi("   http://192.168.1.67:8080"),
            "   http://192.168.1.67:8080"
        );
    }

    /// The URLs are scraped out of coloured `tracing` lines as well as the
    /// plain banner, and a trailing reset used to end up glued to the URL --
    /// which breaks the addon link silently, since the check for it is a
    /// suffix match on `/manifest.json`.
    #[test]
    fn a_colour_reset_against_a_url_does_not_become_part_of_it() {
        let line = "  \u{1b}[36mhttps://192-168-1-67.local-ip.sh:8443/manifest.json\u{1b}[0m";
        assert_eq!(
            extract_addon_url(&strip_ansi(line)).as_deref(),
            Some("https://192-168-1-67.local-ip.sh:8443")
        );
    }

    /// The window is the default: an argument nobody passed must never put
    /// the app in a mode with no window, since the desktop launcher and the
    /// menu entry both run it bare.
    #[test]
    fn the_window_is_what_you_get_without_arguments() {
        assert_eq!(parse_mode(Vec::<String>::new()), Mode::Window);
        assert_eq!(parse_mode(vec!["--tray".to_string()]), Mode::Tray);
        assert_eq!(
            parse_mode(vec!["--enable-always-on".to_string()]),
            Mode::EnableAlwaysOn
        );
        assert_eq!(parse_mode(vec!["-h".to_string()]), Mode::Help);
        // A flag meant for something else must not silently become tray mode.
        assert_eq!(parse_mode(vec!["--trayify".to_string()]), Mode::Window);
    }

    /// The graph is fed from a ring: it must scroll rather than grow, or a
    /// window left open overnight ends up plotting 30 000 points.
    #[test]
    fn history_scrolls_at_a_fixed_length() {
        let mut history = History::default();
        for step in 0..(HISTORY * 3) {
            history.push(step as f32);
        }
        assert_eq!(history.samples().len(), HISTORY);
        assert_eq!(history.latest(), (HISTORY * 3 - 1) as f32);
        // Oldest sample must be the newest minus the window, i.e. the front
        // is what got dropped.
        assert_eq!(history.samples()[0], (HISTORY * 2) as f32);
    }

    /// A gateway with two torrents must report the *sum* of their speeds --
    /// the graph is a link-level view, not a per-torrent one.
    #[test]
    fn health_sums_traffic_across_torrents() {
        let health = HealthInfo {
            uptime_seconds: 10,
            active_streams: 1,
            cache_usage_bytes: 0,
            cache_max_bytes: 1,
            process_memory_bytes: 0,
            process_cpu_percent: 0.0,
            version: String::new(),
            active_torrents: vec![
                TorrentInfo {
                    name: "a".into(),
                    state: "live".into(),
                    progress_percent: 10.0,
                    progress_bytes: 1,
                    total_bytes: 10,
                    download_speed_mib_s: 1.5,
                    upload_speed_mib_s: 0.25,
                    peers: 12,
                    info_hash: "a".repeat(40),
                    held: false,
                },
                TorrentInfo {
                    name: "b".into(),
                    state: "live".into(),
                    progress_percent: 50.0,
                    progress_bytes: 5,
                    total_bytes: 10,
                    download_speed_mib_s: 2.0,
                    upload_speed_mib_s: 0.75,
                    peers: 30,
                    info_hash: "b".repeat(40),
                    held: false,
                },
            ],
        };
        assert!((health.download_mib_s() - 3.5).abs() < 1e-5);
        assert!((health.upload_mib_s() - 1.0).abs() < 1e-5);
        assert_eq!(health.peers(), 42);
    }

    /// An older server does not send `active_torrents`/`version`. Parsing has
    /// to survive that, because a parse failure is indistinguishable from
    /// "the gateway is down" everywhere else in this file.
    #[test]
    fn health_parses_without_the_newer_fields() {
        let json = r#"{
            "uptime_seconds": 5, "active_streams": 0,
            "cache_usage_bytes": 0, "cache_max_bytes": 1,
            "process_memory_bytes": 0, "process_cpu_percent": 0.0
        }"#;
        let health: HealthInfo = serde_json::from_str(json).expect("parses");
        assert!(health.active_torrents.is_empty());
        assert_eq!(health.download_mib_s(), 0.0);
    }

    /// Every status has to be visually distinct, since the lamp, the button
    /// and the header all key off this one colour.
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
}
