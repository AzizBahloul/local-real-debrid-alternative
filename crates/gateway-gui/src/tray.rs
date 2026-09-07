//! The tray icon, and the small always-resident process that owns it.
//!
//! # Why closing the window starts a second process
//!
//! The obvious implementation of "minimise to tray" is to keep the window
//! object alive and hide it. That is not available here: this session's
//! windowing backend can be Wayland, where a client cannot hide its own
//! toplevel and cannot un-minimise one either (winit says so in as many
//! words -- `set_visible` is a no-op and `set_minimized(false)` logs
//! "Unminimizing is ignored on Wayland"). Nor can the window simply be
//! recreated later: winit refuses to build a second event loop in one
//! process for the whole lifetime of that process, so once the window is
//! gone it is gone for good.
//!
//! So the window really does close, and what stays behind is a separate,
//! tiny `--tray` process holding only a D-Bus StatusNotifierItem: no GL
//! context, no event loop, a few hundred KB. Picking "Open" spawns a fresh
//! window process and the tray process exits, so there is never both an icon
//! and a window -- and never two icons.
//!
//! The tray talks to the gateway through the systemd user service (see
//! `service.rs`), which is what makes this safe: the server is not a child of
//! either process, so whichever of them comes and goes, the stream a phone is
//! watching is untouched.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::Duration;

use ksni::blocking::TrayMethods;

use crate::service;

/// How often the tray re-reads the service state so its menu is not stale.
const POLL_INTERVAL: Duration = Duration::from_secs(3);

const PID_FILE: &str = "novastream-tray.pid";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrayEvent {
    /// Open the full window (and retire this tray process).
    Open,
    Start,
    Stop,
    /// Leave the tray entirely. The gateway service keeps running.
    Quit,
}

/// The icon, converted once from the same PNG the window uses.
///
/// StatusNotifierItem wants raw ARGB32, and the bundled helper hands back
/// RGBA, hence the per-pixel rotate.
fn icon() -> Option<ksni::Icon> {
    let data = eframe::icon_data::from_png_bytes(include_bytes!("../assets/icon-256.png")).ok()?;
    let mut rgba = data.rgba;
    for pixel in rgba.chunks_exact_mut(4) {
        pixel.rotate_right(1);
    }
    Some(ksni::Icon {
        width: data.width as i32,
        height: data.height as i32,
        data: rgba,
    })
}

struct NovaTray {
    status: service::Status,
    tx: Sender<TrayEvent>,
}

impl ksni::Tray for NovaTray {
    fn id(&self) -> String {
        "novastream".into()
    }

    fn title(&self) -> String {
        "NovaStream".into()
    }

    fn icon_pixmap(&self) -> Vec<ksni::Icon> {
        icon().into_iter().collect()
    }

    /// Fallback for a host that ignores pixmaps; a generic media icon is
    /// better than the empty square an unknown theme name produces.
    fn icon_name(&self) -> String {
        "video-display".into()
    }

    fn tool_tip(&self) -> ksni::ToolTip {
        ksni::ToolTip {
            title: "NovaStream".into(),
            description: self.status.summary(),
            ..Default::default()
        }
    }

    /// Left click opens the window -- the thing someone clicking the icon
    /// almost always means.
    fn activate(&mut self, _x: i32, _y: i32) {
        let _ = self.tx.send(TrayEvent::Open);
    }

    fn menu(&self) -> Vec<ksni::MenuItem<Self>> {
        use ksni::menu::*;

        let mut items: Vec<ksni::MenuItem<Self>> = vec![
            StandardItem {
                label: "Open NovaStream".into(),
                icon_name: "video-display".into(),
                activate: Box::new(|this: &mut Self| {
                    let _ = this.tx.send(TrayEvent::Open);
                }),
                ..Default::default()
            }
            .into(),
            MenuItem::Separator,
            StandardItem {
                label: format!("Gateway: {}", self.status.summary()),
                enabled: false,
                ..Default::default()
            }
            .into(),
        ];

        if self.status.installed {
            if self.status.active {
                items.push(
                    StandardItem {
                        label: "Stop the gateway".into(),
                        icon_name: "media-playback-stop".into(),
                        activate: Box::new(|this: &mut Self| {
                            let _ = this.tx.send(TrayEvent::Stop);
                        }),
                        ..Default::default()
                    }
                    .into(),
                );
            } else {
                items.push(
                    StandardItem {
                        label: "Start the gateway".into(),
                        icon_name: "media-playback-start".into(),
                        activate: Box::new(|this: &mut Self| {
                            let _ = this.tx.send(TrayEvent::Start);
                        }),
                        ..Default::default()
                    }
                    .into(),
                );
            }
        } else {
            items.push(
                StandardItem {
                    label: "Open the window to turn on always-on mode".into(),
                    enabled: false,
                    ..Default::default()
                }
                .into(),
            );
        }

        items.push(MenuItem::Separator);
        items.push(
            StandardItem {
                // Spelling out what quitting does and does not stop: the
                // whole point of the service is that this menu entry is not
                // the off switch for the stream someone is watching.
                label: if self.status.active {
                    "Quit tray (gateway keeps running)".into()
                } else {
                    "Quit tray".to_string()
                },
                icon_name: "application-exit".into(),
                activate: Box::new(|this: &mut Self| {
                    let _ = this.tx.send(TrayEvent::Quit);
                }),
                ..Default::default()
            }
            .into(),
        );
        items
    }
}

fn runtime_dir() -> PathBuf {
    std::env::var_os("XDG_RUNTIME_DIR")
        .filter(|d| !d.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
}

fn pid_file() -> PathBuf {
    runtime_dir().join(PID_FILE)
}

/// Whether a `/proc/<pid>/cmdline` belongs to this application.
fn looks_like_gui(cmdline: &str) -> bool {
    cmdline.contains(env!("CARGO_BIN_NAME"))
}

/// True if `pid` is a live process whose command line looks like this app.
///
/// The identity check matters as much as the liveness one: pids are recycled,
/// and a stale file left by a crashed tray would otherwise make the next
/// launch either refuse to start (believing a tray is up) or send SIGTERM to
/// whatever unrelated process inherited that number.
fn is_live_gui(pid: u32) -> bool {
    let Ok(cmdline) = std::fs::read(format!("/proc/{pid}/cmdline")) else {
        return false;
    };
    looks_like_gui(&String::from_utf8_lossy(&cmdline))
}

pub fn running_tray_pid() -> Option<u32> {
    let text = std::fs::read_to_string(pid_file()).ok()?;
    let pid: u32 = text.trim().parse().ok()?;
    is_live_gui(pid).then_some(pid)
}

/// Asks any resident tray process to go away.
///
/// Called when the window opens, so the two representations of the app are
/// never on screen at once.
pub fn stop_running_tray() {
    if let Some(pid) = running_tray_pid() {
        if pid != std::process::id() {
            let _ = Command::new("kill")
                .arg("-TERM")
                .arg(pid.to_string())
                .status();
        }
    }
    let _ = std::fs::remove_file(pid_file());
}

/// Starts the resident tray process, detached from this one.
///
/// Deliberately a no-op when a tray is already resident: the window may have
/// been opened from the menu while one was running, and two icons for one app
/// is a bug people report as "it opened twice".
pub fn spawn_tray_process(exe: &Path) -> std::io::Result<()> {
    if running_tray_pid().is_some() {
        return Ok(());
    }
    Command::new(exe)
        .arg("--tray")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map(|_| ())
}

fn spawn_window_process(exe: &Path) -> std::io::Result<()> {
    Command::new(exe)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map(|_| ())
}

/// The whole of `--tray` mode: own the icon, act on its menu, exit when asked.
///
/// Returns the process exit code.
pub fn run_tray_daemon(exe: PathBuf) -> i32 {
    if running_tray_pid().is_some() {
        // Another tray is already up (autostart raced with a window closing,
        // say). Two icons is worse than none.
        return 0;
    }
    let _ = std::fs::write(pid_file(), std::process::id().to_string());

    let (tx, rx): (Sender<TrayEvent>, Receiver<TrayEvent>) = mpsc::channel();
    let handle = match (NovaTray {
        status: service::status(),
        tx,
    })
    .spawn()
    {
        Ok(handle) => handle,
        Err(e) => {
            // No StatusNotifierItem host (a bare X session, a desktop without
            // the AppIndicator extension). Falling back to opening the window
            // beats vanishing with no UI at all.
            eprintln!("no system tray available ({e}); opening the window instead");
            let _ = std::fs::remove_file(pid_file());
            return match spawn_window_process(&exe) {
                Ok(()) => 0,
                Err(e) => {
                    eprintln!("could not open the window either: {e}");
                    1
                }
            };
        }
    };

    let mut last = service::status();
    loop {
        match rx.recv_timeout(POLL_INTERVAL) {
            Ok(TrayEvent::Open) => {
                let _ = std::fs::remove_file(pid_file());
                if let Err(e) = spawn_window_process(&exe) {
                    eprintln!("could not open the window: {e}");
                    // Keep the tray rather than leaving the user with nothing.
                    let _ = std::fs::write(pid_file(), std::process::id().to_string());
                    continue;
                }
                return 0;
            }
            Ok(TrayEvent::Quit) => {
                let _ = std::fs::remove_file(pid_file());
                return 0;
            }
            Ok(TrayEvent::Start) => {
                if let Err(e) = service::start() {
                    eprintln!("{e:#}");
                }
            }
            Ok(TrayEvent::Stop) => {
                if let Err(e) = service::stop() {
                    eprintln!("{e:#}");
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let _ = std::fs::remove_file(pid_file());
                return 0;
            }
        }

        // Repaint the menu only when something actually changed -- the menu
        // is rebuilt over D-Bus and the host redraws on every update.
        let current = service::status();
        if current != last {
            last = current;
            handle.update(move |tray: &mut NovaTray| tray.status = current);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stale_pid_file_is_not_treated_as_a_running_tray() {
        // pid 0 is never a real process; the liveness check is what stops a
        // leftover file from suppressing the tray forever, or from sending
        // SIGTERM to whatever inherited that pid.
        assert!(!is_live_gui(0));
    }

    #[test]
    fn only_this_applications_command_line_is_adopted() {
        // The pid in the file is only trusted when the process behind it is
        // still us; anything else is a recycled pid, and SIGTERM-ing it would
        // kill a stranger's process.
        assert!(looks_like_gui(
            "/usr/bin/streaming-gateway-gui\u{0}--tray\u{0}"
        ));
        assert!(!looks_like_gui("/usr/bin/sleep\u{0}100\u{0}"));
        // The server binary is a near-miss on purpose: its name is a prefix
        // of this one, so a substring check in the other direction would
        // match it and the tray would try to kill the gateway.
        assert!(!looks_like_gui("/usr/bin/streaming-gateway\u{0}"));
    }

    #[test]
    fn the_pid_file_lives_in_the_session_runtime_directory() {
        // /tmp would survive a reboot and be shared between users; the
        // runtime dir is per-user and cleared on logout, which is what makes
        // the "is a tray already up?" answer trustworthy.
        let path = pid_file();
        assert!(path.ends_with(PID_FILE));
        assert!(path.is_absolute());
    }

    #[test]
    fn the_icon_converts_to_argb_at_its_real_size() {
        let icon = icon().expect("the bundled PNG decodes");
        assert_eq!(icon.width, 256);
        assert_eq!(icon.height, 256);
        assert_eq!(icon.data.len(), 256 * 256 * 4);
    }
}
