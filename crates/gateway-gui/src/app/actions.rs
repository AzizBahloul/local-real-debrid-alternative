//! Everything the window does to the world: start, stop, install, purge,
//! export, and the per-torrent controls.

use std::time::Instant;

use super::{
    GatewayApp, Pending, ServiceOutcome, BINARY_MISSING, JOURNAL_BACKLOG, JOURNAL_RETRY_DELAY,
    STOP_GRACE_PERIOD,
};
use crate::http::{self, Lane};
use crate::server_process::{LogTail, ServerProcess};
use crate::text::desktop_dir;
use crate::{service, tray, widgets};

const SERVICE_BUSY: &str = "the previous service action is still running; try again in a moment";

impl GatewayApp {
    /// Runs a blocking `systemctl`/`loginctl` sequence on a thread of its own.
    ///
    /// `systemctl stop` waits for the unit to actually stop, which for a
    /// gateway with open streams is a second or more of a frozen window.
    ///
    /// Returns false, and says so in the always-on panel, when another action
    /// is still running. It never drops one silently: that is how enabling
    /// always-on once stopped a live gateway and then never started it again.
    fn start_service_action(
        &mut self,
        pending: Pending,
        action: impl FnOnce() -> anyhow::Result<()> + Send + 'static,
    ) -> bool {
        let started = self.service_action.spawn(move || ServiceOutcome {
            error: action().err().map(|e| format!("{e:#}")),
            autostart: service::autostart_enabled(),
        });
        if started {
            self.pending = pending;
            self.service_error = None;
        } else {
            self.service_error = Some(SERVICE_BUSY.to_string());
        }
        started
    }

    /// Turns on always-on mode: install the unit, start it, keep it across
    /// reboots, and leave the tray icon behind at login.
    pub(super) fn enable_always_on(&mut self) {
        let Some(binary) = self.binary.clone() else {
            self.service_error = Some(BINARY_MISSING.to_string());
            return;
        };
        if self.service_action.in_flight() {
            self.service_error = Some(SERVICE_BUSY.to_string());
            return;
        }
        // A server running as this window's child would hold the port the
        // service is about to bind, and the service would restart-loop. Only
        // the *child* is stopped -- `service::install` restarts the unit
        // itself -- and it is stopped inside the job, before systemd starts
        // anything, so the port really is free by then.
        let child = self.take_child();
        let port = self.port.clone();
        let cache = self.cache_dir.clone();
        let exe = self.exe.clone();
        let _ = self.start_service_action(Pending::Install, move || {
            if let Some(child) = child {
                ServerProcess::stop_blocking(child, STOP_GRACE_PERIOD);
            }
            service::install(&binary, &port, &cache)?;
            if let Some(exe) = exe {
                // Non-fatal: the gateway is the thing that must survive a
                // reboot; the tray coming back at login is a convenience.
                let _ = service::install_autostart(&exe);
            }
            Ok(())
        });
    }

    // Turning always-on *off* is deliberately not reachable from the window --
    // see `service_panel`. The `--disable-always-on` command-line flag remains
    // the escape hatch, and it calls `service::uninstall` directly.

    /// Puts the login tray entry back without touching the service.
    pub(super) fn write_autostart(&mut self) {
        let Some(exe) = &self.exe else {
            return;
        };
        match service::install_autostart(exe) {
            Ok(()) => self.autostart = true,
            Err(e) => {
                self.service_error = Some(format!("could not add the tray icon to login: {e:#}"))
            }
        }
    }

    pub(super) fn start_server(&mut self) {
        if self.service_mode() {
            let port = self.port.clone();
            let cache = self.cache_dir.clone();
            let _ = self.start_service_action(Pending::Start, move || {
                // Settings first: this is also how an edited port or cache
                // directory reaches a service that is already installed.
                service::write_settings(&port, &cache)?;
                service::start()
            });
            return;
        }
        if self.service_action.in_flight() {
            // The first install runs in this mode, before the unit exists; a
            // child started now would take the port the service is binding.
            self.start_error = Some(SERVICE_BUSY.to_string());
            return;
        }

        let Some(binary) = self.binary.clone() else {
            self.start_error = Some(BINARY_MISSING.to_string());
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
                self.log.reset();
            }
            Err(e) => self.start_error = Some(format!("{e:#}")),
        }
    }

    pub(super) fn stop_server(&mut self) {
        if self.service_mode() {
            let _ = self.start_service_action(Pending::Stop, service::stop);
            return;
        }
        if self.child_stop.in_flight() {
            return;
        }
        if let Some(child) = self.process.take_child() {
            self.health = None;
            let _ = self
                .child_stop
                .spawn(move || ServerProcess::stop_blocking(child, STOP_GRACE_PERIOD));
        }
    }

    /// Detaches this window's child server, if it has one, for somebody else
    /// to stop, and forgets everything learned from it.
    fn take_child(&mut self) -> Option<std::process::Child> {
        let child = self.process.take_child()?;
        self.running = false;
        self.log.forget_urls();
        self.health = None;
        Some(child)
    }

    /// The window is closing for good; a child server goes with it.
    pub fn stop_child_blocking(&mut self) {
        if let Some(child) = self.process.take_child() {
            ServerProcess::stop_blocking(child, STOP_GRACE_PERIOD);
        }
    }

    /// Attaches the log panel to the service's journal, off the UI thread:
    /// finding the unit's current run is a `systemctl` call.
    pub(super) fn attach_journal(&mut self) {
        self.log.next_journal_attempt = Instant::now() + JOURNAL_RETRY_DELAY;
        let _ = self.log.journal_attach.spawn(|| {
            let command =
                service::journal_command(JOURNAL_BACKLOG, service::invocation_id().as_deref());
            LogTail::spawn(command).map_err(|e| format!("{e:#}"))
        });
    }

    /// Asks the running server to delete every torrent and all of its data.
    ///
    /// Goes through the server rather than deleting files here: the engine
    /// holds open handles to what is on disk, so removing the directory
    /// underneath it would leave the session pointing at files that no longer
    /// exist. The endpoint is loopback-only, which is why this works from the
    /// desktop app and from nowhere else.
    pub(super) fn clear_cache(&mut self) {
        let port = self.port.clone();
        let worker = &self.http;
        if self
            .clear
            .start(|| worker.submit(Lane::Action, move |agent| http::clear_cache(agent, &port)))
        {
            self.notice = None;
        }
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
    pub(super) fn export_logs(&mut self) {
        let port = self.port.clone();
        // Snapshotted here rather than read from the worker: `self` cannot
        // cross the thread boundary, and the panel keeps changing.
        let panel: Vec<String> = self.log.lines.iter().map(|l| l.text.clone()).collect();
        let worker = &self.http;
        if self.export.start(|| {
            worker.submit(Lane::Action, move |agent| {
                let events = http::fetch_audit_export(agent, &port);
                let stamp = chrono::Local::now().format("%Y%m%d-%H%M%S");
                let path = desktop_dir().join(format!("novastream-log-{stamp}.txt"));
                std::fs::write(&path, render_report(events, &panel))
                    .map(|()| path.display().to_string())
                    .map_err(|e| format!("could not write {}: {e}", path.display()))
            })
        }) {
            self.export_result = None;
        }
    }

    /// Applies one swarm-row button.
    ///
    /// Delete is armed first and confirmed second, matching the whole-cache
    /// purge: it destroys a download that may have taken an hour to fetch, and
    /// the button sits inches from Pause. Any other click disarms, so an
    /// armed row cannot be confirmed later by accident.
    pub(super) fn act_on_torrent(&mut self, info_hash: String, action: widgets::RowAction) {
        if info_hash.is_empty() {
            self.notice =
                Some("this server is too old to control torrents individually".to_string());
            return;
        }
        let verb = match action {
            widgets::RowAction::Pause => "pause",
            widgets::RowAction::Resume => "resume",
            widgets::RowAction::Delete => {
                if !self.delete_confirm.arm_or_fire(info_hash.clone()) {
                    return;
                }
                "delete"
            }
        };
        self.delete_confirm.disarm();

        let port = self.port.clone();
        let errors = self.action_errors.0.clone();
        // Fire and forget: the next `/health` poll is what redraws the row, so
        // there is nothing to wait for. A failure comes back on
        // `action_errors` and is shown under the controls.
        drop(self.http.submit(Lane::Action, move |agent| {
            if let Err(e) = http::torrent_action(agent, &port, &info_hash, verb) {
                let _ = errors.send(format!("{verb} failed: {e}"));
            }
        }));
    }

    /// Leaves for good, rather than folding into the tray.
    ///
    /// `quitting` is what tells the close handler apart from the window's own
    /// close button: both arrive as the same `close_requested` event, and only
    /// one of them should leave an icon behind.
    pub(super) fn quit(&mut self, ctx: &egui::Context) {
        self.quitting = true;
        ctx.send_viewport_cmd(egui::ViewportCommand::Close);
    }

    /// Closing the window puts the app in the tray instead of ending it.
    ///
    /// The tray is a separate process (see `tray.rs` for why it has to be),
    /// started here just before this one goes away.
    pub fn hand_over_to_tray(&mut self) {
        let Some(exe) = self.exe.clone() else {
            return;
        };
        if let Err(e) = tray::spawn_tray_process(&exe) {
            // Nothing to show it in -- the window is already closing. The
            // stdout line is what the GUI's own Log panel would have caught.
            eprintln!("could not put NovaStream in the tray: {e}");
        }
    }
}

/// The saved report: the server's event log, then this window's panel.
fn render_report(events: String, panel: &[String]) -> String {
    let mut body = events;
    body.push_str("\n\n");
    body.push_str("# ---------------------------------------------\n");
    body.push_str("# Desktop app log panel (server stdout, this run)\n");
    body.push_str("# ---------------------------------------------\n");
    if panel.is_empty() {
        body.push_str("(empty -- the server was not started by this window)\n");
    } else {
        for line in panel {
            body.push_str(line);
            body.push('\n');
        }
    }
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_report_carries_both_halves_and_says_when_one_is_empty() {
        let report = render_report("{\"event\":1}\n".to_string(), &["banner".to_string()]);
        assert!(report.starts_with("{\"event\":1}"));
        assert!(report.contains("Desktop app log panel"));
        assert!(report.ends_with("banner\n"));

        let empty = render_report(String::new(), &[]);
        assert!(empty.contains("(empty -- the server was not started by this window)"));
    }
}
