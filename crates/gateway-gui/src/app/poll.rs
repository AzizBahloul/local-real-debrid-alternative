//! Reading the world back, once per frame, without ever blocking the frame.

use std::borrow::Cow;
use std::time::Instant;

use super::{
    always_on_step, AlwaysOnStep, GatewayApp, LogPanel, Pending, Probe, HEALTH_POLL_INTERVAL,
    LIVENESS_CHECK_INTERVAL, LOG_DRAIN_PER_FRAME, MAX_LOG_LINES, SERVICE_POLL_INTERVAL,
};
use crate::health::HealthView;
use crate::http::{self, Lane};
use crate::server_process::LogLine;
use crate::service;
use crate::text::{extract_addon_url, extract_url, strip_ansi};

impl GatewayApp {
    pub fn poll_background_work(&mut self) {
        let now = Instant::now();
        self.poll_probe();
        self.poll_service_state(now);
        self.ensure_always_on();
        self.track_server(now);
        self.ingest_logs();
        self.poll_requests();
        self.poll_health(now);
    }

    /// The environment probe: kicked off on the first frame, adopted when it
    /// lands.
    fn poll_probe(&mut self) {
        if self.service_known {
            return;
        }
        match self.probe.poll() {
            Some(probe) => {
                self.binary = probe.binary;
                self.systemd = probe.systemd;
                self.autostart = probe.autostart;
                self.set_service(probe.service);
                self.service_known = true;
                self.service_poll.schedule_in(SERVICE_POLL_INTERVAL);
            }
            None if !self.probe.in_flight() => {
                let _ = self.probe.spawn(Probe::run);
            }
            None => {}
        }
    }

    /// Keeps the always-on panel and the service-mode dispatch honest, without
    /// spawning `systemctl` on the UI thread.
    fn poll_service_state(&mut self, now: Instant) {
        if let Some(outcome) = self.service_action.poll() {
            self.service_error = outcome.error;
            self.autostart = outcome.autostart;
            self.finish_service_action(now);
        } else if self.pending != Pending::None && !self.service_action.in_flight() {
            // The worker died without reporting. Saying so beats a button
            // greyed out for the rest of the session.
            self.service_error = Some("the service action ended without reporting back".into());
            self.finish_service_action(now);
        }

        if !self.systemd || !self.service_known {
            return;
        }

        if let Some(status) = self.service_poll.poll() {
            let restarted = status.active && !self.service.active;
            self.set_service(status);
            if restarted {
                // A fresh run means a fresh banner, so re-attach rather than
                // keep tailing the old boot's log.
                self.log.journal = None;
                self.log.next_journal_attempt = now;
            }
        } else if self.service_poll.due(now) {
            self.service_poll.schedule_in(SERVICE_POLL_INTERVAL);
            let _ = self.service_poll.spawn(service::status);
        }
    }

    fn finish_service_action(&mut self, now: Instant) {
        self.pending = Pending::None;
        // The unit just changed state; do not wait out the interval before
        // saying so.
        self.service_poll.expedite();
        self.log.next_journal_attempt = now;
    }

    /// Brings always-on mode up to date the moment the window opens: installs
    /// the unit and login tray icon if the unit is missing, puts back only the
    /// tray entry if that is all that is missing, and starts the gateway if
    /// the unit exists but is not running. See [`always_on_step`].
    ///
    /// Runs from the update loop rather than from a button, because always-on
    /// is now the only mode: a fresh install, a machine where the unit was
    /// removed by hand, or a gateway stopped from the tray should all converge
    /// on "running" without anyone being asked. Opening the app is itself the
    /// request for the gateway to be up -- there is nothing else the window is
    /// for -- so making someone then press start is a step that only ever
    /// costs them a confused minute wondering why their phone sees nothing.
    ///
    /// Waits for the probe, so it never acts on the defaults the window starts
    /// with; guarded on the action job so the poll cannot stack actions, and
    /// on `always_on_attempted` so a genuine failure (no binary, systemd
    /// refusing the unit) is reported once instead of retried every frame.
    fn ensure_always_on(&mut self) {
        if !self.service_known
            || !self.systemd
            || self.always_on_attempted
            || self.service_action.in_flight()
        {
            return;
        }
        // Never act on a "not installed" that a poll in flight is about to
        // contradict -- installing on top of a unit that already exists would
        // restart a gateway somebody is streaming from.
        if self.service_poll.in_flight() && !self.service.installed {
            return;
        }

        self.always_on_attempted = true;
        match always_on_step(self.service.installed, self.autostart, self.service.active) {
            AlwaysOnStep::Nothing => {}
            AlwaysOnStep::Install => self.enable_always_on(),
            AlwaysOnStep::WriteAutostart => self.write_autostart(),
            AlwaysOnStep::Start => self.start_server(),
            AlwaysOnStep::WriteAutostartAndStart => {
                self.write_autostart();
                self.start_server();
            }
        }
    }

    /// Follows whichever process owns the gateway: the unit's journal, or this
    /// window's child.
    fn track_server(&mut self, now: Instant) {
        let check_liveness = now >= self.next_liveness_check;
        if check_liveness {
            self.next_liveness_check = now + LIVENESS_CHECK_INTERVAL;
        }

        if let Some(attached) = self.log.journal_attach.poll() {
            match attached {
                Ok(tail) => {
                    self.log.reset();
                    self.log.journal = Some(tail);
                }
                Err(e) => {
                    self.log.journal = None;
                    self.log
                        .push(format!("[stderr] cannot read the service log: {e}"));
                }
            }
        }

        if self.child_stop.in_flight() {
            let _ = self.child_stop.poll();
            if !self.child_stop.in_flight() {
                self.running = false;
                self.log.forget_urls();
                self.health = None;
            }
        }

        if self.service_mode() {
            // systemd owns the process; this window only reports on it.
            self.running = self.service.active;
            if !self.service.active {
                self.log.journal = None;
                self.log.forget_urls();
                return;
            }
            let tail_down = match self.log.journal.as_mut() {
                None => true,
                Some(tail) => check_liveness && !tail.is_running(),
            };
            if tail_down
                && now >= self.log.next_journal_attempt
                && !self.log.journal_attach.in_flight()
            {
                self.attach_journal();
            }
        } else if self.running && check_liveness && !self.process.is_running() {
            // Reap a server that exited on its own (crash, port conflict, ^C
            // from the terminal it was launched from, etc.).
            self.running = false;
            self.log.forget_urls();
            self.health = None;
        }
    }

    fn ingest_logs(&mut self) {
        let lines = match self.log.journal.as_mut() {
            Some(tail) => tail.drain(LOG_DRAIN_PER_FRAME),
            None => self.process.drain_logs(LOG_DRAIN_PER_FRAME),
        };
        // A journal attach delivers its whole backlog at once. Every line of
        // it is scraped -- the addon URL is in the startup banner, the oldest
        // part -- but only the lines that will survive the panel's cap are
        // stripped, coloured and stored.
        let shown_from = lines.len().saturating_sub(MAX_LOG_LINES);
        for (index, line) in lines.into_iter().enumerate() {
            self.log.ingest(line, index >= shown_from);
        }
    }

    fn poll_requests(&mut self) {
        if let Some(outcome) = self.clear.poll() {
            self.notice = Some(match outcome {
                Ok(0) => "nothing to clear".to_string(),
                Ok(1) => "cleared 1 download".to_string(),
                Ok(n) => format!("cleared {n} downloads"),
                Err(e) => format!("could not clear: {e}"),
            });
            // Force the next health poll so the freed space shows at once
            // rather than after the regular interval.
            self.health_poll.expedite();
        }

        if let Some(outcome) = self.export.poll() {
            self.export_result = Some(match outcome {
                Ok(path) => format!("saved to {path}"),
                Err(e) => e,
            });
        }

        if let Some(error) = self.action_errors.1.try_iter().last() {
            self.notice = Some(error);
        }
    }

    /// Polled whether or not this app started the server. A gateway launched
    /// from a terminal is just as real, and without this the window sat blank
    /// next to a perfectly healthy server -- and the clear button, which needs
    /// a server to talk to, stayed disabled.
    fn poll_health(&mut self, now: Instant) {
        if let Some(result) = self.health_poll.poll() {
            self.traffic.record(result.as_ref());
            self.health = result.map(HealthView::new);
        } else if self.health_poll.due(now) {
            self.health_poll.schedule_in(HEALTH_POLL_INTERVAL);
            let port = self.port.clone();
            let worker = &self.http;
            let _ = self
                .health_poll
                .start(|| worker.submit(Lane::Poll, move |agent| http::fetch_health(agent, &port)));
        }
    }
}

impl LogPanel {
    /// Takes one raw line off a pipe: scrapes it for the server's URLs while
    /// they are still unknown, and stores it when `shown`.
    fn ingest(&mut self, line: LogLine, shown: bool) {
        let wants_url = (self.detected_url.is_none() || self.addon_manifest.is_none())
            && line.text().contains("://");
        if !shown && !wants_url {
            return;
        }

        let is_err = line.is_err();
        // Stripped exactly once, and only copied when there was something to
        // strip. The URLs are scraped from the stripped text, not the raw one:
        // a colour reset sitting against the end of a URL becomes part of it
        // otherwise, and the addon URL then fails its `/manifest.json` check.
        let stripped = match strip_ansi(line.text()) {
            Cow::Owned(stripped) => Some(stripped),
            Cow::Borrowed(_) => None,
        };
        let text = stripped.unwrap_or_else(|| line.into_text());

        if wants_url {
            if self.detected_url.is_none() {
                self.detected_url = extract_url(&text);
            }
            if self.addon_manifest.is_none() {
                self.addon_manifest =
                    extract_addon_url(&text).map(|base| format!("{base}/manifest.json"));
            }
        }
        if shown {
            self.push(if is_err {
                format!("[stderr] {text}")
            } else {
                text
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_are_scraped_from_a_coloured_backlog_even_where_lines_are_not_shown() {
        let mut panel = LogPanel::new();
        panel.ingest(
            LogLine::Out("\u{1b}[36m   http://192.168.1.67:8080\u{1b}[0m".into()),
            false,
        );
        panel.ingest(
            LogLine::Out("   https://192-168-1-67.local-ip.sh:8443/manifest.json".into()),
            false,
        );
        assert!(panel.lines.is_empty(), "unshown lines are not stored");
        assert_eq!(
            panel.detected_url.as_deref(),
            Some("http://192.168.1.67:8080")
        );
        assert_eq!(
            panel.addon_manifest.as_deref(),
            Some("https://192-168-1-67.local-ip.sh:8443/manifest.json")
        );

        panel.ingest(LogLine::Err("\u{1b}[31mboom\u{1b}[0m".into()), true);
        assert_eq!(panel.lines.back().unwrap().text, "[stderr] boom");
    }
}
