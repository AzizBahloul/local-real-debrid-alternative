//! The panels, and the three painted layers around them.

use std::collections::VecDeque;

use egui::{FontId, Ui};

use super::{
    GatewayApp, LogEntry, Pending, Status, BINARY_MISSING, FRAME_INTERVAL, UNFOCUSED_FRAME_INTERVAL,
};
use crate::health::HealthView;
use crate::theme::{self, AMBER, PHOSPHOR, RED, TEXT, TEXT_DIM};
use crate::{fx, widgets};

const TITLE_ALWAYS_ON: &str = "ALWAYS-ON";
const TITLE_NET: &str = "NET I/O";
const TITLE_SYSTEM: &str = "SYSTEM";
const TITLE_ENDPOINTS: &str = "ENDPOINTS";
const TITLE_TTY: &str = "TTY // GATEWAY STDOUT";

const LOCAL_VERSION: &str = concat!("v", env!("CARGO_PKG_VERSION"));
const PROMPT: &str = "root@novastream:~$ ";
const NO_OUTPUT: &str = "-- no output; the gateway is not running --";
const ADDON_MOVED: &str = "ADDRESS CHANGED - the addon in Stremio still uses the old one";
const ADDON_MOVED_WAS: &str = "old: ";
const ADDON_MOVED_HINT: &str = "reinstall it from the address above; copying it clears this";

impl GatewayApp {
    /// Background layer: the rain, painted before any panel so everything
    /// drawn afterwards lands on top of it.
    pub fn paint_rain(&mut self, ctx: &egui::Context, screen: egui::Rect, dt: f32, time: f64) {
        self.chrome.rain.paint(
            &ctx.layer_painter(egui::LayerId::background()),
            screen,
            dt,
            0.13 * fx::flicker(time),
        );
    }

    /// Foreground layer: the CRT glass, over every widget.
    pub fn paint_crt(&mut self, ctx: &egui::Context, screen: egui::Rect) {
        self.chrome.crt.paint(ctx, screen);
    }

    /// Tooltip layer: the cold-boot cover, over the glass. A click skips it.
    pub fn paint_boot(&mut self, ctx: &egui::Context, screen: egui::Rect, time: f64) {
        if self.chrome.boot.paint(ctx, screen, time) && ctx.input(|i| i.pointer.any_click()) {
            self.chrome.boot.skip();
        }
    }

    /// Full rate while someone is looking at the window; a slow tick while it
    /// sits behind a player, which is most of its life.
    pub fn request_next_frame(&self, ctx: &egui::Context) {
        let focused = ctx.input(|i| i.viewport().focused.unwrap_or(true));
        ctx.request_repaint_after(if focused {
            FRAME_INTERVAL
        } else {
            UNFOCUSED_FRAME_INTERVAL
        });
    }

    pub fn hud(&mut self, ui: &mut Ui, status: Status, time: f64) {
        let glitch = self.glitch(time);
        let (detail, version) = match &self.health {
            Some(h) => (h.uptime.as_str(), h.version.as_str()),
            None => ("no carrier", LOCAL_VERSION),
        };
        // The window's own close button hands over to the tray, so "leave for
        // good" needs somewhere to live. Named for what it does to the
        // gateway, which is the only question worth asking before clicking it.
        let quit_label = if self.service_mode() {
            "quit app"
        } else {
            "quit + stop"
        };
        let mut quit = false;

        ui.horizontal(|ui| {
            ui.vertical(|ui| {
                ui.add_space(2.0);
                widgets::wordmark(ui, "NOVASTREAM", 4.0, 1.0, glitch);
                ui.add_space(3.0);
                ui.label(theme::caption(
                    "magnet -> local http stream :: stremio / vlc / lan",
                    TEXT_DIM,
                ));
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
                    ui.label(theme::caption(detail, TEXT_DIM));
                    ui.label(theme::caption(version, TEXT_DIM));
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

    /// How torn the wordmark is right now, 0..1.
    ///
    /// It glitches for a moment every few seconds. Scheduled rather than
    /// random-per-frame so it cannot land twice in a row and read as a
    /// rendering fault.
    fn glitch(&mut self, time: f64) -> f32 {
        let chrome = &mut self.chrome;
        if time > chrome.next_glitch {
            chrome.glitch_until = time + 0.16;
            chrome.next_glitch = time + 6.0 + (time % 5.0);
        }
        if time < chrome.glitch_until {
            ((chrome.glitch_until - time) / 0.16) as f32
        } else {
            0.0
        }
    }

    /// Everything between the HUD and the TTY, top to bottom.
    pub fn dashboard(&mut self, ui: &mut Ui, status: Status, time: f64) {
        egui::ScrollArea::vertical()
            .auto_shrink([false, false])
            .show(ui, |ui| {
                if let Some(err) = &self.start_error {
                    ui.label(theme::body(format!("!! {err}"), RED));
                    ui.add_space(4.0);
                }
                if self.service_known && self.binary.is_none() {
                    ui.label(theme::body(format!("!! {BINARY_MISSING}"), AMBER));
                    ui.add_space(4.0);
                }

                self.controls(ui, status);
                ui.add_space(10.0);
                self.service_panel(ui);
                ui.add_space(10.0);
                self.traffic_panel(ui);

                let armed = self.delete_confirm.armed().map(String::as_str);
                let requested = self.health.as_ref().and_then(|view| {
                    ui.add_space(10.0);
                    system_panel(ui, view);
                    ui.add_space(10.0);
                    swarm_panel(ui, view, armed)
                });
                if let Some((info_hash, action)) = requested {
                    self.act_on_torrent(info_hash, action);
                }

                if self.log.detected_url.is_some() {
                    ui.add_space(10.0);
                    self.endpoint_panel(ui, time);
                }

                ui.add_space(10.0);
                self.settings(ui, status);
                ui.add_space(6.0);
            });
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
    fn controls(&mut self, ui: &mut Ui, status: Status) {
        let (label, settled) = match status {
            Status::Stopped => ("./gateway --start", true),
            Status::Starting => ("linking ...", false),
            Status::Running => ("./gateway --stop", true),
            Status::Stopping => ("unlinking ...", false),
        };
        // Not before the probe has said who owns the server, and never while
        // a service action is running -- whichever mode the window thinks it
        // is in, a second owner started then fights the first for the port.
        let power_enabled =
            settled && self.service_known && self.binary.is_some() && self.pending == Pending::None;

        // Enabled on "a server answered /health", not on "this app started
        // it" -- the gateway is often launched from a terminal, and a button
        // that silently does nothing is worse than one that is plainly
        // unavailable. (The log dump lives in the tty panel with the logs.)
        let live = self.health.is_some();
        let clearing = self.clear.in_flight();
        let armed = self.clear_confirm.is_armed(&());
        let clear_label = if clearing {
            "purging ..."
        } else if armed {
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
            power_clicked =
                widgets::command_button(&mut cols[0], label, PHOSPHOR, power_enabled, 40.0)
                    .clicked();
            clear_clicked = widgets::command_button(
                &mut cols[1],
                clear_label,
                if armed { RED } else { PHOSPHOR },
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

        if clear_clicked && self.clear_confirm.arm_or_fire(()) {
            self.clear_cache();
        }

        if self.clear_confirm.is_armed(&()) {
            ui.horizontal(|ui| {
                ui.label(theme::caption(
                    "!! deletes every download, finished or not",
                    RED,
                ));
                if widgets::ghost_button(ui, "abort", TEXT_DIM).clicked() {
                    self.clear_confirm.disarm();
                }
            });
        }
        if let Some(message) = &self.notice {
            ui.label(theme::caption(message.as_str(), TEXT_DIM));
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
    fn service_panel(&self, ui: &mut Ui) {
        let always_on = self.service.always_on();
        theme::section(
            ui,
            TITLE_ALWAYS_ON,
            if always_on { PHOSPHOR } else { AMBER },
            |ui| {
                if self.service_known && !self.systemd {
                    ui.label(theme::body(
                        "no systemd user session here -- the gateway can only run\n\
                         while this window is open",
                        TEXT_DIM,
                    ));
                    return;
                }

                let state = match self.pending {
                    _ if !self.service_known => "service :: checking ...",
                    Pending::Install => "service :: installing ...",
                    Pending::Start => "service :: starting ...",
                    Pending::Stop => "service :: stopping ...",
                    Pending::None => "",
                };
                let color = if always_on { PHOSPHOR } else { TEXT };
                if state.is_empty() {
                    ui.label(theme::body(
                        format!("service :: {}", self.service_summary),
                        color,
                    ));
                } else {
                    ui.label(theme::body(state, color));
                }

                ui.label(theme::caption(
                    "always on :: starts at boot, keeps running when this window closes\n\
                     to stop it: tray icon -> Exit NovaStream",
                    TEXT_DIM,
                ));

                if let Some(error) = &self.service_error {
                    ui.label(theme::caption(format!("!! {error}"), RED));
                }
            },
        );
    }

    fn traffic_panel(&self, ui: &mut Ui) {
        let traffic = &self.traffic;
        theme::section(ui, TITLE_NET, PHOSPHOR, |ui| {
            ui.horizontal(|ui| {
                ui.label(theme::mono(
                    traffic.down_label.as_str(),
                    theme::SIZE_READOUT,
                    PHOSPHOR,
                ));
                ui.label(theme::caption("DOWN", TEXT_DIM));
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(theme::caption("UP", TEXT_DIM));
                    ui.label(theme::mono(
                        traffic.up_label.as_str(),
                        theme::SIZE_SUBREADOUT,
                        TEXT,
                    ));
                });
            });
            ui.add_space(4.0);
            widgets::traffic_graph(
                ui,
                72.0,
                &[
                    widgets::Trace {
                        samples: traffic.down.samples(),
                        color: PHOSPHOR,
                        label: "down",
                    },
                    // Pale against bright: the two traces stay tellable apart
                    // by weight rather than by a second hue.
                    widgets::Trace {
                        samples: traffic.up.samples(),
                        color: TEXT,
                        label: "up",
                    },
                ],
                "MiB/s",
            );
        });
    }

    fn endpoint_panel(&mut self, ui: &mut Ui, time: f64) {
        let Some(url) = self.log.detected_url.as_deref() else {
            return;
        };
        let manifest = self.log.addon_manifest.as_deref();
        let copied = self
            .chrome
            .copied
            .as_ref()
            .filter(|(_, at)| time - at < 2.0)
            .map(|(text, _)| text.as_str());
        let moved_from = self.addon_moved_from.as_deref();
        let mut to_copy = None;
        let mut copied_manifest = false;

        theme::section(ui, TITLE_ENDPOINTS, PHOSPHOR, |ui| {
            // The addon URL goes first and the plain-http one is labelled for
            // what it is: the http address is not a usable addon URL on a
            // phone, and showing it as one is the single most common way this
            // looks broken.
            ui.label(theme::caption(
                "STREMIO ADDON  (phone / tablet / tv)",
                TEXT_DIM,
            ));
            match manifest {
                Some(manifest) => {
                    if copyable_line(ui, manifest, copied == Some(manifest)) {
                        to_copy = Some(manifest.to_string());
                        copied_manifest = true;
                    }
                    // Stremio gives no reason when an installed addon stops
                    // answering, so the window has to. See `addon_address`.
                    if let Some(previous) = moved_from {
                        ui.label(theme::body(ADDON_MOVED, AMBER));
                        ui.label(theme::caption(
                            format!("{ADDON_MOVED_WAS}{previous}"),
                            TEXT_DIM,
                        ));
                        ui.label(theme::caption(ADDON_MOVED_HINT, TEXT_DIM));
                    }
                }
                None => {
                    ui.label(theme::body(
                        "preparing the https certificate, a few seconds ...",
                        AMBER,
                    ));
                }
            }

            ui.add_space(8.0);
            ui.label(theme::caption("DIRECT ADDRESS  (vlc / browsers)", TEXT_DIM));
            if copyable_line(ui, url, copied == Some(url)) {
                to_copy = Some(url.to_string());
            }
        });

        // Copying the new address is taken as reinstalling the addon with it.
        if copied_manifest && self.addon_moved_from.is_some() {
            self.remember_addon_address();
        }
        if let Some(text) = to_copy {
            ui.output_mut(|o| o.copied_text = text.clone());
            self.chrome.copied = Some((text, time));
        }
    }

    fn settings(&mut self, ui: &mut Ui, status: Status) {
        ui.collapsing(theme::body("> config --edit", TEXT_DIM), |ui| {
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
        });
    }

    pub fn tty_panel(&mut self, ui: &mut Ui, time: f64) {
        // The dump belongs with the logs it dumps. Only offered while a
        // server is answering /health: the export is served by the gateway's
        // own /audit/export endpoint, so without a server there is nothing to
        // dump from.
        let live = self.health.is_some();
        let exporting = self.export.in_flight();
        let export_result = self.export_result.as_deref();
        let lines = &self.log.lines;
        let mut export_clicked = false;

        theme::section(ui, TITLE_TTY, PHOSPHOR, |ui| {
            ui.horizontal(|ui| {
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if live {
                        let label = if exporting {
                            "dumping ..."
                        } else {
                            "dump logs"
                        };
                        if widgets::ghost_button(ui, label, TEXT_DIM).clicked() && !exporting {
                            export_clicked = true;
                        }
                    }
                    if let Some(message) = export_result {
                        ui.label(theme::caption(message, TEXT_DIM));
                    }
                });
            });
            ui.set_min_height(ui.available_height().max(40.0));
            log_view(ui, lines, time);
        });

        if export_clicked {
            self.export_logs();
        }
    }
}

/// The log lines, then the live prompt.
///
/// Only the rows in view are laid out (`show_rows`), which needs every row
/// the same height -- so lines do not wrap, and the area scrolls sideways for
/// the long ones instead of cutting them off.
fn log_view(ui: &mut Ui, lines: &VecDeque<LogEntry>, time: f64) {
    let font = FontId::monospace(theme::SIZE_BODY);
    let row_height = ui.fonts(|f| f.row_height(&font));
    // The placeholder takes the one body row when there is nothing yet.
    let body_rows = lines.len().max(1);

    egui::ScrollArea::both()
        .stick_to_bottom(true)
        .auto_shrink([false, false])
        .show_rows(ui, row_height, body_rows + 1, |ui, rows| {
            for row in rows {
                if row == body_rows {
                    prompt(ui, &font, row_height, time);
                } else if let Some(line) = lines.get(row) {
                    ui.add(egui::Label::new(theme::body(line.text.as_str(), line.color)).extend());
                } else {
                    ui.add(egui::Label::new(theme::body(NO_OUTPUT, TEXT_DIM)).extend());
                }
            }
        });
}

/// The live prompt: the one thing that makes a log panel read as a terminal
/// rather than as a text dump. Painted into exactly one row's height, so the
/// row maths `show_rows` does stays true for the last row too.
fn prompt(ui: &mut Ui, font: &FontId, row_height: f32, time: f64) {
    let galley = ui
        .painter()
        .layout_no_wrap(PROMPT.to_string(), font.clone(), PHOSPHOR);
    let caret = egui::vec2(7.0, row_height.min(12.0));
    let (rect, _) = ui.allocate_exact_size(
        egui::vec2(galley.size().x + caret.x, row_height),
        egui::Sense::hover(),
    );
    let text_pos = egui::pos2(rect.left(), rect.center().y - galley.size().y / 2.0);
    let caret_left = text_pos.x + galley.size().x;
    ui.painter().galley(text_pos, galley, PHOSPHOR);
    if fx::cursor_on(time) {
        ui.painter().rect_filled(
            egui::Rect::from_center_size(
                egui::pos2(caret_left + caret.x / 2.0, rect.center().y),
                caret,
            ),
            egui::Rounding::ZERO,
            PHOSPHOR,
        );
    }
}

/// A selectable URL with a `[copy]` affordance and a two-second confirmation,
/// so a click has visible consequences on a clipboard the user cannot
/// otherwise see. True when `[copy]` was clicked.
fn copyable_line(ui: &mut Ui, text: &str, just_copied: bool) -> bool {
    let mut clicked = false;
    ui.horizontal_wrapped(|ui| {
        ui.label(theme::body(text, TEXT));
        let (label, color) = if just_copied {
            ("copied", PHOSPHOR)
        } else {
            ("copy", TEXT_DIM)
        };
        clicked = widgets::ghost_button(ui, label, color).clicked();
    });
    clicked
}

fn system_panel(ui: &mut Ui, view: &HealthView) {
    theme::section(ui, TITLE_SYSTEM, PHOSPHOR, |ui| {
        for (label, meter) in [
            ("CACHE", &view.cache),
            ("CPU", &view.cpu),
            ("MEM", &view.memory),
        ] {
            widgets::meter(ui, label, meter.fraction, &meter.value, meter.color);
        }
        ui.add_space(4.0);
        ui.horizontal(|ui| {
            for (label, value, color) in [
                ("STREAMS ", &view.streams, PHOSPHOR),
                ("PEERS ", &view.peers, TEXT),
                ("SWARMS ", &view.swarms, TEXT),
            ] {
                ui.label(theme::caption(label, TEXT_DIM));
                ui.label(theme::mono(value.as_str(), theme::SIZE_COUNTER, color));
                ui.add_space(10.0);
            }
        });
    });
}

/// The torrents, one row each. Returns the row button pressed, if any, for
/// the caller to act on once the panel is drawn.
fn swarm_panel(
    ui: &mut Ui,
    view: &HealthView,
    delete_armed: Option<&str>,
) -> Option<(String, widgets::RowAction)> {
    let mut requested = None;
    theme::section(ui, &view.swarm_title, PHOSPHOR, |ui| {
        if view.torrents.is_empty() {
            ui.label(theme::body("idle -- no torrent attached", TEXT_DIM));
            return;
        }
        for (index, torrent) in view.torrents.iter().enumerate() {
            if index > 0 {
                ui.add_space(8.0);
            }
            let row = widgets::SwarmRow {
                name: &torrent.name,
                state: &torrent.state,
                state_label: &torrent.state_label,
                held: torrent.held,
                progress: torrent.progress,
                stats: &torrent.stats,
                delete_armed: delete_armed == Some(torrent.info_hash.as_str()),
            };
            if let Some(action) = widgets::swarm_row(ui, &row) {
                requested = Some((torrent.info_hash.clone(), action));
            }
        }
    });
    requested
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Section titles are drawn as given (see `theme::section`), so they must
    /// already be the capitals the old code produced per frame -- and ASCII,
    /// like every other string this window draws.
    #[test]
    fn section_titles_are_ascii_capitals() {
        for title in [
            TITLE_ALWAYS_ON,
            TITLE_NET,
            TITLE_SYSTEM,
            TITLE_ENDPOINTS,
            TITLE_TTY,
        ] {
            assert!(title.is_ascii(), "{title}");
            assert_eq!(title, title.to_uppercase(), "{title}");
        }
        for text in [
            PROMPT,
            NO_OUTPUT,
            LOCAL_VERSION,
            ADDON_MOVED,
            ADDON_MOVED_WAS,
            ADDON_MOVED_HINT,
        ] {
            assert!(text.is_ascii());
        }
    }
}
