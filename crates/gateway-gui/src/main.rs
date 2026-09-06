mod server_process;

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};

use serde::Deserialize;
use server_process::{find_server_binary, ServerProcess};

const MAX_LOG_LINES: usize = 500;
const HEALTH_POLL_INTERVAL: Duration = Duration::from_secs(2);
const STOP_GRACE_PERIOD: Duration = Duration::from_secs(5);

#[derive(Debug, Deserialize, Clone)]
struct HealthInfo {
    uptime_seconds: u64,
    active_streams: usize,
    cache_usage_bytes: u64,
    cache_max_bytes: u64,
    process_memory_bytes: u64,
    process_cpu_percent: f32,
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

/// A little robot face that reacts to what the gateway is doing. Pure ASCII
/// on purpose: the exact glyph coverage of the font eframe bundles isn't
/// guaranteed beyond plain ASCII, and a "funny face" that renders as a row
/// of tofu boxes on someone else's machine isn't funny.
fn ascii_face(status: Status) -> &'static str {
    match status {
        Status::Stopped => "   .-----.  \n  ( o   o )   zzz...\n   '-----'  ",
        Status::Starting => "   .-----.  \n  ( O   O )   ...\n   '-----'  ",
        Status::Running => "   .-----.  \n  ( ^   ^ )   >> LIVE <<\n   '-----'  ",
        Status::Stopping => "   .-----.  \n  ( -   - )   ...\n   '-----'  ",
    }
}

const ACCENT_GREEN: egui::Color32 = egui::Color32::from_rgb(57, 255, 133);
const ACCENT_RED: egui::Color32 = egui::Color32::from_rgb(255, 82, 82);
const ACCENT_AMBER: egui::Color32 = egui::Color32::from_rgb(255, 196, 0);
const ACCENT_CYAN: egui::Color32 = egui::Color32::from_rgb(0, 224, 255);
const BG_DEEP: egui::Color32 = egui::Color32::from_rgb(8, 11, 16);
const BG_PANEL: egui::Color32 = egui::Color32::from_rgb(15, 20, 28);

fn futuristic_visuals() -> egui::Visuals {
    let mut visuals = egui::Visuals::dark();
    visuals.panel_fill = BG_DEEP;
    visuals.window_fill = BG_DEEP;
    visuals.extreme_bg_color = BG_PANEL;
    visuals.faint_bg_color = BG_PANEL;
    visuals.widgets.noninteractive.bg_fill = BG_PANEL;
    visuals.widgets.inactive.bg_fill = BG_PANEL;
    visuals.widgets.inactive.weak_bg_fill = BG_PANEL;
    visuals.selection.bg_fill = ACCENT_CYAN.linear_multiply(0.35);
    visuals.hyperlink_color = ACCENT_CYAN;
    visuals.window_rounding = egui::Rounding::same(10.0);
    visuals.menu_rounding = egui::Rounding::same(10.0);
    visuals
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

    stopping: bool,
    stop_done_rx: Option<Receiver<()>>,

    start_error: Option<String>,

    /// True once "clear everything" has been clicked and is waiting for the
    /// confirming second click. Deleting every download is not undoable, so
    /// it does not hang off a single mis-tap next to the stop button.
    clear_armed: bool,
    clear_rx: Option<Receiver<Result<usize, String>>>,
    clear_result: Option<String>,
}

impl Default for GatewayApp {
    fn default() -> Self {
        Self {
            binary: find_server_binary(),
            process: ServerProcess::new(),
            running: false,
            port: "8080".to_string(),
            cache_dir: "./cache".to_string(),
            logs: VecDeque::with_capacity(MAX_LOG_LINES),
            detected_url: None,
            addon_url: None,
            health: None,
            health_rx: None,
            next_health_poll: Instant::now(),
            stopping: false,
            stop_done_rx: None,
            start_error: None,
            clear_armed: false,
            clear_rx: None,
            clear_result: None,
        }
    }
}

impl GatewayApp {
    fn status(&self) -> Status {
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
        self.logs.push_back(line);
    }

    fn start_server(&mut self) {
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

    fn stop_server(&mut self) {
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

    fn poll_background_work(&mut self) {
        // Reap a server that exited on its own (crash, port conflict, ^C
        // from the terminal it was launched from, etc.).
        if self.running && !self.process.is_running() {
            self.running = false;
            self.detected_url = None;
            self.addon_url = None;
            self.health = None;
        }

        for line in self.process.drain_logs() {
            if self.detected_url.is_none() {
                if let Some(url) = extract_url(line.text()) {
                    self.detected_url = Some(url);
                }
            }
            if self.addon_url.is_none() {
                if let Some(url) = extract_addon_url(line.text()) {
                    self.addon_url = Some(url);
                }
            }
            let text = if line.is_err() {
                format!("[stderr] {}", line.text())
            } else {
                line.text().to_string()
            };
            self.push_log(text);
        }

        if let Some(rx) = &self.clear_rx {
            if let Ok(outcome) = rx.try_recv() {
                self.clear_result = Some(match outcome {
                    Ok(0) => "Nothing to clear.".to_string(),
                    Ok(1) => "Cleared 1 download.".to_string(),
                    Ok(n) => format!("Cleared {n} downloads."),
                    Err(e) => format!("Could not clear: {e}"),
                });
                self.clear_rx = None;
                // Force the next health poll so the freed space shows at once
                // rather than after the regular interval.
                self.next_health_poll = Instant::now();
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
        {
            if let Some(rx) = &self.health_rx {
                if let Ok(result) = rx.try_recv() {
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
    }
}

impl eframe::App for GatewayApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.poll_background_work();

        let status = self.status();

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.style_mut().override_text_style = Some(egui::TextStyle::Monospace);

            ui.add_space(4.0);
            ui.horizontal(|ui| {
                ui.label(
                    egui::RichText::new(">_")
                        .color(ACCENT_CYAN)
                        .size(26.0)
                        .strong(),
                );
                ui.label(
                    egui::RichText::new("NOVASTREAM")
                        .color(ACCENT_CYAN)
                        .size(22.0)
                        .strong(),
                );
            });
            ui.label(
                egui::RichText::new(
                    "magnet -> local HTTP stream, for Stremio / VLC / phones / TVs",
                )
                .color(egui::Color32::GRAY)
                .size(12.0),
            );
            ui.add_space(10.0);

            if let Some(err) = &self.start_error {
                ui.colored_label(ACCENT_RED, format!("! {err}"));
                ui.add_space(6.0);
            }
            if self.binary.is_none() {
                ui.colored_label(
                    ACCENT_AMBER,
                    "! streaming-gateway binary not found next to this app or on PATH",
                );
                ui.add_space(6.0);
            }

            // The face: same box on every frame, only the contents change, so
            // switching state never reflows the rest of the layout.
            egui::Frame::none()
                .fill(BG_PANEL)
                .rounding(egui::Rounding::same(8.0))
                .inner_margin(egui::Margin::symmetric(12.0, 10.0))
                .show(ui, |ui| {
                    ui.set_width(ui.available_width());
                    let face_color = match status {
                        Status::Stopped => egui::Color32::GRAY,
                        Status::Starting => ACCENT_AMBER,
                        Status::Running => ACCENT_GREEN,
                        Status::Stopping => ACCENT_AMBER,
                    };
                    ui.colored_label(
                        face_color,
                        egui::RichText::new(ascii_face(status)).size(18.0),
                    );
                });

            ui.add_space(12.0);

            // The one big button: what it says and does flips with state, so
            // there is always exactly one obvious next action.
            let (label, color, enabled) = match status {
                Status::Stopped => ("\u{25B6}  START GATEWAY", ACCENT_GREEN, true),
                Status::Starting => ("\u{25CB}  STARTING...", ACCENT_AMBER, false),
                Status::Running => ("\u{25A0}  STOP GATEWAY", ACCENT_RED, true),
                Status::Stopping => ("\u{25CB}  STOPPING...", ACCENT_AMBER, false),
            };
            let width = ui.available_width();
            let big_button = egui::Button::new(
                egui::RichText::new(label)
                    .size(20.0)
                    .strong()
                    .color(BG_DEEP),
            )
            .fill(color)
            .rounding(egui::Rounding::same(8.0))
            .min_size(egui::vec2(width, 56.0));
            if ui
                .add_enabled(enabled && self.binary.is_some(), big_button)
                .clicked()
            {
                match status {
                    Status::Stopped => self.start_server(),
                    Status::Running => self.stop_server(),
                    _ => {}
                }
            }

            // Clear-everything. Only offered while the server is up, because
            // it works by asking the server: with nothing running there is no
            // one to ask, and a button that silently does nothing is worse
            // than one that is plainly unavailable.
            ui.add_space(8.0);
            let clearing = self.clear_rx.is_some();
            let clear_label = if clearing {
                "\u{25CB}  CLEARING...".to_string()
            } else if self.clear_armed {
                "\u{26A0}  TAP AGAIN TO DELETE EVERYTHING".to_string()
            } else {
                "\u{1F5D1}  CLEAR ALL DOWNLOADS".to_string()
            };
            let clear_button = egui::Button::new(
                egui::RichText::new(clear_label)
                    .size(14.0)
                    .strong()
                    .color(if self.clear_armed { BG_DEEP } else { ACCENT_RED }),
            )
            .fill(if self.clear_armed {
                ACCENT_RED
            } else {
                BG_PANEL
            })
            .rounding(egui::Rounding::same(8.0))
            .min_size(egui::vec2(ui.available_width(), 34.0));

            // Enabled on "a server answered /health", not on "this app
            // started it" -- the gateway is often launched outside the app.
            if ui
                .add_enabled(self.health.is_some() && !clearing, clear_button)
                .clicked()
            {
                if self.clear_armed {
                    self.clear_armed = false;
                    self.clear_cache();
                } else {
                    self.clear_armed = true;
                }
            }
            if self.clear_armed {
                ui.label(
                    egui::RichText::new(
                        "Deletes every download, finished or not. Cannot be undone.",
                    )
                    .color(ACCENT_AMBER)
                    .size(11.0),
                );
                if ui.small_button("cancel").clicked() {
                    self.clear_armed = false;
                }
            }
            if let Some(msg) = &self.clear_result {
                ui.label(
                    egui::RichText::new(msg)
                        .color(egui::Color32::GRAY)
                        .size(11.0),
                );
            }

            ui.add_space(12.0);
            ui.collapsing("Advanced settings", |ui| {
                ui.add_enabled_ui(status == Status::Stopped, |ui| {
                    egui::Grid::new("settings_grid")
                        .num_columns(2)
                        .show(ui, |ui| {
                            ui.label("Port:");
                            ui.text_edit_singleline(&mut self.port);
                            ui.end_row();
                            ui.label("Cache directory:");
                            ui.text_edit_singleline(&mut self.cache_dir);
                            ui.end_row();
                        });
                });
            });

            if let Some(url) = self.detected_url.clone() {
                let addon = self.addon_url.clone();
                ui.add_space(10.0);
                egui::Frame::none()
                    .fill(BG_PANEL)
                    .rounding(egui::Rounding::same(8.0))
                    .inner_margin(egui::Margin::symmetric(12.0, 8.0))
                    .show(ui, |ui| {
                        ui.set_width(ui.available_width());
                        // The addon URL goes first and the plain-http one is
                        // labelled for what it is: the http address is not a
                        // usable addon URL on a phone, and showing it as one
                        // is the single most common way this looks broken.
                        ui.colored_label(ACCENT_CYAN, "STREMIO ADDON (phone, tablet, TV)");
                        match &addon {
                            Some(addon) => {
                                let manifest = format!("{addon}/manifest.json");
                                ui.horizontal(|ui| {
                                    ui.monospace(&manifest);
                                    if ui.small_button("copy").clicked() {
                                        ui.output_mut(|o| o.copied_text = manifest.clone());
                                    }
                                });
                            }
                            None => {
                                ui.label(
                                    egui::RichText::new(
                                        "preparing the https certificate, a few seconds...",
                                    )
                                    .color(egui::Color32::GRAY)
                                    .size(11.0),
                                );
                            }
                        }

                        ui.add_space(8.0);
                        ui.colored_label(ACCENT_CYAN, "DIRECT ADDRESS (VLC, browsers)");
                        ui.horizontal(|ui| {
                            ui.monospace(&url);
                            if ui.small_button("copy").clicked() {
                                ui.output_mut(|o| o.copied_text = url.clone());
                            }
                        });
                    });
            }

            if let Some(health) = &self.health {
                ui.add_space(10.0);
                egui::Frame::none()
                    .fill(BG_PANEL)
                    .rounding(egui::Rounding::same(8.0))
                    .inner_margin(egui::Margin::symmetric(12.0, 8.0))
                    .show(ui, |ui| {
                        ui.set_width(ui.available_width());
                        egui::Grid::new("health_grid")
                            .num_columns(2)
                            .show(ui, |ui| {
                                ui.label("uptime");
                                ui.label(human_duration(health.uptime_seconds));
                                ui.end_row();
                                ui.label("active streams");
                                ui.label(health.active_streams.to_string());
                                ui.end_row();
                                ui.label("cache");
                                ui.label(format!(
                                    "{} / {}",
                                    human_bytes(health.cache_usage_bytes),
                                    human_bytes(health.cache_max_bytes)
                                ));
                                ui.end_row();
                                ui.label("process");
                                ui.label(format!(
                                    "{} RAM, {:.1}% CPU",
                                    human_bytes(health.process_memory_bytes),
                                    health.process_cpu_percent
                                ));
                                ui.end_row();
                            });
                    });
            }

            ui.add_space(10.0);
            ui.collapsing("Log", |ui| {
                egui::ScrollArea::vertical()
                    .max_height(200.0)
                    .stick_to_bottom(true)
                    .show(ui, |ui| {
                        for line in &self.logs {
                            ui.monospace(line);
                        }
                    });
            });
        });

        ctx.request_repaint_after(Duration::from_millis(200));
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

fn main() -> eframe::Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([460.0, 560.0])
            .with_min_inner_size([380.0, 480.0])
            .with_icon(app_icon()),
        ..Default::default()
    };
    eframe::run_native(
        "NovaStream",
        options,
        Box::new(|cc| {
            cc.egui_ctx.set_visuals(futuristic_visuals());
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
}
