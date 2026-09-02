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
    let start = line.find("http://")?;
    let rest = &line[start..];
    let end = rest
        .find(|c: char| c.is_whitespace())
        .unwrap_or(rest.len());
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

    health: Option<HealthInfo>,
    health_rx: Option<Receiver<Option<HealthInfo>>>,
    next_health_poll: Instant,

    stopping: bool,
    stop_done_rx: Option<Receiver<()>>,

    start_error: Option<String>,
}

impl Default for GatewayApp {
    fn default() -> Self {
        Self {
            binary: find_server_binary(),
            process: ServerProcess::new(),
            running: false,
            port: "11470".to_string(),
            cache_dir: "./cache".to_string(),
            logs: VecDeque::with_capacity(MAX_LOG_LINES),
            detected_url: None,
            health: None,
            health_rx: None,
            next_health_poll: Instant::now(),
            stopping: false,
            stop_done_rx: None,
            start_error: None,
        }
    }
}

impl GatewayApp {
    fn push_log(&mut self, line: String) {
        if self.logs.len() >= MAX_LOG_LINES {
            self.logs.pop_front();
        }
        self.logs.push_back(line);
    }

    fn start_server(&mut self) {
        let Some(binary) = self.binary.clone() else {
            self.start_error = Some("streaming-gateway binary not found next to this app or on PATH".to_string());
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
                self.logs.clear();
            }
            Err(e) => self.start_error = Some(format!("{e:#}")),
        }
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
            self.health = None;
        }

        for line in self.process.drain_logs() {
            if self.detected_url.is_none() {
                if let Some(url) = extract_url(line.text()) {
                    self.detected_url = Some(url);
                }
            }
            let text = if line.is_err() {
                format!("[stderr] {}", line.text())
            } else {
                line.text().to_string()
            };
            self.push_log(text);
        }

        if let Some(rx) = &self.stop_done_rx {
            if rx.try_recv().is_ok() {
                self.stopping = false;
                self.running = false;
                self.detected_url = None;
                self.health = None;
                self.stop_done_rx = None;
            }
        }

        if self.running {
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

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("Streaming Gateway");
            ui.label("Local Stremio streaming gateway -- start it, then connect Stremio/VLC/phones/TVs on the same Wi-Fi.");
            ui.add_space(8.0);

            if let Some(err) = &self.start_error {
                ui.colored_label(egui::Color32::from_rgb(220, 60, 60), err);
                ui.add_space(6.0);
            }
            if self.binary.is_none() {
                ui.colored_label(
                    egui::Color32::from_rgb(220, 160, 40),
                    "streaming-gateway binary not found. It should be installed alongside this app.",
                );
                ui.add_space(6.0);
            }

            ui.horizontal(|ui| {
                let (dot, label) = if self.stopping {
                    (egui::Color32::from_rgb(220, 160, 40), "Stopping...")
                } else if self.running {
                    (egui::Color32::from_rgb(60, 190, 90), "Running")
                } else {
                    (egui::Color32::GRAY, "Stopped")
                };
                ui.colored_label(dot, "\u{25CF}");
                ui.label(label);
            });

            ui.add_space(6.0);
            ui.add_enabled_ui(!self.running && !self.stopping, |ui| {
                egui::Grid::new("settings_grid").num_columns(2).show(ui, |ui| {
                    ui.label("Port:");
                    ui.text_edit_singleline(&mut self.port);
                    ui.end_row();
                    ui.label("Cache directory:");
                    ui.text_edit_singleline(&mut self.cache_dir);
                    ui.end_row();
                });
            });

            ui.add_space(8.0);
            ui.horizontal(|ui| {
                if ui
                    .add_enabled(!self.running && !self.stopping, egui::Button::new("Start Server"))
                    .clicked()
                {
                    self.start_server();
                }
                if ui
                    .add_enabled(self.running && !self.stopping, egui::Button::new("Stop Server"))
                    .clicked()
                {
                    self.stop_server();
                }
            });

            if let Some(url) = self.detected_url.clone() {
                ui.add_space(8.0);
                ui.separator();
                ui.label("Gateway address:");
                ui.horizontal(|ui| {
                    ui.monospace(&url);
                    if ui.button("Copy").clicked() {
                        ui.output_mut(|o| o.copied_text = url.clone());
                    }
                });
                ui.label(format!("Add to Stremio as: {url}/manifest.json"));
            }

            if let Some(health) = &self.health {
                ui.add_space(8.0);
                ui.separator();
                ui.label(format!("Uptime: {}", human_duration(health.uptime_seconds)));
                ui.label(format!("Active streams: {}", health.active_streams));
                ui.label(format!(
                    "Cache: {} / {}",
                    human_bytes(health.cache_usage_bytes),
                    human_bytes(health.cache_max_bytes)
                ));
                ui.label(format!(
                    "Process: {} RAM, {:.1}% CPU",
                    human_bytes(health.process_memory_bytes),
                    health.process_cpu_percent
                ));
            }

            ui.add_space(8.0);
            ui.separator();
            ui.label("Log:");
            egui::ScrollArea::vertical()
                .max_height(220.0)
                .stick_to_bottom(true)
                .show(ui, |ui| {
                    for line in &self.logs {
                        ui.monospace(line);
                    }
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

fn main() -> eframe::Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([480.0, 620.0]),
        ..Default::default()
    };
    eframe::run_native(
        "Streaming Gateway",
        options,
        Box::new(|_cc| Ok(Box::new(GatewayApp::default()))),
    )
}
