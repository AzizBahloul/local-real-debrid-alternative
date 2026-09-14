//! What `/health` says, and the strings the dashboard draws from it.
//!
//! [`HealthInfo`] is the wire type and stays exactly that. [`HealthView`] is
//! built from it once per reply -- one a second -- so the panels, which are
//! redrawn thirty times a second, only ever borrow text that already exists
//! instead of formatting it again on every frame.

use egui::Color32;
use serde::Deserialize;

use crate::text::{human_bytes, human_duration};
use crate::theme::{AMBER, PHOSPHOR};

/// Samples kept per trace -- two minutes of history at the poll rate.
pub const HISTORY: usize = 120;

#[derive(Debug, Deserialize, Clone)]
pub struct TorrentInfo {
    pub name: String,
    pub state: String,
    pub progress_percent: f64,
    pub progress_bytes: u64,
    pub total_bytes: u64,
    pub download_speed_mib_s: f64,
    pub upload_speed_mib_s: f64,
    pub peers: u32,
    /// Needed to address the row's own pause/resume/delete calls.
    #[serde(default)]
    pub info_hash: String,
    /// Paused by hand rather than by the download queue. Defaulted so this
    /// window still reads an older server's `/health` -- every row then simply
    /// offers Pause, which is true of a server that has no hand-pause concept.
    #[serde(default)]
    pub held: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HealthInfo {
    pub uptime_seconds: u64,
    pub active_streams: usize,
    pub cache_usage_bytes: u64,
    pub cache_max_bytes: u64,
    pub process_memory_bytes: u64,
    pub process_cpu_percent: f32,
    /// Defaulted so a GUI newer than the server it is pointed at degrades to
    /// an empty swarm panel instead of failing to parse the whole response
    /// and reporting the gateway as down.
    #[serde(default)]
    pub active_torrents: Vec<TorrentInfo>,
    #[serde(default)]
    pub version: String,
}

impl HealthInfo {
    pub fn download_mib_s(&self) -> f32 {
        self.active_torrents
            .iter()
            .map(|t| t.download_speed_mib_s as f32)
            .sum()
    }

    pub fn upload_mib_s(&self) -> f32 {
        self.active_torrents
            .iter()
            .map(|t| t.upload_speed_mib_s as f32)
            .sum()
    }

    pub fn peers(&self) -> u32 {
        self.active_torrents.iter().map(|t| t.peers).sum()
    }
}

/// A fixed-length ring of samples for one graph trace.
///
/// A plain `Vec` with a front-pop: at 120 samples and 1 Hz the shift is
/// irrelevant, and the graph wants a contiguous slice.
#[derive(Default)]
pub struct History(Vec<f32>);

impl History {
    pub fn push(&mut self, value: f32) {
        if self.0.len() >= HISTORY {
            self.0.remove(0);
        }
        self.0.push(value);
    }

    pub fn samples(&self) -> &[f32] {
        &self.0
    }

    pub fn latest(&self) -> f32 {
        self.0.last().copied().unwrap_or(0.0)
    }
}

/// Both graph traces and their headline readouts.
///
/// Sampled on the reply rather than on a timer so the graph scrolls at the
/// rate data actually arrived. A dead server still contributes a zero, which
/// is what draws the traffic falling off a cliff instead of freezing mid-air.
pub struct Traffic {
    pub down: History,
    pub up: History,
    pub down_label: String,
    pub up_label: String,
}

impl Default for Traffic {
    fn default() -> Self {
        let mut traffic = Self {
            down: History::default(),
            up: History::default(),
            down_label: String::new(),
            up_label: String::new(),
        };
        traffic.relabel();
        traffic
    }
}

impl Traffic {
    pub fn record(&mut self, health: Option<&HealthInfo>) {
        self.down
            .push(health.map(HealthInfo::download_mib_s).unwrap_or(0.0));
        self.up
            .push(health.map(HealthInfo::upload_mib_s).unwrap_or(0.0));
        self.relabel();
    }

    fn relabel(&mut self) {
        self.down_label = format!("{:>7.2} MiB/s", self.down.latest());
        self.up_label = format!("{:.2} MiB/s", self.up.latest());
    }
}

/// One meter in the system panel.
pub struct Meter {
    pub fraction: f32,
    pub value: String,
    pub color: Color32,
}

/// One swarm row, ready to draw.
pub struct TorrentView {
    pub name: String,
    pub state: String,
    pub state_label: String,
    pub held: bool,
    pub progress: f32,
    pub stats: String,
    pub info_hash: String,
}

/// Everything the dashboard shows about a live server, pre-rendered.
pub struct HealthView {
    pub uptime: String,
    /// The server's own version, falling back to this app's when an older
    /// server does not report one.
    pub version: String,
    pub cache: Meter,
    pub cpu: Meter,
    pub memory: Meter,
    pub streams: String,
    pub peers: String,
    pub swarms: String,
    /// The swarm panel's title, count included.
    pub swarm_title: String,
    pub torrents: Vec<TorrentView>,
}

impl HealthView {
    pub fn new(health: HealthInfo) -> Self {
        let cache_fraction = if health.cache_max_bytes == 0 {
            0.0
        } else {
            health.cache_usage_bytes as f32 / health.cache_max_bytes as f32
        };
        let cpu = health.process_cpu_percent;
        let version = if health.version.is_empty() {
            env!("CARGO_PKG_VERSION")
        } else {
            health.version.as_str()
        };

        Self {
            uptime: format!("up {}", human_duration(health.uptime_seconds)),
            version: format!("v{version}"),
            cache: Meter {
                fraction: cache_fraction,
                value: format!(
                    "{} / {}",
                    human_bytes(health.cache_usage_bytes),
                    human_bytes(health.cache_max_bytes)
                ),
                color: if cache_fraction > 0.9 {
                    AMBER
                } else {
                    PHOSPHOR
                },
            },
            cpu: Meter {
                fraction: cpu / 100.0,
                value: format!("{cpu:.1} %"),
                color: if cpu > 80.0 { AMBER } else { PHOSPHOR },
            },
            // Scaled against 1 GiB: the gateway is expected to sit far below
            // that, so the bar is a "has something run away?" indicator, not
            // a fraction of anything real.
            memory: Meter {
                fraction: health.process_memory_bytes as f32 / (1024.0 * 1024.0 * 1024.0),
                value: human_bytes(health.process_memory_bytes),
                color: PHOSPHOR,
            },
            streams: health.active_streams.to_string(),
            peers: health.peers().to_string(),
            swarms: health.active_torrents.len().to_string(),
            swarm_title: format!("SWARM [{}]", health.active_torrents.len()),
            torrents: health
                .active_torrents
                .into_iter()
                .map(TorrentView::new)
                .collect(),
        }
    }
}

impl TorrentView {
    fn new(torrent: TorrentInfo) -> Self {
        Self {
            state_label: if torrent.held {
                "HELD".to_string()
            } else {
                torrent.state.to_uppercase()
            },
            progress: (torrent.progress_percent / 100.0) as f32,
            stats: format!(
                "{:.1}%  {} / {}  {} peers  {:.2} MiB/s dn  {:.2} MiB/s up",
                torrent.progress_percent,
                human_bytes(torrent.progress_bytes),
                human_bytes(torrent.total_bytes),
                torrent.peers,
                torrent.download_speed_mib_s,
                torrent.upload_speed_mib_s,
            ),
            name: torrent.name,
            state: torrent.state,
            held: torrent.held,
            info_hash: torrent.info_hash,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn torrent(name: &str, down: f64, up: f64, peers: u32) -> TorrentInfo {
        TorrentInfo {
            name: name.into(),
            state: "live".into(),
            progress_percent: 10.0,
            progress_bytes: 1,
            total_bytes: 10,
            download_speed_mib_s: down,
            upload_speed_mib_s: up,
            peers,
            info_hash: name.repeat(40),
            held: false,
        }
    }

    fn health(torrents: Vec<TorrentInfo>) -> HealthInfo {
        HealthInfo {
            uptime_seconds: 10,
            active_streams: 1,
            cache_usage_bytes: 0,
            cache_max_bytes: 1,
            process_memory_bytes: 0,
            process_cpu_percent: 0.0,
            version: String::new(),
            active_torrents: torrents,
        }
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
        let health = health(vec![
            torrent("a", 1.5, 0.25, 12),
            torrent("b", 2.0, 0.75, 30),
        ]);
        assert!((health.download_mib_s() - 3.5).abs() < 1e-5);
        assert!((health.upload_mib_s() - 1.0).abs() < 1e-5);
        assert_eq!(health.peers(), 42);
    }

    /// An older server does not send `active_torrents`/`version`. Parsing has
    /// to survive that, because a parse failure is indistinguishable from
    /// "the gateway is down" everywhere else in this app.
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
        // And the view still has a version to show.
        let view = HealthView::new(health);
        assert_eq!(view.version, format!("v{}", env!("CARGO_PKG_VERSION")));
    }

    #[test]
    fn the_view_renders_what_the_panels_draw() {
        let mut held = torrent("b", 0.0, 0.0, 0);
        held.held = true;
        let view = HealthView::new(health(vec![torrent("a", 1.5, 0.25, 12), held]));
        assert_eq!(view.swarm_title, "SWARM [2]");
        assert_eq!(view.peers, "12");
        assert_eq!(view.uptime, "up 10s");
        assert_eq!(view.torrents[0].state_label, "LIVE");
        // A hand pause names itself rather than whatever librqbit reports.
        assert_eq!(view.torrents[1].state_label, "HELD");
        assert!(view.torrents[0].stats.contains("12 peers"));
    }

    #[test]
    fn a_missing_reply_records_a_zero_rather_than_freezing_the_graph() {
        let mut traffic = Traffic::default();
        traffic.record(Some(&health(vec![torrent("a", 2.0, 0.5, 1)])));
        assert_eq!(traffic.down_label.trim(), "2.00 MiB/s");
        traffic.record(None);
        assert_eq!(traffic.down.samples(), &[2.0, 0.0]);
        assert_eq!(traffic.up_label, "0.00 MiB/s");
    }
}
