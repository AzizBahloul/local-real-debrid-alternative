use std::net::IpAddr;
use std::path::PathBuf;

use clap::Parser;

/// Runtime configuration for the streaming gateway.
///
/// Every field can be set via CLI flag or environment variable (see `env = "..."`
/// on each field) so the same binary works unconfigured (sane defaults), via
/// `.env`/systemd `Environment=`, or via explicit flags.
#[derive(Parser, Debug, Clone)]
#[command(
    name = "streaming-gateway",
    about = "Local high-performance Stremio streaming gateway"
)]
pub struct AppConfig {
    /// Primary port to bind the HTTP gateway on.
    #[arg(long, env = "GATEWAY_PORT", default_value_t = 11470)]
    pub port: u16,

    /// Fallback port used if the primary port is already taken.
    #[arg(long, env = "GATEWAY_FALLBACK_PORT", default_value_t = 8080)]
    pub fallback_port: u16,

    /// Address to bind on. 0.0.0.0 exposes the gateway to the whole LAN.
    #[arg(long, env = "GATEWAY_BIND_ADDR", default_value = "0.0.0.0")]
    pub bind_addr: IpAddr,

    /// Directory used to store downloaded torrent data and session state.
    #[arg(long, env = "CACHE_DIRECTORY", default_value = "./cache")]
    pub cache_dir: PathBuf,

    /// Maximum size of the cache directory, in gigabytes, before old data is evicted.
    #[arg(long, env = "MAX_CACHE_SIZE_GB", default_value_t = 20)]
    pub max_cache_size_gb: u64,

    /// Whether to automatically evict old cached torrents when over the size cap.
    #[arg(long, env = "AUTO_CLEANUP", default_value_t = true)]
    pub auto_cleanup: bool,

    /// How often (seconds) the cache janitor checks disk usage.
    #[arg(long, env = "CACHE_CLEANUP_INTERVAL_SECS", default_value_t = 60)]
    pub cleanup_interval_secs: u64,

    /// Maximum number of torrents actively managed (downloading/seeding) at once.
    #[arg(long, env = "MAX_CONCURRENT_TORRENTS", default_value_t = 8)]
    pub max_concurrent_torrents: usize,

    /// Disable BitTorrent DHT (uses trackers/peer exchange only).
    #[arg(long, env = "DISABLE_DHT", default_value_t = false)]
    pub disable_dht: bool,

    /// How often (seconds) to print the terminal monitoring status.
    #[arg(long, env = "MONITOR_INTERVAL_SECS", default_value_t = 5)]
    pub monitor_interval_secs: u64,

    /// Log level filter (passed to `tracing_subscriber::EnvFilter`).
    #[arg(long, env = "LOG_LEVEL", default_value = "info")]
    pub log_level: String,
}

impl AppConfig {
    pub fn load() -> Self {
        Self::parse()
    }

    pub fn max_cache_size_bytes(&self) -> u64 {
        self.max_cache_size_gb.saturating_mul(1024 * 1024 * 1024)
    }

    pub fn downloads_dir(&self) -> PathBuf {
        self.cache_dir.join("downloads")
    }

    pub fn session_state_dir(&self) -> PathBuf {
        self.cache_dir.join("session")
    }
}
