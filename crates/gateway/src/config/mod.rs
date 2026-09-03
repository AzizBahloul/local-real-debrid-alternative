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

    /// Torrent index queried to turn an IMDB id into candidate torrents.
    /// Must speak the Stremio stream protocol (`/stream/{type}/{id}.json`).
    #[arg(
        long,
        env = "INDEXER_URL",
        default_value = "https://torrentio.strem.fun"
    )]
    pub indexer_url: String,

    /// Disable torrent discovery entirely. The addon then only answers for
    /// ids that already carry a magnet link, and never makes outbound calls.
    #[arg(long, env = "DISABLE_INDEXER", default_value_t = false)]
    pub disable_indexer: bool,

    /// Maximum number of streams shown per title.
    #[arg(long, env = "INDEXER_MAX_RESULTS", default_value_t = 15)]
    pub indexer_max_results: usize,

    /// Timeout (seconds) for a single index query. Kept well under the
    /// player's own patience so a slow index degrades to "no results"
    /// rather than a spinner that never resolves.
    #[arg(long, env = "INDEXER_TIMEOUT_SECS", default_value_t = 10)]
    pub indexer_timeout_secs: u64,

    /// Bytes of the requested range to have in hand before the gateway sends
    /// its first byte of a video response.
    ///
    /// A player treats "headers arrived, then the body stalled" as a broken
    /// stream -- it gives up, or reports a 00:00 duration -- but it waits
    /// patiently on a request that is merely slow to start. Holding the
    /// response until real data exists converts the former into the latter.
    /// Costs nothing once a torrent is cached (the read is then instant).
    /// Set to 0 to disable.
    #[arg(long, env = "PREBUFFER_BYTES", default_value_t = 4 * 1024 * 1024)]
    pub prebuffer_bytes: usize,

    /// Cap on how long that pre-buffer wait may take. On timeout the response
    /// is sent with whatever arrived, so a slow torrent still plays rather
    /// than hanging forever.
    ///
    /// Kept small enough that metadata fetch + initialization + this stays
    /// under the router's 60s request timeout -- see the cold-start budget in
    /// the `torrent` module.
    #[arg(long, env = "PREBUFFER_TIMEOUT_SECS", default_value_t = 15)]
    pub prebuffer_timeout_secs: u64,

    /// How long a video response may produce no bytes at all before the
    /// gateway re-opens its read stream underneath the player.
    ///
    /// A torrent read that is waiting on a piece has no timeout of its own and
    /// nothing wakes it if the swarm goes quiet, so without this a stalled
    /// stream stays stalled forever and the picture simply freezes. Re-opening
    /// restarts peer discovery for the file and re-points the download at the
    /// current playback position -- the same recovery a viewer triggers by
    /// hand when they skip forward, done automatically and invisibly.
    ///
    /// Must stay comfortably above the time to fetch one piece (4-16 MB) or
    /// healthy slow downloads get re-opened needlessly. Set to 0 to disable.
    #[arg(long, env = "STALL_TIMEOUT_SECS", default_value_t = 20)]
    pub stall_timeout_secs: u64,

    /// Pause a torrent after this many seconds with nobody watching it.
    ///
    /// Every running torrent competes for the same upstream bandwidth, so an
    /// abandoned 4K release starves the one you are actually watching. A
    /// torrent with a live reader is never paused regardless of this value.
    ///
    /// Pausing is not free: it drops every peer connection, and they have to
    /// be rediscovered over DHT/trackers next time, which is most of the wait
    /// when a title is slow to start. So this errs on the patient side --
    /// reclaiming bandwidth from something genuinely abandoned is worth a few
    /// minutes, but paying the peer-rediscovery cost on a title someone
    /// stepped away from briefly is not. Set to 0 to never pause.
    #[arg(long, env = "IDLE_PAUSE_SECS", default_value_t = 300)]
    pub idle_pause_secs: u64,

    /// How often (seconds) to look for idle torrents to pause.
    #[arg(long, env = "IDLE_CHECK_INTERVAL_SECS", default_value_t = 30)]
    pub idle_check_interval_secs: u64,

    /// Public base URL the Stremio addon should advertise for video playback.
    /// Defaults to this machine's LAN address, which is what you want: the
    /// manifest may be reached through an https tunnel, but the video itself
    /// should stream straight over the LAN rather than through it.
    #[arg(long, env = "PUBLIC_STREAM_URL")]
    pub public_stream_url: Option<String>,
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
