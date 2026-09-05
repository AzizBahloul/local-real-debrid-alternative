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
    #[arg(long, env = "GATEWAY_PORT", default_value_t = 8080)]
    pub port: u16,

    /// Fallback port used if the primary port is already taken.
    #[arg(long, env = "GATEWAY_FALLBACK_PORT", default_value_t = 11470)]
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

    /// Maximum peers to keep connected per torrent.
    ///
    /// Lower than librqbit's own default of 128, on purpose. That default is
    /// tuned for finishing a download as fast as possible; this gateway is
    /// doing something different -- feeding one player, usually from a machine
    /// on wifi, where the torrent's traffic and the video's traffic share one
    /// radio. Every peer slot is a TCP connection that gets opened, retried
    /// and torn down, and most candidates never answer at all, so a high limit
    /// buys a stream of connection attempts (measured at ~9 a second at 128)
    /// whose cost in router NAT-table churn and airtime outweighs the few
    /// extra peers it lands. 60 still saturates any home connection.
    #[arg(long, env = "MAX_PEERS_PER_TORRENT", default_value_t = 60)]
    pub max_peers_per_torrent: usize,

    /// Cap the torrent engine's upload rate, in MB/s. 0 means unlimited.
    ///
    /// Worth setting when this machine is on wifi rather than ethernet:
    /// seeding competes for the same radio the video is being sent over, and
    /// the viewer feels it as buffering. Do not set it too low -- peers
    /// reciprocate, so throttling upload hard also slows the download.
    #[arg(long, env = "MAX_UPLOAD_MB_S", default_value_t = 0)]
    pub max_upload_mb_s: u64,

    /// Cap the torrent engine's download rate, in MB/s. 0 means unlimited.
    ///
    /// The gateway normally fetches far faster than playback consumes, which
    /// is what makes seeking quick. On a wifi-attached machine that surplus is
    /// spent on the same airtime the video needs, so capping it to a little
    /// above the file's real bitrate (size in GB / hours, roughly) can make
    /// playback smoother even though the download gets slower.
    #[arg(long, env = "MAX_DOWNLOAD_MB_S", default_value_t = 0)]
    pub max_download_mb_s: u64,

    /// How long a client may leave sent data unacknowledged before the gateway
    /// drops its connection, in seconds. 0 disables the timeout.
    ///
    /// This is what reclaims a stream whose viewer vanished without closing
    /// the connection. Generous by default because a player that has buffered
    /// minutes ahead legitimately reads nothing for that long, and a player
    /// that does get dropped simply reconnects. See `network::harden_listener`
    /// for why the default of "probe forever" is actively harmful here.
    #[arg(long, env = "CLIENT_TIMEOUT_SECS", default_value_t = 900)]
    pub client_timeout_secs: u64,

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

    /// Disable the https listener.
    ///
    /// https exists here for one reason: Stremio's Android app will not
    /// install an addon from a plain `http://192.168.x.x` URL. The http
    /// listener stays up either way and still serves the video, so turning
    /// this off only costs the ability to add the addon without a tunnel.
    #[arg(long, env = "DISABLE_HTTPS", default_value_t = false)]
    pub disable_https: bool,

    /// Port for the https listener. Separate from the http one so everything
    /// that already works over http -- VLC, a browser, the desktop app's own
    /// health checks -- keeps working untouched.
    #[arg(long, env = "HTTPS_PORT", default_value_t = 8443)]
    pub https_port: u16,

    /// DNS suffix whose wildcard certificate this gateway serves.
    ///
    /// Must be a service that resolves a dash-encoded address back to itself
    /// (`192-168-1-67.<suffix>` -> `192.168.1.67`) *and* publishes the
    /// matching certificate. See the `tls` module for why this works.
    #[arg(long, env = "TLS_HOST_SUFFIX", default_value = "local-ip.sh")]
    pub tls_host_suffix: String,

    /// Where the wildcard certificate and its key are published.
    #[arg(
        long,
        env = "TLS_CERT_URL",
        default_value = "https://local-ip.sh/server.pem"
    )]
    pub tls_cert_url: String,

    #[arg(
        long,
        env = "TLS_KEY_URL",
        default_value = "https://local-ip.sh/server.key"
    )]
    pub tls_key_url: String,

    /// Serve a certificate from disk instead of fetching a published one.
    ///
    /// This is the option for anyone using a domain they actually control:
    /// the published key is world-readable by design, so it cannot
    /// authenticate this machine to anyone. Both must be given together.
    #[arg(long, env = "TLS_CERT_FILE")]
    pub tls_cert_file: Option<PathBuf>,

    #[arg(long, env = "TLS_KEY_FILE")]
    pub tls_key_file: Option<PathBuf>,

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

#[cfg(test)]
mod tests {
    use super::*;

    fn defaults() -> AppConfig {
        AppConfig::parse_from(["streaming-gateway"])
    }

    /// The port is not a free choice: the addon URL installed in Stremio, the
    /// tunnel pointed at this machine, and any firewall rule all hard-code it,
    /// and none of them notice if it moves. A silent change means the phone
    /// keeps asking a port nothing is listening on, which looks like the whole
    /// gateway being down rather than a config drift.
    #[test]
    fn default_ports_are_stable() {
        let config = defaults();
        assert_eq!(config.port, 8080);
        assert_eq!(config.fallback_port, 11470);
        assert_ne!(
            config.port, config.fallback_port,
            "the fallback must be a different port or it can never rescue a clash"
        );
    }

    #[test]
    fn defaults_are_usable_without_any_configuration() {
        let config = defaults();
        assert!(!config.disable_indexer, "search is on out of the box");
        assert!(config.prebuffer_bytes > 0);
        // Stall recovery and idle pausing both stop working silently at 0, and
        // 0 is a legitimate value to set by hand -- so pin the shipped ones.
        assert!(config.stall_timeout_secs > 0);
        assert!(config.idle_pause_secs > config.idle_check_interval_secs);
    }
}
