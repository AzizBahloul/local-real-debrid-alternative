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

    /// Directory for the audit log (`events.jsonl`).
    ///
    /// Defaults to an absolute path under `XDG_STATE_HOME`, unlike `cache_dir`
    /// which is relative to the working directory. That difference is on
    /// purpose: the installed binaries are started by a desktop launcher from
    /// whatever directory it happened to be in, and a log nobody can find is
    /// the problem this whole subsystem exists to fix.
    #[arg(long, env = "LOG_DIRECTORY")]
    pub log_dir: Option<PathBuf>,

    /// Maximum size of the cache directory, in gigabytes, before old data is evicted.
    ///
    /// Generous on purpose. Every gigabyte evicted is a region of a film that
    /// becomes slow to seek back into, and re-downloading it costs the whole
    /// piece-fetch latency again; disk is by far the cheapest speed available
    /// here. 100 GB is roughly 20-40 films, which is about the horizon over
    /// which someone actually re-watches or resumes something.
    #[arg(long, env = "MAX_CACHE_SIZE_GB", default_value_t = 100)]
    pub max_cache_size_gb: u64,

    /// Whether to automatically evict old cached torrents when over the size cap.
    #[arg(long, env = "AUTO_CLEANUP", default_value_t = true)]
    pub auto_cleanup: bool,

    /// How many torrents to keep on disk at most, newest first. Anything
    /// older is purged automatically even while the cache is under its size
    /// cap -- the size cap then only matters when the survivors are huge.
    /// 0 disables count-based retention and leaves the size cap in charge.
    ///
    /// The default keeps the title being watched plus the one before it
    /// (back-to-back episodes, or "go back and finish the other one"), which
    /// is the whole re-watch horizon a small disk actually needs.
    #[arg(long, env = "MAX_CACHED_TORRENTS", default_value_t = 2)]
    pub max_cached_torrents: usize,

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
    ///
    /// Defaults to a cap rather than "unlimited" because the expected
    /// deployment is a laptop on wifi, where upload and video share one
    /// half-duplex radio and unlimited seeding is felt directly as buffering.
    /// 2 MB/s is high enough that peers keep reciprocating. Set 0 for
    /// unlimited, which is the right value on ethernet.
    #[arg(long, env = "MAX_UPLOAD_MB_S", default_value_t = 2)]
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

    /// Port to accept incoming peer connections on. 0 picks any free port.
    ///
    /// Without a listening socket the gateway can only ever *dial* peers, so
    /// every seeder that would have connected to us after our tracker
    /// announce is simply lost -- which shows up as a handful of peers and a
    /// download that crawls on the first play of a title. If the port cannot
    /// be bound the gateway falls back to an ephemeral one rather than
    /// refusing to start.
    #[arg(long, env = "PEER_PORT", default_value_t = 6881)]
    pub peer_port: u16,

    /// Do not ask the router to forward the peer port via UPnP.
    ///
    /// The forward is what lets peers outside this LAN reach the listening
    /// socket above; without it the listener only helps peers on the local
    /// network, which for a public swarm is almost none of them.
    #[arg(long, env = "DISABLE_UPNP", default_value_t = false)]
    pub disable_upnp: bool,

    /// How many not-yet-known torrents to start fetching while the viewer is
    /// still reading the stream list. 0 (the default) disables prefetching.
    ///
    /// **Off by default, from experience rather than caution.** The idea is
    /// sound -- fetching a torrent's metadata over DHT/trackers is most of a
    /// cold start, and doing it while the list is on screen moves that wait
    /// to where nobody is staring at a spinner. It measured beautifully in
    /// isolation: 3/3 cold starts under 2ms against 2.7-16.4s.
    ///
    /// In real use it made things worse three separate ways, because a
    /// speculative download is competing for the same scarce things a real
    /// viewer needs. It pulled file bytes and diluted piece priority away
    /// from the stream actually being watched; parking it instead to avoid
    /// that dropped its peers, so tapping it paid a full reconnect; and
    /// warming more than one candidate split DHT capacity so the row that
    /// *was* tapped resolved slower than if nothing had been warmed at all.
    /// Each fix moved the cost somewhere else rather than removing it.
    ///
    /// Set it to 1 to try it. The wins that came out of that work and are
    /// unambiguous -- the incoming peer listener, the tracker lists, the
    /// per-hash start lock -- are all still on and are not affected by this.
    #[arg(long, env = "BROWSE_PREFETCH_COUNT", default_value_t = 0)]
    pub browse_prefetch_count: usize,

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
    ///
    /// This is a *ceiling*, not a wait: only the first
    /// `streaming::PREBUFFER_MIN_BYTES` are ever blocked for, and the rest is
    /// topped up from data already on disk. It is still kept small, because on
    /// a warm seek the top-up is real time spent reading before any byte goes
    /// out, and a player needs enough to parse a container header, not a
    /// multi-megabyte head start it is about to buffer again itself.
    #[arg(long, env = "PREBUFFER_BYTES", default_value_t = 1024 * 1024)]
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
    ///
    /// Half an hour, because the cost is asymmetric. Pausing early saves idle
    /// upstream bandwidth on a machine that usually has nothing else running;
    /// pausing a film someone paused to make dinner guarantees them a full
    /// DHT/tracker rediscovery when they come back, which is the single
    /// longest wait this gateway ever imposes.
    #[arg(long, env = "IDLE_PAUSE_SECS", default_value_t = 1800)]
    pub idle_pause_secs: u64,

    /// How often (seconds) to look for idle torrents to pause.
    #[arg(long, env = "IDLE_CHECK_INTERVAL_SECS", default_value_t = 30)]
    pub idle_check_interval_secs: u64,

    /// Cancel a still-empty read as soon as the same player asks for a
    /// different offset in the same file.
    ///
    /// librqbit interleaves piece requests round-robin across every open
    /// stream, so ten abandoned scrubs do not just waste the bandwidth they
    /// already spent -- they permanently take nine tenths of the piece
    /// requests away from the position the viewer actually landed on. A
    /// reader that has not yet produced a single byte is provably showing
    /// nobody anything, so dropping it costs nothing and gives its share back.
    ///
    /// Scoped to the same client address, which is what keeps a second device
    /// watching the same title from cancelling the first one's cold start (and
    /// the first one's retry then cancelling the second's, forever).
    /// A reader that has served bytes is never touched: that is someone
    /// watching. Set false to disable.
    #[arg(long, env = "SEEK_SUPERSEDE", default_value_t = true)]
    pub seek_supersede: bool,

    /// Fetch an mp4's trailing index alongside its opening frames.
    ///
    /// A non-faststart mp4 keeps its `moov` atom at the very end of the file,
    /// and a player cannot start until it has read it -- so it range-requests
    /// the tail *before* byte 0, paying a second full piece-fetch at an offset
    /// no peer has been primed for. Opening a short-lived read at the tail
    /// when the stream first opens puts that piece into the priority set at
    /// the same time as the first one, so the two arrive together instead of
    /// one after the other. Costs one piece of bandwidth on a file that turns
    /// out to be faststart already. Ignored for containers that carry their
    /// index at the front (mkv).
    #[arg(long, env = "MP4_TAIL_WARM", default_value_t = true)]
    pub mp4_tail_warm: bool,

    /// Extra megabytes of read-ahead to claim once playback has settled.
    ///
    /// librqbit gives each open stream a fixed 32 MB priority window and
    /// offers no way to widen it, so the only lever is a second read parked
    /// further ahead. **Off by default, and that is a measured trade rather
    /// than caution**: the piece scheduler interleaves streams round-robin, so
    /// a second window does not add capacity, it splits the existing capacity
    /// between the bytes needed in ten seconds and the bytes needed in two
    /// minutes. On a swarm that is comfortably outrunning playback that is
    /// free insurance against a stall; on one that is barely keeping up it is
    /// actively harmful. Measure before turning it on.
    #[arg(long, env = "READAHEAD_EXTRA_MB", default_value_t = 0)]
    pub readahead_extra_mb: u64,

    /// How many seconds of uninterrupted playback count as "settled" before
    /// the extra read-ahead above is claimed. No effect at 0 extra MB.
    #[arg(long, env = "READAHEAD_SETTLE_SECS", default_value_t = 10)]
    pub readahead_settle_secs: u64,

    /// Peer cap applied to a torrent as it is added, overriding
    /// `MAX_PEERS_PER_TORRENT` for that torrent. 0 means "no override".
    ///
    /// Cold start wants peers fast and has no stream to protect yet, which is
    /// the opposite of the steady-state reasoning behind the lower session
    /// default. The two cannot be fully reconciled here: librqbit reads
    /// `peer_limit` when the torrent is added and there is no way to lower it
    /// afterwards, so a raised limit lasts that torrent's whole life rather
    /// than the first thirty seconds. Off by default for exactly that reason
    /// -- try 100 and measure whether it moves anything on your link before
    /// leaving it on.
    #[arg(long, env = "COLD_START_PEER_LIMIT", default_value_t = 0)]
    pub cold_start_peer_limit: usize,

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

    /// The shipped configuration, as if the binary were started with no
    /// arguments and no environment.
    ///
    /// Exists so tests can start from the real defaults and override the two
    /// or three fields they care about. A struct literal cannot: it must name
    /// every field, so adding one to `AppConfig` breaks test files for reasons
    /// that have nothing to do with what they test.
    pub fn defaults() -> Self {
        Self::parse_from(["streaming-gateway"])
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

    /// Where the audit log goes: the explicit setting, else the XDG default.
    pub fn resolved_log_dir(&self) -> PathBuf {
        self.log_dir
            .clone()
            .unwrap_or_else(crate::audit::default_log_dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn defaults() -> AppConfig {
        AppConfig::defaults()
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

    /// The peer listener is a cold-start lever whose "off" value is a
    /// perfectly ordinary-looking flag, and turning it off costs only speed
    /// -- nothing errors, nothing logs, playback still works, there are just
    /// far fewer peers. So the shipped value is pinned here, because the way
    /// this regresses is silently.
    #[test]
    fn incoming_peers_stay_reachable_by_default() {
        assert!(
            !defaults().disable_upnp,
            "without the port forward the peer listener only reaches this LAN, \
             where a public swarm has no peers at all"
        );
    }

    /// Prefetching is off because it repeatedly made real playback worse by
    /// competing with it (see the field docs). This pins that decision so it
    /// is turned back on deliberately, with measurement, rather than because
    /// a default looked conservative.
    #[test]
    fn browse_prefetch_stays_off_by_default() {
        assert_eq!(defaults().browse_prefetch_count, 0);
    }

    /// The read-ahead widener splits piece priority rather than adding
    /// capacity (see the field docs), so it ships off. This pins that, because
    /// turning it on regresses nothing visibly -- playback still works, it
    /// just stalls more often on a marginal swarm, which is indistinguishable
    /// from a bad night on the wifi.
    #[test]
    fn speculative_levers_stay_off_by_default() {
        let config = defaults();
        assert_eq!(config.readahead_extra_mb, 0);
        assert_eq!(
            config.cold_start_peer_limit, 0,
            "librqbit cannot lower a peer limit again after the add, so raising \
             it is a whole-session decision and must be taken deliberately"
        );
    }

    /// Both of these only ever cost latency, never correctness, so a
    /// regression shows up as "it feels slower" and nothing else.
    #[test]
    fn latency_defaults_are_the_tuned_ones() {
        let config = defaults();
        assert_eq!(config.prebuffer_bytes, 1024 * 1024);
        assert_eq!(config.idle_pause_secs, 1800);
        assert!(config.seek_supersede, "scrub bursts otherwise keep every abandoned reader's piece-priority claim");
        assert!(config.mp4_tail_warm);
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
