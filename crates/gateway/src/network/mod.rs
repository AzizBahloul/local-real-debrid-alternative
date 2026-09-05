//! LAN discovery: figures out the IP address other devices on the same
//! Wi-Fi/LAN should use to reach this gateway, prints the startup banner, and
//! sets the listening sockets up so a client that vanishes actually
//! disconnects (see `harden_listener`).

use std::net::IpAddr;
use std::time::Duration;

use tracing::{debug, warn};

/// Applies the socket options that make a departed viewer actually hang up.
///
/// A player can stop reading without closing its connection -- the app is
/// backgrounded or force-quit, the phone leaves the wifi, someone pauses and
/// never comes back. The socket then fills, the client advertises a zero
/// receive window, and TCP switches to window probing. By default it probes
/// **forever**: the connection stays `ESTAB` indefinitely with the response
/// body parked mid-write, and nothing above ever learns the viewer is gone.
///
/// That is not merely an idle socket. The body owns a `StreamGuard`, and while
/// that guard lives the torrent is exempt from the idle reaper *and* from the
/// cache janitor *and* from the switched-title cleanup -- all three ask
/// `has_open_stream` first, by design, so they never cut a video out from
/// under someone. So each abandoned stream permanently pins one torrent in the
/// running set, where it goes on downloading and competing for bandwidth with
/// whatever is actually being watched. Watch a few episodes and the machine is
/// quietly seeding and fetching several titles at once for no one.
///
/// `TCP_USER_TIMEOUT` is the option that closes this specific hole: it bounds
/// how long data may stay unacknowledged *including while zero-window
/// probing*. Keepalive does not cover it -- keepalive is suppressed whenever
/// probes are already outstanding, which is exactly this case -- so both are
/// set: `TCP_USER_TIMEOUT` for a stuck transfer, keepalive for a connection
/// that is merely idle. When either fires the connection drops, the body task
/// ends, and the guard is released.
///
/// The timeout must stay generous. A player that has buffered several minutes
/// ahead legitimately reads nothing for that whole time, and hanging up on it
/// would turn a healthy stream into a stall. Recovery is cheap either way:
/// `/videos/{hash}/{idx}` is addressable and range-capable, so a player that
/// does get disconnected simply reconnects where it left off.
///
/// Set on the *listening* socket, which Linux copies onto every socket it
/// accepts -- so this covers every connection without the gateway needing an
/// accept loop of its own. Failures are logged and ignored: worse buffering
/// beats refusing to serve video.
#[cfg(target_os = "linux")]
pub fn harden_listener(listener: &impl std::os::fd::AsFd, client_timeout: Duration) {
    use socket2::{SockRef, TcpKeepalive};

    let sock = SockRef::from(listener);

    if !client_timeout.is_zero() {
        if let Err(e) = sock.set_tcp_user_timeout(Some(client_timeout)) {
            warn!(
                "could not set TCP_USER_TIMEOUT ({e}); a player that disappears mid-stream \
                 will keep its torrent running until the gateway restarts"
            );
        } else {
            debug!(
                timeout_secs = client_timeout.as_secs(),
                "clients that stop acknowledging data will be disconnected"
            );
        }
    }

    // Probe well inside the user timeout, so an idle-but-alive connection is
    // confirmed alive rather than counted against it.
    let keepalive = TcpKeepalive::new()
        .with_time(Duration::from_secs(60))
        .with_interval(Duration::from_secs(15));
    if let Err(e) = sock.set_tcp_keepalive(&keepalive) {
        debug!("could not enable TCP keepalive: {e}");
    }
}

/// `TCP_USER_TIMEOUT` is Linux-only and has no portable equivalent, so
/// everywhere else the OS defaults stand.
#[cfg(not(target_os = "linux"))]
pub fn harden_listener<S>(_listener: &S, _client_timeout: Duration) {}

/// Best-effort detection of the LAN-facing IP address (the one printed for
/// phones/TVs to type in). Falls back to `127.0.0.1` if it can't be determined
/// (e.g. no active network interface), in which case only this machine can
/// connect -- the caller should still start up rather than fail.
pub fn detect_lan_ip() -> IpAddr {
    local_ip_address::local_ip().unwrap_or_else(|_| IpAddr::from([127, 0, 0, 1]))
}

/// Prints the startup banner.
///
/// `addon_url` is the https address, when one could be served. It is printed
/// separately and first because it is the only one that can be pasted into
/// Stremio on Android -- the plain http address below it is what players and
/// browsers use, and handing someone the wrong one of the two is the single
/// most common way this ends up looking broken.
pub fn print_banner(lan_ip: IpAddr, port: u16, addon_url: Option<&str>) {
    let url = format!("http://{lan_ip}:{port}");
    let width = addon_url
        .map(|a| a.len() + 16)
        .unwrap_or(0)
        .max(url.len() + 4)
        .max(32);
    let bar = "=".repeat(width);

    println!("\n{bar}");
    println!("   Streaming Gateway Running");
    println!("{bar}");
    println!();
    match addon_url {
        Some(addon) => {
            println!("   PASTE THIS INTO STREMIO (Addons -> search bar):");
            println!("   {addon}/manifest.json");
            println!();
            println!("   No tunnel needed. Works from any device on this wifi.");
        }
        None => {
            println!("   https is not available, so Stremio on Android cannot");
            println!("   add this addon directly -- you still need a tunnel.");
        }
    }
    println!();
    println!("   Direct address (VLC, browsers, TVs):");
    println!("   {url}");
    println!("   e.g. {url}/play?magnet=<magnet-link>");
    println!();
    println!("{bar}\n");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_lan_ip_never_panics() {
        // Whatever the sandboxed test environment's network looks like, this
        // must always resolve to *some* address rather than panicking.
        let _ = detect_lan_ip();
    }

    /// The entire "a departed viewer stops pinning their torrent" fix rests on
    /// one unobvious kernel behaviour: that Linux copies `TCP_USER_TIMEOUT`
    /// from the listening socket onto every socket accepted from it. Setting
    /// it on the listener is only worth doing if that holds -- if it ever
    /// stopped, `harden_listener` would still succeed, the gateway would still
    /// look configured, and connections would silently go back to probing
    /// forever. So assert it against a real accepted connection.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_client_timeout_reaches_accepted_connections() {
        use socket2::SockRef;
        use std::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind a loopback listener");
        let addr = listener.local_addr().expect("listener has an address");
        harden_listener(&listener, Duration::from_secs(30));

        let _client = TcpStream::connect(addr).expect("connect to the listener");
        let (accepted, _) = listener.accept().expect("accept the connection");

        let sock = SockRef::from(&accepted);
        assert_eq!(
            sock.tcp_user_timeout().expect("read TCP_USER_TIMEOUT back"),
            Some(Duration::from_secs(30)),
            "an accepted connection must inherit the timeout, or a wedged \
             player keeps its torrent running for the life of the process"
        );
        assert!(
            sock.keepalive().expect("read SO_KEEPALIVE back"),
            "keepalive covers the idle-but-not-wedged case"
        );
    }

    /// 0 means "leave the OS default alone", which has to stay a real option:
    /// it is the escape hatch if the timeout ever turns out to be cutting off
    /// a legitimately slow player.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_zero_timeout_leaves_the_os_default_in_place() {
        use socket2::SockRef;
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind a loopback listener");
        harden_listener(&listener, Duration::ZERO);

        assert_eq!(
            SockRef::from(&listener)
                .tcp_user_timeout()
                .expect("read TCP_USER_TIMEOUT back"),
            None,
            "a zero timeout must not be written to the socket"
        );
    }
}
