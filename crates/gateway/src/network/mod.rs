//! LAN discovery: figures out the IP address other devices on the same
//! Wi-Fi/LAN should use to reach this gateway, prints the startup banner, and
//! sets the listening sockets up so a client that vanishes actually
//! disconnects (see `harden_listener`).

use std::net::{IpAddr, Ipv4Addr};
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
/// `TCP_NODELAY` rides along for a different reason. Every response here
/// starts with a small write -- the headers, or all of a manifest or stream
/// list -- and with Nagle's algorithm on, a small write that follows another
/// can sit waiting for the client's delayed ACK, up to 40 ms, before it goes
/// out. That is pure added latency on every request, and video gains nothing
/// from it: its chunks are far larger than a segment anyway.
///
/// Set on the *listening* socket, which Linux copies onto every socket it
/// accepts -- so this covers every connection without the gateway needing an
/// accept loop of its own. Failures are logged and ignored: worse buffering
/// beats refusing to serve video.
#[cfg(target_os = "linux")]
pub fn harden_listener(listener: &impl std::os::fd::AsFd, client_timeout: Duration) {
    use socket2::{SockRef, TcpKeepalive};

    let sock = SockRef::from(listener);

    if let Err(e) = sock.set_tcp_nodelay(true) {
        debug!("could not set TCP_NODELAY: {e}");
    }

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
///
/// This machine also has virtual interfaces (`virbr0` from libvirt, Docker
/// bridges) that sit `DOWN` but keep an assigned address, e.g. `192.168.122.1`.
/// `local_ip_address::local_ip()` enumerates interfaces and returned that
/// dead bridge address instead of the real Wi-Fi IP after one wake-from-sleep,
/// which broke the addon URL Stremio had cached. Asking the kernel's routing
/// table which interface it would actually use to reach the internet -- the
/// same lookup `ip route get` performs -- is what UDP `connect()` does here;
/// no packet is sent, it only resolves a route. That is immune to unrelated
/// bridges sitting on the machine, because the kernel would only route
/// through one of them if it were the real default route.
///
/// A LAN with no internet has no default route, so that lookup fails there.
/// The fallback asks the routing table directly for the subnets this machine
/// is on, and resolves the address it would use to reach one of them -- the
/// same question, asked of a route that does exist. This used to be the
/// `local_ip_address` crate, which enumerates interfaces and is exactly the
/// lookup that picked the dead bridge above.
pub fn detect_lan_ip() -> IpAddr {
    local_addr_towards(IpAddr::from([1, 1, 1, 1]))
        .or_else(lan_ip_from_route_table)
        .unwrap_or(IpAddr::from([127, 0, 0, 1]))
}

/// The local address the kernel would send from to reach `target`. No packet
/// is sent: connecting a UDP socket only resolves a route.
fn local_addr_towards(target: IpAddr) -> Option<IpAddr> {
    let sock = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    sock.connect((target, 80)).ok()?;
    let ip = sock.local_addr().ok()?.ip();
    (!ip.is_unspecified() && !ip.is_loopback()).then_some(ip)
}

#[cfg(target_os = "linux")]
fn lan_ip_from_route_table() -> Option<IpAddr> {
    let table = std::fs::read_to_string("/proc/net/route").ok()?;
    let is_up = |iface: &str| {
        std::fs::read_to_string(format!("/sys/class/net/{iface}/operstate"))
            .is_ok_and(|state| state.trim() == "up")
    };
    route_targets(&table, is_up)
        .into_iter()
        .find_map(|target| local_addr_towards(IpAddr::V4(target)))
}

/// Without a routing table to read there is nothing better to try.
#[cfg(not(target_os = "linux"))]
fn lan_ip_from_route_table() -> Option<IpAddr> {
    None
}

/// Interface-name prefixes of virtual networks that are never the LAN a phone
/// is on: container and VM bridges, their veth pairs, and VPN tunnels.
const VIRTUAL_INTERFACE_PREFIXES: [&str; 7] = ["lo", "docker", "br-", "virbr", "veth", "tun", "wg"];

/// Addresses worth routing towards, best first, from the text of
/// `/proc/net/route`: the gateway of any default route, then the first host of
/// every subnet route, by metric. Only routes that are up, on interfaces that
/// are up and not virtual.
///
/// Pure, so the parsing is testable without a real routing table.
fn route_targets(table: &str, is_up: impl Fn(&str) -> bool) -> Vec<Ipv4Addr> {
    const RTF_UP: u32 = 0x1;
    // (is not a default route, metric, target): sorts defaults first.
    let mut targets: Vec<(bool, u32, Ipv4Addr)> = Vec::new();

    for line in table.lines().skip(1) {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let [iface, destination, gateway, flags, _refcnt, _use, metric, mask, ..] = fields[..]
        else {
            continue;
        };
        let (Some(destination), Some(gateway), Some(mask)) = (
            route_address(destination),
            route_address(gateway),
            route_address(mask),
        ) else {
            continue;
        };
        let flags = u32::from_str_radix(flags, 16).unwrap_or(0);
        let metric = metric.parse().unwrap_or(u32::MAX);
        if flags & RTF_UP == 0
            || VIRTUAL_INTERFACE_PREFIXES
                .iter()
                .any(|p| iface.starts_with(p))
            || !is_up(iface)
        {
            continue;
        }

        if destination.is_unspecified() {
            if !gateway.is_unspecified() {
                targets.push((false, metric, gateway));
            }
            continue;
        }
        if destination.is_link_local() || mask.is_unspecified() {
            continue;
        }
        // The first host of the subnet: any address inside it resolves to the
        // same interface and source address.
        let network = u32::from(destination) & u32::from(mask);
        targets.push((true, metric, Ipv4Addr::from(network.wrapping_add(1))));
    }

    targets.sort_by_key(|(not_default, metric, _)| (*not_default, *metric));
    targets.into_iter().map(|(_, _, target)| target).collect()
}

/// One address column of `/proc/net/route`: the kernel prints the address's
/// network-order bytes read as a host-order integer, so the host-order bytes
/// of the parsed number are the address.
fn route_address(hex: &str) -> Option<Ipv4Addr> {
    let raw = u32::from_str_radix(hex, 16).ok()?;
    Some(Ipv4Addr::from(raw.to_ne_bytes()))
}

/// What the banner can say about the https addon URL at the moment it prints.
///
/// The banner used to wait for the certificate fetch (up to 15s against a
/// slow provider) purely so it could print the https address -- during which
/// the http listener, the only thing the GUI health-checks, was not serving
/// at all. Printing immediately with `Preparing` and letting the https task
/// announce itself when ready is what makes startup feel instant.
pub enum AddonStatus<'a> {
    /// The https listener is up at this URL.
    Ready(&'a str),
    /// The certificate is being fetched in the background; the URL is
    /// printed by `print_addon_ready` the moment it is servable.
    Preparing,
    /// https was disabled by configuration.
    Disabled,
}

/// Prints the startup banner.
///
/// The https addon URL is printed separately and first because it is the only
/// one that can be pasted into Stremio on Android -- the plain http address
/// below it is what players and browsers use, and handing someone the wrong
/// one of the two is the single most common way this ends up looking broken.
pub fn print_banner(lan_ip: IpAddr, port: u16, addon: AddonStatus<'_>) {
    let url = format!("http://{lan_ip}:{port}");
    let width = match &addon {
        AddonStatus::Ready(a) => a.len() + 16,
        _ => 0,
    }
    .max(url.len() + 4)
    .max(32);
    let bar = "=".repeat(width);

    println!("\n{bar}");
    println!("   Streaming Gateway Running");
    println!("{bar}");
    println!();
    match addon {
        AddonStatus::Ready(addon) => print_paste_instructions(addon),
        AddonStatus::Preparing => {
            println!("   The Stremio addon URL (https) is being prepared and");
            println!("   will be printed below in a few seconds.");
        }
        AddonStatus::Disabled => {
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

/// Printed by the background https task once the certificate is loaded and
/// the listener is actually serving -- the deferred half of the banner.
pub fn print_addon_ready(addon_url: &str) {
    let bar = "=".repeat(manifest_line(addon_url).len().max(32));
    println!("\n{bar}");
    print_paste_instructions(addon_url);
    println!("{bar}\n");
}

/// The addon URL line. The desktop app finds the URL in the log by exactly
/// this shape (see `gateway-gui`'s `addon_url_from_line`), so both banners
/// print it from here.
fn manifest_line(addon_url: &str) -> String {
    format!("   {addon_url}/manifest.json")
}

fn print_paste_instructions(addon_url: &str) {
    println!("   PASTE THIS INTO STREMIO (Addons -> search bar):");
    println!("{}", manifest_line(addon_url));
    println!();
    println!("   No tunnel needed. Works from any device on this wifi.");
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

    #[cfg(target_os = "linux")]
    #[test]
    fn accepted_connections_do_not_wait_on_nagle() {
        use socket2::SockRef;
        use std::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind a loopback listener");
        let addr = listener.local_addr().expect("listener has an address");
        harden_listener(&listener, Duration::ZERO);

        let _client = TcpStream::connect(addr).expect("connect to the listener");
        let (accepted, _) = listener.accept().expect("accept the connection");
        assert!(
            SockRef::from(&accepted)
                .tcp_nodelay()
                .expect("read TCP_NODELAY back"),
            "TCP_NODELAY must reach the accepted socket, or it was set for nothing"
        );
    }

    /// A real table from a laptop on wifi with docker installed.
    const ROUTE_TABLE: &str = "\
Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT
wlp2s0\t00000000\t0101A8C0\t0003\t0\t0\t600\t00000000\t0\t0\t0
docker0\t000011AC\t00000000\t0001\t0\t0\t0\t0000FFFF\t0\t0\t0
wlp2s0\t0001A8C0\t00000000\t0001\t0\t0\t600\t00FFFFFF\t0\t0\t0
virbr0\t007AA8C0\t00000000\t0001\t0\t0\t0\t00FFFFFF\t0\t0\t0
eth0\t0000FEA9\t00000000\t0001\t0\t0\t1000\t0000FFFF\t0\t0\t0
";

    #[test]
    fn the_route_table_prefers_the_default_gateway_then_real_subnets() {
        let targets = route_targets(ROUTE_TABLE, |_| true);
        assert_eq!(
            targets,
            vec![Ipv4Addr::new(192, 168, 1, 1), Ipv4Addr::new(192, 168, 1, 1)],
            "container and VM bridges, and link-local routes, are never the LAN"
        );
    }

    #[test]
    fn routes_on_a_down_interface_are_ignored() {
        let targets = route_targets(ROUTE_TABLE, |iface| iface != "wlp2s0");
        assert!(targets.is_empty(), "got {targets:?}");
    }

    /// A LAN with no internet: no default route, only the subnet.
    #[test]
    fn an_offline_lan_still_yields_its_subnet() {
        let table = "Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\n\
                     eth0\t0000000A\t00000000\t0001\t0\t0\t100\t000000FF\n";
        assert_eq!(
            route_targets(table, |_| true),
            vec![Ipv4Addr::new(10, 0, 0, 1)]
        );
    }

    #[test]
    fn a_garbled_route_table_yields_nothing_rather_than_panicking() {
        assert!(route_targets("", |_| true).is_empty());
        assert!(route_targets("header\nwlp2s0 zz", |_| true).is_empty());
        assert!(route_targets("header\nwlp2s0\tXYZ\t0\t1\t0\t0\t0\t0", |_| true).is_empty());
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
