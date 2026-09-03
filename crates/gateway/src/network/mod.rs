//! LAN discovery: figures out the IP address other devices on the same
//! Wi-Fi/LAN should use to reach this gateway, and prints the startup banner.

use std::net::IpAddr;

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
}
