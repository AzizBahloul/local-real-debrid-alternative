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

pub fn print_banner(lan_ip: IpAddr, port: u16) {
    let url = format!("http://{lan_ip}:{port}");
    let width = url.len().max(28) + 4;
    let bar = "=".repeat(width);

    println!("\n{bar}");
    println!("   Streaming Gateway Running");
    println!("{bar}");
    println!();
    println!("   Local Address:");
    println!("   {url}");
    println!();
    println!("   Add this URL + /manifest.json to Stremio,");
    println!("   or open {url}/play?magnet=<magnet-link> directly.");
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
