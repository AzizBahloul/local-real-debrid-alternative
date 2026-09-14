//! The command line: which face the process shows, and the headless
//! always-on switch.

use std::path::Path;

use crate::app::{DEFAULT_CACHE_DIR, DEFAULT_PORT};
use crate::server_process::find_server_binary;
use crate::service;

/// How the process was asked to present itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// The full window.
    Window,
    /// Just the tray icon, no window and no GL context.
    Tray,
    /// The always-on switch, without opening anything. Exists so a headless
    /// box (or `setup.sh`) can turn the service on without a display, and so
    /// the one that writes the unit file is the same code the window uses.
    EnableAlwaysOn,
    DisableAlwaysOn,
    Status,
    Help,
}

pub fn parse_mode<I: IntoIterator<Item = String>>(args: I) -> Mode {
    for arg in args {
        match arg.as_str() {
            "--tray" => return Mode::Tray,
            "--enable-always-on" => return Mode::EnableAlwaysOn,
            "--disable-always-on" => return Mode::DisableAlwaysOn,
            "--status" => return Mode::Status,
            "-h" | "--help" => return Mode::Help,
            _ => {}
        }
    }
    Mode::Window
}

pub const USAGE: &str = "\
NovaStream desktop app

usage: streaming-gateway-gui [--tray | --enable-always-on | --disable-always-on | --status]

  (no arguments)       open the window
  --tray               run as a tray icon only; this is what the window hands
                       over to when you close it, and what the login autostart
                       entry launches
  --enable-always-on   install and start the gateway as a systemd user service
                       so it survives this app closing and a reboot, then exit
  --disable-always-on  stop and remove that service
  --status             print what the service is currently doing
  -h, --help           show this

With always-on enabled the gateway runs as the systemd user service
`novastream.service`; closing the window then leaves it running and only puts
this app in the tray.
";

/// The headless half of the always-on switch.
pub fn run_service_command(mode: Mode, exe: &Path) -> i32 {
    if !service::available() {
        eprintln!("no systemd user session here; always-on mode needs one");
        return 1;
    }
    match mode {
        Mode::EnableAlwaysOn => {
            let Some(binary) = find_server_binary() else {
                eprintln!("streaming-gateway binary not found next to this app or on PATH");
                return 1;
            };
            let (port, cache) = service::read_settings()
                .unwrap_or_else(|| (DEFAULT_PORT.to_string(), DEFAULT_CACHE_DIR.to_string()));
            if let Err(e) = service::install(&binary, &port, &cache) {
                eprintln!("{e:#}");
                return 1;
            }
            if let Err(e) = service::install_autostart(exe) {
                // The gateway is up either way; only the tray-at-login part
                // failed, so this is a warning and not the exit code.
                eprintln!("warning: {e:#}");
            }
            println!("always-on: {}", service::status().summary());
            0
        }
        Mode::DisableAlwaysOn => {
            if let Err(e) = service::uninstall() {
                eprintln!("{e:#}");
                return 1;
            }
            let _ = service::remove_autostart();
            println!("always-on: off");
            0
        }
        _ => {
            println!("always-on: {}", service::status().summary());
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The window is the default: an argument nobody passed must never put
    /// the app in a mode with no window, since the desktop launcher and the
    /// menu entry both run it bare.
    #[test]
    fn the_window_is_what_you_get_without_arguments() {
        assert_eq!(parse_mode(Vec::<String>::new()), Mode::Window);
        assert_eq!(parse_mode(vec!["--tray".to_string()]), Mode::Tray);
        assert_eq!(
            parse_mode(vec!["--enable-always-on".to_string()]),
            Mode::EnableAlwaysOn
        );
        assert_eq!(parse_mode(vec!["-h".to_string()]), Mode::Help);
        // A flag meant for something else must not silently become tray mode.
        assert_eq!(parse_mode(vec!["--trayify".to_string()]), Mode::Window);
    }
}
