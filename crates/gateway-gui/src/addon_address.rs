//! Remembers the addon address Stremio was last given, so that a change is
//! announced in the window rather than discovered as an addon that silently
//! stopped answering.
//!
//! The https addon URL names the PC's LAN address
//! (`192-168-1-67.local-ip.sh:8443`). When the router hands the PC a
//! different address, or the port changes, the gateway serves the new URL
//! at once, but the addon installed on every phone still points at the old
//! one. Stremio then shows no streams and gives no reason. Seen 2026-09-15
//! when the PC moved from WiFi to a wired link: the installed addon was dead,
//! and the only fix was to reinstall it from the new URL.

use std::path::PathBuf;

use anyhow::Result;

use crate::service;

/// What the address shown now means for an addon that was already installed.
#[derive(Debug, PartialEq, Eq)]
pub enum AddressCheck {
    /// Nothing remembered yet, so this is the address to remember.
    First,
    Unchanged,
    /// Installed addons still point at `previous`, which no longer answers.
    Changed {
        previous: String,
    },
}

pub fn check(remembered: Option<&str>, current: &str) -> AddressCheck {
    match remembered {
        None => AddressCheck::First,
        Some(previous) if previous == current => AddressCheck::Unchanged,
        Some(previous) => AddressCheck::Changed {
            previous: previous.to_string(),
        },
    }
}

/// Next to the service's settings, which is the one config directory this
/// app already owns.
fn path() -> Result<PathBuf> {
    Ok(service::env_path()?.with_file_name("addon-url"))
}

/// The remembered address, or `None` when there is none or it cannot be read.
/// Either way the next address shown is simply remembered.
pub fn load() -> Option<String> {
    let text = std::fs::read_to_string(path().ok()?).ok()?;
    let url = text.trim();
    (!url.is_empty()).then(|| url.to_string())
}

pub fn remember(url: &str) -> Result<()> {
    service::write_file(&path()?, &format!("{url}\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const OLD: &str = "https://192-168-1-67.local-ip.sh:8443/manifest.json";
    const NEW: &str = "https://192-168-1-128.local-ip.sh:8443/manifest.json";

    #[test]
    fn the_first_address_ever_shown_is_remembered_without_a_warning() {
        assert_eq!(check(None, NEW), AddressCheck::First);
    }

    #[test]
    fn the_same_address_as_last_time_needs_nothing() {
        assert_eq!(check(Some(NEW), NEW), AddressCheck::Unchanged);
    }

    /// The case this exists for: the phone's addon still names the old
    /// address, so the window has to show both.
    #[test]
    fn a_different_address_reports_the_one_installed_addons_still_use() {
        assert_eq!(
            check(Some(OLD), NEW),
            AddressCheck::Changed {
                previous: OLD.to_string()
            }
        );
    }

    /// A port change breaks an installed addon just as surely as an address
    /// change does.
    #[test]
    fn a_port_change_is_a_change() {
        let moved_port = "https://192-168-1-128.local-ip.sh:9443/manifest.json";
        assert!(matches!(
            check(Some(NEW), moved_port),
            AddressCheck::Changed { .. }
        ));
    }
}
