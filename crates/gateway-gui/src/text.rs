//! Plain string work with no UI in it: number formatting, scraping the
//! server's banner, stripping terminal escapes, and finding the Desktop.

use std::borrow::Cow;
use std::path::{Path, PathBuf};

pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

pub fn human_duration(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{h}h {m}m")
    } else if m > 0 {
        format!("{m}m {s}s")
    } else {
        format!("{s}s")
    }
}

/// Pulls the `http://<ip>:<port>` the server printed in its own startup
/// banner out of a log line, so the GUI shows the exact address the server
/// actually bound (including the fallback-port case) instead of guessing.
pub fn extract_url(line: &str) -> Option<String> {
    extract_scheme_url(line, "http://")
}

/// Pulls the https addon base URL out of the server's "PASTE THIS INTO
/// STREMIO" line.
///
/// This has to be scraped rather than derived, because Stremio on Android
/// refuses a plain-http addon outright: building `{http address}/manifest.json`
/// hands the user the one URL their phone is guaranteed to reject, which looks
/// exactly like the gateway being broken. The https URL also has a different
/// host *and* port (`<ip-with-dashes>.local-ip.sh:8443`), so it cannot be
/// reconstructed from the http one anyway.
///
/// Anchored on the `/manifest.json` suffix on purpose -- plenty of other log
/// lines carry an unrelated `https://` (the indexer, the certificate provider,
/// the addon logo).
pub fn extract_addon_url(line: &str) -> Option<String> {
    let url = extract_scheme_url(line, "https://")?;
    let base = url.strip_suffix("/manifest.json")?;
    Some(base.to_string())
}

fn extract_scheme_url(line: &str, scheme: &str) -> Option<String> {
    let start = line.find(scheme)?;
    let rest = &line[start..];
    let end = rest.find(|c: char| c.is_whitespace()).unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

/// Drops ANSI colour escapes from a log line.
///
/// The server colours its own output whether or not anything is a terminal,
/// which is right for a console and wrong for both of the log panel's sources:
/// a pipe and journald keep the escapes verbatim, and egui has no notion of
/// them, so every line would be fenced with literal `[2m`/`[0m`. Stripping
/// also matters for meaning, not just looks -- `widgets::log_color` decides a
/// line's severity by reading it, and an escape sequence in front of the word
/// it looks for defeats that.
///
/// Borrowed when there is nothing to strip, which is every line that did not
/// come out of `tracing`'s colouring.
pub fn strip_ansi(line: &str) -> Cow<'_, str> {
    if !line.contains('\u{1b}') {
        return Cow::Borrowed(line);
    }
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        if c != '\u{1b}' {
            out.push(c);
            continue;
        }
        // CSI sequences (ESC [ ... final byte in @-~) are all the server
        // emits; anything else is dropped up to the next plausible end so a
        // stray escape cannot swallow the rest of the line.
        if chars.next() == Some('[') {
            for c in chars.by_ref() {
                if ('\u{40}'..='\u{7e}').contains(&c) {
                    break;
                }
            }
        }
    }
    Cow::Owned(out)
}

/// Where "save to Desktop" actually saves.
///
/// The desktop folder is not always `$HOME/Desktop`: on a localized install it
/// carries a translated name, and XDG records the real one in
/// `user-dirs.dirs`. Falling straight back to `$HOME` rather than creating a
/// directory is deliberate — a report the user cannot find is the failure this
/// feature exists to fix, and inventing an English `Desktop/` next to their
/// real localized one would do exactly that.
pub fn desktop_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("XDG_DESKTOP_DIR").filter(|s| !s.is_empty()) {
        return PathBuf::from(dir);
    }

    let home = std::env::var_os("HOME").map(PathBuf::from);
    if let Some(home) = &home {
        if let Some(dir) = desktop_from_user_dirs(home) {
            return dir;
        }
        let conventional = home.join("Desktop");
        if conventional.is_dir() {
            return conventional;
        }
        return home.clone();
    }
    PathBuf::from(".")
}

/// Reads `XDG_DESKTOP_DIR` out of `~/.config/user-dirs.dirs`.
fn desktop_from_user_dirs(home: &Path) -> Option<PathBuf> {
    let text = std::fs::read_to_string(home.join(".config/user-dirs.dirs")).ok()?;
    parse_user_dirs_desktop(&text, home)
}

/// The desktop entry of a `user-dirs.dirs` file, with the home directory
/// expanded.
///
/// `xdg-user-dirs-update` writes `XDG_DESKTOP_DIR="$HOME/Bureau"`, but the
/// file is shell syntax and hand-edited copies use `${HOME}` just as often;
/// leaving that form unexpanded produced a relative directory literally named
/// `${HOME}` and a report saved nowhere anyone would look.
pub fn parse_user_dirs_desktop(text: &str, home: &Path) -> Option<PathBuf> {
    let value = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.starts_with('#'))
        .find_map(|line| line.strip_prefix("XDG_DESKTOP_DIR="))
        .map(|value| value.trim().trim_matches('"'))
        .filter(|value| !value.is_empty())?;
    let home = home.to_string_lossy();
    Some(PathBuf::from(
        value.replace("${HOME}", &home).replace("$HOME", &home),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact lines `network::print_banner` / `print_addon_ready` emit.
    /// The GUI scrapes the server's stdout, so these strings are a real
    /// cross-crate contract -- if the banner is reworded, this test is what
    /// notices before the GUI silently shows nothing.
    const ADDON_LINE: &str = "   https://192-168-1-67.local-ip.sh:8443/manifest.json";
    const DIRECT_LINE: &str = "   http://192.168.1.67:8080";

    #[test]
    fn scrapes_both_urls_from_the_banner() {
        assert_eq!(
            extract_addon_url(ADDON_LINE).as_deref(),
            Some("https://192-168-1-67.local-ip.sh:8443")
        );
        assert_eq!(
            extract_url(DIRECT_LINE).as_deref(),
            Some("http://192.168.1.67:8080")
        );
    }

    /// The regression this whole change exists for: `find("http://")` must not
    /// match inside an `https://` URL, or the GUI captures the addon line as
    /// the direct address and shows a manifest URL Android refuses.
    #[test]
    fn http_scrape_never_matches_an_https_url() {
        assert_eq!(extract_url(ADDON_LINE), None);
    }

    /// Other log lines carry an unrelated `https://` (indexer, certificate
    /// provider, addon logo). Only the manifest line may be adopted.
    #[test]
    fn ignores_https_urls_that_are_not_the_manifest() {
        for line in [
            "torrent discovery enabled url=https://torrentio.strem.fun",
            "fetching https://local-ip.sh/server.pem",
            "logo https://raw.githubusercontent.com/Stremio/x/icon.png",
        ] {
            assert_eq!(extract_addon_url(line), None, "adopted: {line}");
        }
    }

    /// The fallback-port case: the GUI must show what the server actually
    /// bound, not the configured default.
    #[test]
    fn scrapes_the_fallback_port() {
        assert_eq!(
            extract_url("   http://192.168.1.67:11470").as_deref(),
            Some("http://192.168.1.67:11470")
        );
    }

    /// The gateway colours its output whether or not it is talking to a
    /// terminal, and both of the panel's sources (a pipe, and journald)
    /// preserve those escapes byte for byte.
    #[test]
    fn colour_escapes_never_reach_the_log_panel() {
        let raw = "\u{1b}[2m2026-09-07T00:49:30Z\u{1b}[0m \u{1b}[32m INFO\u{1b}[0m ready";
        assert_eq!(strip_ansi(raw), "2026-09-07T00:49:30Z  INFO ready");
        // A plain line must come through untouched, and without a copy: most
        // of a journal backlog has nothing to strip.
        let plain = strip_ansi(DIRECT_LINE);
        assert_eq!(plain, DIRECT_LINE);
        assert!(matches!(plain, Cow::Borrowed(_)));
    }

    /// The URLs are scraped out of coloured `tracing` lines as well as the
    /// plain banner, and a trailing reset used to end up glued to the URL --
    /// which breaks the addon link silently, since the check for it is a
    /// suffix match on `/manifest.json`.
    #[test]
    fn a_colour_reset_against_a_url_does_not_become_part_of_it() {
        let line = "  \u{1b}[36mhttps://192-168-1-67.local-ip.sh:8443/manifest.json\u{1b}[0m";
        assert_eq!(
            extract_addon_url(&strip_ansi(line)).as_deref(),
            Some("https://192-168-1-67.local-ip.sh:8443")
        );
    }

    #[test]
    fn byte_counts_read_in_the_largest_whole_unit() {
        assert_eq!(human_bytes(0), "0.0 B");
        assert_eq!(human_bytes(1536), "1.5 KB");
        assert_eq!(human_bytes(3 * 1024 * 1024 * 1024), "3.0 GB");
        assert_eq!(human_duration(59), "59s");
        assert_eq!(human_duration(61), "1m 1s");
        assert_eq!(human_duration(3 * 3600 + 120), "3h 2m");
    }

    #[test]
    fn the_desktop_directory_expands_both_spellings_of_home() {
        let home = Path::new("/home/someone");
        for text in [
            "XDG_DESKTOP_DIR=\"$HOME/Bureau\"\n",
            "XDG_DESKTOP_DIR=\"${HOME}/Bureau\"\n",
        ] {
            assert_eq!(
                parse_user_dirs_desktop(text, home).as_deref(),
                Some(Path::new("/home/someone/Bureau")),
                "{text}"
            );
        }
        assert_eq!(
            parse_user_dirs_desktop("XDG_DESKTOP_DIR=\"/data/desk\"\n", home).as_deref(),
            Some(Path::new("/data/desk"))
        );
    }

    #[test]
    fn a_commented_or_empty_desktop_entry_is_not_adopted() {
        let home = Path::new("/home/someone");
        assert_eq!(
            parse_user_dirs_desktop("# XDG_DESKTOP_DIR=\"$HOME/Old\"\n", home),
            None
        );
        assert_eq!(
            parse_user_dirs_desktop("XDG_DESKTOP_DIR=\"\"\n", home),
            None
        );
        // The commented line is skipped, not treated as the end of the search.
        let text = "# XDG_DESKTOP_DIR=\"$HOME/Old\"\nXDG_DOWNLOAD_DIR=\"$HOME/dl\"\n\
                    XDG_DESKTOP_DIR=\"$HOME/New\"\n";
        assert_eq!(
            parse_user_dirs_desktop(text, home).as_deref(),
            Some(Path::new("/home/someone/New"))
        );
    }
}
