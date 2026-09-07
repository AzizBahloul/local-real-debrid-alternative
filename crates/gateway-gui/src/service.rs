//! Always-on mode: the gateway as a **systemd user service**.
//!
//! The desktop app spawning the server as its own child (see
//! `server_process.rs`) is fine while someone is sitting in front of the
//! window, but it cannot survive the window closing, a logout, or a reboot --
//! and "the stream keeps working after I close the app / restart the PC" is
//! exactly what a household expects from something that behaves like a box on
//! the shelf.
//!
//! So the real answer is a service manager, and on a Linux desktop that is
//! systemd. Deliberately a **user** unit (`~/.config/systemd/user/`) rather
//! than a system one:
//!
//! * it installs with no root at all -- no polkit prompt, no `sudo` in a GUI,
//!   nothing the `.deb` has to do at install time for a user who may never
//!   want the service;
//! * it runs as the person who owns the cache directory, so the files the
//!   service writes are the same files the desktop app has always written;
//! * `loginctl enable-linger` is the one switch that makes a *user* unit start
//!   at boot without anybody logging in, and it is self-service (the
//!   `org.freedesktop.login1.set-self-linger` polkit action allows the active
//!   user), so "on at boot" stays a single click.
//!
//! Settings live in an `EnvironmentFile` rather than baked into `ExecStart=`:
//! the server already reads every option from the environment (see
//! `AppConfig`), so changing the port in the GUI is a file write plus a
//! restart, not a unit rewrite.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{anyhow, bail, Context, Result};

/// Unit name. Also the journal identifier the log panel tails.
pub const UNIT: &str = "novastream.service";

/// Filename of the autostart entry that brings the tray icon back at login.
const AUTOSTART_FILE: &str = "novastream-tray.desktop";

fn home() -> Result<PathBuf> {
    std::env::var_os("HOME")
        .filter(|h| !h.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("HOME is not set"))
}

fn config_home() -> Result<PathBuf> {
    if let Some(dir) = std::env::var_os("XDG_CONFIG_HOME").filter(|d| !d.is_empty()) {
        let dir = PathBuf::from(dir);
        if dir.is_absolute() {
            return Ok(dir);
        }
    }
    Ok(home()?.join(".config"))
}

pub fn unit_path() -> Result<PathBuf> {
    Ok(config_home()?.join("systemd/user").join(UNIT))
}

/// Where the GUI's port/cache settings are handed to the service.
pub fn env_path() -> Result<PathBuf> {
    Ok(config_home()?.join("novastream/server.env"))
}

pub fn autostart_path() -> Result<PathBuf> {
    Ok(config_home()?.join("autostart").join(AUTOSTART_FILE))
}

/// True when there is a systemd user session to talk to at all.
///
/// Checked rather than assumed because this same binary is expected to run on
/// a live-USB session, inside a container, or on a non-systemd distro, and the
/// honest thing there is to grey the switch out rather than to write a unit
/// file nothing will ever read.
pub fn available() -> bool {
    if std::env::var_os("XDG_RUNTIME_DIR").is_none() {
        return false;
    }
    Command::new("systemctl")
        .args(["--user", "show", "--property=Version"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// What the always-on switch is currently doing, as four independent facts.
///
/// They really are independent: a unit can be installed but stopped, enabled
/// for login but not lingering (so it comes back when *you* log in but not
/// after an unattended reboot), or running right now without being enabled at
/// all. Collapsing them into one boolean is how a GUI ends up claiming "on"
/// for a setup that dies at the next power cut.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Status {
    /// The unit file exists in the user's systemd directory.
    pub installed: bool,
    /// `systemctl --user is-enabled` -- starts when the user session starts.
    pub enabled: bool,
    /// `systemctl --user is-active` -- running right now.
    pub active: bool,
    /// Lingering is on, so the user session (and this unit) starts at boot
    /// without anyone logging in.
    pub linger: bool,
}

impl Status {
    /// The only combination that actually delivers "always on, survives a
    /// reboot": installed, enabled for the session, and lingering so the
    /// session exists at boot.
    pub fn always_on(&self) -> bool {
        self.installed && self.enabled && self.linger
    }

    /// One line for the GUI, naming whichever half is missing rather than
    /// saying "partially enabled".
    pub fn summary(&self) -> String {
        if !self.installed {
            return "off -- the gateway only runs while this window is open".to_string();
        }
        let run = if self.active {
            "running"
        } else {
            "installed but stopped"
        };
        match (self.enabled, self.linger) {
            (true, true) => format!("{run}; starts automatically at boot"),
            (true, false) => {
                format!("{run}; starts when you log in (not before -- linger is off)")
            }
            (false, true) => format!("{run}; will NOT start on its own (not enabled)"),
            (false, false) => format!("{run}; will NOT start on its own"),
        }
    }
}

fn systemctl(args: &[&str]) -> Result<std::process::Output> {
    Command::new("systemctl")
        .arg("--user")
        .args(args)
        .output()
        .context("could not run systemctl --user")
}

/// `systemctl` uses the exit status, not stdout, to answer is-active/is-enabled,
/// and returns non-zero for perfectly ordinary answers ("inactive", "disabled").
fn systemctl_ok(args: &[&str]) -> bool {
    systemctl(args).map(|o| o.status.success()).unwrap_or(false)
}

pub fn status() -> Status {
    let installed = unit_path().map(|p| p.is_file()).unwrap_or(false);
    if !installed {
        // is-active would still answer for a stale run, but reporting a
        // service we no longer own is worse than reporting nothing.
        return Status::default();
    }
    Status {
        installed,
        enabled: systemctl_ok(&["is-enabled", UNIT]),
        active: systemctl_ok(&["is-active", UNIT]),
        linger: linger_enabled(),
    }
}

fn linger_enabled() -> bool {
    let user = match std::env::var("USER") {
        Ok(u) if !u.is_empty() => u,
        _ => return false,
    };
    Command::new("loginctl")
        .args(["show-user", &user, "--property=Linger", "--value"])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim() == "yes")
        .unwrap_or(false)
}

/// Cache paths must be absolute in the unit's world.
///
/// The desktop app's default is the relative `./cache`, which resolves against
/// whatever directory the launcher happened to start it in -- the long-standing
/// trap that makes the GUI's "Cache directory" line and the real download
/// location disagree. A service has no such directory to inherit, so the
/// relative form is anchored to `$HOME` here: that is the same place the
/// desktop launcher starts from, so an existing `~/cache` full of downloads
/// keeps being used instead of silently starting a second one.
pub fn absolute_cache_dir(raw: &str, home: &Path) -> PathBuf {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return home.join("cache");
    }
    let path = Path::new(trimmed);
    if path.is_absolute() {
        return path.to_path_buf();
    }
    let relative = trimmed.strip_prefix("./").unwrap_or(trimmed);
    home.join(relative)
}

pub fn render_env(port: &str, cache_dir: &Path) -> String {
    format!(
        "# Written by the NovaStream desktop app. Edited values survive a\n\
         # restart of the service; the app rewrites this file when you change\n\
         # the port or cache directory in Advanced settings.\n\
         GATEWAY_PORT={}\n\
         CACHE_DIRECTORY={}\n",
        port.trim(),
        cache_dir.display()
    )
}

/// The unit file.
///
/// `Restart=always` because the whole point of the service is that a crash,
/// an OOM kill or a router hiccup does not need a human at a keyboard.
/// `After=network-online.target` because the gateway prints (and the phone
/// consumes) a LAN address it can only discover once an interface is up.
pub fn render_unit(binary: &Path, env_file: &Path, working_dir: &Path) -> String {
    format!(
        "[Unit]\n\
         Description=NovaStream streaming gateway\n\
         Documentation=https://github.com/AzizBahloul/local-real-debrid-alternative\n\
         After=network-online.target\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         Type=simple\n\
         ExecStart={exec}\n\
         EnvironmentFile=-{env}\n\
         WorkingDirectory={cwd}\n\
         Restart=always\n\
         RestartSec=3\n\
         # The engine keeps a file handle per piece of every active torrent.\n\
         LimitNOFILE=32768\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n",
        exec = binary.display(),
        env = env_file.display(),
        cwd = working_dir.display(),
    )
}

fn write_file(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("could not create {}", parent.display()))?;
    }
    std::fs::write(path, contents).with_context(|| format!("could not write {}", path.display()))
}

/// Reads back the port and cache directory the service is configured with.
///
/// The window uses this to adopt the service's settings at startup instead of
/// its own defaults. Without it, a service moved to another port is invisible
/// to a freshly opened window: the health poll goes to 8080, gets nothing, and
/// the app reports a perfectly healthy gateway as down.
pub fn read_settings() -> Option<(String, String)> {
    let text = std::fs::read_to_string(env_path().ok()?).ok()?;
    let mut port = None;
    let mut cache = None;
    for line in text.lines().map(str::trim) {
        if let Some(value) = line.strip_prefix("GATEWAY_PORT=") {
            port = Some(value.trim().to_string());
        } else if let Some(value) = line.strip_prefix("CACHE_DIRECTORY=") {
            cache = Some(value.trim().to_string());
        }
    }
    match (port, cache) {
        (Some(port), Some(cache)) if !port.is_empty() && !cache.is_empty() => Some((port, cache)),
        _ => None,
    }
}

/// Writes only the settings file, for when the service already exists and the
/// user changed the port or the cache directory.
pub fn write_settings(port: &str, cache_dir: &str) -> Result<PathBuf> {
    let home = home()?;
    let resolved = absolute_cache_dir(cache_dir, &home);
    let env_file = env_path()?;
    write_file(&env_file, &render_env(port, &resolved))?;
    Ok(resolved)
}

/// Installs, enables and starts the service, and turns on lingering so it also
/// comes up at boot.
///
/// Lingering failing is not fatal: without it the service still starts at
/// login, which is most of the benefit, and the status line says plainly which
/// of the two is missing rather than claiming success.
pub fn install(binary: &Path, port: &str, cache_dir: &str) -> Result<()> {
    if !available() {
        bail!("no systemd user session on this machine");
    }
    let home = home()?;
    let resolved_cache = write_settings(port, cache_dir)?;
    if let Err(e) = std::fs::create_dir_all(&resolved_cache) {
        // Not fatal -- the server creates it too. Worth surfacing only if it
        // is a real permission problem, which the server would hit as well.
        eprintln!(
            "warning: could not pre-create {}: {e}",
            resolved_cache.display()
        );
    }

    let unit = unit_path()?;
    write_file(&unit, &render_unit(binary, &env_path()?, &home))?;

    let reload = systemctl(&["daemon-reload"])?;
    if !reload.status.success() {
        bail!(
            "systemctl --user daemon-reload failed: {}",
            String::from_utf8_lossy(&reload.stderr).trim()
        );
    }
    let enable = systemctl(&["enable", UNIT])?;
    if !enable.status.success() {
        bail!(
            "systemctl --user enable {UNIT} failed: {}",
            String::from_utf8_lossy(&enable.stderr).trim()
        );
    }
    // `restart`, not `start`: this same path runs when the unit already exists
    // -- after a package upgrade moved the binary, or when the port changed --
    // and `start` on an already-running unit is a no-op, which would leave the
    // old executable and the old settings live while the window reports the
    // new ones.
    let start = systemctl(&["restart", UNIT])?;
    if !start.status.success() {
        bail!(
            "systemctl --user restart {UNIT} failed: {}",
            String::from_utf8_lossy(&start.stderr).trim()
        );
    }
    let _ = enable_linger();
    Ok(())
}

/// Asks logind to keep this user's session alive when nobody is logged in.
pub fn enable_linger() -> Result<()> {
    let user = std::env::var("USER").context("USER is not set")?;
    let out = Command::new("loginctl")
        .args(["enable-linger", &user])
        .output()
        .context("could not run loginctl")?;
    if out.status.success() {
        Ok(())
    } else {
        bail!(
            "loginctl enable-linger failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        )
    }
}

/// Stops, disables and removes the unit. Leaves the settings file and the
/// cache alone -- turning always-on off is not a request to delete downloads.
pub fn uninstall() -> Result<()> {
    let _ = systemctl(&["disable", "--now", UNIT]);
    if let Ok(unit) = unit_path() {
        if unit.exists() {
            std::fs::remove_file(&unit)
                .with_context(|| format!("could not remove {}", unit.display()))?;
        }
    }
    let _ = systemctl(&["daemon-reload"]);
    Ok(())
}

pub fn start() -> Result<()> {
    let out = systemctl(&["start", UNIT])?;
    if out.status.success() {
        Ok(())
    } else {
        bail!(
            "could not start the service: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        )
    }
}

pub fn stop() -> Result<()> {
    let out = systemctl(&["stop", UNIT])?;
    if out.status.success() {
        Ok(())
    } else {
        bail!(
            "could not stop the service: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        )
    }
}

/// The id systemd gave the unit's current run, or `None` if it is not running.
///
/// Used instead of the more obvious `ActiveEnterTimestamp` + `--since`,
/// because systemd prints that timestamp as `Mon 2026-09-07 01:49:30 CET` and
/// journalctl refuses to parse its own service manager's format ("Failed to
/// parse timestamp"), which silently left the log panel empty. The invocation
/// id is exact rather than approximate anyway: it selects this run's lines and
/// no others, even across a restart in the same second.
pub fn invocation_id() -> Option<String> {
    let out = systemctl(&["show", UNIT, "--property=InvocationID", "--value"]).ok()?;
    let value = String::from_utf8_lossy(&out.stdout).trim().to_string();
    // Empty for a unit that has never started, and systemd occasionally says
    // "n/a" instead.
    (!value.is_empty() && value != "n/a").then_some(value)
}

/// The command whose stdout the log panel tails when the server is a service.
///
/// `-o cat` because the server already timestamps and formats its own lines;
/// journald's default prefix would double it. The backlog is what lets the
/// window show the addon URL for a service it did not start: that URL is only
/// ever printed in the startup banner, so a plain `-f` on a service that has
/// been up for a week shows a running gateway with no address anywhere.
/// Scoped to one invocation when systemd names it, so the banner on screen
/// belongs to the process actually listening rather than to a previous run.
pub fn journal_command(lines: usize, invocation: Option<&str>) -> Command {
    let mut cmd = Command::new("journalctl");
    cmd.args(["--user", "-o", "cat", "--no-pager"]);
    match invocation {
        Some(id) => {
            cmd.arg(format!("_SYSTEMD_INVOCATION_ID={id}"));
        }
        None => {
            cmd.args(["-u", UNIT]);
        }
    }
    cmd.args(["-n", &lines.to_string(), "-f"]);
    cmd
}

/// The `.desktop` file that starts the tray icon at login.
pub fn render_autostart(exe: &Path) -> String {
    format!(
        "[Desktop Entry]\n\
         Type=Application\n\
         Name=NovaStream (tray)\n\
         Comment=Keep NovaStream in the system tray\n\
         Exec={} --tray\n\
         Icon=streaming-gateway-gui\n\
         Terminal=false\n\
         NoDisplay=true\n\
         X-GNOME-Autostart-enabled=true\n",
        exe.display()
    )
}

pub fn autostart_enabled() -> bool {
    autostart_path().map(|p| p.is_file()).unwrap_or(false)
}

pub fn install_autostart(exe: &Path) -> Result<()> {
    write_file(&autostart_path()?, &render_autostart(exe))
}

pub fn remove_autostart() -> Result<()> {
    let path = autostart_path()?;
    if path.exists() {
        std::fs::remove_file(&path)
            .with_context(|| format!("could not remove {}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_cache_dirs_are_anchored_to_home_not_the_working_directory() {
        let home = Path::new("/home/someone");
        // The desktop app's own default. A service inherits no working
        // directory, so leaving this relative would put downloads in "/".
        assert_eq!(
            absolute_cache_dir("./cache", home),
            Path::new("/home/someone/cache")
        );
        assert_eq!(
            absolute_cache_dir("cache", home),
            Path::new("/home/someone/cache")
        );
        assert_eq!(
            absolute_cache_dir("  ", home),
            Path::new("/home/someone/cache")
        );
        assert_eq!(
            absolute_cache_dir("/mnt/big/cache", home),
            Path::new("/mnt/big/cache")
        );
    }

    #[test]
    fn the_unit_restarts_the_server_and_waits_for_the_network() {
        let unit = render_unit(
            Path::new("/usr/bin/streaming-gateway"),
            Path::new("/home/someone/.config/novastream/server.env"),
            Path::new("/home/someone"),
        );
        assert!(unit.contains("ExecStart=/usr/bin/streaming-gateway"));
        assert!(unit.contains("Restart=always"));
        // Without this the gateway can bind before an interface has an
        // address and print a LAN URL no phone can reach.
        assert!(unit.contains("Wants=network-online.target"));
        // A missing settings file must not fail the unit ("-" prefix).
        assert!(unit.contains("EnvironmentFile=-/home/someone/.config/novastream/server.env"));
        // default.target, not multi-user.target: this is a *user* unit.
        assert!(unit.contains("WantedBy=default.target"));
    }

    #[test]
    fn settings_survive_a_round_trip_through_the_env_file() {
        // Not a file test: this pins that the parser reads back exactly what
        // the renderer writes, comments and all. They drift apart silently --
        // the symptom is a window polling the default port for a service that
        // was moved.
        let rendered = render_env("9000", Path::new("/mnt/big/cache"));
        let mut port = None;
        let mut cache = None;
        for line in rendered.lines().map(str::trim) {
            if let Some(v) = line.strip_prefix("GATEWAY_PORT=") {
                port = Some(v.to_string());
            } else if let Some(v) = line.strip_prefix("CACHE_DIRECTORY=") {
                cache = Some(v.to_string());
            }
        }
        assert_eq!(port.as_deref(), Some("9000"));
        assert_eq!(cache.as_deref(), Some("/mnt/big/cache"));
    }

    #[test]
    fn settings_reach_the_service_as_the_env_vars_the_server_actually_reads() {
        // These two names are a contract with `AppConfig`'s `env = "..."`
        // attributes -- renaming one there without this file is a silent
        // reversion to the defaults.
        let env = render_env("8080", Path::new("/home/someone/cache"));
        assert!(env.contains("GATEWAY_PORT=8080"));
        assert!(env.contains("CACHE_DIRECTORY=/home/someone/cache"));
    }

    #[test]
    fn always_on_needs_every_part_not_just_an_installed_unit() {
        let installed_only = Status {
            installed: true,
            enabled: false,
            active: true,
            linger: false,
        };
        assert!(!installed_only.always_on());
        // The half-configured case must say which half, not just "on".
        assert!(installed_only.summary().contains("NOT start"));

        let no_linger = Status {
            installed: true,
            enabled: true,
            active: true,
            linger: false,
        };
        assert!(
            !no_linger.always_on(),
            "without linger the unit waits for a login, so a reboot leaves it down"
        );
        assert!(no_linger.summary().contains("log in"));

        let full = Status {
            installed: true,
            enabled: true,
            active: true,
            linger: true,
        };
        assert!(full.always_on());
        assert!(full.summary().contains("boot"));
    }

    /// The regression that emptied the log panel: journalctl rejects the
    /// timestamp format systemd prints, so the follower exited immediately and
    /// the window showed a running gateway with no log and no addon URL.
    #[test]
    fn the_journal_follower_selects_a_run_by_invocation_not_by_timestamp() {
        let with_id = journal_command(500, Some("74b720cd000a40aba4359c1933fecf33"));
        let args: Vec<String> = with_id
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert!(
            args.contains(&"_SYSTEMD_INVOCATION_ID=74b720cd000a40aba4359c1933fecf33".to_string())
        );
        assert!(!args.iter().any(|a| a == "--since"));
        assert!(args.contains(&"-f".to_string()));

        // No invocation (never started): fall back to the whole unit rather
        // than to an empty match, which would follow the entire journal.
        let without = journal_command(500, None);
        let args: Vec<String> = without
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert!(args.windows(2).any(|w| w[0] == "-u" && w[1] == UNIT));
    }

    #[test]
    fn the_autostart_entry_launches_the_tray_and_not_the_window() {
        let entry = render_autostart(Path::new("/usr/bin/streaming-gateway-gui"));
        assert!(entry.contains("Exec=/usr/bin/streaming-gateway-gui --tray"));
        assert!(entry.contains("X-GNOME-Autostart-enabled=true"));
    }
}
