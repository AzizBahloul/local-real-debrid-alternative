# Streaming Gateway (NovaStream)

Rust workspace: a local Stremio addon that turns a torrent into an HTTP video
stream over LAN. Two crates, one binary each — no other members, no build tool
besides Cargo (no Node/Python anywhere in this repo).

- `crates/gateway` (`streaming-gateway`) — the headless server: axum HTTP,
  librqbit torrent engine, Stremio addon protocol, TLS, caching, monitoring.
- `crates/gateway-gui` (`streaming-gateway-gui`) — egui desktop launcher that
  spawns/monitors the server binary and shows its stdout in the Log panel.

Full architecture rationale (TLS design, port history, stream-URL shape,
idle-reaper behavior) lives in `README.md` under "Design decisions" and in
module doc-comments (`src/tls/mod.rs`, `src/torrent/mod.rs`) — read those
before touching that code; this file only covers what isn't already written
down there.

## Commands
```
Build (release, both crates):  cargo build --release
Test:                          cargo test --release       # integration tests, real router+engine, temp dir — never touches a live swarm
Lint:                          cargo clippy --release --all-targets -- -D warnings
Package .deb:                  cargo deb -p streaming-gateway-gui
Package .rpm:                  cargo generate-rpm -p crates/gateway-gui -o out.rpm   # build FIRST, it does not
Package Arch:                  cd packaging/arch && makepkg -si
Full clean-machine setup:      ./setup.sh [--gui] [--no-build] [--service] [--user]
```

## Rebuilding, installing and relaunching without a password

**This is the loop to use after changing anything, and it needs no `sudo` at
all — so do it yourself rather than handing the user a command to paste.**

```
systemctl --user stop novastream          # stop the server
pkill -f streaming-gateway-gui            # and the window / tray, if open
./setup.sh --gui --user                   # build + install into ~/.local
setsid ~/.local/bin/streaming-gateway-gui >/dev/null 2>&1 &   # relaunch
```

Why this works, and the traps:

- `--user` installs the same four files to the same freedesktop locations
  under `$HOME/.local` instead of `/usr`, and skips the distro-package step
  (the only part that genuinely needs root). It reuses `install_manually`, so
  the four-places invariant below still covers it.
- `~/.profile` **prepends** `~/.local/bin` to `PATH` on every mainstream
  distro, so the user copy shadows any older `/usr/bin` one — which matters
  because the `.desktop` entry's `Exec=` is a bare program name resolved
  through `PATH`. A leftover `.deb` is therefore harmless but confusing;
  clear it with `sudo dpkg -r streaming-gateway-gui` when convenient.
- **The unit records an absolute `ExecStart`**, so switching prefixes leaves
  the service running the *old* binary until the unit is regenerated. Running
  `~/.local/bin/streaming-gateway-gui --enable-always-on` rewrites it (the GUI
  owns the only definition of that unit — never hand-write one).
- Under `--user` the icon is installed to the hicolor theme **as well as**
  `share/pixmaps`, because `~/.local/share/pixmaps` is not on any icon-lookup
  search path (`/usr/share/pixmaps` is a legacy fallback that is).
- Launching the window is enough to start the server: `ensure_always_on`
  installs the unit if missing and starts it if it is installed but stopped.
- The release profile is `lto = true` + `codegen-units = 1`, so each of the
  three binaries takes 1.5–2 minutes to *link*, single-threaded, with the
  progress bar apparently frozen at the last crate. That is normal, not a
  hang. For a throwaway iteration build only:
  `cargo build --release --config profile.release.lto=false --config profile.release.codegen-units=16`.
No CI config in this repo — these commands are the only source of truth for
build/test/lint. Run build+test+clippy before calling any change done.

## Gotchas
- **The build needs a C compiler and Rust. Nothing else, on any distro.**
  Do not add `-dev` packages to `setup.sh` on the assumption something links
  against them — measured 2026-09-07, `ldd` on both release binaries reports
  only libc/libm/libgcc_s. winit and glutin `dlopen` X11, Wayland, xkbcommon
  and GL at *runtime* (the `libloading`/`x11-dl`/`wayland-sys` dlopen
  features), there is no GTK in the tree at all (the tray is `ksni`, pure-Rust
  D-Bus), and aws-lc-sys builds with the `cc` crate — a full rebuild with
  cmake, perl, pkg-config, nasm and go stubbed to exit 127 succeeds. The only
  distro packages `setup.sh` installs beyond a toolchain are the GUI's
  dlopened runtime libs, and only under `--gui`.
- **`setup.sh` dispatches on the package manager, never on the os-release ID.**
  There are six managers and hundreds of distro IDs; a derivative is handled
  by whichever manager it inherited. Adding a branch means adding *both*
  `BUILD_PACKAGES` and `GUI_RUNTIME_PACKAGES` — `tests/packaging.rs` counts
  them and fails if one is missing, because a branch with build deps and no
  runtime deps compiles fine and then cannot open a window.
- **The install is described in four places** — `[package.metadata.deb]` and
  `[package.metadata.generate-rpm]` in `gateway-gui/Cargo.toml`,
  `packaging/arch/PKGBUILD`, and `install_manually()` in `setup.sh`. They must
  agree on all four files; `crates/gateway-gui/tests/packaging.rs` fails the
  build when they drift. A package that puts the binary where the `.desktop`
  entry does not point still installs cleanly and does nothing when clicked.
  Note `cargo generate-rpm -p` takes a **path**, despite `--help` saying crate
  name, and `find -newermt` is GNU-only (bfs ships as `find` on some distros).
- **Immutable/atomic distros are out of scope** (Silverblue, Kinoite, Bazzite,
  SteamOS): no host package manager, read-only `/usr`. `setup.sh` correctly
  stops at the "no C compiler" check instead of half-installing. Don't add a
  branch that claims to support them without testing on one.
- Always-on mode needs systemd. On Alpine/Void/Devuan/Artix the switch greys
  itself out by design (`service::available()`), and `setup.sh --service`
  says so rather than writing a unit nothing reads.
- **The `./cache` shown in the GUI is relative to the process's CWD, not the
  repo root.** The installed `.deb` binaries run from wherever they were
  launched, so their actual cache can land in `$HOME/cache/` while the repo's
  own `./cache/` sits stale/empty. Don't assume repo-local `cache/` reflects
  what a running installed instance is actually using — check `/health` or
  the GUI's own "Cache directory" line for the real path.
- **Port defaults: 8080 primary, 11470 fallback**, pinned in both
  `crates/gateway/src/config/mod.rs` and hardcoded in `gateway-gui/src/main.rs`
  — a test pins both. If you change one you must change the other or the GUI
  binds a port the server doesn't advertise.
- **The GUI draws ASCII text only; everything else is painted.** Bars, frames,
  the wordmark and the status lamp are `egui::Painter` rectangles, not block or
  box-drawing glyphs — eframe's bundled font only guarantees ASCII coverage, so
  a `█` progress bar renders as tofu boxes on someone else's machine. Tests in
  `gateway-gui/src/{fx,widgets}.rs` pin this. Layer order also matters and is
  easy to break: rain in the background layer (painted before any panel, which
  is why panel frames are transparent and `clear_color` supplies the black),
  widgets in the panel layer, CRT overlay in the foreground, boot cover above
  that.
- **Zero-OpenSSL is load-bearing.** Every TLS-adjacent dependency
  (`librqbit`, `reqwest`, `axum-server`, `rustls`) is pinned to rust-tls/rustls
  features specifically so `cargo build` needs no system `pkg-config`/
  `libssl-dev`. Don't add a dependency that pulls in `default-tls`/OpenSSL.
- **`torrent/mod.rs` "focused" torrent logic is intentionally narrow**:
  switching titles discards only the torrent you just left (if it has no open
  stream and isn't finished) — never the whole unfinished backlog. An earlier
  version swept everything on every switch and wiped queued titles; see the
  `action_for_abandoned` doc-comment before changing this. Separately, the
  cache janitor *does* sweep the backlog on purpose: `MAX_CACHED_TORRENTS`
  (default 10) keeps only the most recently used torrents and purges the rest,
  even under the size cap — that's the requested retention policy, not the old
  bug coming back. See "Retention is a count" in README's Design decisions.
- **`MAX_CACHED_TORRENTS` must stay ≥ `MAX_ACTIVE_DOWNLOADS`, and retention
  must keep sparing torrents that are still downloading.** The download queue
  (`torrent::download_slots`, default 4 wide, ordered by librqbit's
  incrementing `TorrentId`) deliberately runs titles nobody is watching yet —
  so a queued download has no open reader and no recent HTTP request, which is
  exactly the shape the janitor's retention rule treats as cold backlog. The
  `is_downloading` exemption in `cache::may_purge` is the only thing standing
  between the queue and the janitor deleting its output mid-write, on a cache
  nowhere near its size cap. `cache::tests::retention_never_deletes_a_download_
  still_in_progress` and `config::tests::the_cache_keeps_at_least_as_many_
  titles_as_the_queue_downloads` pin both halves. Note `is_downloading` is
  narrower than `is_running` on purpose: a *finished* torrent sits unpaused
  forever, so sparing everything running would exempt the watched-and-done
  backlog retention exists to reclaim.
- **A live reader blocks the idle reaper regardless of recency** — players
  buffer ahead and go quiet, and librqbit's reader has no timeout of its own,
  so treating "quiet" as "idle" freezes an actively-watched stream.
- **`torrent::MetadataArchive` is why replaying an old title works at all, and
  it must stay in its own subdirectory.** librqbit writes `<hash>.torrent` into
  the session dir and *deletes it with the torrent*, so once the janitor
  reclaims a title every trace of its metadata is gone and the next play falls
  back to resolving the magnet from the swarm — a DHT-only lookup (257 of 263
  advertised hashes carry no trackers) against a release whose seeders have
  moved on. That times out at `ADD_TORRENT_TIMEOUT`, the torrent never enters
  the session, and the visible symptom is the gateway not reacting to pressing
  play at all, with the 45s `START_FAILURE_COOLDOWN` then instant-500ing every
  retry so nothing ever appears in the swarm list. The archive is written on
  resolve, on a successful start, and for everything restored at startup
  (`archive_session_metadata`). Do not "tidy" it into the session directory.
- **A hand pause is not librqbit's paused flag.** `TorrentEngine::held` is a
  separate set, because the download queue's timer resumes anything inside
  `download_slots` about a second later — a pause that only called
  `api_torrent_action_pause` would visibly undo itself. `download_slots`
  excludes held hashes (so the hold hands its slot to the next title rather
  than wasting it), `enforce_download_slots` skips them in both directions,
  and `start_file`/`forget_activity` clear the hold. `/health` reports it as
  `held` so the GUI only offers Resume for a pause the viewer can actually
  undo — offering it for a queue-parked torrent promises something the queue
  immediately reverses.
- **Always-on has no off switch in the window, by design.** `ensure_always_on`
  installs the unit and the login tray icon once per session; the off switch is
  the tray's "Exit NovaStream", and `--disable-always-on` remains the headless
  escape hatch. Re-adding a disable button in `service_panel` puts back the
  failure mode where the addon silently stops answering the phone because
  someone toggled it.
- The GUI has no persistent log file — logs only exist in the GUI's in-memory
  Log panel and the server's stdout (piped, not written to disk). Use
  `GET /health` on the running port for a live sanity check instead of
  grepping for a log file. **Exception: in always-on mode** the server is a
  systemd user service, so its output *is* in the journal
  (`journalctl --user -u novastream`), which is where the GUI's panel reads
  from — and it follows by `_SYSTEMD_INVOCATION_ID`, because journalctl
  rejects the timestamp format systemd's own `ActiveEnterTimestamp` prints.
- **Closing the GUI window spawns a `--tray` process; it does not hide the
  window.** Wayland forbids a client hiding or un-minimising its own toplevel,
  and winit allows one event loop per process, so a closed window is gone for
  good — the tray has to be a second process. `tray.rs` keeps exactly one of
  each via a pid file in `$XDG_RUNTIME_DIR`. Don't "simplify" this back into
  `ViewportCommand::Visible(false)`: it is a silent no-op on Wayland.
- **Two owners of the server, and the GUI dispatches on which.**
  `service.installed` (the systemd user unit exists) means start/stop go
  through `systemctl --user` and logs come from the journal; otherwise the
  server is this window's child as before. Anything that reads `self.running`
  must go through that split or it reports on the wrong process.
- The service's `EnvironmentFile` (`~/.config/novastream/server.env`) is also
  what a freshly opened window reads its port and cache directory from — a
  service moved off 8080 is otherwise invisible to the health poll.

## Do not
- Don't `git push` or restart/kill a running gateway process unless asked —
  the desktop app may be actively serving a real stream to a phone.
- Don't add a second chunked/TLS/HTTP client stack — reuse the existing
  axum/reqwest/rustls setup.
