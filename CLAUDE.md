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
Full clean-machine setup:      ./setup.sh [--gui] [--no-build]
```
No CI config in this repo — these commands are the only source of truth for
build/test/lint. Run build+test+clippy before calling any change done.

## Gotchas
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
  (default 2) keeps only the most recently used torrents and purges the rest,
  even under the size cap — that's the requested retention policy, not the old
  bug coming back. See "Retention is a count" in README's Design decisions.
- **A live reader blocks the idle reaper regardless of recency** — players
  buffer ahead and go quiet, and librqbit's reader has no timeout of its own,
  so treating "quiet" as "idle" freezes an actively-watched stream.
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
