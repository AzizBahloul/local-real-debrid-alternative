<div align="center">

# ⚡ streaming-gateway

### Turn a magnet link into a seekable HTTP video stream — for Stremio, VLC, phones, and TVs.

[![License: MIT](https://img.shields.io/badge/license-MIT-39FF85?style=for-the-badge)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-stable-e05d44?style=for-the-badge&logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![Stremio compatible](https://img.shields.io/badge/stremio-compatible-7b5bf5?style=for-the-badge)](https://www.stremio.com/)
[![Platform: Linux](https://img.shields.io/badge/platform-Ubuntu%20%2F%20Linux-0ea5e9?style=for-the-badge&logo=ubuntu&logoColor=white)](setup.sh)
[![Powered by librqbit](https://img.shields.io/badge/torrent%20engine-librqbit-f5a623?style=for-the-badge)](https://github.com/ikatson/rqbit)

*Runs entirely on your own PC. No cloud, no account, no subscription — just BitTorrent in, HTTP out.*

</div>

---

## Table of contents

- [What this is](#what-this-is)
- [Why Rust, and why not reinvent the torrent engine](#why-rust-and-why-not-reinvent-the-torrent-engine)
- [The GUI launcher](#the-gui-launcher)
- [Project layout](#project-layout)
- [Installation](#installation)
- [Starting the server](#starting-the-server)
- [Playing something](#playing-something)
- [Setting up your phone](#setting-up-your-phone)
- [Setting up a smart TV](#setting-up-a-smart-tv)
- [Monitoring](#monitoring)
- [Security notes](#security-notes)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)

---

## What this is

```
Stremio / VLC / phone / TV  ──HTTP + Range requests──▶  streaming-gateway  ──▶  torrent engine  ──▶  local cache
```

A local, high-performance streaming gateway that turns a magnet link or
BitTorrent info hash you already have into a normal HTTP video stream (with
working seek) on your home network. It is **not** a torrent search engine or
indexer — it resolves torrents you already know about; it does not find them
for you. See [How Stremio integration actually works](#how-stremio-integration-actually-works)
for why that distinction matters.

> [!TIP]
> The fastest way to understand this project: it's the same "magnet → local
> playable stream" trick Stremio's own bundled engine does, packaged as a
> standalone LAN service any device can hit over plain HTTP — Stremio, VLC,
> a browser, a Chromecast, a Samsung TV, whatever's on the Wi-Fi.

## Why Rust, and why not reinvent the torrent engine

The priority order for this project is latency, throughput, and low
CPU/RAM — a Python/FastAPI stack would be the easy path but not the fast one.
Rust with a `tokio` + `axum` async core gives the performance/memory profile
this needs without giving up productivity. Language choice was the easy part;
the harder decision was the BitTorrent engine.

[`librqbit`](https://github.com/ikatson/rqbit) — an actively maintained,
pure-Rust BitTorrent client library — already implements the two hardest
parts of this project correctly:

- **Piece prioritization for streaming**: first piece and last piece of a
  file first (fast "does it play" + fast seek-to-end), then sequential
  fill-in, with an `AsyncRead`/`AsyncSeek` file stream that blocks on a
  missing piece and wakes up the moment it arrives.
- **HTTP range serving** semantics for exactly that streaming reader.

Reimplementing that from scratch would just produce a worse copy of a
battle-tested implementation. So this project's own code (`src/torrent`,
`src/streaming`) is deliberately thin: it wraps `librqbit`'s `Session`/`Api`,
restricts a torrent's download to only the file being played, maps HTTP Range
headers onto it, and adds what `librqbit` intentionally doesn't do on its own
— a Stremio-shaped API, LAN discovery, a cache size cap with eviction, and
monitoring.

## The GUI launcher

Don't want a terminal? `streaming-gateway-gui` is a small native desktop app
that starts/stops the server and shows you what it's doing — one big button,
live stats, and a mood that changes with it.

<table>
<tr>
<td><img src="docs/screenshots/stopped.png" width="380" alt="Streaming Gateway GUI, stopped"></td>
<td><img src="docs/screenshots/running.png" width="380" alt="Streaming Gateway GUI, running"></td>
</tr>
<tr>
<td align="center"><sub><b>Stopped</b> — one click away</sub></td>
<td align="center"><sub><b>Running</b> — live address, health stats, big red stop</sub></td>
</tr>
</table>

Install the `.deb` and it shows up as **Streaming Gateway** in your
applications menu, no terminal required ever again. See
[Installation](#installation) below for the two-line build-and-install.

## Project layout

```
crates/
  gateway/            the server (this is the actual product)
    src/
      main.rs           thin entry point (calls streaming_gateway::run())
      lib.rs            router assembly, startup, shared AppState
      torrent/          librqbit wrapper: magnet/hash resolution, file
                        selection, starting downloads, activity tracking
      streaming/        HTTP Range serving (RFC 7233) + /play endpoint
      stremio/          /manifest.json + /stream/{type}/{id}.json
      cache/            size-capped cache eviction (never evicts an active stream)
      network/          LAN IP detection + startup banner
      config/           CLI flags / env vars (clap)
      monitoring/       /health + periodic terminal status
      error.rs          shared JSON error response type
    tests/http_api.rs   integration tests against the real router
  gateway-gui/        optional desktop launcher (start/stop + status)
    src/
      main.rs           eframe/egui UI
      server_process.rs subprocess lifecycle (spawn, stream logs, stop)
    assets/*.desktop    applications-menu entry
setup.sh              prepares a clean Ubuntu machine, builds, verifies
Dockerfile            multi-stage container build (server only)
```

## Installation

### Option A: `setup.sh` (Ubuntu)

```bash
./setup.sh          # server only -- works on a headless box too
./setup.sh --gui    # also builds the GUI launcher and packages a .deb
```

Detects your OS, installs only what's missing, installs Rust via `rustup` if
it isn't already there, builds a release binary, and verifies it starts.
Re-running it is safe — it never reinstalls something already present.
Notably **not installed**: Node, Python, or OpenSSL dev headers — none of
them are needed (see below).

> [!NOTE]
> The GUI needs GTK/X11 dev headers to *build* (`--gui` installs them); the
> plain server needs none of that and builds/runs fine on a headless NAS or
> home server. That split is intentional — see
> [setup.sh](setup.sh).

### Option B: the GUI + `.deb` package

```bash
./setup.sh --gui
sudo dpkg -i target/debian/streaming-gateway_*.deb
```

Installs both binaries to `/usr/bin/` and adds **Streaming Gateway** to your
applications menu (`Icon=video-x-generic`, category Audio/Video). From then
on: open the app, click the big green button, done. Closing the app also
stops the server (SIGTERM, escalating to SIGKILL after 5s if it doesn't
exit).

### Option C: manual (server only)

```bash
# Requires: a C compiler + cmake + perl (for rustls's crypto backend) and Rust stable.
cargo build --release -p streaming-gateway
./target/release/streaming-gateway
```

### Option D: Docker

```bash
docker build -t streaming-gateway .
docker run -d --name streaming-gateway \
  --network host \
  -v streaming-gateway-data:/data \
  streaming-gateway
```

> [!WARNING]
> Use `--network host`. The gateway prints its own LAN IP in a startup
> banner; inside the default bridge network that's the *container's*
> internal IP, not your machine's — useless for a phone on the same Wi-Fi to
> connect to. Publishing ports (`-p 11470:11470`) works too, but then ignore
> the banner and use your host's real LAN IP (`hostname -I`) instead.

No OpenSSL, `libssl-dev`, or `pkg-config` show up anywhere in this project's
own dependency chain by design: `librqbit` is built with its `rust-tls`
feature (pure-Rust `rustls` + `ring`/`aws-lc-rs` instead of OpenSSL), so the
server builds on a bare Ubuntu box with just a C compiler — see the comment
on the `librqbit` line in `crates/gateway/Cargo.toml`.

## Starting the server

```bash
./target/release/streaming-gateway
```

You'll see:

```
================================
   Streaming Gateway Running
================================

   Local Address:
   http://192.168.1.42:11470

   Add this URL + /manifest.json to Stremio,
   or open http://192.168.1.42:11470/play?magnet=<magnet-link> directly.

================================
```

Binds `0.0.0.0:11470` by default (falls back to `8080` if `11470` is already
taken). Every device on the same Wi-Fi/LAN can reach it at
`http://<that-ip>:<port>`.

### Configuration

Every setting is a CLI flag or an environment variable:

| Env var | Flag | Default | Meaning |
|---|---|---|---|
| `GATEWAY_PORT` | `--port` | `11470` | primary port |
| `GATEWAY_FALLBACK_PORT` | `--fallback-port` | `8080` | used if the primary port is taken |
| `GATEWAY_BIND_ADDR` | `--bind-addr` | `0.0.0.0` | set to `127.0.0.1` to disable LAN access entirely |
| `CACHE_DIRECTORY` | `--cache-dir` | `./cache` | where torrent data + session state live |
| `MAX_CACHE_SIZE_GB` | `--max-cache-size-gb` | `20` | cache size cap |
| `AUTO_CLEANUP` | `--auto-cleanup` | `true` | evict old torrents over the cap |
| `CACHE_CLEANUP_INTERVAL_SECS` | `--cleanup-interval-secs` | `60` | how often the janitor checks |
| `MAX_CONCURRENT_TORRENTS` | `--max-concurrent-torrents` | `8` | cap on simultaneously-managed torrents |
| `DISABLE_DHT` | `--disable-dht` | `false` | trackers/PEX only, no DHT |
| `MONITOR_INTERVAL_SECS` | `--monitor-interval-secs` | `5` | terminal status print interval |
| `LOG_LEVEL` | `--log-level` | `info` | `tracing` filter, e.g. `debug`, `warn` |

## Playing something

### The direct way (works everywhere: VLC, browsers, phones, TVs)

```
http://<gateway-ip>:11470/play?magnet=<url-encoded magnet link>
```

Open that URL in VLC ("Open Network Stream"), in a mobile player app, cast it
to a smart TV app that accepts a URL, or paste it into a browser. The gateway
resolves the torrent, picks the largest video file automatically (or pass
`&fileIdx=N` to choose a specific file), starts downloading it
piece-by-piece in playback order, and streams it back with working seek
(forward and backward) from the first request.

**The magnet link must be URL-encoded** as the value of `magnet=` (it
contains `&` and `:` characters that are otherwise query-string
delimiters). Most tools that let you "share a link" do this for you
automatically; if you're building the URL by hand, percent-encode it first.

### How Stremio integration actually works

Add the addon in Stremio via "Install add-on from URL":

```
http://<gateway-ip>:11470/manifest.json
```

Stremio then queries `GET /stream/{type}/{id}.json` for whatever id it's
currently showing (normally an IMDB id like `tt1234567`). This gateway has no
search index, so it can only answer when the `id` itself already is a magnet
link — for anything else (most of the time, browsing normal titles) it
correctly returns an empty stream list, same as any other addon that simply
doesn't have that title. This is why `/play?magnet=...` above, not the
addon-catalog flow, is the primary way to use this project day to day; the
addon endpoints exist for protocol compliance and for tools/workflows that
already hand Stremio a magnet-shaped id.

## Setting up your phone

1. Make sure your phone is on the **same Wi-Fi network** as the computer
   running the gateway.
2. Find the gateway's address from the startup banner, e.g.
   `http://192.168.1.42:11470`.
3. **Stremio app (Android/iOS):** open Stremio → the add-ons/puzzle-piece
   icon → "Install from URL" → paste `http://192.168.1.42:11470/manifest.json`
   → Install. To play a specific magnet you already have, open
   `http://192.168.1.42:11470/play?magnet=<...>` directly in the phone's
   browser — most Android browsers hand a video stream straight to Stremio,
   VLC, or another installed player via the system "Open with" chooser.
4. **VLC for Android/iOS:** VLC → the network icon → "Stream" / "Open Network
   Stream" → paste the `/play?magnet=...` URL → Play.
5. **Any other player app** that supports "open network URL" or "open stream
   URL" works the same way.
6. If nothing loads: open `http://192.168.1.42:11470/health` in the phone's
   browser first — you should get back a small JSON status blob. If that
   fails, it's a network/firewall problem (see [Troubleshooting](#troubleshooting)),
   not a Stremio problem.

## Setting up a smart TV

- **TVs with a Stremio app** (Android TV, some Samsung/LG models): same as
  the phone instructions — install the addon from the manifest URL, and use
  `/play?magnet=...` directly for anything not reachable through the addon
  catalog flow.
- **TVs/set-top boxes with VLC or a generic network player**: point it at
  `http://<gateway-ip>:11470/play?magnet=<...>` the same way as VLC on a
  phone.
- **Chromecast/DLNA-only devices**: cast the `/play?magnet=...` URL from a
  casting-capable app on your phone/laptop; the response carries the
  `Accept-Ranges`/`Content-Type` headers DLNA/cast receivers expect.

## Monitoring

- `GET /health` — JSON status: uptime, active torrents (name, progress %,
  down/up speed, peer count), active stream count, cache usage vs. cap,
  process memory/CPU.
- The running process also prints a periodic terminal status block
  (`MONITOR_INTERVAL_SECS`, default every 5s) with the same information in a
  human-readable form, plus which client IPs have made requests recently.
- The GUI launcher polls `/health` every 2 seconds and shows the same data
  live, no terminal needed.

## Security notes

- Every client-supplied identifier is validated before use: info hashes must
  be exactly 40 hex characters, magnet links must carry a well-formed
  `xt=urn:btih:` topic and are capped at 8 KB. There is no code path where a
  client-supplied string is used as a filesystem path — file access is
  always mediated through `librqbit`'s own metadata table (torrent id + file
  index), never a raw path.
- CORS is wide open (`Access-Control-Allow-Origin: *`) — required by the
  Stremio addon protocol and needed for LAN devices to fetch cross-origin.
  There's no cookie/credential-based auth on this server for CORS to leak,
  by design: this is a LAN convenience tool, not something meant to be
  exposed to the public internet. Set `GATEWAY_BIND_ADDR=127.0.0.1` if you
  only ever want to reach it from the same machine, and don't port-forward
  it through your router.
- Request bodies are capped at 16 KB (every endpoint is a GET with
  query/path params — this is defense in depth, not a real-world limit).
- A 60s timeout bounds *time to first byte* (resolving metadata / opening a
  file stream) — it does not cut off an in-progress video download/stream,
  since the HTTP response is handed back as soon as headers are ready and
  the body streams independently after that.

## Testing

```bash
cargo test               # 16 unit + 7 integration tests (server) + 3 unit tests (GUI)
cargo clippy --all-targets
cargo fmt -- --check
```

The integration tests (`crates/gateway/tests/http_api.rs`) boot the real
`TorrentEngine` and router (DHT disabled, a temp cache dir) and exercise
routing, the Stremio protocol shapes, `/health`, and every input-validation
rejection path. The GUI's tests (`crates/gateway-gui/src/server_process.rs`)
spawn a real subprocess to verify start/stream-logs/SIGTERM-then-SIGKILL
stop logic. Neither suite completes a real BitTorrent download — that needs
live peers/trackers on the public network, which would make the suite flaky
and dependent on external state. The `/play` → download → stream path and
the GUI's start/stop cycle were both verified manually end-to-end (binary
boots, banner prints the real LAN IP, `/health`/`/manifest.json` respond
correctly, same checks repeated inside the built Docker image and inside the
installed `.deb`'s file layout, all running as expected).

## Troubleshooting

- **Nothing on the phone can reach the gateway**: check the PC's firewall.
  `setup.sh` opens ports `11470`/`8080` automatically if `ufw` is active; if
  you use a different firewall, allow inbound TCP on the gateway's port from
  your LAN subnet. Confirm both devices are actually on the same
  Wi-Fi/subnet (not one on a guest network).
- **`/play?magnet=...` hangs for a long time**: the gateway is waiting on
  DHT/trackers to find peers and fetch metadata for a poorly-seeded torrent.
  This is a property of the torrent itself, not the gateway.
- **Playback stutters**: check `/health` — if the active torrent's download
  speed is low, that's a peer/seed availability problem, not a gateway
  bottleneck (the gateway's own overhead is low single-digit % CPU / tens of
  MB RAM at idle, see `/health`'s `process_*` fields).
- **Docker: banner shows an address that doesn't work**: see the
  `--network host` note under [Installation](#installation) — a
  bridge-networked container prints its own internal IP, not your host's.
- **GUI says "binary not found"**: `streaming-gateway-gui` looks for
  `streaming-gateway` next to its own executable first, then on `$PATH`.
  Installing the `.deb` puts both in `/usr/bin/`, so this only happens with
  a manual/partial build — run `cargo build --release` (without `-p`) to
  build both.

---

<div align="center">
<sub>rust · tokio · axum · librqbit · egui — no Node, no Python, no OpenSSL</sub>
</div>
