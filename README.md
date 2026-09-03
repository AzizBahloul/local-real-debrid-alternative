<div align="center">

# 🎬 Streaming Gateway

### Watch any movie on your phone, streamed from your own PC.

<p>
<img alt="Rust" src="https://img.shields.io/badge/Rust-000000?style=for-the-badge&logo=rust&logoColor=white">
<img alt="Linux" src="https://img.shields.io/badge/Linux-FCC624?style=for-the-badge&logo=linux&logoColor=black">
<img alt="Stremio" src="https://img.shields.io/badge/Stremio-8A5AAB?style=for-the-badge&logo=stremio&logoColor=white">
<img alt="License" src="https://img.shields.io/badge/License-MIT-green?style=for-the-badge">
</p>

</div>

---

## What is this?

You search for a movie in **Stremio** on your phone. It starts playing in a few
seconds. There is no subscription, no cloud service, and nothing is stored on
your phone — your own PC finds the movie, downloads only the part you are
currently watching, and streams it over your WiFi.

Think of it as a **personal Real-Debrid that runs on your own computer**.

**How it works, in one line:** your PC runs a small server → it searches for
torrents → it downloads only the piece you are watching → it serves that to
your phone as a normal video stream you can pause and seek.

### What you get

| | |
|---|---|
| 🔎 **Search built in** | Type a movie name in Stremio, get results. No magnet links to copy. |
| ⚡ **Starts in seconds** | Downloads the beginning first, not the whole file. |
| ⏩ **Seek anywhere** | Jump to any point; it fetches just that part. |
| 📱 **Any device** | Stremio, VLC, browsers, smart TVs — anything that plays a URL. |
| 🖥️ **One-click app** | Desktop app with a big Start button. No terminal needed. |
| 🔒 **Stays on your network** | Video never leaves your WiFi. |

---

# Setup guide

Follow these in order. Total time: about 10 minutes.

## Step 1 — Install on your PC

Open a terminal in this folder and run:

```bash
./setup.sh --gui
```

That one command does everything: installs what's missing, builds the app,
creates the installer package, installs it, and puts a **NovaStream** icon on
your desktop and in your applications menu.

> [!NOTE]
> It asks for your password once (to install the package). It never touches
> Node, Python, or OpenSSL — none are needed.

**Server only, no desktop app?** Use `./setup.sh` instead. Good for a headless
home server or NAS.

## Step 2 — Start the server

Click the **NovaStream** icon on your desktop, then click the big green
**START GATEWAY** button.

<table>
<tr>
<td><img src="docs/screenshots/stopped.png" width="360" alt="NovaStream, stopped"></td>
<td><img src="docs/screenshots/running.png" width="360" alt="NovaStream, running"></td>
</tr>
<tr>
<td align="center"><sub>Click to start</sub></td>
<td align="center"><sub>Running — shows your address</sub></td>
</tr>
</table>

The app shows an address like `http://192.168.1.67:11470`. **Write it down** —
you need it in step 4.

Prefer a terminal? `./target/release/streaming-gateway` does the same thing.

## Step 3 — Make it reachable from your phone

Stremio's Android app will not connect to a plain `http://192.168.x.x` address.
It requires `https`. So you need a free tunnel that gives your PC an https
address.

Install [ngrok](https://ngrok.com/download) (free account), then run:

```bash
ngrok http 11470
```

It prints a line like:

```
Forwarding  https://abc123.ngrok-free.app -> http://localhost:11470
```

**Copy that `https://...` address.** That is your addon address.

> [!IMPORTANT]
> Only tiny text data goes through this tunnel — the video itself streams
> directly over your WiFi. You will not burn through ngrok's free bandwidth
> limit by watching movies.

> [!WARNING]
> While the tunnel runs, anyone with that exact link can reach your gateway.
> The link is random and changes each restart. Stop ngrok (`Ctrl+C`) when done.

## Step 4 — Add it to Stremio on your phone

1. Install **Stremio** from the Play Store and sign in
2. Make sure your phone is on the **same WiFi** as your PC
3. Open **Addons** (the puzzle-piece icon at the bottom)
4. Tap the search bar at the top
5. Paste your address with `/manifest.json` on the end:

```
https://abc123.ngrok-free.app/manifest.json
```

6. Tap **Install**

You should now see **Local Streaming Gateway** in your installed addons.

## Step 5 — Watch something

1. Search for a movie in Stremio
2. Open it and look at the stream list
3. Pick an entry that says **Local Gateway**
4. Press play, then **wait about 10 seconds** for the first start

<div align="center">

### 🎉 That's it. You're watching.

</div>

---

## Picking a good stream

Each result shows two numbers that decide whether it plays smoothly:

```
Local Gateway 1080p
Movie.2026.1080p.WEB-DL.x265
👤 1463   💾 1.56 GB
   ▲         ▲
   │         └── file size — smaller starts faster
   └── seeders — more is better, this matters most
```

**Rules of thumb:**

- **Pick high 👤 seeders.** This is the single best predictor of smooth playback.
- **Prefer smaller files.** A 1.5 GB x265 release needs far less speed than a
  5 GB one and starts much sooner.
- **Avoid 4K / 2160p** unless your internet is fast. A 23 GB 4K movie needs
  roughly 3.6 MB/s sustained — more than most home connections deliver.

**Not sure if your connection can handle it?** Divide the file size (GB) by the
movie length (hours). Under 2 GB/hour is comfortable on most connections.

---

## Watching from a different WiFi

If your phone is on a **WiFi extender**, a guest network, or mobile data, it may
not be able to reach your PC's local address directly.

The gateway handles this automatically: each movie shows a second entry marked
**(remote)**. It plays through the tunnel instead of your local network, so it
works from anywhere — just slower, and it uses tunnel bandwidth.

**Use the normal entry when you can, the (remote) one when you must.**

---

## Watching on VLC, a browser, or a TV

You don't need Stremio. Any player that opens a URL works:

```
http://192.168.1.67:11470/play?magnet=<your-magnet-link>
```

- **VLC (phone):** ☰ menu → Stream → paste the URL
- **VLC (desktop):** Media → Open Network Stream → paste
- **Browser:** just open the URL
- **Smart TV:** any "play from URL" app, e.g. VLC for Android TV

---

## Settings

Everything has a sensible default. Change these only if you need to:

| Setting | Default | What it does |
|---|---|---|
| `GATEWAY_PORT` | `11470` | Port to listen on |
| `MAX_CACHE_SIZE_GB` | `20` | Disk limit before old movies are deleted |
| `IDLE_PAUSE_SECS` | `300` | Pause a movie you stopped watching, to free bandwidth |
| `PREBUFFER_BYTES` | `4 MB` | Data to gather before playback starts |
| `STALL_TIMEOUT_SECS` | `20` | If playback gets no data for this long, quietly reconnect |
| `INDEXER_URL` | Torrentio | Where movies are searched for |
| `DISABLE_INDEXER` | `false` | Turn off search entirely |
| `LOG_LEVEL` | `info` | Set to `debug` for troubleshooting |

Set them like this:

```bash
MAX_CACHE_SIZE_GB=100 ./target/release/streaming-gateway
```

> [!TIP]
> If you watch 4K, raise `MAX_CACHE_SIZE_GB` to at least **100**. A single 4K
> movie can be 23 GB — larger than the default cache limit, which would make it
> delete itself while you watch.

---

## Troubleshooting

<details>
<summary><b>Stremio won't install the addon / it just spins forever</b></summary>

You're using an `http://` address. Stremio's Android app requires `https`.
Go back to **Step 3** and use the ngrok address instead.
</details>

<details>
<summary><b>The addon installed, but no "Local Gateway" streams appear</b></summary>

Stremio caches the old addon. **Uninstall** it in the Addons list, then install
it again. Also check the addon's description says *"Streams movies and series…"*
— if it says *"Not a search/indexer"*, that's a stale cached copy.
</details>

<details>
<summary><b>It says "Switching to VLC/Exo" and never plays</b></summary>

Almost always means you pressed play too early. The torrent needs ~10 seconds to
find peers. Go back, wait, and press play again.

If it still fails, the release probably has too few seeders — pick one with a
higher 👤 number.
</details>

<details>
<summary><b>Playback keeps buffering</b></summary>

Check the file size against your connection. A 5 GB movie needs about 0.8 MB/s
sustained; a 23 GB 4K one needs 3.6 MB/s. **Pick a smaller release** — this
fixes it far more often than anything else.

Check what's using bandwidth:
```bash
curl -s http://localhost:11470/health | python3 -m json.tool
```
</details>

<details>
<summary><b>Downloads keep disappearing</b></summary>

Your cache limit is too small, so it deletes movies to stay under it. Raise it:
```bash
MAX_CACHE_SIZE_GB=100 ./target/release/streaming-gateway
```
</details>

<details>
<summary><b>Seeking takes a long time</b></summary>

Jumping to a part that hasn't downloaded yet means fetching it first — usually a
few seconds. Torrents transfer in chunks of 4–16 MB, so even one second of video
requires a whole chunk. Seeking backwards into what you've already watched is
instant.
</details>

<details>
<summary><b>My phone can't reach the PC at all</b></summary>

Use the **(remote)** stream entry — it works from any network. See
[Watching from a different WiFi](#watching-from-a-different-wifi).
</details>

---

## Is this legal?

This tool is a **BitTorrent client with a video player attached**. It does not
host, index, or provide any content — exactly like qBittorrent or Transmission.

**You are responsible for what you download.** Downloading copyrighted material
without permission is illegal in most countries. Use it for public-domain films,
Creative Commons media, Linux ISOs, or content you own.

---

<details>
<summary><h2 style="display:inline">For developers</h2></summary>

### Why Rust

Streaming is a latency problem, not a throughput one: a seek must translate into
"fetch this piece now" with no GC pause. Rust gives predictable latency, real
parallelism, and a single static binary with no runtime.

The torrent engine is [`librqbit`](https://github.com/ikatson/rqbit) — already
correct on the hard parts (streaming-aware piece selection, a reader that blocks
on a missing piece and wakes when it arrives). Reimplementing that would only
produce a worse copy. This project adds what it deliberately doesn't: a
Stremio-shaped API, LAN discovery, a size-capped cache, and monitoring.

### Layout

```
crates/
  gateway/              the server
    src/
      lib.rs              router, startup, shared state
      torrent/            librqbit wrapper, file selection, idle pausing
      streaming/          HTTP Range serving + pre-buffering
      stremio/            /manifest.json + /stream/{type}/{id}.json
      indexer/            torrent discovery by IMDB id
      cache/              size-capped eviction
      network/  config/  monitoring/  error.rs
    tests/http_api.rs     integration tests against the real router
  gateway-gui/          desktop launcher (eframe/egui)
setup.sh   Dockerfile
```

### Design notes

- **Manifest and video travel different paths.** Stremio's Android app refuses
  a plain-http addon manifest but its *player* accepts one, so JSON goes through
  the https tunnel (kilobytes) while video streams straight over the LAN
  (gigabytes). This is what keeps tunnel bandwidth irrelevant.
- **Stream URLs carry no query string.** Stremio hands URLs to external players
  via Android intents, where a long percent-encoded magnet is easy to mangle.
  `/videos/<hash>/<idx>` survives that.
- **`/videos` only starts hashes the gateway advertised.** Otherwise anyone who
  could reach the port could make it join arbitrary swarms.
- **Idle torrents pause; the janitor only evicts paused ones.** Request-recency
  alone is not a safe "in use" signal — players buffer minutes ahead and go
  quiet, so an actively watched movie looks idle and gets deleted mid-playback.
- **Responses are withheld until data exists.** Players treat "headers, then a
  stalled body" as a broken stream, but wait patiently on a slow request.

### Build & test

```bash
cargo build --release              # both crates
cargo test --release               # 52 tests
cargo clippy --release --all-targets -- -D warnings
cargo deb -p streaming-gateway-gui # .deb package
```

Docker (server only):
```bash
docker build -t streaming-gateway .
docker run -d --network host -v gw-data:/data streaming-gateway
```
Use `--network host`, or the startup banner prints the container's internal IP
instead of your LAN address.

### API

| Endpoint | Purpose |
|---|---|
| `GET /manifest.json` | Stremio addon manifest |
| `GET /stream/{type}/{id}.json` | Stream list for a title |
| `GET /videos/{hash}/{idx}` | The video itself (Range supported) |
| `GET /play?magnet=…` | Resolve a magnet and redirect to the video |
| `GET /health` | Status, active torrents, cache usage |

</details>

---

<div align="center">
<sub>MIT licensed · Built with Rust, axum, and librqbit</sub>
</div>
