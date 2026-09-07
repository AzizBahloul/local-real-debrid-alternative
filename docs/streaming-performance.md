# Making playback start faster — and seeking cheaper

A grounded guide to where the seconds actually go in this gateway, written
against the real code paths (`crates/gateway/src/streaming/mod.rs`,
`crates/gateway/src/torrent/mod.rs`, `crates/gateway/src/config/mod.rs`).

Everything here is ordered by **payoff per unit of effort**. Part 1 costs
nothing and needs no rebuild. Part 4 is where code changes start.

> **Status (2026-09-07, second pass).** §4.1–§4.6 are implemented and the §6
> profile is the shipped default. The measurement §4.1 was built for has now
> happened, against a real swarm on this link, and it found two things that
> argument had missed:
>
> 1. **The tail warmer was excluded from mkv on a structural argument that the
>    log falsifies.** A 1.16 GB mkv start spent **12.2 s of its 20 s** on one
>    request for the last 21 KB of the file. Matroska's index is nominally at
>    the front; players probe the tail for the Cues regardless. mkv/webm are now
>    in `TAIL_INDEXED_EXTENSIONS`.
> 2. **The pre-buffer's own timeout path produced the failure the pre-buffer
>    exists to prevent.** On timeout it sent the response "with whatever
>    arrived" — and when that was *zero bytes*, the player got a `206` over an
>    empty body, read it as a broken stream, and stopped. It now answers `503`,
>    which a player retries.
>
> Both were found by reading `events.jsonl`, neither was visible from the code,
> and the second had a doc-comment predicting it verbatim
> (`audit::Event::Prebuffer`: *"a `timed_out` here with `bytes: 0` is a stream
> that was handed to the player empty"*). Two levers still ship **off** on
> purpose (§4.5, §4.6); the reasons are under each one.

---

## 0. First, the honest framing

> **A rewrite in Go or C++ would save you well under 1% of the start time.**

Every second you wait is a second spent *blocked on a network you don't
control*. The CPU work on the start path is: parse a bencode blob
(sub-millisecond), SHA-1 verify arriving pieces (hardware-accelerated, ~2–4 ms
per 4 MB), a TLS handshake, and memcpy from the torrent engine to a socket.
Call it 5–50 ms of compute inside a 4-second start.

The only defensible "rewrite" argument is swapping the **torrent engine**
(`librqbit` → `libtorrent-rasterbar`) for its more mature DHT bootstrap and
piece scheduling — and that is a library swap, not a language one, and it is
unmeasured. Do everything in this document first.

---

## 1. The four phases of a cold start

Pressing play on a title never seen before runs these in sequence. The budget
is documented at [`torrent/mod.rs:37-46`](../crates/gateway/src/torrent/mod.rs#L37-L46).

| # | Phase | Timeout | What it is actually doing |
|---|---|---|---|
| 1 | Index query (Torrentio) | 10 s | HTTP round-trip to a remote server you don't own |
| 2 | Metadata fetch | 25 s | DHT + tracker announce, then pull the file list from a peer |
| 3 | Initialize | 15 s | leave `Initializing`; hash-check whatever is already on disk |
| 4 | Pre-buffer | 15 s | **wait for peers to actually send bytes** |

Worst case 55 s, deliberately under the router's 60 s request timeout so a dead
torrent fails fast instead of 504-ing after a full minute.

**Phases 2 and 4 are where real time goes.** Phase 1 is usually ~300 ms. Phase
3 is instant on a cold torrent (nothing to check) and is only slow when
re-verifying a large partially-downloaded file.

### Where seeking fits

Seeking does **not** re-run phases 1–3. A seek is a fresh HTTP `Range` request
against the already-running `/videos/{hash}/{idx}` URL, which lands at
[`streaming/mod.rs:137`](../crates/gateway/src/streaming/mod.rs#L137) and pays
only **phase 4** (`open_stream_at` + `prebuffer`). That is the design working
as intended — but phase 4 on an un-downloaded region is the expensive phase, so
seeking is still slow, for the reason in the next section.

---

## 2. The hard floor no code change can remove

**BitTorrent transfers whole pieces, and a piece is unreadable until every byte
of it has arrived and its SHA-1 checks out.**

A typical 1080p release uses **1–16 MB pieces** (commonly 4–8 MB for a 2–8 GB
file). So when you seek to the middle of a video, the gateway cannot return a
single byte until the entire piece containing that offset is downloaded and
verified.

That gives you a floor:

```
minimum seek latency  ≈  piece_size / effective_download_speed
```

| Piece size | @ 1 MB/s (2.4 GHz wifi, busy) | @ 3.5 MB/s (measured here) | @ 12 MB/s (gigabit ethernet) |
|---|---|---|---|
| 2 MB | 2.0 s | 0.6 s | 0.2 s |
| 4 MB | 4.0 s | 1.1 s | 0.3 s |
| 8 MB | 8.0 s | 2.3 s | 0.7 s |
| 16 MB | 16.0 s | 4.6 s | 1.3 s |

Read that table as: **your seek latency is set by your link speed, not your
code.** Doubling throughput halves every number in it. No language, no
algorithm, and no config flag beats that arithmetic — the only escapes are
(a) go faster, or (b) already have the data.

### Why "skip forward a lot at the beginning" is the pathological case

It is the single most expensive thing you can do to a torrent:

1. Each skip lands in a piece that is almost certainly **not** downloaded.
2. It **throws away the read-ahead** the engine had queued for the old
   position — those in-flight piece requests become wasted bandwidth.
3. It re-points the piece-priority set
   ([`open_stream_at`, torrent/mod.rs:1195-1228](../crates/gateway/src/torrent/mod.rs#L1195-L1228))
   and re-runs peer discovery for the new region.
4. Each skip pays a **fresh pre-buffer wait** (up to 15 s, min 128 KB) before
   the player sees a byte.

Ten quick scrubs is ten cold starts, each cancelling the last one's work. The
engine never gets a run long enough to build up speed.

**Practical rule:** seek *once* to where you want to be and let it settle. Two
seeks 5 seconds apart cost far more than one seek and 10 seconds of patience.

---

## 3. Zero-code wins — do these first

All of these are environment variables or CLI flags. No rebuild.

> Most of this section is now the **shipped default** — the values below are
> what you get with no configuration at all. They are kept here with their
> reasoning because a default you cannot justify is a default nobody dares
> change. Where a default has moved, it says so.

### 3.1 The physical link (biggest single lever)

Your gateway host is on **2.4 GHz WiFi**. That band is shared, narrow, and
half-duplex, and every byte the torrent downloads *and* every byte you stream
to the phone crosses the same radio, competing with each other.

| Action | Realistic effect |
|---|---|
| Move the host to **ethernet** | Often 3–10× throughput; halves-or-better every number in the piece table |
| Move the host to **5 GHz** | 2–4× typical |
| Nothing else in this document | comes close to this |

Do this before you touch a single config value.

### 3.2 Cut first-byte latency

```bash
PREBUFFER_BYTES=1048576        # 1 MB — now the default, was 4 MB
```

This is a pure *latency* setting: the viewer waits for it before a single byte
goes out. The blocking floor is `PREBUFFER_MIN_BYTES = 128 KB`, now further
capped by the piece arithmetic in §4.2 — phase 1 of `prebuffer()` blocks for
that much, then phase 2 only tops up with data that is *already* on disk, which
is exactly what keeps a warm seek fast.

- Lower = faster visible start, slightly higher risk of a player seeing an
  early stall.
- **Do not set it to 0.** Serving headers over an empty body is what makes
  Stremio report a 00:00 duration or cycle through players — the comment at
  [`streaming/mod.rs:364-380`](../crates/gateway/src/streaming/mod.rs#L364-L380)
  is describing a bug that was actually observed.
- It costs nothing once cached — the read completes at memory speed.

### 3.3 Stop paying for peer rediscovery

```bash
IDLE_PAUSE_SECS=1800           # 30 min — now the default, was 300
# or 0 to never pause at all
```

Pausing **drops every peer connection**, and getting them back means a fresh
DHT/tracker round. Your own config comment
([`config/mod.rs:210-222`](../crates/gateway/src/config/mod.rs#L210-L222)) calls
this "most of the wait when a title is slow to start."

If you routinely pause a film and come back 20 minutes later, the 300 s default
is guaranteeing you a cold restart. Raising it trades idle upstream bandwidth
for a warm resume — worth it on a machine that isn't running other torrents.

### 3.4 Keep watched data on disk

```bash
MAX_CACHE_SIZE_GB=100          # now the default, was 20
```

Every GB evicted is a region that becomes slow to seek back into. 20 GB is
about 4–8 films; going back to something you watched last week means
re-downloading it. Disk is the cheapest speed you can buy here.

> Remember the CWD gotcha: the installed `.deb` binary's `./cache` is relative
> to wherever it was launched. Check `GET /health` for the real path before
> assuming the repo's `cache/` is the one in use.

### 3.5 Make sure inbound peers can reach you

```bash
PEER_PORT=6881                 # default
DISABLE_UPNP=false             # default — leave UPnP on
```

Without a reachable listening socket the gateway can only **dial out**, and
every seeder that would have connected to *you* after your tracker announce is
lost. The symptom is exactly what it sounds like: a handful of peers and a
first play that crawls
([`torrent/mod.rs:509-522`](../crates/gateway/src/torrent/mod.rs#L509-L522)).

**Verify it rather than assuming.** If your router has UPnP disabled (many do,
for good reason), forward TCP 6881 → this host manually. This is frequently the
difference between 8 peers and 40, and it costs nothing.

### 3.6 Stop seeding from stealing your airtime

```bash
MAX_UPLOAD_MB_S=2              # now the default, was 0 (unlimited)
MAX_UPLOAD_MB_S=0              # set this back on ethernet
```

Upload competes with the video for the same radio. Don't go lower than ~1–2
MB/s — peers reciprocate, so throttling upload hard also throttles your
download ([`config/mod.rs`](../crates/gateway/src/config/mod.rs)). The default
now assumes the expected deployment (a laptop on wifi); on ethernet there is no
shared radio to protect and `0` is the right value.

### 3.7 Pick better torrents (free, and it matters more than most code)

The indexer already sorts by **seeders descending, then size ascending**
([`indexer/mod.rs:171-181`](../crates/gateway/src/indexer/mod.rs#L171-L181)) —
so the top row is usually the right one. Two habits on top of that:

- **A release with 200 seeders starts faster than one with 8, always.** No
  setting compensates for a dead swarm.
- **Smaller file = smaller pieces = shorter seeks.** A 2 GB 1080p encode seeks
  measurably faster than an 18 GB remux, and on 2.4 GHz wifi the remux may not
  sustain playback at all.

### 3.8 Prefer MKV over MP4 when you have the choice

A non-faststart MP4 keeps its `moov` atom (the index) at the **end of the
file**. The player therefore cannot start until it has range-requested the
*tail* — which means downloading a whole piece from the far end of the torrent,
at a position with no peers primed for it, **before** it even requests byte 0.
That is a second full cold-start penalty hiding inside what looks like one.

MKV carries its index at the front. If two releases are otherwise equal, the
MKV starts faster. (This is also a good candidate for a code fix — see §4.3.)

### 3.9 Try the browse prefetch — but measure it

```bash
BROWSE_PREFETCH_COUNT=1
```

Off by default from experience, not caution — the long comment at
[`config/mod.rs:127-147`](../crates/gateway/src/config/mod.rs#L127-L147)
documents three separate ways it made things *worse* (it diluted piece priority
away from the stream being watched, parking it instead cost a full reconnect,
and warming >1 candidate split DHT capacity). It measured 3/3 cold starts under
2 ms in isolation, so it is worth one honest re-test at `1` on your current
setup — just be ready to turn it back off.

---

## 4. Code changes, ranked

### 4.1 Instrument the four phases — **done**

Every phase now records where its time went, to two places:

- **`GET /health` → `recent_starts`** — the last 24 measured waits, oldest
  first. Each is either a `cold_start` (with `metadata_ms` / `initialize_ms` /
  `total_ms` / `outcome`) or a `prebuffer` (with `ms`, `bytes`, `warm`,
  `piece_remainder`, and whether it timed out).
- **The audit log** (`GET /audit/export`, loopback only) — the same events,
  permanently, as JSONL. `grep '"event":"cold_start"'` is a complete query.

Two fields are worth knowing about:

- **`warm`** — whether the pre-buffer was served from data already on disk. It
  is inferred from the wait, and the inference only runs one way: librqbit's
  reader cannot return a byte that is not already local, so a full-size fill in
  under 50 ms could only have come from disk. A warm read that happened to be
  slow is recorded as cold, which errs toward flagging a wait rather than
  explaining it away.
- **`piece_remainder`** — how many bytes stood between the requested offset and
  the end of its piece. This is the floor from §2, measured rather than
  estimated: a `prebuffer` whose `ms` matches `piece_remainder / link speed` is
  the arithmetic working, not a defect. It is the single field that separates
  "this gateway is slow" from "this release has 8 MB pieces", which look
  identical from a stopwatch.

### 4.2 Make the pre-buffer adaptive instead of fixed — **done**

The blocking part of the pre-buffer is now capped at **what remains of the piece
the offset lands in** (`piece_aware_floor`, `streaming/mod.rs`). Nothing at that
offset can be read until its whole piece arrives, so anything within the
remainder is free — it comes with the piece already being waited for. One byte
past it means waiting for a *second* piece, which on an 8 MB-piece release
doubles the wait for bytes the player has not asked for.

A floor of 64 KB overrides the arithmetic when an offset lands almost exactly on
a boundary: crossing it is then unavoidable, and a response with headers and 200
bytes of body is the worse failure (it is the 00:00-duration bug from §3.2).

Honest bound on the payoff: at 4 MB pieces, a random offset has roughly a 1.5%
chance of falling in the window where this changes anything. The measurement it
enables (`piece_remainder` above) is the larger deliverable.

### 4.3 Handle the MP4 tail request explicitly — **done**

Both halves:

- A range starting within 24 MB of EOF is classified as an **index probe**
  (`TAIL_PROBE_ZONE`) and gets a 256 KB pre-buffer instead of the full 1 MB. The
  player wants an index, not a head start.
- When a playback stream opens on a `.mp4` / `.m4v` / `.mov`, a detached
  **tail warmer** (`warm_mp4_tail`) opens a short-lived read at the end of the
  file, putting the last piece into the priority set alongside the first. The
  player's own tail request then lands on data that is already here instead of
  paying a second cold start for it. Once per file, best-effort, and nothing
  waits on it. mkv/webm are skipped — their index is at the front.

Cost when it guesses wrong (the mp4 was faststart after all): one piece of
bandwidth, once. Turn it off with `MP4_TAIL_WARM=false`.

### 4.4 Debounce rapid seeks — **done**

This is the one that addresses "skip forward a lot at the beginning".

The mechanism matters: librqbit interleaves piece requests **round-robin across
every open stream** (`TorrentStreams::iter_next_pieces`). An abandoned scrub is
therefore not merely idle — it keeps taking its share of the request slots for a
position nobody is watching. Ten scrubs leave the position the viewer finally
landed on receiving a *tenth* of the download.

So a new range request now retires the reads this client has already given up
on (`register_reader`). Cancellation reaches both the pre-buffer wait and the
response body, so a retired read releases its claim in milliseconds rather than
after the 15 s pre-buffer or the 20 s stall timeout. The retired request answers
`409 Conflict`.

What gets retired is deliberately narrow, and each clause is load-bearing:

| Rule | What breaks without it |
|---|---|
| Only reads that have served **zero bytes** | cancelling a read that is producing output stops playback dead |
| Only from the **same client address** | two devices on one title cancel each other's cold start, then each other's retry, and neither ever plays |
| Never an **index probe** | an mp4's `moov` read is a second read the same player needs *concurrently*; killing it breaks exactly the players §4.3 helps |
| Only the **same file index** | one player legitimately reads an episode and its subtitles at once |

Set `SEEK_SUPERSEDE=false` to disable.

### 4.5 Widen read-ahead after a settled seek — **implemented, off by default**

librqbit's per-stream look-ahead is a fixed 32 MB (`PER_STREAM_BUF_DEFAULT`) and
is not configurable from outside the library. The only lever is a **second read
parked further ahead**, which is what `READAHEAD_EXTRA_MB` does: after
`READAHEAD_SETTLE_SECS` of uninterrupted playback, a claim is opened at
`position + 32 MB + extra` and re-pointed as playback advances. Dropping the
response drops the claim.

**It ships off, and that is a measured trade rather than caution.** Because the
scheduler interleaves streams round-robin, a second window does not add
capacity — it *splits* the existing capacity between the bytes needed in ten
seconds and the bytes needed in two minutes. On a swarm comfortably outrunning
playback that is free insurance against a stall; on one that is barely keeping
up it is actively harmful. Measure before turning it on.

### 4.6 Raise the peer cap for cold start — **implemented, off by default**

`COLD_START_PEER_LIMIT` sets a per-torrent `peer_limit` at add time, overriding
the session-wide `MAX_PEERS_PER_TORRENT=60`.

The "…for the first 30 seconds only" half of this idea **cannot be built on
librqbit 9**: `peer_limit` is read once, when the torrent is added, and stored
in an immutable `ManagedTorrentShared`. There is no way to walk it back down
once the stream is flowing. So a raised limit lasts that torrent's whole life,
which is exactly the steady-state NAT-churn cost the low default exists to
avoid — hence off by default. Try `100` and measure whether it moves anything on
your link before leaving it on.

---

## 5. What will *not* help

Listed because each of these is a tempting dead end:

| Idea | Why it doesn't pay |
|---|---|
| **Rewriting in Go / C++** | <1% of start time is CPU. Go would be marginally worse (GC on a byte-shuffling workload); C++ identical. |
| **More threads / more async** | The path is already fully async and idle-waiting, not CPU-saturated. |
| **A bigger `PREBUFFER_BYTES`** | Pure added latency at the start. It is a floor against empty responses, not a playback buffer — the player does its own. |
| **Setting `PREBUFFER_BYTES=0`** | Reintroduces the 00:00-duration / player-cycling bug the pre-buffer exists to fix. |
| **Removing the pre-buffer's phase-2 top-up** | It already only takes *already-downloaded* bytes and returns in 50 ms. It is not costing you anything. |
| **Cranking `MAX_PEERS_PER_TORRENT` to 128+ permanently** | Measured to buy NAT churn and wifi airtime, not throughput. See §4.6 for the nuanced version. |
| **Lowering `STALL_TIMEOUT_SECS` below ~15** | Must stay above the time to fetch one piece, or healthy slow downloads get re-opened needlessly and you make stalls *more* frequent. |
| **Disabling DHT to "reduce overhead"** | Trackers alone find fewer peers; fewer peers is slower, full stop. |

---

## 6. The shipped profile, and what is left to turn on

**This is now the default** — running the gateway with an empty environment
gives you all of it. It biases hard toward fast starts and cheap seeks, at the
cost of disk and idle bandwidth:

```bash
# --- latency ---
PREBUFFER_BYTES=1048576        # 1 MB — visible start, down from 4 MB
PREBUFFER_TIMEOUT_SECS=15      # unchanged; the cold-start budget depends on it

# --- keep swarms and data warm ---
IDLE_PAUSE_SECS=1800           # don't drop peers on a 5-minute pause
MAX_CACHE_SIZE_GB=100          # don't evict what you might seek back into

# --- reachability ---
PEER_PORT=6881                 # forward this TCP port on the router if UPnP is off
DISABLE_UPNP=false

# --- wifi: stop seeding stealing the radio (set 0 on ethernet) ---
MAX_UPLOAD_MB_S=2

# --- the §4 behaviours ---
SEEK_SUPERSEDE=true            # retire scrubs the player abandoned (§4.4)
MP4_TAIL_WARM=true             # fetch an mp4's index in parallel (§4.3)
```

Deliberately **off**, because each one is a trade this link has not been
measured on:

```bash
READAHEAD_EXTRA_MB=0           # >0 splits piece priority rather than adding it (§4.5)
READAHEAD_SETTLE_SECS=10       # only matters when the above is non-zero
COLD_START_PEER_LIMIT=0        # >0 lasts the torrent's whole life, not 30s (§4.6)
BROWSE_PREFETCH_COUNT=0        # made real playback worse three ways (§3.9)
```

Then, in order:

1. **Move the host to ethernet** if at all possible. Re-measure. This will
   dominate everything above, and every number in the §2 table.
2. Use §4.1 — `GET /health` → `recent_starts` — to see which phase your slow
   starts actually are, and compare `prebuffer.ms` against `piece_remainder`.
   If they match the §2 arithmetic, the link is the answer and no further code
   change will help.
3. Only then decide whether the four off-by-default levers above are worth
   enabling, one at a time, measuring each.

---

## 7. How to actually measure

- `GET /health` on the running port — live sanity check, real cache path,
  active torrents, and (since §4.1) `recent_starts`: the last 24 measured
  waits with their phase breakdown. Start here.
  ```bash
  curl -s http://<host>:8080/health | jq '.recent_starts'
  ```
- `GET /audit/export` (**loopback only** — it records client IPs and titles) for
  the same events permanently, as JSONL:
  ```bash
  curl -s http://127.0.0.1:8080/audit/export | grep '"event":"cold_start"'
  ```
- Time a cold start with `curl` rather than the player, so you measure the
  gateway and not Stremio's own buffering:
  ```bash
  curl -o /dev/null -s -w 'ttfb=%{time_starttransfer}s total=%{time_total}s\n' \
    -r 0-1048576 'http://<host>:8080/videos/<hash>/<idx>'
  ```
- Time a **mid-video seek** the same way, with a byte offset well past anything
  downloaded:
  ```bash
  curl -o /dev/null -s -w 'ttfb=%{time_starttransfer}s\n' \
    -r 2000000000-2001048576 'http://<host>:8080/videos/<hash>/<idx>'
  ```
  Compare that number against the piece table in §2. If it matches the row for
  your link speed, **the gateway is not the problem — the link is**, and no
  code change will help.
- Always test the same info hash twice: once cold, once warm. The gap between
  them is the entire subject of this document.
