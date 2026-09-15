# librqbit 9.0.1, patched

This is the published `librqbit` 9.0.1 crate, copied from crates.io, with one
bug fixed. The workspace uses it through `[patch.crates-io]` in the root
`Cargo.toml`. Upstream: <https://github.com/ikatson/rqbit> (Apache-2.0, see
`LICENSE`).

Removed from the published crate because nothing here builds them: `webui/`
(an npm project, only built under librqbit's `webui` feature), `examples/`
(and their `[[example]]` entries in `Cargo.toml`), `Cargo.toml.orig`,
`Cargo.lock` and `.cargo_vcs_info.json`. Everything else is byte for byte
the crate.

## The fix: a downloaded piece was requested again, forever

Changed files: `src/piece_tracker.rs`, `src/chunk_tracker.rs`,
`src/torrent_state/live/mod.rs`. Unfixed on upstream `main` as of
2026-09-15; the code in question dates from 8c84f549 (2026-01-18).

When a piece's last chunk arrives, librqbit takes the piece out of
`inflight` and then hash-checks it, and only after the check marks it `have`.
In between it is in neither set. `PieceTracker::acquire_piece` picks
streaming-priority pieces by "not `have` and not `inflight`", so a peer asking
for work during that window reserved the piece again and requested all of its
chunks. Each of them came back `PreviouslyCompleted` and was thrown away, and
because nothing takes a piece out of `inflight` on that path, the piece stayed
reserved with no requests outstanding. Every 10x a peer's average piece time
(about 6.5 s in the lab) another peer stole it, re-requested it, and the same
thing happened again, for as long as a player kept the piece in its 32 MB
window.

Only streams trigger it, since only the priority pass has the gap, which is
why a plain download was unaffected.

Measured 2026-09-15: three local seeders at 4 MiB/s, one reader seeking to
ten offsets, 80 s per run. Stock kept 98%, 84% and 69% of the bytes the
seeders sent (29, 253 and 497 steals). Patched kept 99.7%, 99.9% and 99.8%.
In the Stremio desktop app, a seek to 45% of Sintel went from 10-13 s to
7 s, and the four-seek run from 25-27 s to 19-21 s.

The change:

* `ChunkTracker::is_piece_awaiting_check`: every chunk is marked but the
  piece is not `have` yet.
* The priority pass in `PieceTracker::acquire_piece` skips such pieces.
  `test_priority_piece_awaiting_its_hash_check_is_not_reserved_again` fails
  without it.
* `PreviouslyCompleted` in `on_received_piece` releases the stale
  reservation it has just proved, so a piece orphaned any other way is not
  stolen back and forth either.

## Upgrading librqbit

Delete this directory and the `[patch.crates-io]` entry, bump the version in
`crates/gateway/Cargo.toml`, and check whether upstream has fixed the priority
pass. If not, re-apply the three changes above to the new version.
