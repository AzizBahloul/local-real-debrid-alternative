//! Turning a `Range` header into the exact bytes, status and headers of a
//! response. Pure, so every edge of RFC 7233 is testable without a torrent.

use axum::http::StatusCode;

/// What a request for a file of a given length should be answered with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangePlan {
    /// `200` for the whole file, `206` for a satisfiable range.
    pub status: StatusCode,
    /// First byte to send.
    pub start: u64,
    /// One past the last byte to send.
    pub end: u64,
    /// The `Content-Range` value for a `206`, `None` for a `200`.
    pub content_range: Option<String>,
}

impl RangePlan {
    pub fn len(&self) -> u64 {
        self.end - self.start
    }
}

/// The range could not be served at all: its first byte lies past the end of
/// the file. Answered with `416` and the real length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unsatisfiable;

/// Decides what to send for `range_header` against a file of `file_len`
/// bytes.
///
/// A last byte past the end of the file is *clamped*, not refused. RFC 7233
/// §2.1: "If the last-byte-pos value is absent, or if the value is greater
/// than or equal to the current length of the representation data, the byte
/// range is interpreted as the remainder of the representation." Players rely
/// on this -- asking for `bytes=0-99999999` on a file they do not know the
/// length of yet is ordinary -- and answering those with `416` turned a
/// perfectly playable file into an error.
///
/// Only a *first* byte past the end is unsatisfiable. A header that does not
/// parse is ignored and the whole file is served, as the RFC requires of a
/// `Range` a server does not understand.
pub fn plan_range(range_header: Option<&str>, file_len: u64) -> Result<RangePlan, Unsatisfiable> {
    let Some((start, end)) = range_header.and_then(|v| parse_range_header(v, file_len)) else {
        return Ok(RangePlan {
            status: StatusCode::OK,
            start: 0,
            end: file_len,
            content_range: None,
        });
    };

    if start >= file_len {
        return Err(Unsatisfiable);
    }
    let end = end.map_or(file_len, |end| end.min(file_len));
    Ok(RangePlan {
        status: StatusCode::PARTIAL_CONTENT,
        start,
        end,
        content_range: Some(format!("bytes {start}-{}/{file_len}", end - 1)),
    })
}

/// Parses a single-range `Range: bytes=start-end` header into `(start,
/// exclusive end)`. Multi-range requests (`bytes=0-10,20-30`) are not
/// supported (no player used in practice sends them for video) and fall back
/// to a full-content response, same as the header being absent.
///
/// Handles the RFC 7233 *suffix* form `bytes=-N` ("the last N bytes"), which
/// needs `file_len` to resolve into an absolute offset. That form is not
/// exotic: an MKV stores its Cues/SeekHead index at the *end* of the file, so
/// Android players routinely ask for the tail first to learn the duration
/// before playing a frame. Failing to parse it here did not produce an error
/// -- it looked identical to a request with no `Range` header at all, so the
/// gateway answered `200` and began streaming the whole file from byte 0. The
/// player then waited forever for an index that would only arrive gigabytes
/// later, which is exactly the "spins on 00:00 / 00:00 and never plays"
/// symptom. `TAIL_PROBE_ZONE` downstream exists to serve this read cheaply and
/// was unreachable while this returned `None`.
///
/// A last byte before the first (`bytes=500-100`) is syntactically invalid
/// under RFC 7233 §2.1, and an invalid `Range` is ignored rather than refused.
pub fn parse_range_header(value: &str, file_len: u64) -> Option<(u64, Option<u64>)> {
    let spec = value.strip_prefix("bytes=")?.trim();
    let (start, end) = spec.split_once('-')?;
    let (start, end) = (start.trim(), end.trim());

    if start.is_empty() {
        // Suffix form: `bytes=-N` is the last N bytes, always running to EOF.
        // A zero-length suffix is unsatisfiable rather than "the whole file",
        // and N larger than the file legitimately means the entire file.
        let n = end.parse::<u64>().ok()?;
        if n == 0 {
            return None;
        }
        return Some((file_len.saturating_sub(n), None));
    }

    let start = start.parse::<u64>().ok()?;
    if end.is_empty() {
        return Some((start, None));
    }
    let last = end.parse::<u64>().ok()?;
    if last < start {
        return None;
    }
    Some((start, Some(last.saturating_add(1))))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_LEN: u64 = 1_000_000;

    #[test]
    fn parses_open_ended_range() {
        assert_eq!(
            parse_range_header("bytes=100-", TEST_LEN),
            Some((100, None))
        );
    }

    #[test]
    fn parses_closed_range_as_exclusive_end() {
        assert_eq!(
            parse_range_header("bytes=0-99", TEST_LEN),
            Some((0, Some(100)))
        );
    }

    #[test]
    fn rejects_malformed_range_headers() {
        assert_eq!(parse_range_header("nonsense", TEST_LEN), None);
        assert_eq!(parse_range_header("bytes=abc-def", TEST_LEN), None);
    }

    /// The regression that made Android sit on `00:00 / 00:00` forever.
    ///
    /// An MKV keeps its Cues index at the end, so a player asks for the tail
    /// before it can report a duration. This form used to fail to parse, which
    /// was indistinguishable from "no Range header" -- the gateway answered
    /// `200` with the whole file from byte 0 and the player waited on an index
    /// that was gigabytes away. It must resolve to an absolute offset running
    /// to EOF, so the response is a `206` covering only the tail.
    #[test]
    fn parses_suffix_range_as_the_last_n_bytes() {
        assert_eq!(
            parse_range_header("bytes=-65536", TEST_LEN),
            Some((TEST_LEN - 65536, None))
        );
    }

    /// A suffix larger than the file is not an error -- RFC 7233 says it means
    /// the whole file. Clamping to 0 rather than underflowing is what keeps it
    /// from becoming a wild offset near u64::MAX.
    #[test]
    fn a_suffix_longer_than_the_file_starts_at_zero() {
        assert_eq!(
            parse_range_header("bytes=-99999999", TEST_LEN),
            Some((0, None))
        );
    }

    /// `bytes=-0` requests the last zero bytes, which is unsatisfiable. It must
    /// not fall through to "start at the end of the file", nor be mistaken for
    /// a request for the entire file.
    #[test]
    fn a_zero_length_suffix_is_rejected() {
        assert_eq!(parse_range_header("bytes=-0", TEST_LEN), None);
    }

    #[test]
    fn a_range_that_ends_before_it_starts_is_ignored() {
        assert_eq!(parse_range_header("bytes=500-100", TEST_LEN), None);
        assert_eq!(
            plan_range(Some("bytes=500-100"), TEST_LEN).unwrap().status,
            StatusCode::OK,
            "an invalid Range is ignored, not refused"
        );
    }

    #[test]
    fn no_range_serves_the_whole_file() {
        let plan = plan_range(None, TEST_LEN).unwrap();
        assert_eq!(plan.status, StatusCode::OK);
        assert_eq!((plan.start, plan.end), (0, TEST_LEN));
        assert_eq!(plan.content_range, None);
    }

    #[test]
    fn a_closed_range_is_served_exactly() {
        let plan = plan_range(Some("bytes=100-199"), TEST_LEN).unwrap();
        assert_eq!(plan.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!((plan.start, plan.end, plan.len()), (100, 200, 100));
        assert_eq!(plan.content_range.as_deref(), Some("bytes 100-199/1000000"));
    }

    /// The bug this function was extracted to fix: a last byte past EOF was a
    /// `416`, although the RFC defines it as "to the end of the file".
    #[test]
    fn a_last_byte_past_the_end_is_clamped_not_refused() {
        let plan = plan_range(Some("bytes=999990-5000000"), TEST_LEN).unwrap();
        assert_eq!(plan.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!((plan.start, plan.end), (999_990, TEST_LEN));
        assert_eq!(
            plan.content_range.as_deref(),
            Some("bytes 999990-999999/1000000")
        );
    }

    #[test]
    fn a_first_byte_past_the_end_is_unsatisfiable() {
        assert_eq!(
            plan_range(Some("bytes=1000000-"), TEST_LEN),
            Err(Unsatisfiable)
        );
        assert_eq!(
            plan_range(Some("bytes=2000000-2000010"), TEST_LEN),
            Err(Unsatisfiable)
        );
    }

    /// An empty file has no byte to start from, whatever the suffix says.
    #[test]
    fn nothing_in_an_empty_file_is_satisfiable() {
        assert_eq!(plan_range(Some("bytes=-10"), 0), Err(Unsatisfiable));
        assert_eq!(plan_range(None, 0).unwrap().len(), 0);
    }
}
