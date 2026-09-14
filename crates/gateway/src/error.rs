//! A single JSON error shape used by every handler, so a client never has to
//! guess whether an error comes back as plain text, HTML, or JSON.

use axum::http::header::{HeaderName, HeaderValue, RETRY_AFTER};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ApiErrorResponse {
    #[serde(skip)]
    pub status: StatusCode,
    pub error: String,
    /// Headers the status needs to be meaningful: `Retry-After` on a 503,
    /// `Content-Range` on a 416. A short list rather than a `HeaderMap`,
    /// which is several times the size and would make every
    /// `Result<_, ApiErrorResponse>` in the crate that much larger to move.
    #[serde(skip)]
    headers: Vec<(HeaderName, HeaderValue)>,
}

impl ApiErrorResponse {
    fn new(status: StatusCode, msg: impl Into<String>) -> Self {
        Self {
            status,
            error: msg.into(),
            headers: Vec::new(),
        }
    }

    /// Adds a response header to the error.
    pub fn with_header(mut self, name: HeaderName, value: HeaderValue) -> Self {
        self.headers.push((name, value));
        self
    }

    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, msg)
    }

    /// The route exists, but not for this caller. Used by the loopback-only
    /// routes, which destroy data or expose the audit log.
    pub fn forbidden(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, msg)
    }

    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, msg)
    }

    /// RFC 7233 asks for `Content-Range: bytes */<length>` alongside a 416, so
    /// a client that guessed an offset learns the real length in the same
    /// round-trip instead of issuing a second request to find out.
    pub fn range_not_satisfiable(msg: impl Into<String>, file_len: u64) -> Self {
        Self::new(StatusCode::RANGE_NOT_SATISFIABLE, msg).with_header(
            axum::http::header::CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes */{file_len}")).expect("ascii digits only"),
        )
    }

    /// The client asked for something else before this could be answered.
    ///
    /// 409 rather than a 5xx because nothing failed: the request was overtaken
    /// by a later one from the same client, which is a conflict between two of
    /// its own requests and not an error on either side. Players treat it as a
    /// non-retryable answer to a request they have already abandoned, which is
    /// exactly right — the response they are waiting for is the newer one.
    pub fn superseded(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, msg)
    }

    /// The swarm has not produced a single byte of this range yet.
    ///
    /// 503 rather than a 206 over an empty body, which is what this replaced.
    /// A player given headers and no data concludes the stream is broken and
    /// stops — the exact failure the pre-buffer exists to prevent, reached by
    /// the pre-buffer's own timeout path. A 503 is a *retryable* answer: the
    /// player asks again, and the ask is cheap because the attempt that just
    /// timed out already re-pointed piece priority at this offset. Waiting
    /// longer instead would hold that claim while the viewer watches nothing.
    ///
    /// `Retry-After: 1` says so explicitly. Without it a client has to guess
    /// its own back-off, and the ones that guess long turn a seek that was a
    /// second from ready into a visible stall.
    pub fn not_ready(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, msg)
            .with_header(RETRY_AFTER, HeaderValue::from_static("1"))
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, msg)
    }
}

impl IntoResponse for ApiErrorResponse {
    fn into_response(mut self) -> Response {
        let headers = std::mem::take(&mut self.headers);
        let mut response = (self.status, Json(self)).into_response();
        for (name, value) in headers {
            response.headers_mut().insert(name, value);
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_not_ready_answer_tells_the_player_when_to_ask_again() {
        let response = ApiErrorResponse::not_ready("x").into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[RETRY_AFTER], "1");
    }

    #[test]
    fn an_unsatisfiable_range_reports_the_real_length() {
        let response = ApiErrorResponse::range_not_satisfiable("x", 1234).into_response();
        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(
            response.headers()[axum::http::header::CONTENT_RANGE],
            "bytes */1234"
        );
    }

    #[test]
    fn extra_headers_do_not_leak_into_the_json_body() {
        let body = serde_json::to_value(ApiErrorResponse::not_ready("wait")).unwrap();
        assert_eq!(body, serde_json::json!({ "error": "wait" }));
    }
}
