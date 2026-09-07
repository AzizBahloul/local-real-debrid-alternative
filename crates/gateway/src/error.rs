//! A single JSON error shape used by every handler, so a client never has to
//! guess whether an error comes back as plain text, HTML, or JSON.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ApiErrorResponse {
    #[serde(skip)]
    pub status: StatusCode,
    pub error: String,
}

impl ApiErrorResponse {
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            error: msg.into(),
        }
    }

    pub fn not_found(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            error: msg.into(),
        }
    }

    pub fn range_not_satisfiable(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::RANGE_NOT_SATISFIABLE,
            error: msg.into(),
        }
    }

    /// The client asked for something else before this could be answered.
    ///
    /// 409 rather than a 5xx because nothing failed: the request was overtaken
    /// by a later one from the same client, which is a conflict between two of
    /// its own requests and not an error on either side. Players treat it as a
    /// non-retryable answer to a request they have already abandoned, which is
    /// exactly right — the response they are waiting for is the newer one.
    pub fn superseded(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            error: msg.into(),
        }
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error: msg.into(),
        }
    }
}

impl IntoResponse for ApiErrorResponse {
    fn into_response(self) -> Response {
        (self.status, Json(self)).into_response()
    }
}
