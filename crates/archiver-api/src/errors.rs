//! Error response helpers that hide internal details.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

/// Return a generic 500 response, logging the real error.
pub(crate) fn internal_error(e: impl std::fmt::Display) -> Response {
    tracing::error!("Internal error: {e}");
    (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error").into_response()
}

/// Return a generic 502 response, logging the real error.
pub(crate) fn bad_gateway(e: impl std::fmt::Display) -> Response {
    tracing::warn!("Gateway error: {e}");
    (StatusCode::BAD_GATEWAY, "Upstream service unavailable").into_response()
}
