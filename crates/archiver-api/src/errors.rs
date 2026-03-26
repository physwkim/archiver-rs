//! Centralized API error type.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("Bad request: {0}")]
    BadRequest(String),
    #[error("Unauthorized")]
    Unauthorized,
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Conflict: {0}")]
    Conflict(String),
    #[error("Too many requests")]
    TooManyRequests,
    /// 500 — logs the real error internally, returns a generic message to clients.
    #[error("Internal error: {0}")]
    Internal(String),
    /// 502 — logs the real error internally, returns a generic message to clients.
    #[error("Bad gateway: {0}")]
    BadGateway(String),
}

impl ApiError {
    pub fn internal(e: impl std::fmt::Display) -> Self {
        Self::Internal(e.to_string())
    }
    pub fn bad_gateway(e: impl std::fmt::Display) -> Self {
        Self::BadGateway(e.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg).into_response(),
            ApiError::Unauthorized => {
                (StatusCode::UNAUTHORIZED, "Invalid or missing API key").into_response()
            }
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg).into_response(),
            ApiError::Conflict(msg) => (StatusCode::CONFLICT, msg).into_response(),
            ApiError::TooManyRequests => {
                (StatusCode::TOO_MANY_REQUESTS, "Too many requests").into_response()
            }
            ApiError::Internal(msg) => {
                tracing::error!("Internal error: {msg}");
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error").into_response()
            }
            ApiError::BadGateway(msg) => {
                tracing::warn!("Gateway error: {msg}");
                (StatusCode::BAD_GATEWAY, "Upstream service unavailable").into_response()
            }
        }
    }
}
