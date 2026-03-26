//! Centralized API error type.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

#[derive(Debug)]
pub enum ApiError {
    BadRequest(String),
    Unauthorized,
    NotFound(String),
    Conflict(String),
    TooManyRequests,
    /// 500 — logs the real error internally, returns a generic message to clients.
    Internal(String),
    /// 502 — logs the real error internally, returns a generic message to clients.
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

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiError::BadRequest(msg) => write!(f, "Bad request: {msg}"),
            ApiError::Unauthorized => write!(f, "Unauthorized"),
            ApiError::NotFound(msg) => write!(f, "Not found: {msg}"),
            ApiError::Conflict(msg) => write!(f, "Conflict: {msg}"),
            ApiError::TooManyRequests => write!(f, "Too many requests"),
            ApiError::Internal(msg) => write!(f, "Internal error: {msg}"),
            ApiError::BadGateway(msg) => write!(f, "Bad gateway: {msg}"),
        }
    }
}
