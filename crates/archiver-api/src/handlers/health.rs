use axum::extract::State;
use axum::response::{IntoResponse, Response};

use crate::errors::ApiError;
use crate::AppState;

pub async fn health() -> impl IntoResponse {
    "OK"
}

pub async fn metrics_endpoint(
    State(state): State<AppState>,
) -> Result<Response, ApiError> {
    match &state.metrics_handle {
        Some(handle) => {
            let output = handle.render();
            Ok((
                axum::http::StatusCode::OK,
                [("content-type", "text/plain; version=0.0.4")],
                output,
            )
                .into_response())
        }
        None => Err(ApiError::NotFound("Metrics not enabled".to_string())),
    }
}
