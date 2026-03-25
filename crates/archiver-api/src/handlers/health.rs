use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::AppState;

pub async fn health() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

pub async fn metrics_endpoint(State(state): State<AppState>) -> Response {
    match &state.metrics_handle {
        Some(handle) => {
            let output = handle.render();
            (StatusCode::OK, [("content-type", "text/plain; version=0.0.4")], output).into_response()
        }
        None => (StatusCode::NOT_FOUND, "Metrics not enabled").into_response(),
    }
}
