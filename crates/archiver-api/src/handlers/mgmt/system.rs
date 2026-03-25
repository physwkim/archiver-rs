use axum::response::{IntoResponse, Response};

pub async fn get_versions() -> Response {
    let resp = serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "status": "running",
        "archiver": "beamalignment"
    });
    axum::Json(resp).into_response()
}
