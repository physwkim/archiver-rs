//! Management UI — serves the static HTML management console.

use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;

use crate::AppState;

const MGMT_HTML: &str = include_str!("../static/mgmt.html");

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/mgmt/ui/index.html", get(serve_mgmt_ui))
        .route("/mgmt/ui/", get(serve_mgmt_ui))
        .route("/mgmt/ui", get(serve_mgmt_ui))
}

async fn serve_mgmt_ui() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        MGMT_HTML,
    )
        .into_response()
}
