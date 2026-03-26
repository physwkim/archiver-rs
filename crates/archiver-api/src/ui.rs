//! Management UI — serves the static HTML management console and data viewer.

use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;

use crate::AppState;

const MGMT_HTML: &str = include_str!("../static/mgmt.html");
const PVDATA_HTML: &str = include_str!("../static/pvdata.html");
const UPLOT_JS: &str = include_str!("../static/vendor/uplot.min.js");
const UPLOT_CSS: &str = include_str!("../static/vendor/uplot.min.css");

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/mgmt/ui/index.html", get(serve_mgmt_ui))
        .route("/mgmt/ui/", get(serve_mgmt_ui))
        .route("/mgmt/ui", get(serve_mgmt_ui))
        .route("/mgmt/ui/pvdata.html", get(serve_pvdata))
        .route("/mgmt/ui/vendor/uplot.min.js", get(serve_uplot_js))
        .route("/mgmt/ui/vendor/uplot.min.css", get(serve_uplot_css))
}

async fn serve_mgmt_ui() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        MGMT_HTML,
    )
        .into_response()
}

async fn serve_pvdata() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        PVDATA_HTML,
    )
        .into_response()
}

async fn serve_uplot_js() -> Response {
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/javascript; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
        ],
        UPLOT_JS,
    )
        .into_response()
}

async fn serve_uplot_css() -> Response {
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/css; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
        ],
        UPLOT_CSS,
    )
        .into_response()
}
