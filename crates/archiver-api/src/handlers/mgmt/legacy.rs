//! Java archiver endpoints we do not implement, surfaced as `410 Gone`
//! with a clear reason. Listed here so a Java-aware client gets a
//! definitive answer rather than 404 confusion.
//!
//! These fall into two families:
//!
//! 1. Channel Archiver bridge — the Java archiver can ingest data from
//!    the legacy "Channel Archiver" daemon (predecessor to the Java
//!    archiver appliance). archiver-rs does not bundle that bridge;
//!    operators should use the Java archiver if they need it.
//!
//! 2. Workflow / state surfaces that don't apply — archive_pv in
//!    archiver-rs is synchronous (no "workflow thread" to restart),
//!    static-content sync isn't needed (we don't serve clustered
//!    static assets), `SkipAliasCheckAction` is unnecessary because
//!    add_alias enforces the constraint at insertion time.

use axum::http::StatusCode;
use axum::response::IntoResponse;

fn gone(reason: &'static str) -> impl IntoResponse {
    (
        StatusCode::GONE,
        axum::Json(serde_json::json!({
            "status": "gone",
            "reason": reason,
        })),
    )
}

pub async fn add_external_archiver_server() -> impl IntoResponse {
    gone("Channel Archiver bridge is not implemented in archiver-rs.")
}

pub async fn add_external_archiver_server_archives() -> impl IntoResponse {
    gone("Channel Archiver bridge is not implemented in archiver-rs.")
}

pub async fn remove_external_archiver_server() -> impl IntoResponse {
    gone("Channel Archiver bridge is not implemented in archiver-rs.")
}

pub async fn channel_archiver_list_view() -> impl IntoResponse {
    gone("Channel Archiver bridge is not implemented in archiver-rs.")
}

pub async fn import_channel_archiver_config() -> impl IntoResponse {
    gone("Channel Archiver bridge is not implemented in archiver-rs.")
}

pub async fn upload_channel_archiver_config() -> impl IntoResponse {
    gone("Channel Archiver bridge is not implemented in archiver-rs.")
}

pub async fn refresh_pv_data_from_channel_archivers() -> impl IntoResponse {
    gone("Channel Archiver bridge is not implemented in archiver-rs.")
}

pub async fn restart_archive_workflow_thread() -> impl IntoResponse {
    gone("archiver-rs has no per-PV workflow thread to restart.")
}

pub async fn skip_alias_check_action() -> impl IntoResponse {
    gone("add_alias validates target existence atomically; no skip option.")
}

pub async fn sync_static_content_headers_footers() -> impl IntoResponse {
    gone("archiver-rs serves static content directly; no sync needed.")
}
