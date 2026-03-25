use std::time::SystemTime;

use axum::extract::{OriginalUri, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use archiver_core::retrieval::query::{parse_post_processor, query_data};
use archiver_core::storage::traits::EventStream;
use archiver_core::types::{ArchiverSample, ArchiverValue};

use crate::AppState;

/// Try to proxy a data request to the correct cluster peer.
/// Returns Some(Response) if proxied, None if should handle locally.
async fn try_cluster_proxy(
    state: &AppState,
    pv_name: &str,
    path: &str,
    headers: &HeaderMap,
    uri: &axum::http::Uri,
) -> Option<Response> {
    // Don't proxy if already proxied (circular prevention).
    if headers.get("X-Archiver-Proxied").is_some() {
        return None;
    }

    // Don't proxy if no cluster configured.
    let cluster = state.cluster.as_ref()?;

    // Don't proxy if PV is local.
    if state.registry.get_pv(pv_name).ok().flatten().is_some() {
        return None;
    }

    // Resolve the peer that archives this PV.
    let peer_url = cluster.resolve_peer(pv_name).await?;
    let qs = uri.query().unwrap_or("");

    match cluster.proxy_retrieval(&peer_url, path, qs).await {
        Ok(resp) => Some(resp),
        Err(e) => {
            tracing::warn!(pv = pv_name, "Cluster proxy failed: {e}");
            None
        }
    }
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/retrieval/data/getData.json", get(get_data_json))
        .route("/retrieval/data/getData.csv", get(get_data_csv))
        .route("/retrieval/data/getData.raw", get(get_data_raw))
}

#[derive(Debug, Deserialize)]
struct GetDataParams {
    pv: String,
    from: Option<String>,
    to: Option<String>,
    limit: Option<usize>,
}

/// JSON response compatible with the Java archiver viewer tools.
#[derive(Serialize)]
struct JsonPvData {
    meta: JsonMeta,
    data: Vec<JsonSample>,
}

#[derive(Serialize)]
struct JsonMeta {
    name: String,
    #[serde(rename = "PREC")]
    prec: Option<String>,
    #[serde(rename = "EGU")]
    egu: Option<String>,
}

#[derive(Serialize)]
struct JsonSample {
    secs: i64,
    nanos: i32,
    val: serde_json::Value,
    severity: i32,
    status: i32,
}

fn parse_iso8601(s: &str) -> Option<SystemTime> {
    // Try RFC3339 first, then common ISO8601 variants.
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.into());
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Some(dt.and_utc().into());
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(dt.and_utc().into());
    }
    None
}

/// Parse PV specification: optionally "postprocessor(pvname)" or just "pvname".
fn parse_pv_spec(spec: &str) -> (String, Option<String>) {
    if let Some(paren_pos) = spec.find('(') {
        if spec.ends_with(')') {
            let pp = &spec[..paren_pos];
            let pv = &spec[paren_pos + 1..spec.len() - 1];
            return (pv.to_string(), Some(pp.to_string()));
        }
    }
    (spec.to_string(), None)
}

fn sample_to_json_value(value: &ArchiverValue) -> serde_json::Value {
    match value {
        ArchiverValue::ScalarString(v) => serde_json::Value::String(v.clone()),
        ArchiverValue::ScalarByte(v) => serde_json::json!(v),
        ArchiverValue::ScalarShort(v) => serde_json::json!(v),
        ArchiverValue::ScalarInt(v) => serde_json::json!(v),
        ArchiverValue::ScalarEnum(v) => serde_json::json!(v),
        ArchiverValue::ScalarFloat(v) => serde_json::json!(v),
        ArchiverValue::ScalarDouble(v) => serde_json::json!(v),
        ArchiverValue::VectorString(v) => serde_json::json!(v),
        ArchiverValue::VectorChar(v) => serde_json::json!(v),
        ArchiverValue::VectorShort(v) => serde_json::json!(v),
        ArchiverValue::VectorInt(v) => serde_json::json!(v),
        ArchiverValue::VectorEnum(v) => serde_json::json!(v),
        ArchiverValue::VectorFloat(v) => serde_json::json!(v),
        ArchiverValue::VectorDouble(v) => serde_json::json!(v),
        ArchiverValue::V4GenericBytes(v) => serde_json::json!(v),
    }
}

fn collect_samples(
    stream: &mut dyn EventStream,
    start: SystemTime,
    end: SystemTime,
    limit: Option<usize>,
) -> Vec<ArchiverSample> {
    let mut samples = Vec::new();
    while let Ok(Some(sample)) = stream.next_event() {
        if sample.timestamp > end {
            break; // Stream is time-ordered; no more valid samples.
        }
        if sample.timestamp >= start {
            samples.push(sample);
            if let Some(max) = limit {
                if samples.len() >= max {
                    break;
                }
            }
        }
    }
    samples
}

async fn get_data_json(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Query(params): Query<GetDataParams>,
) -> Response {
    let (pv_name, _) = parse_pv_spec(&params.pv);

    // Try cluster proxy first.
    if let Some(resp) = try_cluster_proxy(&state, &pv_name, "data/getData.json", &headers, &uri).await {
        return resp;
    }

    let now = SystemTime::now();
    let start = params
        .from
        .as_deref()
        .and_then(parse_iso8601)
        .unwrap_or(now - std::time::Duration::from_secs(3600));
    let end = params
        .to
        .as_deref()
        .and_then(parse_iso8601)
        .unwrap_or(now);

    let (pv_name, pp_spec) = parse_pv_spec(&params.pv);
    let post_processor = pp_spec.and_then(|s| parse_post_processor(&s));

    let mut stream = match query_data(state.storage.as_ref(), &pv_name, start, end, post_processor)
        .await
    {
        Ok(s) => s,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let samples = collect_samples(stream.as_mut(), start, end, params.limit);
    let data: Vec<JsonSample> = samples
        .iter()
        .map(|s| {
            let dt = DateTime::<Utc>::from(s.timestamp);
            JsonSample {
                secs: dt.timestamp(),
                nanos: dt.timestamp_subsec_nanos() as i32,
                val: sample_to_json_value(&s.value),
                severity: s.severity,
                status: s.status,
            }
        })
        .collect();

    let result = vec![JsonPvData {
        meta: JsonMeta {
            name: pv_name,
            prec: None,
            egu: None,
        },
        data,
    }];

    axum::Json(result).into_response()
}

async fn get_data_csv(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Query(params): Query<GetDataParams>,
) -> Response {
    let (pv_name_check, _) = parse_pv_spec(&params.pv);
    if let Some(resp) = try_cluster_proxy(&state, &pv_name_check, "data/getData.csv", &headers, &uri).await {
        return resp;
    }

    let now = SystemTime::now();
    let start = params
        .from
        .as_deref()
        .and_then(parse_iso8601)
        .unwrap_or(now - std::time::Duration::from_secs(3600));
    let end = params
        .to
        .as_deref()
        .and_then(parse_iso8601)
        .unwrap_or(now);

    let (pv_name, pp_spec) = parse_pv_spec(&params.pv);
    let post_processor = pp_spec.and_then(|s| parse_post_processor(&s));

    let mut stream = match query_data(state.storage.as_ref(), &pv_name, start, end, post_processor)
        .await
    {
        Ok(s) => s,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let samples = collect_samples(stream.as_mut(), start, end, params.limit);

    let mut csv = String::from("seconds,nanos,val,severity,status\n");
    for s in &samples {
        let dt = DateTime::<Utc>::from(s.timestamp);
        let val_str = match &s.value {
            ArchiverValue::ScalarDouble(v) => v.to_string(),
            ArchiverValue::ScalarFloat(v) => v.to_string(),
            ArchiverValue::ScalarInt(v) => v.to_string(),
            ArchiverValue::ScalarShort(v) => v.to_string(),
            ArchiverValue::ScalarEnum(v) => v.to_string(),
            ArchiverValue::ScalarString(v) => format!("\"{v}\""),
            other => format!("{other:?}"),
        };
        csv.push_str(&format!(
            "{},{},{},{},{}\n",
            dt.timestamp(),
            dt.timestamp_subsec_nanos(),
            val_str,
            s.severity,
            s.status,
        ));
    }

    (
        [(axum::http::header::CONTENT_TYPE, "text/csv")],
        csv,
    )
        .into_response()
}

async fn get_data_raw(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Query(params): Query<GetDataParams>,
) -> Response {
    let (pv_name_check, _) = parse_pv_spec(&params.pv);
    if let Some(resp) = try_cluster_proxy(&state, &pv_name_check, "data/getData.raw", &headers, &uri).await {
        return resp;
    }

    let now = SystemTime::now();
    let start = params
        .from
        .as_deref()
        .and_then(parse_iso8601)
        .unwrap_or(now - std::time::Duration::from_secs(3600));
    let end = params
        .to
        .as_deref()
        .and_then(parse_iso8601)
        .unwrap_or(now);

    let (pv_name, _) = parse_pv_spec(&params.pv);

    let streams = match state.storage.get_data(&pv_name, start, end).await {
        Ok(s) => s,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    // Raw PB format: header line + sample lines from each stream.
    let mut body = Vec::new();
    for mut stream in streams {
        // Write PayloadInfo header.
        let desc = stream.description().clone();
        let header = archiver_proto::epics_event::PayloadInfo {
            r#type: desc.db_type as i32,
            pvname: desc.pv_name.clone(),
            year: desc.year,
            element_count: desc.element_count,
            unused00: None,
            unused01: None,
            unused02: None,
            unused03: None,
            unused04: None,
            unused05: None,
            unused06: None,
            unused07: None,
            unused08: None,
            unused09: None,
            headers: desc
                .headers
                .iter()
                .map(|(n, v)| archiver_proto::epics_event::FieldValue {
                    name: n.clone(),
                    val: v.clone(),
                })
                .collect(),
        };
        use prost::Message;
        let header_bytes = header.encode_to_vec();
        let escaped_header = archiver_core::storage::plainpb::codec::escape(&header_bytes);
        body.extend_from_slice(&escaped_header);
        body.push(archiver_core::storage::plainpb::codec::NEWLINE);

        // Write samples.
        while let Ok(Some(sample)) = stream.next_event() {
            if sample.timestamp > end {
                break;
            }
            if sample.timestamp >= start {
                if let Ok(sample_bytes) =
                    archiver_core::storage::plainpb::writer::encode_sample(desc.db_type, &sample)
                {
                    let escaped =
                        archiver_core::storage::plainpb::codec::escape(&sample_bytes);
                    body.extend_from_slice(&escaped);
                    body.push(archiver_core::storage::plainpb::codec::NEWLINE);
                }
            }
        }
    }

    (
        [(
            axum::http::header::CONTENT_TYPE,
            "application/x-protobuf",
        )],
        body,
    )
        .into_response()
}
