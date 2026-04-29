use std::time::SystemTime;

use axum::extract::{OriginalUri, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio_stream::wrappers::ReceiverStream;

use archiver_core::retrieval::query::{parse_post_processor, TwoWeekRawProcessor};
use archiver_core::storage::traits::EventStream;
use archiver_core::types::{ArchiverSample, ArchiverValue};

use crate::errors::ApiError;
use crate::AppState;

/// Default time range when no `from` parameter is specified (1 hour).
const DEFAULT_TIME_RANGE_SECS: u64 = 3600;
/// Maximum number of samples a single retrieval request can return.
const MAX_RETRIEVAL_LIMIT: usize = 1_000_000;

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
    if state.pv_query.get_pv(pv_name).ok().flatten().is_some() {
        return None;
    }

    // Resolve the peer that archives this PV.
    let resolved = cluster.resolve_peer(pv_name).await?;
    let qs = uri.query().unwrap_or("");

    match cluster.proxy_retrieval(&resolved.retrieval_url, path, qs).await {
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
        .route(
            "/retrieval/data/getDataAtTime",
            axum::routing::post(get_data_at_time),
        )
}

/// `POST /retrieval/data/getDataAtTime?at=<iso8601>`
/// Body: JSON list of PV names. Returns the most-recent sample at-or-
/// before `at` for each PV, keyed by canonical name. Pulls from
/// `storage.get_last_known_event` and falls back to a bounded scan
/// when the latest is newer than `at`. Mirrors the Java archiver's
/// `/retrieval/data/getDataAtTime` REST entry that Phoebus uses to
/// seed gauge values.
#[derive(Deserialize)]
struct GetDataAtTimeParams {
    at: Option<String>,
}

async fn get_data_at_time(
    State(state): State<AppState>,
    axum::extract::Query(p): axum::extract::Query<GetDataAtTimeParams>,
    crate::pv_input::PvListInput(pvs): crate::pv_input::PvListInput,
) -> Response {
    let target = p
        .at
        .as_deref()
        .and_then(parse_iso8601)
        .unwrap_or_else(SystemTime::now);

    let mut out = serde_json::Map::new();
    for pv in pvs {
        let canonical = state
            .pv_query
            .canonical_name(&pv)
            .unwrap_or_else(|_| pv.clone());

        // Stage 1: cheapest path — last known event across tiers.
        let latest = match state.storage.get_last_known_event(&canonical).await {
            Ok(opt) => opt,
            Err(e) => {
                tracing::warn!(pv, "get_last_known_event failed: {e}");
                None
            }
        };
        let pick = match latest {
            Some(l) if l.timestamp <= target => Some(l),
            _ => {
                // Stage 2: target is in the past relative to the newest
                // sample. Scan a 30-day window ending at target.
                let lower = target
                    .checked_sub(std::time::Duration::from_secs(30 * 86_400))
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                match state.storage.get_data(&canonical, lower, target).await {
                    Ok(streams) => {
                        let mut last = None;
                        for mut s in streams {
                            while let Ok(Some(sample)) = s.next_event() {
                                if sample.timestamp <= target {
                                    last = Some(sample);
                                }
                            }
                        }
                        last
                    }
                    Err(_) => None,
                }
            }
        };

        let entry = match pick {
            Some(s) => {
                let (year, secs, nanos) = s.decompose_timestamp();
                let val = sample_value_to_json(&s.value);
                serde_json::json!({
                    "secs": SystemTime::from(
                        chrono::DateTime::<chrono::Utc>::from(s.timestamp)
                    )
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                    "nanos": nanos,
                    "year": year,
                    "secondsIntoYear": secs,
                    "val": val,
                    "severity": s.severity,
                    "status": s.status,
                })
            }
            None => serde_json::Value::Null,
        };
        out.insert(canonical, entry);
    }

    axum::Json(serde_json::Value::Object(out)).into_response()
}

fn sample_value_to_json(v: &ArchiverValue) -> serde_json::Value {
    use serde_json::Value;
    match v {
        ArchiverValue::ScalarString(s) => Value::String(s.clone()),
        ArchiverValue::ScalarShort(n) => (*n).into(),
        ArchiverValue::ScalarInt(n) => (*n).into(),
        ArchiverValue::ScalarEnum(n) => (*n).into(),
        ArchiverValue::ScalarFloat(f) => (*f as f64).into(),
        ArchiverValue::ScalarDouble(f) => (*f).into(),
        ArchiverValue::ScalarByte(b) => Value::Array(b.iter().map(|x| (*x).into()).collect()),
        ArchiverValue::VectorString(arr) => {
            Value::Array(arr.iter().map(|s| Value::String(s.clone())).collect())
        }
        ArchiverValue::VectorChar(arr) => Value::Array(arr.iter().map(|x| (*x).into()).collect()),
        ArchiverValue::VectorShort(arr) => Value::Array(arr.iter().map(|x| (*x).into()).collect()),
        ArchiverValue::VectorInt(arr) => Value::Array(arr.iter().map(|x| (*x).into()).collect()),
        ArchiverValue::VectorEnum(arr) => Value::Array(arr.iter().map(|x| (*x).into()).collect()),
        ArchiverValue::VectorFloat(arr) => {
            Value::Array(arr.iter().map(|x| (*x as f64).into()).collect())
        }
        ArchiverValue::VectorDouble(arr) => Value::Array(arr.iter().map(|x| (*x).into()).collect()),
        ArchiverValue::V4GenericBytes(b) => Value::Array(b.iter().map(|x| (*x).into()).collect()),
    }
}

#[derive(Debug, Deserialize)]
struct GetDataParams {
    pv: String,
    from: Option<String>,
    to: Option<String>,
    limit: Option<usize>,
    /// When "true", applies TwoWeekRaw policy (raw for last 2 weeks,
    /// sparsified for older data). Compatible with Java Archiver Appliance.
    usereduced: Option<String>,
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
    // Handle nanosecond-precision timestamps like %Y-%m-%dT%H:%M:%S.%fZ
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.fZ") {
        return Some(dt.and_utc().into());
    }
    None
}

/// Parse PV specification: optionally "postprocessor(pvname)" or just "pvname".
fn parse_pv_spec(spec: &str) -> (String, Option<String>) {
    if let Some(paren_pos) = spec.find('(')
        && spec.ends_with(')') {
            let pp = &spec[..paren_pos];
            let pv = &spec[paren_pos + 1..spec.len() - 1];
            return (pv.to_string(), Some(pp.to_string()));
        }
    (spec.to_string(), None)
}

/// Parsed retrieval parameters shared across all getData endpoints.
struct RetrievalParams {
    pv_name: String,
    pp_spec: Option<String>,
    use_reduced: bool,
    start: SystemTime,
    end: SystemTime,
    limit: Option<usize>,
}

/// If the requested PV is an alias, return its target. Falls back to the input
/// on errors so proxying can still route to a peer that may know the name.
fn resolve_alias(state: &AppState, name: &str) -> String {
    state
        .pv_query
        .canonical_name(name)
        .unwrap_or_else(|_| name.to_string())
}

/// Build the EventStream for a retrieval request, applying failover merge if
/// configured. The post-processor (if any) runs on the merged + deduped
/// stream, so peer samples are summarised together with local ones rather
/// than each side being summarised independently and then mixed.
async fn build_retrieval_stream(
    state: &AppState,
    rp: &RetrievalParams,
) -> anyhow::Result<Box<dyn archiver_core::storage::traits::EventStream>> {
    use archiver_core::retrieval::merge::{DedupTimestampStream, MergedEventStream};
    use archiver_core::retrieval::query::query_data;

    let post = resolve_post_processor(rp);

    // Fast path: no failover peers configured. Keep behaviour identical to
    // pre-Phase-7 by handing post-processing to query_data.
    let Some(ref failover) = state.failover else {
        return query_data(state.storage.as_ref(), &rp.pv_name, rp.start, rp.end, post).await;
    };

    // Fetch local + each peer as raw streams (no post-processing yet).
    let local = query_data(state.storage.as_ref(), &rp.pv_name, rp.start, rp.end, None).await?;
    let mut streams: Vec<Box<dyn archiver_core::storage::traits::EventStream>> = vec![local];

    for peer in &failover.peers {
        match fetch_peer_raw_stream(peer, &rp.pv_name, rp.start, rp.end, failover.timeout).await {
            Ok(s) => streams.push(s),
            Err(e) => {
                tracing::warn!(peer, pv = rp.pv_name, "Failover peer fetch failed: {e}");
            }
        }
    }

    let desc = streams[0].description().clone();
    let merged: Box<dyn archiver_core::storage::traits::EventStream> =
        Box::new(MergedEventStream::new(desc, streams));
    let dedup: Box<dyn archiver_core::storage::traits::EventStream> =
        Box::new(DedupTimestampStream::new(merged));

    Ok(match post {
        Some(pp) => pp.process(dedup),
        None => dedup,
    })
}

/// HTTP GET `<peer>/data/getData.raw?pv=...&from=...&to=...` and parse the
/// PlainPB response into an in-memory EventStream. The peer URL is expected
/// to be the retrieval base (e.g. `https://archiver-b/retrieval`).
async fn fetch_peer_raw_stream(
    peer_base: &str,
    pv: &str,
    start: SystemTime,
    end: SystemTime,
    timeout: std::time::Duration,
) -> anyhow::Result<Box<dyn archiver_core::storage::traits::EventStream>> {
    use archiver_core::storage::plainpb::reader::PbBytesReader;

    let from_iso = chrono::DateTime::<chrono::Utc>::from(start).to_rfc3339();
    let to_iso = chrono::DateTime::<chrono::Utc>::from(end).to_rfc3339();
    let url = format!(
        "{}/data/getData.raw?pv={}&from={}&to={}",
        peer_base.trim_end_matches('/'),
        urlencoding::encode(pv),
        urlencoding::encode(&from_iso),
        urlencoding::encode(&to_iso),
    );

    let client = reqwest::Client::builder().timeout(timeout).build()?;
    let resp = client
        .get(&url)
        .header("X-Archiver-Failover", "1")
        .send()
        .await?;
    if !resp.status().is_success() {
        anyhow::bail!("peer {peer_base} returned {}", resp.status());
    }
    let bytes = resp.bytes().await?.to_vec();
    if bytes.is_empty() {
        // Empty body — caller's loop logs the warning and skips this peer
        // rather than pushing a no-op stream into the merge.
        anyhow::bail!("peer {peer_base} returned empty body for {pv}");
    }
    let reader = PbBytesReader::from_bytes(bytes)?;
    Ok(Box::new(reader))
}

/// Parse and validate common retrieval parameters.
fn parse_retrieval_params(params: &GetDataParams) -> RetrievalParams {
    let (pv_name, pp_spec) = parse_pv_spec(&params.pv);
    let now = SystemTime::now();
    let start = params
        .from
        .as_deref()
        .and_then(parse_iso8601)
        .unwrap_or(now - std::time::Duration::from_secs(DEFAULT_TIME_RANGE_SECS));
    let end = params
        .to
        .as_deref()
        .and_then(parse_iso8601)
        .unwrap_or(now);
    let limit = Some(params.limit.unwrap_or(MAX_RETRIEVAL_LIMIT).min(MAX_RETRIEVAL_LIMIT));
    let use_reduced = params
        .usereduced
        .as_deref()
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));
    RetrievalParams {
        pv_name,
        pp_spec,
        use_reduced,
        start,
        end,
        limit,
    }
}

/// Resolve the post-processor for a retrieval request.
/// Priority: explicit PV spec (e.g. `mean_600(PV)`) > usereduced toggle > none.
///
/// `optimized_N` is handled specially here because it needs the time range
/// to calculate the bin interval: interval = (end - start) / N.
fn resolve_post_processor(
    rp: &RetrievalParams,
) -> Option<Box<dyn archiver_core::storage::traits::PostProcessor>> {
    if let Some(ref spec) = rp.pp_spec {
        if let Some(num_bins) = spec.strip_prefix("optimized_").and_then(|s| s.parse::<u64>().ok()) {
            let range_secs = rp.end.duration_since(rp.start).unwrap_or_default().as_secs();
            let interval = range_secs.checked_div(num_bins).unwrap_or(1).max(1);
            return Some(Box::new(
                archiver_core::etl::decimation::MeanDecimation::new(interval),
            ));
        }
        return parse_post_processor(spec);
    }
    if rp.use_reduced {
        return Some(Box::new(TwoWeekRawProcessor));
    }
    None
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

fn sample_to_json(s: &ArchiverSample) -> JsonSample {
    let dt = DateTime::<Utc>::from(s.timestamp);
    JsonSample {
        secs: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
        val: sample_to_json_value(&s.value),
        severity: s.severity,
        status: s.status,
    }
}

fn sample_to_csv_row(s: &ArchiverSample) -> String {
    let dt = DateTime::<Utc>::from(s.timestamp);
    let val_str = match &s.value {
        ArchiverValue::ScalarDouble(v) => v.to_string(),
        ArchiverValue::ScalarFloat(v) => v.to_string(),
        ArchiverValue::ScalarInt(v) => v.to_string(),
        ArchiverValue::ScalarShort(v) => v.to_string(),
        ArchiverValue::ScalarEnum(v) => v.to_string(),
        ArchiverValue::ScalarString(v) => csv_escape(v),
        other => format!("{other:?}"),
    };
    format!(
        "{},{},{},{},{}\n",
        dt.timestamp(),
        dt.timestamp_subsec_nanos(),
        val_str,
        s.severity,
        s.status,
    )
}

/// RFC 4180 CSV escaping: wrap in quotes if the value contains comma,
/// quote, or newline, and double any internal quotes.
fn csv_escape(s: &str) -> String {
    if s.contains('"') || s.contains(',') || s.contains('\n') || s.contains('\r') {
        let escaped = s.replace('"', "\"\"");
        format!("\"{escaped}\"")
    } else {
        format!("\"{s}\"")
    }
}

/// Maximum time a single retrieval query may run before being terminated.
const RETRIEVAL_DEADLINE_SECS: u64 = 300;

/// Batch size for draining samples through the channel.
/// Sending batches instead of individual samples reduces channel overhead.
const DRAIN_BATCH_SIZE: usize = 256;

/// Iterate an EventStream, sending batches of samples through the channel.
/// Runs synchronously (EventStream::next_event is sync) inside spawn_blocking.
/// Terminates if the deadline is exceeded to prevent thread-pool starvation.
fn drain_stream(
    mut stream: Box<dyn EventStream>,
    start: SystemTime,
    end: SystemTime,
    limit: Option<usize>,
    tx: std::sync::mpsc::Sender<Vec<ArchiverSample>>,
) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(RETRIEVAL_DEADLINE_SECS);
    let mut count = 0usize;
    let mut batch = Vec::with_capacity(DRAIN_BATCH_SIZE);

    while let Ok(Some(sample)) = stream.next_event() {
        if sample.timestamp > end {
            break;
        }
        if sample.timestamp >= start {
            batch.push(sample);
            count += 1;

            if batch.len() >= DRAIN_BATCH_SIZE
                && tx.send(std::mem::replace(&mut batch, Vec::with_capacity(DRAIN_BATCH_SIZE))).is_err()
            {
                return;
            }

            if let Some(max) = limit
                && count >= max {
                    break;
                }
        }
        // Check deadline every 10k samples to avoid excessive clock reads.
        if count.is_multiple_of(10_000) && std::time::Instant::now() > deadline {
            tracing::warn!(count, "Retrieval deadline exceeded, truncating response");
            break;
        }
    }

    // Send remaining samples.
    if !batch.is_empty() {
        let _ = tx.send(batch);
    }
}

async fn get_data_json(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Query(params): Query<GetDataParams>,
) -> Response {
    let mut rp = parse_retrieval_params(&params);
    rp.pv_name = resolve_alias(&state, &rp.pv_name);

    if let Some(resp) = try_cluster_proxy(&state, &rp.pv_name, "data/getData.json", &headers, &uri).await {
        return resp;
    }

    let stream = match build_retrieval_stream(&state, &rp).await {
        Ok(s) => s,
        Err(e) => {
            return ApiError::internal(e).into_response();
        }
    };

    let start = rp.start;
    let end = rp.end;
    let limit = rp.limit;

    // Stream JSON: [{"meta":...,"data":[sample,sample,...]}]
    // Use larger channel buffer (512) to reduce backpressure stalls.
    let (chunk_tx, chunk_rx) = tokio::sync::mpsc::channel::<Result<String, std::io::Error>>(512);

    // Look up metadata from registry.
    let (prec, egu) = state
        .pv_query
        .get_pv(&rp.pv_name)
        .ok()
        .flatten()
        .map(|r| (r.prec, r.egu))
        .unwrap_or((None, None));

    let pv_name = rp.pv_name;

    tokio::spawn(async move {
        let meta = JsonMeta {
            name: pv_name,
            prec,
            egu,
        };
        let meta_json = match serde_json::to_string(&meta) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("Failed to serialize PV metadata: {e}");
                "{}".to_string()
            }
        };
        let header = format!("[{{\"meta\":{meta_json},\"data\":[");
        if chunk_tx.send(Ok(header)).await.is_err() {
            return;
        }

        // Drain the EventStream in batches via a blocking task.
        let (sample_tx, sample_rx) = std::sync::mpsc::channel::<Vec<ArchiverSample>>();
        tokio::task::spawn_blocking(move || {
            drain_stream(stream, start, end, limit, sample_tx);
        });

        // Serialize batches of samples into a single String chunk to reduce
        // per-sample allocation and channel overhead.
        let mut first = true;
        let mut buf = String::with_capacity(32 * 1024);
        while let Ok(batch) = sample_rx.recv() {
            buf.clear();
            for sample in &batch {
                let js = sample_to_json(sample);
                if first {
                    first = false;
                } else {
                    buf.push(',');
                }
                match serde_json::to_string(&js) {
                    Ok(s) => buf.push_str(&s),
                    Err(e) => {
                        tracing::warn!("Failed to serialize sample: {e}");
                    }
                }
            }
            if !buf.is_empty()
                && chunk_tx.send(Ok(buf.clone())).await.is_err()
            {
                break;
            }
        }

        let _ = chunk_tx.send(Ok("]}]".to_string())).await;
    });

    let stream = ReceiverStream::new(chunk_rx);
    let body = axum::body::Body::from_stream(stream);
    Response::builder()
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(body)
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
        .into_response()
}

async fn get_data_csv(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Query(params): Query<GetDataParams>,
) -> Response {
    let mut rp = parse_retrieval_params(&params);
    rp.pv_name = resolve_alias(&state, &rp.pv_name);

    if let Some(resp) = try_cluster_proxy(&state, &rp.pv_name, "data/getData.csv", &headers, &uri).await {
        return resp;
    }

    let stream = match build_retrieval_stream(&state, &rp).await {
        Ok(s) => s,
        Err(e) => {
            return ApiError::internal(e).into_response();
        }
    };

    let start = rp.start;
    let end = rp.end;
    let limit = rp.limit;

    let (chunk_tx, chunk_rx) = tokio::sync::mpsc::channel::<Result<String, std::io::Error>>(512);

    tokio::spawn(async move {
        if chunk_tx
            .send(Ok("seconds,nanos,val,severity,status\n".to_string()))
            .await
            .is_err()
        {
            return;
        }

        let (sample_tx, sample_rx) = std::sync::mpsc::channel::<Vec<ArchiverSample>>();
        tokio::task::spawn_blocking(move || {
            drain_stream(stream, start, end, limit, sample_tx);
        });

        let mut buf = String::with_capacity(32 * 1024);
        while let Ok(batch) = sample_rx.recv() {
            buf.clear();
            for sample in &batch {
                buf.push_str(&sample_to_csv_row(sample));
            }
            if !buf.is_empty()
                && chunk_tx.send(Ok(buf.clone())).await.is_err()
            {
                break;
            }
        }
    });

    let stream = ReceiverStream::new(chunk_rx);
    let body = axum::body::Body::from_stream(stream);
    Response::builder()
        .header(axum::http::header::CONTENT_TYPE, "text/csv")
        .body(body)
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
        .into_response()
}

async fn get_data_raw(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Query(params): Query<GetDataParams>,
) -> Response {
    let mut rp = parse_retrieval_params(&params);
    rp.pv_name = resolve_alias(&state, &rp.pv_name);

    if let Some(resp) = try_cluster_proxy(&state, &rp.pv_name, "data/getData.raw", &headers, &uri).await {
        return resp;
    }

    let start = rp.start;
    let end = rp.end;

    let streams = match state.storage.get_data(&rp.pv_name, start, end).await {
        Ok(s) => s,
        Err(e) => {
            return ApiError::internal(e).into_response();
        }
    };

    // Raw PB: stream header + samples from each partition file.
    let (chunk_tx, chunk_rx) = tokio::sync::mpsc::channel::<Result<Vec<u8>, std::io::Error>>(64);

    tokio::task::spawn_blocking(move || {
        let deadline = std::time::Instant::now()
            + std::time::Duration::from_secs(RETRIEVAL_DEADLINE_SECS);
        let mut count = 0usize;
        for mut stream in streams {
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
            let mut chunk = archiver_core::storage::plainpb::codec::escape(&header_bytes);
            chunk.push(archiver_core::storage::plainpb::codec::NEWLINE);
            if chunk_tx.blocking_send(Ok(chunk)).is_err() {
                return;
            }

            while let Ok(Some(sample)) = stream.next_event() {
                if sample.timestamp > end {
                    break;
                }
                if sample.timestamp >= start
                    && let Ok(sample_bytes) =
                        archiver_core::storage::plainpb::writer::encode_sample(desc.db_type, &sample)
                    {
                        let mut escaped =
                            archiver_core::storage::plainpb::codec::escape(&sample_bytes);
                        escaped.push(archiver_core::storage::plainpb::codec::NEWLINE);
                        if chunk_tx.blocking_send(Ok(escaped)).is_err() {
                            return;
                        }
                        count += 1;
                    }
                // Bound total wall-clock time so a slow/stalled client cannot
                // tie up a blocking worker indefinitely.
                if count.is_multiple_of(10_000) && std::time::Instant::now() > deadline {
                    tracing::warn!(count, "Raw retrieval deadline exceeded, truncating response");
                    return;
                }
            }
        }
    });

    let stream = ReceiverStream::new(chunk_rx);
    let body = axum::body::Body::from_stream(stream);
    Response::builder()
        .header(axum::http::header::CONTENT_TYPE, "application/x-protobuf")
        .body(body)
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
        .into_response()
}
