//! PVA retrieval RPC service.
//!
//! Mirrors the Java archiver's PVA-side retrieval surface (`archappl/getData`,
//! `archappl/getDataAtTime`). Each request is an NTURI value with a `query`
//! sub-structure naming the PV and time range; responses are an NTTable
//! with one column per archived attribute (timestamp, value, severity,
//! status). Wired into [`run_pva_retrieval_server`] which spawns a single
//! tokio task hosting the PVA TCP+UDP listeners.
//!
//! ## Runtime requirement
//!
//! `OnRpcFn` from `epics-pva-rs` is sync, so the RPC handlers below run
//! the async retrieval pipeline inside `tokio::task::block_in_place +
//! Handle::current().block_on(...)`. **`block_in_place` requires a
//! multi-threaded tokio runtime** — calling it from a `current_thread`
//! runtime panics. archiver-rs's main binary uses `#[tokio::main]` (multi-
//! threaded by default) so this is safe in production; tests that spin
//! up a `PvaServer` must use `#[tokio::test(flavor = "multi_thread", ...)]`.
//!
//! The two RPC names exposed:
//! - `archappl/getData` — required: `pv`, `from`, `to`. Optional:
//!   `processing` (post-processor spec, e.g. `mean_60`).
//! - `archappl/getDataAtTime` — single-sample lookup at `at`, returns the
//!   most recent sample at-or-before that time. Convenience wrapper used
//!   by Phoebus DataBrowser to populate gauges.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use chrono::DateTime;
use epics_rs::pva::nt::NTTable;
use epics_rs::pva::pvdata::{FieldDesc, PvField, PvStructure, ScalarType, ScalarValue};
use epics_rs::pva::server_native::{
    run_pva_server, PvaServer, PvaServerConfig, SharedPV, SharedSource,
};

use archiver_core::retrieval::query::{parse_post_processor, query_data};
use archiver_core::storage::traits::StoragePlugin;
use archiver_core::types::{ArchiverSample, ArchiverValue};

use crate::services::traits::PvQueryRepository;

/// Maximum samples a single PVA RPC call may return.
const PVA_RPC_LIMIT: usize = 1_000_000;

/// Build the `SharedSource` exposing both archiver RPC PVs.
pub fn build_archiver_pva_source(
    storage: Arc<dyn StoragePlugin>,
    pv_query: Arc<dyn PvQueryRepository>,
) -> Arc<SharedSource> {
    let source = Arc::new(SharedSource::new());

    // archappl/getData — full time-range fetch.
    {
        let pv = SharedPV::new();
        // Open with a placeholder NTTable so introspection-only clients see
        // the right shape. The actual response is built per-call by the RPC
        // handler.
        let desc = response_table().build();
        let initial = response_table().create();
        pv.open(desc, initial);

        let storage = storage.clone();
        let pv_query = pv_query.clone();
        pv.on_rpc(move |_pv, _req_desc, req_value| {
            run_get_data_rpc(&storage, pv_query.as_ref(), req_value)
        });
        source.add("archappl/getData", pv);
    }

    // archappl/getDataAtTime — last sample at-or-before a given timestamp.
    {
        let pv = SharedPV::new();
        let desc = response_table().build();
        let initial = response_table().create();
        pv.open(desc, initial);

        let storage = storage.clone();
        let pv_query = pv_query.clone();
        pv.on_rpc(move |_pv, _req_desc, req_value| {
            run_get_data_at_time_rpc(&storage, pv_query.as_ref(), req_value)
        });
        source.add("archappl/getDataAtTime", pv);
    }

    source
}

/// Spawn the PVA server hosting the archiver RPC PVs. Returns the running
/// `PvaServer` handle so the caller (supervisor) can stop it on shutdown.
pub async fn run_pva_retrieval_server(
    source: Arc<SharedSource>,
    bind_port: u16,
) -> anyhow::Result<()> {
    let config = PvaServerConfig {
        tcp_port: bind_port,
        ..PvaServerConfig::default()
    };
    run_pva_server(source, config)
        .await
        .map_err(|e| anyhow::anyhow!("PVA server error: {e}"))
}

/// Spawn the PVA server but return the handle so callers can `stop()` /
/// `wait()` explicitly. Mirrors `run_pva_retrieval_server` but doesn't
/// block.
pub fn start_pva_retrieval_server(
    source: Arc<SharedSource>,
    bind_port: u16,
    udp_port: u16,
) -> PvaServer {
    let config = PvaServerConfig {
        tcp_port: bind_port,
        udp_port,
        ..PvaServerConfig::default()
    };
    PvaServer::start(source, config)
}

/// NTTable column shape for archiver responses. Five columns: epoch
/// seconds, nanoseconds, value (as double), severity, status.
fn response_table() -> NTTable {
    NTTable::new()
        .add_column(ScalarType::Long, "secondsPastEpoch", Some("Time (s)"))
        .add_column(ScalarType::Int, "nanoseconds", Some("Time (ns)"))
        .add_column(ScalarType::Double, "value", Some("Value"))
        .add_column(ScalarType::Int, "severity", Some("Severity"))
        .add_column(ScalarType::Int, "status", Some("Status"))
}

/// Pull a string field out of the NTURI `query` substructure, defaulting
/// to empty when absent.
fn query_string_field(req: &PvField, name: &str) -> String {
    let Some(query) = nturi_query(req) else {
        return String::new();
    };
    for (fname, fval) in &query.fields {
        if fname == name
            && let PvField::Scalar(ScalarValue::String(s)) = fval
        {
            return s.clone();
        }
    }
    String::new()
}

fn nturi_query(req: &PvField) -> Option<&PvStructure> {
    let PvField::Structure(root) = req else {
        return None;
    };
    for (fname, fval) in &root.fields {
        if fname == "query"
            && let PvField::Structure(q) = fval
        {
            return Some(q);
        }
    }
    None
}

/// `archappl/getData` handler. Pulls pv/from/to/processing out of the NTURI
/// query, runs `query_data` synchronously inside a `block_in_place` so we
/// don't deadlock the runtime, and returns the columnar response.
fn run_get_data_rpc(
    storage: &Arc<dyn StoragePlugin>,
    pv_query: &dyn PvQueryRepository,
    req: PvField,
) -> Result<(FieldDesc, PvField), String> {
    let pv_in = query_string_field(&req, "pv");
    let from = query_string_field(&req, "from");
    let to = query_string_field(&req, "to");
    let processing = {
        let s = query_string_field(&req, "processing");
        if s.is_empty() { None } else { Some(s) }
    };

    if pv_in.is_empty() {
        return Err("pv field is required".to_string());
    }
    let canonical = pv_query.canonical_name(&pv_in).unwrap_or(pv_in.clone());

    let start = parse_iso8601(&from)
        .unwrap_or_else(|| SystemTime::now() - Duration::from_secs(3600));
    let end = parse_iso8601(&to).unwrap_or_else(SystemTime::now);

    let post = processing.as_deref().and_then(parse_post_processor);

    let samples = collect_samples(storage, &canonical, start, end, post)
        .map_err(|e| format!("{e}"))?;
    Ok((response_table().build(), build_table_value(&samples)))
}

/// `archappl/getDataAtTime` handler. Returns one row containing the last
/// sample whose timestamp is <= `at`, or an empty table if none exists.
///
/// Two-stage strategy keeps the call O(1) on the common path:
/// 1. Query `storage.get_last_known_event(pv)`. If it predates `at`, that
///    is the answer.
/// 2. Otherwise scan backwards in widening windows starting from `at`,
///    capped at the current LTS partition granularity × a few partitions.
///    This avoids the 720-iteration worst case the previous impl had.
fn run_get_data_at_time_rpc(
    storage: &Arc<dyn StoragePlugin>,
    pv_query: &dyn PvQueryRepository,
    req: PvField,
) -> Result<(FieldDesc, PvField), String> {
    let pv_in = query_string_field(&req, "pv");
    let at = query_string_field(&req, "at");
    if pv_in.is_empty() {
        return Err("pv field is required".to_string());
    }
    let canonical = pv_query.canonical_name(&pv_in).unwrap_or(pv_in.clone());
    let target = parse_iso8601(&at).unwrap_or_else(SystemTime::now);

    let storage = storage.clone();
    let sample_opt = tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async move {
            // Stage 1: cheap path — last known event for the PV across all
            // tiers. If its timestamp is at-or-before target, return it.
            if let Some(latest) = storage.get_last_known_event(&canonical).await?
                && latest.timestamp <= target
            {
                return anyhow::Ok(Some(latest));
            }

            // Stage 2: target is in the past and the last sample is newer.
            // Scan a window ending at `target`, doubling on miss, capped at
            // 30 days. Most queries land in the first iteration.
            let mut window = Duration::from_secs(3_600);
            let max_window = Duration::from_secs(30 * 86_400);
            let mut last = None;
            loop {
                let lower = target.checked_sub(window).unwrap_or(SystemTime::UNIX_EPOCH);
                let streams = storage.get_data(&canonical, lower, target).await?;
                for mut s in streams {
                    while let Some(sample) = s.next_event()? {
                        if sample.timestamp <= target {
                            last = Some(sample);
                        }
                    }
                }
                if last.is_some() || window >= max_window || lower == SystemTime::UNIX_EPOCH {
                    break;
                }
                window = window.saturating_mul(2).min(max_window);
            }
            anyhow::Ok(last)
        })
    })
    .map_err(|e| format!("{e}"))?;

    let samples: Vec<ArchiverSample> = sample_opt.into_iter().collect();
    Ok((response_table().build(), build_table_value(&samples)))
}

/// Drive an EventStream synchronously inside `block_in_place` so we can call
/// it from the sync RPC handler without blocking a runtime worker.
fn collect_samples(
    storage: &Arc<dyn StoragePlugin>,
    pv: &str,
    start: SystemTime,
    end: SystemTime,
    post: Option<Box<dyn archiver_core::storage::traits::PostProcessor>>,
) -> anyhow::Result<Vec<ArchiverSample>> {
    let storage = storage.clone();
    let pv = pv.to_string();
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async move {
            let mut stream = query_data(storage.as_ref(), &pv, start, end, post).await?;
            let mut out = Vec::new();
            while let Some(sample) = stream.next_event()? {
                if sample.timestamp < start || sample.timestamp > end {
                    continue;
                }
                out.push(sample);
                if out.len() >= PVA_RPC_LIMIT {
                    break;
                }
            }
            anyhow::Ok(out)
        })
    })
}

/// Build the NTTable response value from a list of samples.
fn build_table_value(samples: &[ArchiverSample]) -> PvField {
    let mut secs: Vec<ScalarValue> = Vec::with_capacity(samples.len());
    let mut nanos: Vec<ScalarValue> = Vec::with_capacity(samples.len());
    let mut values: Vec<ScalarValue> = Vec::with_capacity(samples.len());
    let mut severities: Vec<ScalarValue> = Vec::with_capacity(samples.len());
    let mut statuses: Vec<ScalarValue> = Vec::with_capacity(samples.len());

    for s in samples {
        let dur = s.timestamp.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
        secs.push(ScalarValue::Long(dur.as_secs() as i64));
        nanos.push(ScalarValue::Int(dur.subsec_nanos() as i32));
        let v = sample_to_double(&s.value);
        values.push(ScalarValue::Double(v));
        severities.push(ScalarValue::Int(s.severity));
        statuses.push(ScalarValue::Int(s.status));
    }

    let mut root = PvStructure::new("epics:nt/NTTable:1.0");
    let labels: Vec<ScalarValue> = ["Time (s)", "Time (ns)", "Value", "Severity", "Status"]
        .iter()
        .map(|s| ScalarValue::String((*s).to_string()))
        .collect();
    root.fields
        .push(("labels".into(), PvField::ScalarArray(labels)));

    let mut value_struct = PvStructure::new("");
    value_struct
        .fields
        .push(("secondsPastEpoch".into(), PvField::ScalarArray(secs)));
    value_struct
        .fields
        .push(("nanoseconds".into(), PvField::ScalarArray(nanos)));
    value_struct
        .fields
        .push(("value".into(), PvField::ScalarArray(values)));
    value_struct
        .fields
        .push(("severity".into(), PvField::ScalarArray(severities)));
    value_struct
        .fields
        .push(("status".into(), PvField::ScalarArray(statuses)));
    root.fields
        .push(("value".into(), PvField::Structure(value_struct)));
    root.fields.push((
        "descriptor".into(),
        PvField::Scalar(ScalarValue::String(String::new())),
    ));
    root.fields
        .push(("alarm".into(), epics_rs::pva::nt::meta::alarm_default()));
    root.fields
        .push(("timeStamp".into(), epics_rs::pva::nt::meta::time_default()));
    PvField::Structure(root)
}

/// Best-effort conversion of an ArchiverValue to f64 (matches the JSON
/// retrieval surface). Non-scalar / non-numeric types are emitted as NaN
/// so the column stays uniform; clients that need string or array data
/// should use the REST `getData.raw` endpoint instead.
fn sample_to_double(v: &ArchiverValue) -> f64 {
    match v {
        ArchiverValue::ScalarDouble(d) => *d,
        ArchiverValue::ScalarFloat(f) => *f as f64,
        ArchiverValue::ScalarInt(i) => *i as f64,
        ArchiverValue::ScalarShort(i) => *i as f64,
        ArchiverValue::ScalarEnum(i) => *i as f64,
        _ => f64::NAN,
    }
}

fn parse_iso8601(s: &str) -> Option<SystemTime> {
    if s.is_empty() {
        return None;
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.into());
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Some(dt.and_utc().into());
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(dt.and_utc().into());
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.fZ") {
        return Some(dt.and_utc().into());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use archiver_core::registry::{PvRegistry, SampleMode};
    use archiver_core::storage::partition::PartitionGranularity;
    use archiver_core::storage::plainpb::PlainPbStoragePlugin;
    use archiver_core::storage::traits::AppendMeta;
    use archiver_core::types::ArchDbType;
    use std::sync::Arc;

    use crate::services::impls::RegistryRepository;

    /// Verify the NTTable layout matches what callers expect.
    #[test]
    fn response_table_has_five_columns() {
        let table = response_table().build();
        let FieldDesc::Structure { struct_id, fields } = &table else {
            panic!("expected Structure");
        };
        assert_eq!(struct_id, "epics:nt/NTTable:1.0");
        // labels + value + descriptor + alarm + timeStamp = 5
        assert_eq!(fields.len(), 5);
    }

    /// `getData` populates the columns from real samples.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_data_returns_columns_for_stored_samples() {
        let dir = tempfile::tempdir().unwrap();
        let storage: Arc<dyn StoragePlugin> = Arc::new(PlainPbStoragePlugin::new(
            "sts",
            dir.path().to_path_buf(),
            PartitionGranularity::Year,
        ));
        let registry = Arc::new(PvRegistry::in_memory().unwrap());
        registry
            .register_pv(
                "PV:Test",
                ArchDbType::ScalarDouble,
                &SampleMode::Monitor,
                1,
            )
            .unwrap();

        // Year-2024 samples: t=10s and t=20s.
        let year_start = chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc();
        for (offset, val) in [(10u64, 1.5), (20u64, 2.5)] {
            let ts = year_start + chrono::Duration::seconds(offset as i64);
            let sample = ArchiverSample::new(
                ts.into(),
                ArchiverValue::ScalarDouble(val),
            );
            storage
                .append_event_with_meta(
                    "PV:Test",
                    ArchDbType::ScalarDouble,
                    &sample,
                    &AppendMeta::default(),
                )
                .await
                .unwrap();
        }
        storage.flush_writes().await.unwrap();

        let pv_query: Arc<dyn PvQueryRepository> =
            Arc::new(RegistryRepository::new(registry.clone()));

        // Build a request: NTURI with query.{pv, from, to}.
        let mut q = PvStructure::new("");
        q.fields.push((
            "pv".into(),
            PvField::Scalar(ScalarValue::String("PV:Test".into())),
        ));
        q.fields.push((
            "from".into(),
            PvField::Scalar(ScalarValue::String(
                chrono::DateTime::<chrono::Utc>::from(SystemTime::from(year_start))
                    .to_rfc3339(),
            )),
        ));
        q.fields.push((
            "to".into(),
            PvField::Scalar(ScalarValue::String(
                chrono::DateTime::<chrono::Utc>::from(SystemTime::from(
                    year_start + chrono::Duration::seconds(60),
                ))
                .to_rfc3339(),
            )),
        ));
        let mut root = PvStructure::new("epics:nt/NTURI:1.0");
        root.fields
            .push(("query".into(), PvField::Structure(q)));
        let req = PvField::Structure(root);

        let (_desc, value) =
            run_get_data_rpc(&storage, pv_query.as_ref(), req).expect("rpc ok");

        let PvField::Structure(resp) = value else {
            panic!("expected struct");
        };
        // Locate the value sub-structure.
        let value_struct = resp
            .fields
            .iter()
            .find_map(|(name, f)| {
                if name == "value" {
                    if let PvField::Structure(s) = f {
                        Some(s)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .unwrap();
        let lookup = |col: &str| -> &Vec<ScalarValue> {
            value_struct
                .fields
                .iter()
                .find_map(|(n, f)| {
                    if n == col {
                        if let PvField::ScalarArray(a) = f {
                            Some(a)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .unwrap()
        };

        let secs = lookup("secondsPastEpoch");
        let values = lookup("value");
        assert_eq!(secs.len(), 2);
        assert_eq!(values.len(), 2);
        if let (ScalarValue::Double(v0), ScalarValue::Double(v1)) = (&values[0], &values[1]) {
            assert!((*v0 - 1.5).abs() < 1e-9);
            assert!((*v1 - 2.5).abs() < 1e-9);
        } else {
            panic!("expected double values");
        }
    }
}
