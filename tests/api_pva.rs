//! End-to-end PVA RPC retrieval test. Spins up the archiver's PVA RPC
//! server in isolated mode (loopback + ephemeral ports), uses the
//! `epics-pva-rs` client to call `archappl/getData`, and checks the
//! returned NTTable carries the stored samples.

use std::sync::Arc;
use std::time::SystemTime;

use epics_rs::pva::nt::NTURI;
use epics_rs::pva::pvdata::{PvField, PvStructure, ScalarType, ScalarValue};
use epics_rs::pva::server_native::PvaServer;

use archiver_api::pva::build_archiver_pva_source;
use archiver_api::services::impls::RegistryRepository;
use archiver_core::registry::{PvRegistry, SampleMode};
use archiver_core::storage::partition::PartitionGranularity;
use archiver_core::storage::plainpb::PlainPbStoragePlugin;
use archiver_core::storage::traits::{AppendMeta, StoragePlugin};
use archiver_core::types::{ArchDbType, ArchiverSample, ArchiverValue};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pva_rpc_get_data_end_to_end() {
    // ── Storage with two samples in 2024 ──
    let dir = tempfile::tempdir().unwrap();
    let storage: Arc<dyn StoragePlugin> = Arc::new(PlainPbStoragePlugin::new(
        "sts",
        dir.path().to_path_buf(),
        PartitionGranularity::Year,
    ));
    let registry = Arc::new(PvRegistry::in_memory().unwrap());
    registry
        .register_pv("PV:Pva", ArchDbType::ScalarDouble, &SampleMode::Monitor, 1)
        .unwrap();

    let year_start = chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc();
    for (offset, val) in [(10u64, 1.5), (20u64, 2.5)] {
        let ts: SystemTime = (year_start + chrono::Duration::seconds(offset as i64)).into();
        let sample = ArchiverSample::new(ts, ArchiverValue::ScalarDouble(val));
        storage
            .append_event_with_meta(
                "PV:Pva",
                ArchDbType::ScalarDouble,
                &sample,
                &AppendMeta::default(),
            )
            .await
            .unwrap();
    }
    storage.flush_writes().await.unwrap();

    // ── PVA server ──
    let pv_query = Arc::new(RegistryRepository::new(registry.clone()));
    let source = build_archiver_pva_source(storage.clone(), pv_query.clone());
    let server = PvaServer::isolated(source);

    // ── Client built against the isolated server ──
    let client = server.client_config();

    // ── Build the NTURI request ──
    let nturi = NTURI::new()
        .arg_scalar("pv", ScalarType::String)
        .arg_scalar("from", ScalarType::String)
        .arg_scalar("to", ScalarType::String);
    let req_desc = nturi.build();
    // Construct the value with our specific arguments.
    let mut query = PvStructure::new("");
    query.fields.push((
        "pv".into(),
        PvField::Scalar(ScalarValue::String("PV:Pva".into())),
    ));
    query.fields.push((
        "from".into(),
        PvField::Scalar(ScalarValue::String(
            chrono::DateTime::<chrono::Utc>::from(SystemTime::from(year_start)).to_rfc3339(),
        )),
    ));
    query.fields.push((
        "to".into(),
        PvField::Scalar(ScalarValue::String(
            chrono::DateTime::<chrono::Utc>::from(SystemTime::from(
                year_start + chrono::Duration::seconds(60),
            ))
            .to_rfc3339(),
        )),
    ));
    let mut root = PvStructure::new("epics:nt/NTURI:1.0");
    root.fields.push((
        "scheme".into(),
        PvField::Scalar(ScalarValue::String("pva".into())),
    ));
    root.fields.push((
        "authority".into(),
        PvField::Scalar(ScalarValue::String(String::new())),
    ));
    root.fields.push((
        "path".into(),
        PvField::Scalar(ScalarValue::String("archappl/getData".into())),
    ));
    root.fields.push(("query".into(), PvField::Structure(query)));
    let req_value = PvField::Structure(root);

    // ── Call the RPC ──
    let (_resp_desc, resp_value) = client
        .pvrpc("archappl/getData", &req_desc, &req_value)
        .await
        .expect("rpc call ok");

    // ── Inspect the NTTable: value.value column should contain 1.5, 2.5 ──
    let PvField::Structure(resp) = resp_value else {
        panic!("expected struct response");
    };
    let value_struct = resp
        .fields
        .iter()
        .find_map(|(n, f)| {
            if n == "value" {
                if let PvField::Structure(s) = f {
                    Some(s)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .expect("value substructure");
    let values_col = value_struct
        .fields
        .iter()
        .find_map(|(n, f)| {
            if n == "value" {
                if let PvField::ScalarArray(a) = f {
                    Some(a)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .expect("value column");
    assert_eq!(values_col.len(), 2);
    if let (ScalarValue::Double(a), ScalarValue::Double(b)) = (&values_col[0], &values_col[1]) {
        assert!((*a - 1.5).abs() < 1e-9);
        assert!((*b - 2.5).abs() < 1e-9);
    } else {
        panic!("expected double values, got {:?}", values_col);
    }

    // Sanity: secondsPastEpoch column also present and has 2 entries.
    let secs_col = value_struct
        .fields
        .iter()
        .find_map(|(n, f)| {
            if n == "secondsPastEpoch" {
                if let PvField::ScalarArray(a) = f {
                    Some(a)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .expect("secondsPastEpoch column");
    assert_eq!(secs_col.len(), 2);

    // Tear down so the test exits promptly.
    server.stop();
}
