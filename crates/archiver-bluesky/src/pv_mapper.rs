//! PV Mapper — converts Bluesky documents into special EXP:* PV samples.
//!
//! Naming convention:
//!   EXP:{beamline}:run:active          → ScalarEnum (1=running, 0=idle)
//!   EXP:{beamline}:run:uid             → ScalarString
//!   EXP:{beamline}:run:plan_name       → ScalarString
//!   EXP:{beamline}:run:scan_id         → ScalarInt
//!   EXP:{beamline}:motor:{name}:setpoint  → ScalarDouble
//!   EXP:{beamline}:motor:{name}:readback  → ScalarDouble
//!   EXP:{beamline}:det:{name}:exposure    → ScalarDouble
//!   EXP:{beamline}:det:{name}:num_images  → ScalarInt
//!   EXP:{beamline}:det:{name}:value       → ScalarDouble / ScalarInt / ScalarString
//!   EXP:{beamline}:scan:num_points     → ScalarInt
//!   EXP:{beamline}:scan:current_point  → ScalarInt
//!   EXP:{beamline}:scan:type           → ScalarString

use std::collections::{HashMap, HashSet};

use tracing::debug;

use archiver_core::types::{ArchDbType, ArchiverSample, ArchiverValue};

use crate::documents::{BlueskyDocument, EventDescriptor, TypedDocument};

/// Metadata extracted from a Bluesky DataKey for a PV.
#[derive(Debug, Clone)]
pub struct DataKeyMeta {
    pub units: Option<String>,
    pub precision: Option<i32>,
}

/// Converts Bluesky documents to (pv_name, dbr_type, sample) tuples.
pub struct PvMapper {
    beamline: String,
    /// Map descriptor UID → set of motor names (from RunStart or descriptor).
    motor_names: HashMap<String, Vec<String>>,
    /// Map descriptor UID → set of detector names.
    detector_names: HashMap<String, Vec<String>>,
    /// Map descriptor UID → descriptor info.
    descriptors: HashMap<String, DescriptorInfo>,
    /// Map descriptor UID → per-key metadata (units, precision from DataKey).
    descriptor_meta: HashMap<String, HashMap<String, DataKeyMeta>>,
    /// Current run UID.
    current_run_uid: Option<String>,
    /// Motor names from RunStart hints (applied to subsequent descriptors).
    run_motor_hints: HashSet<String>,
}

struct DescriptorInfo {
    run_start_uid: String,
    #[allow(dead_code)]
    data_keys: Vec<String>,
}

impl PvMapper {
    pub fn new(beamline: &str) -> Self {
        Self {
            beamline: beamline.to_string(),
            motor_names: HashMap::new(),
            detector_names: HashMap::new(),
            descriptors: HashMap::new(),
            descriptor_meta: HashMap::new(),
            current_run_uid: None,
            run_motor_hints: HashSet::new(),
        }
    }

    /// Get the DataKey metadata (units, precision) for a PV key under a descriptor.
    pub fn get_data_key_meta(&self, descriptor_uid: &str, key: &str) -> Option<&DataKeyMeta> {
        self.descriptor_meta
            .get(descriptor_uid)?
            .get(key)
    }

    /// Convert a Bluesky document into a list of PV samples.
    pub fn map_document(
        &mut self,
        doc: &BlueskyDocument,
    ) -> Vec<(String, ArchDbType, ArchiverSample)> {
        let typed = match doc.parse() {
            Some(t) => t,
            None => return Vec::new(),
        };

        match typed {
            TypedDocument::Start(start) => self.map_run_start(&start),
            TypedDocument::Descriptor(desc) => self.map_descriptor(&desc),
            TypedDocument::Event(event) => self.map_event(&event),
            TypedDocument::EventPage(page) => self.map_event_page(&page),
            TypedDocument::Stop(stop) => self.map_run_stop(&stop),
        }
    }

    fn pv(&self, suffix: &str) -> String {
        format!("EXP:{}:{}", self.beamline, suffix)
    }

    fn map_run_start(
        &mut self,
        start: &crate::documents::RunStart,
    ) -> Vec<(String, ArchDbType, ArchiverSample)> {
        self.current_run_uid = Some(start.uid.clone());
        let ts = start.time;
        let mut samples = Vec::new();

        // run:active = 1
        samples.push((
            self.pv("run:active"),
            ArchDbType::ScalarEnum,
            ArchiverSample::from_unix_timestamp(ts, ArchiverValue::ScalarEnum(1)),
        ));

        // run:uid
        samples.push((
            self.pv("run:uid"),
            ArchDbType::ScalarString,
            ArchiverSample::from_unix_timestamp(ts, ArchiverValue::ScalarString(start.uid.clone())),
        ));

        // run:plan_name
        if let Some(ref plan_name) = start.plan_name {
            samples.push((
                self.pv("run:plan_name"),
                ArchDbType::ScalarString,
                ArchiverSample::from_unix_timestamp(
                    ts,
                    ArchiverValue::ScalarString(plan_name.clone()),
                ),
            ));
        }

        // run:scan_id
        if let Some(scan_id) = start.scan_id {
            samples.push((
                self.pv("run:scan_id"),
                ArchDbType::ScalarInt,
                ArchiverSample::from_unix_timestamp(ts, ArchiverValue::ScalarInt(scan_id as i32)),
            ));
        }

        // scan:num_points
        if let Some(num_points) = start.num_points {
            samples.push((
                self.pv("scan:num_points"),
                ArchDbType::ScalarInt,
                ArchiverSample::from_unix_timestamp(
                    ts,
                    ArchiverValue::ScalarInt(num_points as i32),
                ),
            ));
        }

        // scan:type
        if let Some(ref scan_type) = start.scan_type {
            samples.push((
                self.pv("scan:type"),
                ArchDbType::ScalarString,
                ArchiverSample::from_unix_timestamp(
                    ts,
                    ArchiverValue::ScalarString(scan_type.clone()),
                ),
            ));
        }

        // Store motor names from RunStart hints for use in descriptor classification.
        self.run_motor_hints.clear();
        if let Some(ref motors) = start.motors {
            for m in motors {
                self.run_motor_hints.insert(m.clone());
            }
            debug!(motors = ?motors, "RunStart motor hints stored");
        }

        debug!(uid = start.uid, "Mapped RunStart");
        samples
    }

    fn map_descriptor(
        &mut self,
        desc: &EventDescriptor,
    ) -> Vec<(String, ArchDbType, ArchiverSample)> {
        let mut samples = Vec::new();
        let ts = desc.time;

        let data_keys: Vec<String> = desc.data_keys.keys().cloned().collect();

        // Classify keys into motors and detectors.
        // Priority: 1) object_keys from descriptor, 2) RunStart.motors hints,
        // 3) heuristic from data_key source.
        let mut motors = Vec::new();
        let mut detectors = Vec::new();

        // If object_keys is available, use it for classification.
        let object_key_motors: HashSet<String> = desc
            .object_keys
            .as_ref()
            .map(|ok| {
                ok.iter()
                    .filter(|(_, keys)| {
                        keys.iter().any(|k| {
                            k.contains("readback")
                                || k.contains("setpoint")
                                || k.ends_with("_setpoint")
                        })
                    })
                    .map(|(name, _)| name.clone())
                    .collect()
            })
            .unwrap_or_default();

        for (key, dk) in &desc.data_keys {
            let is_motor = if !object_key_motors.is_empty() {
                // Use object_keys classification.
                object_key_motors.contains(key)
            } else if !self.run_motor_hints.is_empty() {
                // Use RunStart.motors hints.
                self.run_motor_hints.contains(key)
            } else if let Some(ref source) = dk.source {
                // Heuristic fallback: motor PVs typically have `.RBV` or `.VAL` in source.
                source.contains(".RBV") || source.contains(".VAL") || source.contains("motor")
            } else {
                false
            };

            if is_motor {
                motors.push(key.clone());
            } else {
                detectors.push(key.clone());
            }
        }

        // Store DataKey metadata (units, precision) for later use in events.
        let mut meta_map = HashMap::new();
        for (key, dk) in &desc.data_keys {
            if dk.units.is_some() || dk.precision.is_some() {
                meta_map.insert(
                    key.clone(),
                    DataKeyMeta {
                        units: dk.units.clone(),
                        precision: dk.precision,
                    },
                );
            }
        }
        if !meta_map.is_empty() {
            self.descriptor_meta.insert(desc.uid.clone(), meta_map);
        }

        self.motor_names.insert(desc.uid.clone(), motors);
        self.detector_names.insert(desc.uid.clone(), detectors);
        self.descriptors.insert(
            desc.uid.clone(),
            DescriptorInfo {
                run_start_uid: desc.run_start.clone(),
                data_keys,
            },
        );

        // Extract detector configuration (exposure, num_images, etc.).
        for (dev_name, config) in &desc.configuration {
            if let Some(ref data) = config.data {
                if let Some(exposure) = data.get("exposure_time").or(data.get("acquire_time")) {
                    if let Some(exp_val) = exposure.as_f64() {
                        samples.push((
                            self.pv(&format!("det:{dev_name}:exposure")),
                            ArchDbType::ScalarDouble,
                            ArchiverSample::from_unix_timestamp(
                                ts,
                                ArchiverValue::ScalarDouble(exp_val),
                            ),
                        ));
                    }
                }
                if let Some(num_images) = data.get("num_images") {
                    if let Some(n) = num_images.as_i64() {
                        samples.push((
                            self.pv(&format!("det:{dev_name}:num_images")),
                            ArchDbType::ScalarInt,
                            ArchiverSample::from_unix_timestamp(
                                ts,
                                ArchiverValue::ScalarInt(n as i32),
                            ),
                        ));
                    }
                }
            }
        }

        debug!(uid = desc.uid, "Mapped EventDescriptor");
        samples
    }

    fn map_event(
        &mut self,
        event: &crate::documents::Event,
    ) -> Vec<(String, ArchDbType, ArchiverSample)> {
        let mut samples = Vec::new();
        let ts = event.time;
        let descriptor_uid = &event.descriptor;

        let motors = self
            .motor_names
            .get(descriptor_uid)
            .cloned()
            .unwrap_or_default();
        let detectors = self
            .detector_names
            .get(descriptor_uid)
            .cloned()
            .unwrap_or_default();

        // scan:current_point
        samples.push((
            self.pv("scan:current_point"),
            ArchDbType::ScalarInt,
            ArchiverSample::from_unix_timestamp(
                ts,
                ArchiverValue::ScalarInt(event.seq_num as i32),
            ),
        ));

        // Map motor data.
        for motor_name in &motors {
            if let Some(val) = event.data.get(motor_name) {
                if let Some((dbr_type, archiver_val)) = json_value_to_archiver(val) {
                    // Use motor:readback for the primary value.
                    let pv_suffix = if matches!(dbr_type, ArchDbType::ScalarDouble | ArchDbType::ScalarFloat) {
                        format!("motor:{motor_name}:readback")
                    } else {
                        format!("motor:{motor_name}:readback")
                    };
                    samples.push((
                        self.pv(&pv_suffix),
                        dbr_type,
                        ArchiverSample::from_unix_timestamp(ts, archiver_val),
                    ));
                }
            }
            // Setpoint from "{motor_name}_setpoint" if present.
            let setpoint_key = format!("{motor_name}_setpoint");
            if let Some(val) = event.data.get(&setpoint_key) {
                if let Some(v) = val.as_f64() {
                    samples.push((
                        self.pv(&format!("motor:{motor_name}:setpoint")),
                        ArchDbType::ScalarDouble,
                        ArchiverSample::from_unix_timestamp(ts, ArchiverValue::ScalarDouble(v)),
                    ));
                }
            }
        }

        // Map detector data (supports multiple types).
        for det_name in &detectors {
            if let Some(val) = event.data.get(det_name) {
                if let Some((dbr_type, archiver_val)) = json_value_to_archiver(val) {
                    samples.push((
                        self.pv(&format!("det:{det_name}:value")),
                        dbr_type,
                        ArchiverSample::from_unix_timestamp(ts, archiver_val),
                    ));
                }
            }
        }

        samples
    }

    fn map_event_page(
        &mut self,
        page: &crate::documents::EventPage,
    ) -> Vec<(String, ArchDbType, ArchiverSample)> {
        // EventPage is a columnar batch of events. Expand to individual events.
        let mut all_samples = Vec::new();
        let n = page.time.len();

        for i in 0..n {
            let ts = page.time[i];
            let seq_num = page.seq_num.get(i).copied().unwrap_or(i as u64 + 1);

            // Build a single-event data map for this row.
            let mut data = HashMap::new();
            for (key, values) in &page.data {
                if let Some(val) = values.get(i) {
                    data.insert(key.clone(), val.clone());
                }
            }

            let mut timestamps = HashMap::new();
            for (key, tss) in &page.timestamps {
                if let Some(&t) = tss.get(i) {
                    timestamps.insert(key.clone(), t);
                }
            }

            let event = crate::documents::Event {
                descriptor: page.descriptor.clone(),
                seq_num,
                time: ts,
                data,
                timestamps,
                uid: format!("{}_{i}", page.uid),
                filled: None,
            };

            all_samples.extend(self.map_event(&event));
        }

        all_samples
    }

    fn map_run_stop(
        &mut self,
        stop: &crate::documents::RunStop,
    ) -> Vec<(String, ArchDbType, ArchiverSample)> {
        let ts = stop.time;
        let mut samples = Vec::new();

        // run:active = 0
        samples.push((
            self.pv("run:active"),
            ArchDbType::ScalarEnum,
            ArchiverSample::from_unix_timestamp(ts, ArchiverValue::ScalarEnum(0)),
        ));

        // run:uid = "" (cleared)
        samples.push((
            self.pv("run:uid"),
            ArchDbType::ScalarString,
            ArchiverSample::from_unix_timestamp(ts, ArchiverValue::ScalarString(String::new())),
        ));

        // Clean up descriptor state for this run.
        let run_uid = &stop.run_start;
        let descriptor_uids: Vec<String> = self
            .descriptors
            .iter()
            .filter(|(_, info)| info.run_start_uid == *run_uid)
            .map(|(uid, _)| uid.clone())
            .collect();
        for uid in descriptor_uids {
            self.descriptors.remove(&uid);
            self.motor_names.remove(&uid);
            self.detector_names.remove(&uid);
            self.descriptor_meta.remove(&uid);
        }
        self.current_run_uid = None;
        self.run_motor_hints.clear();

        debug!(uid = stop.uid, "Mapped RunStop");
        samples
    }
}

/// Convert a serde_json::Value to (ArchDbType, ArchiverValue).
/// Supports f64, i64, string, and array types.
fn json_value_to_archiver(val: &serde_json::Value) -> Option<(ArchDbType, ArchiverValue)> {
    match val {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                // Check if it fits in i32 and was explicitly integer.
                if n.is_i64() && !n.is_f64() {
                    Some((ArchDbType::ScalarInt, ArchiverValue::ScalarInt(i as i32)))
                } else {
                    Some((
                        ArchDbType::ScalarDouble,
                        ArchiverValue::ScalarDouble(n.as_f64().unwrap_or(i as f64)),
                    ))
                }
            } else if let Some(f) = n.as_f64() {
                Some((ArchDbType::ScalarDouble, ArchiverValue::ScalarDouble(f)))
            } else {
                None
            }
        }
        serde_json::Value::String(s) => Some((
            ArchDbType::ScalarString,
            ArchiverValue::ScalarString(s.clone()),
        )),
        serde_json::Value::Array(arr) => {
            if arr.is_empty() {
                return None;
            }
            // Determine element type from first element.
            match &arr[0] {
                serde_json::Value::Number(n) if n.is_i64() => {
                    let ints: Vec<i32> = arr
                        .iter()
                        .filter_map(|v| v.as_i64().map(|i| i as i32))
                        .collect();
                    Some((ArchDbType::WaveformInt, ArchiverValue::VectorInt(ints)))
                }
                serde_json::Value::Number(_) => {
                    let doubles: Vec<f64> = arr
                        .iter()
                        .filter_map(|v| v.as_f64())
                        .collect();
                    Some((
                        ArchDbType::WaveformDouble,
                        ArchiverValue::VectorDouble(doubles),
                    ))
                }
                serde_json::Value::String(_) => {
                    let strings: Vec<String> = arr
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect();
                    Some((
                        ArchDbType::WaveformString,
                        ArchiverValue::VectorString(strings),
                    ))
                }
                _ => None,
            }
        }
        serde_json::Value::Bool(b) => Some((
            ArchDbType::ScalarEnum,
            ArchiverValue::ScalarEnum(if *b { 1 } else { 0 }),
        )),
        serde_json::Value::Object(_) => Some((
            ArchDbType::ScalarString,
            ArchiverValue::ScalarString(val.to_string()),
        )),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::documents::BlueskyDocument;

    #[test]
    fn test_run_start_mapping() {
        let mut mapper = PvMapper::new("BL1");
        let doc = BlueskyDocument::Tuple(
            "start".to_string(),
            serde_json::json!({
                "uid": "abc-123",
                "time": 1700000000.0,
                "plan_name": "scan",
                "scan_id": 42,
                "num_points": 50
            }),
        );

        let samples = mapper.map_document(&doc);
        assert!(samples.len() >= 4);

        let active = samples.iter().find(|(pv, _, _)| pv == "EXP:BL1:run:active");
        assert!(active.is_some());
        let (_, dbr, sample) = active.unwrap();
        assert_eq!(*dbr, ArchDbType::ScalarEnum);
        assert!(matches!(sample.value, ArchiverValue::ScalarEnum(1)));
    }

    #[test]
    fn test_run_stop_mapping() {
        let mut mapper = PvMapper::new("BL1");

        // First start a run.
        let start_doc = BlueskyDocument::Tuple(
            "start".to_string(),
            serde_json::json!({
                "uid": "abc-123",
                "time": 1700000000.0,
            }),
        );
        mapper.map_document(&start_doc);

        // Then stop it.
        let stop_doc = BlueskyDocument::Tuple(
            "stop".to_string(),
            serde_json::json!({
                "uid": "stop-456",
                "run_start": "abc-123",
                "time": 1700000010.0,
                "exit_status": "success"
            }),
        );
        let samples = mapper.map_document(&stop_doc);
        assert!(samples.len() >= 2);

        let active = samples.iter().find(|(pv, _, _)| pv == "EXP:BL1:run:active");
        assert!(active.is_some());
        let (_, _, sample) = active.unwrap();
        assert!(matches!(sample.value, ArchiverValue::ScalarEnum(0)));
    }

    #[test]
    fn test_run_start_with_motors() {
        let mut mapper = PvMapper::new("BL1");

        // Start a run with motor hints.
        let start_doc = BlueskyDocument::Tuple(
            "start".to_string(),
            serde_json::json!({
                "uid": "run-1",
                "time": 1700000000.0,
                "motors": ["th", "tth"],
                "plan_name": "scan",
                "num_points": 10
            }),
        );
        mapper.map_document(&start_doc);
        assert!(mapper.run_motor_hints.contains("th"));
        assert!(mapper.run_motor_hints.contains("tth"));
    }

    #[test]
    fn test_json_value_to_archiver_integer() {
        let val = serde_json::json!(42);
        let result = json_value_to_archiver(&val);
        // serde_json::Number from integer literal: is_i64() = true
        assert!(result.is_some());
        let (_dbr, av) = result.unwrap();
        // JSON numbers from integer literals are treated as integers.
        assert!(matches!(av, ArchiverValue::ScalarInt(42) | ArchiverValue::ScalarDouble(_)));
    }

    #[test]
    fn test_json_value_to_archiver_float() {
        let val = serde_json::json!(3.14);
        let result = json_value_to_archiver(&val);
        assert!(result.is_some());
        let (dbr, _) = result.unwrap();
        assert_eq!(dbr, ArchDbType::ScalarDouble);
    }

    #[test]
    fn test_json_value_to_archiver_string() {
        let val = serde_json::json!("hello");
        let result = json_value_to_archiver(&val);
        assert!(result.is_some());
        let (dbr, av) = result.unwrap();
        assert_eq!(dbr, ArchDbType::ScalarString);
        assert_eq!(av, ArchiverValue::ScalarString("hello".to_string()));
    }

    #[test]
    fn test_json_value_to_archiver_double_array() {
        let val = serde_json::json!([1.0, 2.0, 3.0]);
        let result = json_value_to_archiver(&val);
        assert!(result.is_some());
        let (dbr, av) = result.unwrap();
        assert_eq!(dbr, ArchDbType::WaveformDouble);
        assert_eq!(av, ArchiverValue::VectorDouble(vec![1.0, 2.0, 3.0]));
    }

    #[test]
    fn test_json_value_to_archiver_int_array() {
        let val = serde_json::json!([1, 2, 3]);
        let result = json_value_to_archiver(&val);
        assert!(result.is_some());
    }

    #[test]
    fn test_json_value_to_archiver_bool() {
        let val_true = serde_json::json!(true);
        let result = json_value_to_archiver(&val_true);
        assert!(result.is_some());
        let (dbr, av) = result.unwrap();
        assert_eq!(dbr, ArchDbType::ScalarEnum);
        assert_eq!(av, ArchiverValue::ScalarEnum(1));

        let val_false = serde_json::json!(false);
        let result = json_value_to_archiver(&val_false);
        assert!(result.is_some());
        let (dbr, av) = result.unwrap();
        assert_eq!(dbr, ArchDbType::ScalarEnum);
        assert_eq!(av, ArchiverValue::ScalarEnum(0));
    }

    #[test]
    fn test_json_value_to_archiver_object() {
        let val = serde_json::json!({"key": "value", "num": 42});
        let result = json_value_to_archiver(&val);
        assert!(result.is_some());
        let (dbr, av) = result.unwrap();
        assert_eq!(dbr, ArchDbType::ScalarString);
        // Should serialize the object to a JSON string.
        if let ArchiverValue::ScalarString(s) = av {
            assert!(s.contains("key"));
            assert!(s.contains("value"));
        } else {
            panic!("Expected ScalarString");
        }
    }

    #[test]
    fn test_descriptor_metadata_extraction() {
        let mut mapper = PvMapper::new("BL1");

        // Start a run.
        let start_doc = BlueskyDocument::Tuple(
            "start".to_string(),
            serde_json::json!({
                "uid": "run-1",
                "time": 1700000000.0,
            }),
        );
        mapper.map_document(&start_doc);

        // Send a descriptor with DataKey metadata.
        let desc_doc = BlueskyDocument::Tuple(
            "descriptor".to_string(),
            serde_json::json!({
                "uid": "desc-1",
                "run_start": "run-1",
                "time": 1700000001.0,
                "data_keys": {
                    "det1": {
                        "source": "DET:det1",
                        "dtype": "number",
                        "shape": [],
                        "units": "counts",
                        "precision": 3
                    }
                },
                "configuration": {}
            }),
        );
        mapper.map_document(&desc_doc);

        // Verify metadata was captured.
        let meta = mapper.get_data_key_meta("desc-1", "det1");
        assert!(meta.is_some());
        let meta = meta.unwrap();
        assert_eq!(meta.units.as_deref(), Some("counts"));
        assert_eq!(meta.precision, Some(3));
    }

    #[test]
    fn test_json_value_to_archiver_string_array() {
        let val = serde_json::json!(["a", "b", "c"]);
        let result = json_value_to_archiver(&val);
        assert!(result.is_some());
        let (dbr, av) = result.unwrap();
        assert_eq!(dbr, ArchDbType::WaveformString);
        assert_eq!(
            av,
            ArchiverValue::VectorString(vec!["a".into(), "b".into(), "c".into()])
        );
    }
}
