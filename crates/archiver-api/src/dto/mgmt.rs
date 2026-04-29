use serde::{Deserialize, Serialize};

use archiver_core::registry::{PvRecord, PvStatus, SampleMode};

#[derive(Serialize, Deserialize)]
pub struct BulkResult {
    #[serde(rename = "pvName")]
    pub pv_name: String,
    pub status: String,
}

#[derive(Serialize)]
pub struct ReportEntry {
    #[serde(rename = "pvName")]
    pub pv_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(rename = "lastEvent", skip_serializing_if = "Option::is_none")]
    pub last_event: Option<String>,
}

#[derive(Deserialize)]
pub struct ClusterParam {
    #[serde(default)]
    pub cluster: Option<bool>,
}

#[derive(Deserialize)]
pub struct MatchingPvsParams {
    pub pv: String,
    #[serde(default)]
    pub cluster: Option<bool>,
}

#[derive(Deserialize)]
pub struct PvStatusParams {
    pub pv: String,
    #[serde(default)]
    pub cluster: Option<bool>,
}

#[derive(Serialize)]
pub struct PvStatusResponse {
    pub pv_name: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dbr_type: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub element_count: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_event_timestamp: Option<String>,
}

#[derive(Deserialize, Clone)]
pub struct ArchivePvRequest {
    pub pv: Option<String>,
    #[serde(default)]
    pub sampling_period: Option<f64>,
    #[serde(default)]
    pub sampling_method: Option<String>,
    #[serde(default)]
    pub appliance: Option<String>,
}

#[derive(Deserialize)]
pub struct PausePvParams {
    pub pv: String,
}

#[derive(Serialize)]
pub struct PvCountResponse {
    pub total: u64,
    pub active: u64,
    pub paused: u64,
    /// Number of cluster peers that failed to respond (omitted when zero).
    #[serde(rename = "failedPeers", skip_serializing_if = "Option::is_none")]
    pub failed_peers: Option<usize>,
}

#[derive(Deserialize)]
pub struct DeletePvParams {
    pub pv: String,
    #[serde(rename = "deleteData", default)]
    pub delete_data: Option<bool>,
}

#[derive(Deserialize)]
pub struct ChangeParamsQuery {
    pub pv: String,
    #[serde(default)]
    pub samplingperiod: Option<f64>,
    #[serde(default)]
    pub samplingmethod: Option<String>,
}

#[derive(Deserialize)]
pub struct PvNameParam {
    pub pv: String,
}

#[derive(Serialize)]
pub struct PvTypeInfoResponse {
    #[serde(rename = "pvName")]
    pub pv_name: String,
    #[serde(rename = "DBRType")]
    pub dbr_type: i32,
    #[serde(rename = "samplingMethod")]
    pub sampling_method: String,
    #[serde(rename = "samplingPeriod")]
    pub sampling_period: f64,
    #[serde(rename = "elementCount")]
    pub element_count: i32,
    pub status: String,
    #[serde(rename = "PREC", skip_serializing_if = "Option::is_none")]
    pub prec: Option<String>,
    #[serde(rename = "EGU", skip_serializing_if = "Option::is_none")]
    pub egu: Option<String>,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(rename = "aliasFor", skip_serializing_if = "Option::is_none")]
    pub alias_for: Option<String>,
    #[serde(rename = "archiveFields", skip_serializing_if = "Vec::is_empty", default)]
    pub archive_fields: Vec<String>,
    #[serde(rename = "policyName", skip_serializing_if = "Option::is_none")]
    pub policy_name: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ExportRecord {
    #[serde(rename = "pvName")]
    pub pv_name: String,
    #[serde(rename = "DBRType")]
    pub dbr_type: i32,
    #[serde(rename = "samplingMethod")]
    pub sampling_method: String,
    #[serde(rename = "samplingPeriod")]
    pub sampling_period: f64,
    #[serde(rename = "elementCount")]
    pub element_count: i32,
    #[serde(rename = "PREC", skip_serializing_if = "Option::is_none")]
    pub prec: Option<String>,
    #[serde(rename = "EGU", skip_serializing_if = "Option::is_none")]
    pub egu: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(rename = "createdAt", skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    /// When set, the row is an alias pointing at this target PV name.
    #[serde(rename = "aliasFor", skip_serializing_if = "Option::is_none", default)]
    pub alias_for: Option<String>,
    /// EPICS metadata field names archived alongside the value (.HIHI/.LOLO/...).
    #[serde(rename = "archiveFields", skip_serializing_if = "Option::is_none", default)]
    pub archive_fields: Option<Vec<String>>,
    /// Policy that selected the sampling configuration for this PV.
    #[serde(rename = "policyName", skip_serializing_if = "Option::is_none", default)]
    pub policy_name: Option<String>,
}

// --- Conversion functions ---

/// Minimum allowed scan period (100ms).
const MIN_SCAN_PERIOD_SECS: f64 = 0.1;

pub fn parse_sample_mode(method: Option<&str>, period: Option<f64>) -> SampleMode {
    match method {
        Some("scan") | Some("Scan") | Some("SCAN") => {
            let period_secs = period.unwrap_or(1.0).max(MIN_SCAN_PERIOD_SECS);
            SampleMode::Scan { period_secs }
        }
        _ => SampleMode::Monitor,
    }
}

pub fn record_to_report_entry(r: PvRecord) -> ReportEntry {
    let status_str = match r.status {
        PvStatus::Active => "Being archived",
        PvStatus::Paused => "Paused",
        PvStatus::Error => "Error",
        PvStatus::Inactive => "Inactive",
    };
    ReportEntry {
        pv_name: r.pv_name,
        status: Some(status_str.to_string()),
        last_event: r
            .last_timestamp
            .map(|ts| chrono::DateTime::<chrono::Utc>::from(ts).to_rfc3339()),
    }
}

pub fn record_to_type_info(r: &PvRecord) -> PvTypeInfoResponse {
    let (method, period) = match &r.sample_mode {
        SampleMode::Monitor => ("Monitor".to_string(), 0.0),
        SampleMode::Scan { period_secs } => ("Scan".to_string(), *period_secs),
    };
    let status_str = match r.status {
        PvStatus::Active => "Being archived",
        PvStatus::Paused => "Paused",
        PvStatus::Error => "Error",
        PvStatus::Inactive => "Inactive",
    };
    PvTypeInfoResponse {
        pv_name: r.pv_name.clone(),
        dbr_type: r.dbr_type as i32,
        sampling_method: method,
        sampling_period: period,
        element_count: r.element_count,
        status: status_str.to_string(),
        prec: r.prec.clone(),
        egu: r.egu.clone(),
        created_at: r.created_at.to_rfc3339(),
        alias_for: r.alias_for.clone(),
        archive_fields: r.archive_fields.clone(),
        policy_name: r.policy_name.clone(),
    }
}
