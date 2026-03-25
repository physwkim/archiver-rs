//! Bluesky document types — deserialized from Kafka JSON messages.
//!
//! These are parsing-only types. They are never stored directly; instead,
//! the PV mapper converts them to ArchiverSamples on special EXP:* PVs.

use std::collections::HashMap;

use serde::Deserialize;

/// Top-level Bluesky document envelope as published to Kafka.
///
/// Bluesky-kafka publishes documents as JSON arrays: `["start", {doc}]`
/// or as objects with `name` and `doc` fields.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum BlueskyDocument {
    /// Array form: ["name", {doc}]
    Tuple(String, serde_json::Value),
    /// Object form: {"name": "start", "doc": {...}}
    Object { name: String, doc: serde_json::Value },
}

impl BlueskyDocument {
    pub fn name(&self) -> &str {
        match self {
            Self::Tuple(name, _) => name,
            Self::Object { name, .. } => name,
        }
    }

    pub fn doc(&self) -> &serde_json::Value {
        match self {
            Self::Tuple(_, doc) => doc,
            Self::Object { doc, .. } => doc,
        }
    }

    /// Parse the inner document into a typed variant.
    pub fn parse(&self) -> Option<TypedDocument> {
        let name = self.name();
        let doc = self.doc();
        match name {
            "start" => serde_json::from_value(doc.clone()).ok().map(TypedDocument::Start),
            "descriptor" => serde_json::from_value(doc.clone())
                .ok()
                .map(TypedDocument::Descriptor),
            "event" => serde_json::from_value(doc.clone()).ok().map(TypedDocument::Event),
            "event_page" => serde_json::from_value(doc.clone())
                .ok()
                .map(TypedDocument::EventPage),
            "stop" => serde_json::from_value(doc.clone()).ok().map(TypedDocument::Stop),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum TypedDocument {
    Start(RunStart),
    Descriptor(EventDescriptor),
    Event(Event),
    EventPage(EventPage),
    Stop(RunStop),
}

#[derive(Debug, Clone, Deserialize)]
pub struct RunStart {
    pub uid: String,
    pub time: f64,
    #[serde(default)]
    pub plan_name: Option<String>,
    #[serde(default)]
    pub scan_id: Option<i64>,
    #[serde(default)]
    pub hints: Option<serde_json::Value>,
    #[serde(default)]
    pub motors: Option<Vec<String>>,
    #[serde(default)]
    pub num_points: Option<u64>,
    #[serde(default)]
    pub scan_type: Option<String>,
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EventDescriptor {
    pub uid: String,
    pub run_start: String,
    pub time: f64,
    #[serde(default)]
    pub data_keys: HashMap<String, DataKey>,
    #[serde(default)]
    pub configuration: HashMap<String, Configuration>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub object_keys: Option<HashMap<String, Vec<String>>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DataKey {
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub dtype: Option<String>,
    #[serde(default)]
    pub shape: Option<Vec<i64>>,
    #[serde(default)]
    pub units: Option<String>,
    #[serde(default)]
    pub precision: Option<i32>,
    #[serde(default)]
    pub lower_ctrl_limit: Option<f64>,
    #[serde(default)]
    pub upper_ctrl_limit: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Configuration {
    #[serde(default)]
    pub data: Option<HashMap<String, serde_json::Value>>,
    #[serde(default)]
    pub data_keys: Option<HashMap<String, DataKey>>,
    #[serde(default)]
    pub timestamps: Option<HashMap<String, f64>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Event {
    pub descriptor: String,
    pub seq_num: u64,
    pub time: f64,
    #[serde(default)]
    pub data: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub timestamps: HashMap<String, f64>,
    pub uid: String,
    #[serde(default)]
    pub filled: Option<HashMap<String, bool>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EventPage {
    pub descriptor: String,
    #[serde(default)]
    pub seq_num: Vec<u64>,
    #[serde(default)]
    pub time: Vec<f64>,
    #[serde(default)]
    pub data: HashMap<String, Vec<serde_json::Value>>,
    #[serde(default)]
    pub timestamps: HashMap<String, Vec<f64>>,
    pub uid: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RunStop {
    pub uid: String,
    pub run_start: String,
    pub time: f64,
    #[serde(default)]
    pub exit_status: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub num_events: Option<HashMap<String, u64>>,
}
