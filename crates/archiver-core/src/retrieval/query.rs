use std::time::SystemTime;

use crate::retrieval::merge::MergedEventStream;
use crate::storage::traits::{EventStream, PostProcessor, StoragePlugin};
use crate::types::EventStreamDesc;

/// Execute a data query across a storage plugin, optionally applying a post-processor.
pub async fn query_data(
    storage: &dyn StoragePlugin,
    pv: &str,
    start: SystemTime,
    end: SystemTime,
    post_processor: Option<Box<dyn PostProcessor>>,
) -> anyhow::Result<Box<dyn EventStream>> {
    let streams = storage.get_data(pv, start, end).await?;

    if streams.is_empty() {
        let desc = EventStreamDesc {
            pv_name: pv.to_string(),
            db_type: crate::types::ArchDbType::ScalarDouble,
            year: chrono::Utc::now().year() as i32,
            element_count: None,
            headers: Vec::new(),
        };
        return Ok(Box::new(EmptyStream { desc }));
    }

    let desc = streams[0].description().clone();
    let merged: Box<dyn EventStream> = Box::new(MergedEventStream::new(desc, streams));

    match post_processor {
        Some(pp) => Ok(pp.process(merged)),
        None => Ok(merged),
    }
}

/// Parse a post-processor spec like "mean_600" or "firstSample_3600".
pub fn parse_post_processor(spec: &str) -> Option<Box<dyn PostProcessor>> {
    let parts: Vec<&str> = spec.splitn(2, '_').collect();
    if parts.len() != 2 {
        return None;
    }
    let interval: u64 = parts[1].parse().ok()?;
    match parts[0] {
        "mean" => Some(Box::new(
            crate::etl::decimation::MeanDecimation::new(interval),
        )),
        "firstSample" => Some(Box::new(
            crate::etl::decimation::FirstSampleDecimation::new(interval),
        )),
        "max" => Some(Box::new(
            crate::retrieval::postprocessors::statistics::MaxPostProcessor::new(interval),
        )),
        "min" => Some(Box::new(
            crate::retrieval::postprocessors::statistics::MinPostProcessor::new(interval),
        )),
        "std" => Some(Box::new(
            crate::retrieval::postprocessors::statistics::StdPostProcessor::new(interval),
        )),
        _ => None,
    }
}

use chrono::Datelike;

struct EmptyStream {
    desc: EventStreamDesc,
}

impl EventStream for EmptyStream {
    fn description(&self) -> &EventStreamDesc {
        &self.desc
    }

    fn next_event(&mut self) -> anyhow::Result<Option<crate::types::ArchiverSample>> {
        Ok(None)
    }
}
