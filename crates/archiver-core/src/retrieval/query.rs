use std::time::{Duration, SystemTime};

use crate::retrieval::merge::MergedEventStream;
use crate::storage::traits::{EventStream, PostProcessor, StoragePlugin};
use crate::types::{ArchiverSample, EventStreamDesc};

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
            year: chrono::Utc::now().year(),
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
    use crate::retrieval::postprocessors::{counts, last_sample, statistics};
    match parts[0] {
        "mean" => Some(Box::new(crate::etl::decimation::MeanDecimation::new(interval))),
        "firstSample" => Some(Box::new(crate::etl::decimation::FirstSampleDecimation::new(interval))),
        "max" => Some(Box::new(statistics::MaxPostProcessor::new(interval))),
        "min" => Some(Box::new(statistics::MinPostProcessor::new(interval))),
        "std" => Some(Box::new(statistics::StdPostProcessor::new(interval))),
        "median" => Some(Box::new(statistics::MedianPostProcessor::new(interval))),
        "variance" => Some(Box::new(statistics::VariancePostProcessor::new(interval))),
        "rms" => Some(Box::new(statistics::RmsPostProcessor::new(interval))),
        "count" => Some(Box::new(counts::CountPostProcessor::new(interval))),
        "ncount" => Some(Box::new(counts::NCountPostProcessor::new(interval))),
        "lastSample" => Some(Box::new(last_sample::LastSamplePostProcessor::new(interval))),
        _ => None,
    }
}

use chrono::Datelike;

// ── TwoWeekRaw post-processor ──
// Compatible with Java EPICS Archiver Appliance's TwoWeekRaw policy:
// - Data within the last 2 weeks: pass through raw
// - Data older than 2 weeks: firstSample binning (one per 15-minute bin)

const TWO_WEEKS_SECS: u64 = 2 * 7 * 86400;
const SPARSIFY_INTERVAL_SECS: u64 = 900; // 15 minutes (PostProcessors.DEFAULT_SUMMARIZING_INTERVAL)

pub struct TwoWeekRawProcessor;

impl PostProcessor for TwoWeekRawProcessor {
    fn name(&self) -> &str {
        "twoweek"
    }

    fn interval_secs(&self) -> u64 {
        SPARSIFY_INTERVAL_SECS
    }

    fn process(&self, input: Box<dyn EventStream>) -> Box<dyn EventStream> {
        let two_weeks_ago = SystemTime::now() - Duration::from_secs(TWO_WEEKS_SECS);
        Box::new(TwoWeekRawStream {
            input,
            two_weeks_ago,
            interval_secs: SPARSIFY_INTERVAL_SECS,
            last_bin: None,
        })
    }
}

struct TwoWeekRawStream {
    input: Box<dyn EventStream>,
    two_weeks_ago: SystemTime,
    interval_secs: u64,
    last_bin: Option<u64>,
}

impl EventStream for TwoWeekRawStream {
    fn description(&self) -> &EventStreamDesc {
        self.input.description()
    }

    fn next_event(&mut self) -> anyhow::Result<Option<ArchiverSample>> {
        loop {
            match self.input.next_event()? {
                Some(sample) => {
                    if sample.timestamp >= self.two_weeks_ago {
                        // Recent data: pass through raw
                        return Ok(Some(sample));
                    }
                    // Old data: firstSample binning
                    let epoch_secs = sample
                        .timestamp
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    let bin = epoch_secs / self.interval_secs;
                    if self.last_bin != Some(bin) {
                        self.last_bin = Some(bin);
                        return Ok(Some(sample));
                    }
                    // Skip: same bin as previous sample
                }
                None => return Ok(None),
            }
        }
    }
}

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
