use std::time::{Duration, SystemTime};

use crate::retrieval::merge::MergedEventStream;
use crate::storage::traits::{EventStream, PostProcessor, StoragePlugin};
use crate::types::{ArchiverSample, EventStreamDesc};

/// Execute a data query across a storage plugin, optionally applying a post-processor.
///
/// Prepends the last sample whose timestamp is strictly before `start` so
/// plots stay continuous when the requested window starts in a gap. Java's
/// `getLastEventOfPreviousPartitionBeforeTimeAsStream` (5666a8b5) does the
/// same: a slow PV last sampled 10 days ago, queried for the last 5
/// minutes, returns that 10-day-old sample as the leading point instead
/// of an empty stream.
pub async fn query_data(
    storage: &dyn StoragePlugin,
    pv: &str,
    start: SystemTime,
    end: SystemTime,
    post_processor: Option<Box<dyn PostProcessor>>,
) -> anyhow::Result<Box<dyn EventStream>> {
    let streams = storage.get_data(pv, start, end).await?;
    let prefix = storage.get_last_event_before(pv, start).await.unwrap_or(None);

    if streams.is_empty() && prefix.is_none() {
        let desc = EventStreamDesc {
            pv_name: pv.to_string(),
            db_type: crate::types::ArchDbType::ScalarDouble,
            year: chrono::Utc::now().year(),
            element_count: None,
            headers: Vec::new(),
        };
        return Ok(Box::new(EmptyStream { desc }));
    }

    // Pick a description: prefer the first in-range stream's description
    // (it carries the right year/dbr_type for the PV); fall back to a
    // synthetic one if only the prefix sample exists.
    let desc = if let Some(s) = streams.first() {
        s.description().clone()
    } else {
        EventStreamDesc {
            pv_name: pv.to_string(),
            db_type: crate::types::ArchDbType::ScalarDouble,
            year: chrono::Utc::now().year(),
            element_count: None,
            headers: Vec::new(),
        }
    };

    let mut all_streams = Vec::with_capacity(streams.len() + 1);
    if let Some(sample) = prefix {
        all_streams.push(Box::new(SingleSampleStream {
            sample: Some(sample),
            desc: desc.clone(),
        }) as Box<dyn EventStream>);
    }
    all_streams.extend(streams);

    let merged: Box<dyn EventStream> = Box::new(MergedEventStream::new(desc, all_streams));

    match post_processor {
        Some(pp) => Ok(pp.process(merged)),
        None => Ok(merged),
    }
}

/// One-shot stream that yields a single pre-built sample. Used to prepend
/// the prior-partition continuity sample to a query's results.
struct SingleSampleStream {
    sample: Option<ArchiverSample>,
    desc: EventStreamDesc,
}

impl EventStream for SingleSampleStream {
    fn description(&self) -> &EventStreamDesc {
        &self.desc
    }

    fn next_event(&mut self) -> anyhow::Result<Option<ArchiverSample>> {
        Ok(self.sample.take())
    }
}

/// Parse a post-processor spec like "mean_600" or "firstSample_3600".
///
/// Java's `SummaryStatsPostProcessor` enforces `intervalSecs >= 1`
/// (CSSTUDIO-2134) — without that floor, `mean_0` makes every sample its
/// own bin (`elapsed >= 0` always true) which both misuses the operator
/// and risks divide-by-zero in any future `epoch_secs / interval_secs`
/// path. Reject `_0` here so the API surface is consistent.
pub fn parse_post_processor(spec: &str) -> Option<Box<dyn PostProcessor>> {
    let parts: Vec<&str> = spec.splitn(2, '_').collect();
    if parts.len() != 2 {
        return None;
    }
    let interval: u64 = parts[1].parse().ok()?;
    if interval == 0 {
        return None;
    }
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
