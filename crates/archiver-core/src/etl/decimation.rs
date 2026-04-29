use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::storage::traits::{EventStream, PostProcessor};
use crate::types::{ArchiverSample, ArchiverValue, EventStreamDesc};

/// Compute the bin number that a timestamp falls into for a given interval.
/// Mirrors Java's `epochSeconds / intervalSecs`. Two PVs aggregating with
/// the same `interval_secs` produce bins anchored on the same wall-clock
/// instants regardless of when their first sample arrived.
pub fn bin_of(ts: SystemTime, interval_secs: u64) -> u64 {
    let secs = ts.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    secs / interval_secs
}

/// Convert a bin number back to the wall-clock instant of its leading edge.
pub fn bin_start(bin: u64, interval_secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(bin.saturating_mul(interval_secs))
}

/// Mean decimation post-processor: averages numeric values over fixed intervals.
pub struct MeanDecimation {
    interval_secs: u64,
}

impl MeanDecimation {
    pub fn new(interval_secs: u64) -> Self {
        Self { interval_secs }
    }
}

impl PostProcessor for MeanDecimation {
    fn name(&self) -> &str {
        "mean"
    }

    fn interval_secs(&self) -> u64 {
        self.interval_secs
    }

    fn process(&self, input: Box<dyn EventStream>) -> Box<dyn EventStream> {
        Box::new(MeanDecimationStream {
            input,
            interval_secs: self.interval_secs,
            buffer: Vec::new(),
            current_bin: None,
            finished: false,
        })
    }
}

struct MeanDecimationStream {
    input: Box<dyn EventStream>,
    interval_secs: u64,
    buffer: Vec<f64>,
    current_bin: Option<u64>,
    finished: bool,
}

impl EventStream for MeanDecimationStream {
    fn description(&self) -> &EventStreamDesc {
        self.input.description()
    }

    fn next_event(&mut self) -> anyhow::Result<Option<ArchiverSample>> {
        if self.finished {
            return Ok(None);
        }

        loop {
            match self.input.next_event()? {
                Some(sample) => {
                    let bin = bin_of(sample.timestamp, self.interval_secs);

                    // Bin transition: emit the prior bin's mean before
                    // starting the new one.
                    if let Some(prev_bin) = self.current_bin
                        && bin != prev_bin
                        && !self.buffer.is_empty()
                    {
                        let mean =
                            self.buffer.iter().sum::<f64>() / self.buffer.len() as f64;
                        let result = ArchiverSample::new(
                            bin_start(prev_bin, self.interval_secs),
                            ArchiverValue::ScalarDouble(mean),
                        );
                        self.buffer.clear();
                        self.current_bin = Some(bin);
                        if let Some(v) = sample.value.as_f64() {
                            self.buffer.push(v);
                        }
                        return Ok(Some(result));
                    }

                    self.current_bin = Some(bin);
                    if let Some(v) = sample.value.as_f64() {
                        self.buffer.push(v);
                    }
                }
                None => {
                    self.finished = true;
                    if let Some(prev_bin) = self.current_bin
                        && !self.buffer.is_empty()
                    {
                        let mean =
                            self.buffer.iter().sum::<f64>() / self.buffer.len() as f64;
                        let result = ArchiverSample::new(
                            bin_start(prev_bin, self.interval_secs),
                            ArchiverValue::ScalarDouble(mean),
                        );
                        self.buffer.clear();
                        return Ok(Some(result));
                    }
                    return Ok(None);
                }
            }
        }
    }
}

/// First-sample decimation: takes only the first sample in each interval.
pub struct FirstSampleDecimation {
    interval_secs: u64,
}

impl FirstSampleDecimation {
    pub fn new(interval_secs: u64) -> Self {
        Self { interval_secs }
    }
}

impl PostProcessor for FirstSampleDecimation {
    fn name(&self) -> &str {
        "firstSample"
    }

    fn interval_secs(&self) -> u64 {
        self.interval_secs
    }

    fn process(&self, input: Box<dyn EventStream>) -> Box<dyn EventStream> {
        Box::new(FirstSampleStream {
            input,
            interval_secs: self.interval_secs,
            current_bin: None,
        })
    }
}

struct FirstSampleStream {
    input: Box<dyn EventStream>,
    interval_secs: u64,
    /// The bin we've already emitted a sample for, so subsequent samples
    /// in the same bin are skipped. `None` means "haven't emitted yet".
    current_bin: Option<u64>,
}

impl EventStream for FirstSampleStream {
    fn description(&self) -> &EventStreamDesc {
        self.input.description()
    }

    fn next_event(&mut self) -> anyhow::Result<Option<ArchiverSample>> {
        loop {
            match self.input.next_event()? {
                Some(sample) => {
                    let bin = bin_of(sample.timestamp, self.interval_secs);
                    if self.current_bin != Some(bin) {
                        self.current_bin = Some(bin);
                        return Ok(Some(sample));
                    }
                }
                None => return Ok(None),
            }
        }
    }
}
