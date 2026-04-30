//! `count_N` and `ncount_N` post-processors.
//!
//! - `count`: number of samples in each N-second bin.
//! - `ncount`: total number of samples across the whole query (single emitted
//!   sample at the start of the input range, mirroring the Java
//!   `NCount.java` semantics — useful as a quick sanity check from
//!   archiver clients).

use std::time::SystemTime;

use crate::storage::traits::{EventStream, PostProcessor};
use crate::types::{ArchiverSample, ArchiverValue, EventStreamDesc};

pub struct CountPostProcessor {
    interval_secs: u64,
}

impl CountPostProcessor {
    pub fn new(interval_secs: u64) -> Self {
        Self { interval_secs }
    }
}

impl PostProcessor for CountPostProcessor {
    fn name(&self) -> &str {
        "count"
    }
    fn interval_secs(&self) -> u64 {
        self.interval_secs
    }
    fn process(&self, input: Box<dyn EventStream>) -> Box<dyn EventStream> {
        Box::new(CountStream {
            input,
            interval_secs: self.interval_secs,
            count: 0,
            current_bin: None,
            finished: false,
        })
    }
}

struct CountStream {
    input: Box<dyn EventStream>,
    interval_secs: u64,
    count: u64,
    /// Wall-clock-aligned bin (epoch_secs / interval_secs) for the
    /// in-progress count.
    current_bin: Option<u64>,
    finished: bool,
}

impl EventStream for CountStream {
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
                    let bin = crate::etl::decimation::bin_of(sample.timestamp, self.interval_secs);
                    if let Some(prev_bin) = self.current_bin
                        && bin != prev_bin
                        && self.count > 0
                    {
                        let result = ArchiverSample::new(
                            crate::etl::decimation::bin_start(prev_bin, self.interval_secs),
                            ArchiverValue::ScalarDouble(self.count as f64),
                        );
                        self.count = 1;
                        self.current_bin = Some(bin);
                        return Ok(Some(result));
                    }
                    self.current_bin = Some(bin);
                    self.count += 1;
                }
                None => {
                    self.finished = true;
                    if let Some(prev_bin) = self.current_bin
                        && self.count > 0
                    {
                        let result = ArchiverSample::new(
                            crate::etl::decimation::bin_start(prev_bin, self.interval_secs),
                            ArchiverValue::ScalarDouble(self.count as f64),
                        );
                        self.count = 0;
                        return Ok(Some(result));
                    }
                    return Ok(None);
                }
            }
        }
    }
}

/// `ncount_N` — emits a single sample whose value is the total number of
/// samples in the input. The interval parameter is ignored, mirroring the
/// Java `NCount` behaviour.
pub struct NCountPostProcessor;

impl NCountPostProcessor {
    pub fn new(_interval_secs: u64) -> Self {
        Self
    }
}

impl PostProcessor for NCountPostProcessor {
    fn name(&self) -> &str {
        "ncount"
    }
    fn interval_secs(&self) -> u64 {
        0
    }
    fn process(&self, input: Box<dyn EventStream>) -> Box<dyn EventStream> {
        Box::new(NCountStream {
            input,
            count: 0,
            first_ts: None,
            done: false,
        })
    }
}

struct NCountStream {
    input: Box<dyn EventStream>,
    count: u64,
    first_ts: Option<SystemTime>,
    done: bool,
}

impl EventStream for NCountStream {
    fn description(&self) -> &EventStreamDesc {
        self.input.description()
    }

    fn next_event(&mut self) -> anyhow::Result<Option<ArchiverSample>> {
        if self.done {
            return Ok(None);
        }
        while let Some(sample) = self.input.next_event()? {
            if self.first_ts.is_none() {
                self.first_ts = Some(sample.timestamp);
            }
            self.count += 1;
        }
        self.done = true;
        match self.first_ts {
            Some(ts) => Ok(Some(ArchiverSample::new(
                ts,
                ArchiverValue::ScalarDouble(self.count as f64),
            ))),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ArchDbType;
    use std::time::{Duration, UNIX_EPOCH};

    struct VecStream {
        desc: EventStreamDesc,
        items: std::vec::IntoIter<(u64, f64)>,
        start: SystemTime,
    }

    impl VecStream {
        fn new(items: Vec<(u64, f64)>) -> Self {
            Self {
                desc: EventStreamDesc {
                    pv_name: "TEST".to_string(),
                    db_type: ArchDbType::ScalarDouble,
                    year: 2024,
                    element_count: Some(1),
                    headers: Vec::new(),
                },
                items: items.into_iter(),
                start: UNIX_EPOCH + Duration::from_secs(1_700_000_000),
            }
        }
    }

    impl EventStream for VecStream {
        fn description(&self) -> &EventStreamDesc {
            &self.desc
        }
        fn next_event(&mut self) -> anyhow::Result<Option<ArchiverSample>> {
            Ok(self.items.next().map(|(offset, v)| {
                ArchiverSample::new(
                    self.start + Duration::from_secs(offset),
                    ArchiverValue::ScalarDouble(v),
                )
            }))
        }
    }

    fn drain(pp: Box<dyn PostProcessor>, items: Vec<(u64, f64)>) -> Vec<f64> {
        let stream = pp.process(Box::new(VecStream::new(items)));
        let mut out = Vec::new();
        let mut s = stream;
        while let Some(sample) = s.next_event().unwrap() {
            if let ArchiverValue::ScalarDouble(v) = sample.value {
                out.push(v);
            }
        }
        out
    }

    #[test]
    fn count_per_bin() {
        let pp: Box<dyn PostProcessor> = Box::new(CountPostProcessor::new(10));
        // 4 samples in [0..10), 2 in [10..20).
        let items = vec![(0, 1.0), (3, 1.0), (6, 1.0), (9, 1.0), (10, 1.0), (12, 1.0)];
        assert_eq!(drain(pp, items), vec![4.0, 2.0]);
    }

    #[test]
    fn ncount_total() {
        let pp: Box<dyn PostProcessor> = Box::new(NCountPostProcessor::new(0));
        let items = vec![(0, 1.0), (1, 2.0), (2, 3.0), (10, 4.0), (12, 5.0)];
        assert_eq!(drain(pp, items), vec![5.0]);
    }

    #[test]
    fn ncount_empty() {
        let pp: Box<dyn PostProcessor> = Box::new(NCountPostProcessor::new(0));
        assert!(drain(pp, vec![]).is_empty());
    }
}
