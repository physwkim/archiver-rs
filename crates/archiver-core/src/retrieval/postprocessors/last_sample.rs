//! `lastSample_N` — emits the last sample of each N-second bin.
//!
//! Buffers one event per bin and yields it when the bin closes (next event
//! crosses the boundary, or the input ends).

use crate::storage::traits::{EventStream, PostProcessor};
use crate::types::{ArchiverSample, EventStreamDesc};

pub struct LastSamplePostProcessor {
    interval_secs: u64,
}

impl LastSamplePostProcessor {
    pub fn new(interval_secs: u64) -> Self {
        Self { interval_secs }
    }
}

impl PostProcessor for LastSamplePostProcessor {
    fn name(&self) -> &str {
        "lastSample"
    }
    fn interval_secs(&self) -> u64 {
        self.interval_secs
    }
    fn process(&self, input: Box<dyn EventStream>) -> Box<dyn EventStream> {
        Box::new(LastSampleStream {
            input,
            interval_secs: self.interval_secs,
            current_bin: None,
            pending: None,
        })
    }
}

struct LastSampleStream {
    input: Box<dyn EventStream>,
    interval_secs: u64,
    /// Wall-clock-aligned bin (epoch_secs / interval_secs) of `pending`.
    current_bin: Option<u64>,
    /// Last sample seen in the current bin, awaiting bin close.
    pending: Option<ArchiverSample>,
}

impl EventStream for LastSampleStream {
    fn description(&self) -> &EventStreamDesc {
        self.input.description()
    }

    fn next_event(&mut self) -> anyhow::Result<Option<ArchiverSample>> {
        loop {
            match self.input.next_event()? {
                Some(sample) => {
                    let bin = crate::etl::decimation::bin_of(sample.timestamp, self.interval_secs);
                    if self.current_bin != Some(bin) {
                        // Crossed the boundary: emit the pending sample and
                        // start a fresh bin with the current one as the new
                        // pending.
                        let emit = self.pending.take();
                        self.current_bin = Some(bin);
                        self.pending = Some(sample);
                        if let Some(out) = emit {
                            return Ok(Some(out));
                        }
                        // No pending in the prior bin (first ever sample);
                        // continue to wait for next.
                        continue;
                    }
                    // Still in the current bin: replace pending.
                    self.pending = Some(sample);
                }
                None => {
                    // Drain the final bin.
                    return Ok(self.pending.take());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ArchDbType, ArchiverValue};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

    #[test]
    fn last_sample_per_bin() {
        // 10s bins. Bin [0,10): values 1,2,3 → emit 3. Bin [10,20): values 4,5 → emit 5.
        let pp = LastSamplePostProcessor::new(10);
        let mut s = pp.process(Box::new(VecStream::new(vec![
            (0, 1.0),
            (3, 2.0),
            (9, 3.0),
            (10, 4.0),
            (15, 5.0),
        ])));
        let v1 = match s.next_event().unwrap().unwrap().value {
            ArchiverValue::ScalarDouble(v) => v,
            _ => panic!("wrong type"),
        };
        let v2 = match s.next_event().unwrap().unwrap().value {
            ArchiverValue::ScalarDouble(v) => v,
            _ => panic!("wrong type"),
        };
        assert_eq!(v1, 3.0);
        assert_eq!(v2, 5.0);
        assert!(s.next_event().unwrap().is_none());
    }
}
