use crate::storage::traits::{EventStream, PostProcessor};
use crate::types::{ArchiverSample, ArchiverValue, EventStreamDesc};

/// Generate a `PostProcessor` impl backed by `StatStream` for one `StatOp`.
macro_rules! stat_processor {
    ($Type:ident, $name:literal, $op:expr) => {
        pub struct $Type {
            interval_secs: u64,
        }

        impl $Type {
            pub fn new(interval_secs: u64) -> Self {
                Self { interval_secs }
            }
        }

        impl PostProcessor for $Type {
            fn name(&self) -> &str {
                $name
            }
            fn interval_secs(&self) -> u64 {
                self.interval_secs
            }
            fn process(&self, input: Box<dyn EventStream>) -> Box<dyn EventStream> {
                Box::new(StatStream::new(input, self.interval_secs, $op))
            }
        }
    };
}

stat_processor!(MaxPostProcessor, "max", StatOp::Max);
stat_processor!(MinPostProcessor, "min", StatOp::Min);
stat_processor!(StdPostProcessor, "std", StatOp::Std);
stat_processor!(MedianPostProcessor, "median", StatOp::Median);
stat_processor!(VariancePostProcessor, "variance", StatOp::Variance);
stat_processor!(RmsPostProcessor, "rms", StatOp::Rms);

// ── Shared implementation ──

enum StatOp {
    Max,
    Min,
    Std,
    Median,
    Variance,
    Rms,
}

struct StatStream {
    input: Box<dyn EventStream>,
    interval_secs: u64,
    op: StatOp,
    buffer: Vec<f64>,
    window_start: Option<std::time::SystemTime>,
    finished: bool,
}

impl StatStream {
    fn new(input: Box<dyn EventStream>, interval_secs: u64, op: StatOp) -> Self {
        Self {
            input,
            interval_secs,
            op,
            buffer: Vec::new(),
            window_start: None,
            finished: false,
        }
    }

    fn compute(&self) -> f64 {
        match self.op {
            StatOp::Max => self
                .buffer
                .iter()
                .cloned()
                .fold(f64::NEG_INFINITY, f64::max),
            StatOp::Min => self
                .buffer
                .iter()
                .cloned()
                .fold(f64::INFINITY, f64::min),
            StatOp::Std => sample_std(&self.buffer),
            StatOp::Variance => {
                if self.buffer.len() < 2 {
                    return 0.0;
                }
                let n = self.buffer.len() as f64;
                let mean = self.buffer.iter().sum::<f64>() / n;
                self.buffer.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (n - 1.0)
            }
            StatOp::Median => {
                if self.buffer.is_empty() {
                    return 0.0;
                }
                let mut sorted: Vec<f64> = self.buffer.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let mid = sorted.len() / 2;
                if sorted.len().is_multiple_of(2) {
                    (sorted[mid - 1] + sorted[mid]) / 2.0
                } else {
                    sorted[mid]
                }
            }
            StatOp::Rms => {
                if self.buffer.is_empty() {
                    return 0.0;
                }
                let n = self.buffer.len() as f64;
                let sum_sq: f64 = self.buffer.iter().map(|v| v * v).sum();
                (sum_sq / n).sqrt()
            }
        }
    }
}

fn sample_std(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (n - 1.0);
    variance.sqrt()
}

impl EventStream for StatStream {
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
                    let ts = sample.timestamp;
                    let window_start = *self.window_start.get_or_insert(ts);
                    let elapsed = ts
                        .duration_since(window_start)
                        .unwrap_or_default()
                        .as_secs();

                    if elapsed >= self.interval_secs && !self.buffer.is_empty() {
                        let result_val = self.compute();
                        let result = ArchiverSample::new(
                            window_start,
                            ArchiverValue::ScalarDouble(result_val),
                        );
                        self.buffer.clear();
                        self.window_start = Some(ts);
                        if let Some(v) = sample.value.as_f64() {
                            self.buffer.push(v);
                        }
                        return Ok(Some(result));
                    }

                    if let Some(v) = sample.value.as_f64() {
                        self.buffer.push(v);
                    }
                }
                None => {
                    self.finished = true;
                    if !self.buffer.is_empty() {
                        let result_val = self.compute();
                        let result = ArchiverSample::new(
                            self.window_start.expect("non-empty buffer implies window_start set"),
                            ArchiverValue::ScalarDouble(result_val),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ArchDbType;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    /// Toy stream that yields a fixed list of (offset_seconds, value) pairs
    /// starting from `start`.
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
    fn median_odd_and_even() {
        // 10s bin, values 1..=5 in first bin → median 3.
        let pp: Box<dyn PostProcessor> = Box::new(MedianPostProcessor::new(10));
        let items = vec![(0, 1.0), (1, 2.0), (2, 3.0), (3, 4.0), (4, 5.0)];
        assert_eq!(drain(pp, items), vec![3.0]);

        // even count → average of two middles
        let pp: Box<dyn PostProcessor> = Box::new(MedianPostProcessor::new(10));
        let items = vec![(0, 1.0), (1, 2.0), (2, 3.0), (3, 4.0)];
        assert_eq!(drain(pp, items), vec![2.5]);
    }

    #[test]
    fn variance_sample_formula() {
        // values 2,4,4,4,5,5,7,9 → mean 5, sample variance = 32/7 ≈ 4.571
        let pp: Box<dyn PostProcessor> = Box::new(VariancePostProcessor::new(100));
        let items: Vec<(u64, f64)> =
            (0u64..8).zip([2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]).collect();
        let out = drain(pp, items);
        assert_eq!(out.len(), 1);
        assert!((out[0] - (32.0 / 7.0)).abs() < 1e-9);
    }

    #[test]
    fn rms_simple() {
        // sqrt((3^2 + 4^2)/2) = sqrt(12.5) ≈ 3.5355
        let pp: Box<dyn PostProcessor> = Box::new(RmsPostProcessor::new(100));
        let out = drain(pp, vec![(0, 3.0), (1, 4.0)]);
        assert_eq!(out.len(), 1);
        assert!((out[0] - 12.5_f64.sqrt()).abs() < 1e-9);
    }

    #[test]
    fn max_min_std_remain_correct() {
        let pp: Box<dyn PostProcessor> = Box::new(MaxPostProcessor::new(100));
        assert_eq!(drain(pp, vec![(0, 1.0), (1, 5.0), (2, 3.0)]), vec![5.0]);

        let pp: Box<dyn PostProcessor> = Box::new(MinPostProcessor::new(100));
        assert_eq!(drain(pp, vec![(0, 1.0), (1, 5.0), (2, 3.0)]), vec![1.0]);

        let pp: Box<dyn PostProcessor> = Box::new(StdPostProcessor::new(100));
        let out = drain(pp, vec![(0, 1.0), (1, 2.0), (2, 3.0)]);
        // sample std of 1,2,3 = 1.0
        assert_eq!(out.len(), 1);
        assert!((out[0] - 1.0).abs() < 1e-9);
    }
}
