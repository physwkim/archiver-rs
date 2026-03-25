use crate::storage::traits::{EventStream, PostProcessor};
use crate::types::{ArchiverSample, ArchiverValue, EventStreamDesc};

// ── Max ──

pub struct MaxPostProcessor {
    interval_secs: u64,
}

impl MaxPostProcessor {
    pub fn new(interval_secs: u64) -> Self {
        Self { interval_secs }
    }
}

impl PostProcessor for MaxPostProcessor {
    fn name(&self) -> &str {
        "max"
    }
    fn interval_secs(&self) -> u64 {
        self.interval_secs
    }
    fn process(&self, input: Box<dyn EventStream>) -> Box<dyn EventStream> {
        Box::new(StatStream::new(input, self.interval_secs, StatOp::Max))
    }
}

// ── Min ──

pub struct MinPostProcessor {
    interval_secs: u64,
}

impl MinPostProcessor {
    pub fn new(interval_secs: u64) -> Self {
        Self { interval_secs }
    }
}

impl PostProcessor for MinPostProcessor {
    fn name(&self) -> &str {
        "min"
    }
    fn interval_secs(&self) -> u64 {
        self.interval_secs
    }
    fn process(&self, input: Box<dyn EventStream>) -> Box<dyn EventStream> {
        Box::new(StatStream::new(input, self.interval_secs, StatOp::Min))
    }
}

// ── Std ──

pub struct StdPostProcessor {
    interval_secs: u64,
}

impl StdPostProcessor {
    pub fn new(interval_secs: u64) -> Self {
        Self { interval_secs }
    }
}

impl PostProcessor for StdPostProcessor {
    fn name(&self) -> &str {
        "std"
    }
    fn interval_secs(&self) -> u64 {
        self.interval_secs
    }
    fn process(&self, input: Box<dyn EventStream>) -> Box<dyn EventStream> {
        Box::new(StatStream::new(input, self.interval_secs, StatOp::Std))
    }
}

// ── Shared implementation ──

enum StatOp {
    Max,
    Min,
    Std,
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
            StatOp::Std => {
                if self.buffer.len() < 2 {
                    return 0.0;
                }
                let n = self.buffer.len() as f64;
                let mean = self.buffer.iter().sum::<f64>() / n;
                let variance =
                    self.buffer.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (n - 1.0);
                variance.sqrt()
            }
        }
    }
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
                            self.window_start.unwrap(),
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
