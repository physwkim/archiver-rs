use crate::storage::traits::{EventStream, PostProcessor};
use crate::types::{ArchiverSample, ArchiverValue, EventStreamDesc};

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
            window_start: None,
            finished: false,
        })
    }
}

struct MeanDecimationStream {
    input: Box<dyn EventStream>,
    interval_secs: u64,
    buffer: Vec<f64>,
    window_start: Option<std::time::SystemTime>,
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
                    let ts = sample.timestamp;
                    let window_start = *self.window_start.get_or_insert(ts);
                    let elapsed = ts
                        .duration_since(window_start)
                        .unwrap_or_default()
                        .as_secs();

                    if elapsed >= self.interval_secs && !self.buffer.is_empty() {
                        // Emit averaged sample for the completed window.
                        let mean =
                            self.buffer.iter().sum::<f64>() / self.buffer.len() as f64;
                        let result = ArchiverSample::new(
                            window_start,
                            ArchiverValue::ScalarDouble(mean),
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
                    // Emit final window.
                    if !self.buffer.is_empty() {
                        let mean =
                            self.buffer.iter().sum::<f64>() / self.buffer.len() as f64;
                        let result = ArchiverSample::new(
                            self.window_start.unwrap(),
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
            window_start: None,
            emitted: false,
        })
    }
}

struct FirstSampleStream {
    input: Box<dyn EventStream>,
    interval_secs: u64,
    window_start: Option<std::time::SystemTime>,
    emitted: bool,
}

impl EventStream for FirstSampleStream {
    fn description(&self) -> &EventStreamDesc {
        self.input.description()
    }

    fn next_event(&mut self) -> anyhow::Result<Option<ArchiverSample>> {
        loop {
            match self.input.next_event()? {
                Some(sample) => {
                    let ts = sample.timestamp;
                    let window_start = *self.window_start.get_or_insert(ts);
                    let elapsed = ts
                        .duration_since(window_start)
                        .unwrap_or_default()
                        .as_secs();

                    if elapsed >= self.interval_secs {
                        self.window_start = Some(ts);
                        self.emitted = true;
                        return Ok(Some(sample));
                    }

                    if !self.emitted {
                        self.emitted = true;
                        return Ok(Some(sample));
                    }
                }
                None => return Ok(None),
            }
        }
    }
}
