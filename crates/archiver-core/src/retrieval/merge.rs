use crate::storage::traits::EventStream;
use crate::types::{ArchiverSample, EventStreamDesc};

/// Merges multiple EventStreams in timestamp order.
/// Assumes each input stream is already sorted by time.
pub struct MergedEventStream {
    desc: EventStreamDesc,
    streams: Vec<Box<dyn EventStream>>,
    /// Buffer of (stream_index, sample) — one lookahead per stream.
    heads: Vec<Option<(usize, ArchiverSample)>>,
    initialized: bool,
}

impl MergedEventStream {
    pub fn new(desc: EventStreamDesc, streams: Vec<Box<dyn EventStream>>) -> Self {
        let count = streams.len();
        Self {
            desc,
            streams,
            heads: vec![None; count],
            initialized: false,
        }
    }

    fn initialize(&mut self) -> anyhow::Result<()> {
        for (i, stream) in self.streams.iter_mut().enumerate() {
            if let Some(sample) = stream.next_event()? {
                self.heads[i] = Some((i, sample));
            }
        }
        self.initialized = true;
        Ok(())
    }
}

impl EventStream for MergedEventStream {
    fn description(&self) -> &EventStreamDesc {
        &self.desc
    }

    fn next_event(&mut self) -> anyhow::Result<Option<ArchiverSample>> {
        if !self.initialized {
            self.initialize()?;
        }

        // Find the stream with the earliest timestamp.
        let mut earliest_idx = None;
        let mut earliest_ts = None;

        for (i, head) in self.heads.iter().enumerate() {
            if let Some((_, sample)) = head {
                let ts = sample.timestamp;
                if earliest_ts.is_none_or(|e| ts < e) {
                    earliest_ts = Some(ts);
                    earliest_idx = Some(i);
                }
            }
        }

        let idx = match earliest_idx {
            Some(i) => i,
            None => return Ok(None),
        };

        // Take the sample and advance that stream.
        let (stream_idx, sample) = self.heads[idx]
            .take()
            .expect("earliest_idx points to a Some entry");
        if let Some(next_sample) = self.streams[stream_idx].next_event()? {
            self.heads[idx] = Some((stream_idx, next_sample));
        }

        Ok(Some(sample))
    }
}
