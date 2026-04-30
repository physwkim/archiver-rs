use std::time::SystemTime;

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

/// Wraps another `EventStream` and drops samples whose timestamp equals the
/// previously emitted one. Used by failover retrieval to deduplicate when the
/// local archiver and a peer both have identical events for the same PV.
pub struct DedupTimestampStream {
    inner: Box<dyn EventStream>,
    last_ts: Option<SystemTime>,
}

impl DedupTimestampStream {
    pub fn new(inner: Box<dyn EventStream>) -> Self {
        Self {
            inner,
            last_ts: None,
        }
    }
}

impl EventStream for DedupTimestampStream {
    fn description(&self) -> &EventStreamDesc {
        self.inner.description()
    }

    fn next_event(&mut self) -> anyhow::Result<Option<ArchiverSample>> {
        loop {
            match self.inner.next_event()? {
                Some(sample) => {
                    if Some(sample.timestamp) == self.last_ts {
                        continue;
                    }
                    self.last_ts = Some(sample.timestamp);
                    return Ok(Some(sample));
                }
                None => return Ok(None),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ArchDbType, ArchiverValue};
    use std::time::{Duration, UNIX_EPOCH};

    struct VecStream {
        desc: EventStreamDesc,
        items: std::vec::IntoIter<ArchiverSample>,
    }

    impl VecStream {
        fn new(items: Vec<ArchiverSample>) -> Self {
            Self {
                desc: EventStreamDesc {
                    pv_name: "T".to_string(),
                    db_type: ArchDbType::ScalarDouble,
                    year: 2024,
                    element_count: Some(1),
                    headers: Vec::new(),
                },
                items: items.into_iter(),
            }
        }
    }

    impl EventStream for VecStream {
        fn description(&self) -> &EventStreamDesc {
            &self.desc
        }
        fn next_event(&mut self) -> anyhow::Result<Option<ArchiverSample>> {
            Ok(self.items.next())
        }
    }

    fn s(secs: u64, val: f64) -> ArchiverSample {
        ArchiverSample::new(
            UNIX_EPOCH + Duration::from_secs(secs),
            ArchiverValue::ScalarDouble(val),
        )
    }

    fn drain(mut stream: Box<dyn EventStream>) -> Vec<f64> {
        let mut out = Vec::new();
        while let Some(sample) = stream.next_event().unwrap() {
            if let ArchiverValue::ScalarDouble(v) = sample.value {
                out.push(v);
            }
        }
        out
    }

    #[test]
    fn merge_interleaves_two_streams() {
        let a = VecStream::new(vec![s(1, 1.0), s(3, 3.0), s(5, 5.0)]);
        let b = VecStream::new(vec![s(2, 2.0), s(4, 4.0), s(6, 6.0)]);
        let merged: Box<dyn EventStream> = Box::new(MergedEventStream::new(
            a.desc.clone(),
            vec![Box::new(a), Box::new(b)],
        ));
        assert_eq!(drain(merged), vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
    }

    #[test]
    fn dedup_drops_duplicate_timestamps() {
        // A and B both have a sample at t=2; merge produces 1,2,2,3 → dedup yields 1,2,3.
        let a = VecStream::new(vec![s(1, 1.0), s(2, 2.0), s(3, 3.0)]);
        let b = VecStream::new(vec![s(2, 99.0)]); // duplicate at t=2
        let merged = Box::new(MergedEventStream::new(
            a.desc.clone(),
            vec![Box::new(a), Box::new(b)],
        ));
        let dedup: Box<dyn EventStream> = Box::new(DedupTimestampStream::new(merged));
        let out = drain(dedup);
        // First t=2 sample wins (value 2.0); duplicate dropped.
        assert_eq!(out, vec![1.0, 2.0, 3.0]);
    }
}
