use std::collections::HashSet;
use std::sync::Mutex;

use async_trait::async_trait;

use archiver_core::registry::SampleMode;

use crate::services::traits::{ArchiverCommand, ArchiverQuery, ConnectionInfoDto, PvCountersDto};

struct FakeState {
    archived: HashSet<String>,
    paused: HashSet<String>,
    stopped: HashSet<String>,
}

pub struct FakeArchiverControl {
    state: Mutex<FakeState>,
}

impl FakeArchiverControl {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(FakeState {
                archived: HashSet::new(),
                paused: HashSet::new(),
                stopped: HashSet::new(),
            }),
        }
    }
}

impl Default for FakeArchiverControl {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ArchiverQuery for FakeArchiverControl {
    fn get_connection_info(&self, pv: &str) -> Option<ConnectionInfoDto> {
        let state = self.state.lock().unwrap();
        if state.archived.contains(pv) {
            Some(ConnectionInfoDto {
                connected_since: Some(std::time::SystemTime::now()),
                last_event_time: None,
                is_connected: !state.paused.contains(pv),
            })
        } else {
            None
        }
    }

    fn get_never_connected_pvs(&self) -> Vec<String> {
        Vec::new()
    }

    fn get_currently_disconnected_pvs(&self) -> Vec<String> {
        let state = self.state.lock().unwrap();
        state.paused.iter().cloned().collect()
    }

    async fn live_value(
        &self,
        pv: &str,
        _timeout_secs: u64,
    ) -> Option<Result<serde_json::Value, String>> {
        let state = self.state.lock().unwrap();
        if state.archived.contains(pv) {
            Some(Ok(serde_json::Value::Null))
        } else {
            None
        }
    }

    fn extras_snapshot(&self, _pv: &str) -> std::collections::HashMap<String, String> {
        std::collections::HashMap::new()
    }

    fn all_pv_counters(&self) -> Vec<(String, PvCountersDto)> {
        // Fake: return a zeroed counter per archived PV so handlers
        // exercising this path see a sensible shape under test.
        let state = self.state.lock().unwrap();
        state
            .archived
            .iter()
            .cloned()
            .map(|pv| {
                (
                    pv,
                    PvCountersDto {
                        events_received: 0,
                        events_stored: 0,
                        first_event_unix_secs: None,
                        buffer_overflow_drops: 0,
                        timestamp_drops: 0,
                        type_change_drops: 0,
                        disconnect_count: 0,
                        last_disconnect_unix_secs: None,
                    },
                )
            })
            .collect()
    }
}

#[async_trait]
impl ArchiverCommand for FakeArchiverControl {
    async fn archive_pv(&self, pv: &str, _mode: &SampleMode) -> anyhow::Result<()> {
        let mut state = self.state.lock().unwrap();
        state.archived.insert(pv.to_string());
        Ok(())
    }

    async fn pause_pv(&self, pv: &str) -> anyhow::Result<()> {
        let mut state = self.state.lock().unwrap();
        state.paused.insert(pv.to_string());
        Ok(())
    }

    async fn resume_pv(&self, pv: &str) -> anyhow::Result<()> {
        let mut state = self.state.lock().unwrap();
        state.paused.remove(pv);
        Ok(())
    }

    async fn stop_pv(&self, pv: &str) -> anyhow::Result<()> {
        let mut state = self.state.lock().unwrap();
        state.stopped.insert(pv.to_string());
        state.archived.remove(pv);
        Ok(())
    }

    async fn destroy_pv(&self, pv: &str) -> anyhow::Result<()> {
        let mut state = self.state.lock().unwrap();
        state.archived.remove(pv);
        state.paused.remove(pv);
        state.stopped.remove(pv);
        Ok(())
    }

    async fn update_archive_fields(&self, _pv: &str, _fields: &[String]) -> anyhow::Result<()> {
        // No-op for fakes — the command just persists to the registry which
        // is exercised separately. The real engine respawns CA tasks here.
        Ok(())
    }
}
