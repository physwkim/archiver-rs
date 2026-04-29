use std::sync::Arc;

use async_trait::async_trait;

use archiver_core::registry::SampleMode;
use archiver_engine::channel_manager::ChannelManager;

use crate::services::traits::{ArchiverCommand, ArchiverQuery, ConnectionInfoDto, PvCountersDto};

pub struct ChannelArchiverControl {
    inner: Arc<ChannelManager>,
}

impl ChannelArchiverControl {
    pub fn new(mgr: Arc<ChannelManager>) -> Self {
        Self { inner: mgr }
    }
}

impl ArchiverQuery for ChannelArchiverControl {
    fn get_connection_info(&self, pv: &str) -> Option<ConnectionInfoDto> {
        self.inner.get_connection_info(pv).map(|c| ConnectionInfoDto {
            connected_since: c.connected_since,
            last_event_time: c.last_event_time,
            is_connected: c.is_connected,
        })
    }

    fn get_never_connected_pvs(&self) -> Vec<String> {
        self.inner.get_never_connected_pvs()
    }

    fn get_currently_disconnected_pvs(&self) -> Vec<String> {
        self.inner.get_currently_disconnected_pvs()
    }

    fn all_pv_counters(&self) -> Vec<(String, PvCountersDto)> {
        self.inner
            .all_pv_counters()
            .into_iter()
            .map(|(pv, c)| {
                (
                    pv,
                    PvCountersDto {
                        events_received: c.events_received,
                        events_stored: c.events_stored,
                        first_event_unix_secs: c.first_event_unix_secs,
                        buffer_overflow_drops: c.buffer_overflow_drops,
                        timestamp_drops: c.timestamp_drops,
                        type_change_drops: c.type_change_drops,
                        disconnect_count: c.disconnect_count,
                        last_disconnect_unix_secs: c.last_disconnect_unix_secs,
                    },
                )
            })
            .collect()
    }
}

#[async_trait]
impl ArchiverCommand for ChannelArchiverControl {
    async fn archive_pv(&self, pv: &str, mode: &SampleMode) -> anyhow::Result<()> {
        self.inner.archive_pv(pv, mode).await
    }

    fn pause_pv(&self, pv: &str) -> anyhow::Result<()> {
        self.inner.pause_pv(pv)
    }

    async fn resume_pv(&self, pv: &str) -> anyhow::Result<()> {
        self.inner.resume_pv(pv).await
    }

    fn stop_pv(&self, pv: &str) -> anyhow::Result<()> {
        self.inner.stop_pv(pv)
    }

    fn destroy_pv(&self, pv: &str) -> anyhow::Result<()> {
        self.inner.destroy_pv(pv)
    }

    async fn update_archive_fields(&self, pv: &str, fields: &[String]) -> anyhow::Result<()> {
        self.inner.update_archive_fields(pv, fields).await
    }
}
