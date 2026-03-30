use std::time::Duration;

use tokio::sync::watch;
use tokio::task::{AbortHandle, JoinHandle};
use tracing::{error, info, warn};

pub struct RuntimeSupervisor {
    shutdown_rx: watch::Receiver<bool>,
    handles: Vec<(String, JoinHandle<()>)>,
    abort_handles: Vec<(String, AbortHandle)>,
}

impl RuntimeSupervisor {
    pub fn new(shutdown_rx: watch::Receiver<bool>) -> Self {
        Self {
            shutdown_rx,
            handles: Vec::new(),
            abort_handles: Vec::new(),
        }
    }

    pub fn shutdown_rx(&self) -> watch::Receiver<bool> {
        self.shutdown_rx.clone()
    }

    pub fn spawn(
        &mut self,
        name: &str,
        fut: impl std::future::Future<Output = ()> + Send + 'static,
    ) {
        let handle = tokio::spawn(fut);
        self.abort_handles
            .push((name.to_string(), handle.abort_handle()));
        info!(task = name, "Spawned background task");
        self.handles.push((name.to_string(), handle));
    }

    pub async fn shutdown(self, timeout: Duration) {
        let count = self.handles.len();
        info!(tasks = count, "Waiting for background tasks to complete");

        let abort_handles = self.abort_handles;
        let result = tokio::time::timeout(timeout, async {
            for (name, handle) in self.handles {
                match handle.await {
                    Ok(()) => info!(task = name, "Task completed"),
                    Err(e) => error!(task = name, "Task panicked: {e}"),
                }
            }
        })
        .await;

        if result.is_err() {
            warn!(
                "Shutdown timed out after {}s, aborting remaining tasks",
                timeout.as_secs()
            );
            for (name, abort) in &abort_handles {
                if !abort.is_finished() {
                    abort.abort();
                    warn!(task = name, "Aborted task");
                }
            }
        }
    }
}
