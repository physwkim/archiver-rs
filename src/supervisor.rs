use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{error, info};

pub struct RuntimeSupervisor {
    shutdown_rx: watch::Receiver<bool>,
    handles: Vec<(String, JoinHandle<()>)>,
}

impl RuntimeSupervisor {
    pub fn new(shutdown_rx: watch::Receiver<bool>) -> Self {
        Self {
            shutdown_rx,
            handles: Vec::new(),
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
        info!(task = name, "Spawned background task");
        self.handles.push((name.to_string(), handle));
    }

    pub async fn shutdown(self, timeout: Duration) {
        info!(
            tasks = self.handles.len(),
            "Waiting for background tasks to complete"
        );

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
            error!(
                "Shutdown timed out after {}s, some tasks may still be running",
                timeout.as_secs()
            );
        }
    }
}
