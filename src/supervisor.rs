use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::{AbortHandle, JoinHandle};
use tracing::{error, info, warn};

pub struct RuntimeSupervisor {
    shutdown_tx: watch::Sender<bool>,
    handles: Vec<(String, JoinHandle<()>)>,
    abort_handles: Vec<(String, AbortHandle)>,
    /// Name of the first critical task that exited (or panicked)
    /// before shutdown was requested. Set by the wrapper spawned in
    /// [`spawn_critical`](Self::spawn_critical); read by
    /// [`shutdown`](Self::shutdown) to fail the process.
    critical_exit: Arc<Mutex<Option<String>>>,
}

impl RuntimeSupervisor {
    /// `shutdown_tx` is the process-wide shutdown watch. The
    /// supervisor sends `true` on it when a critical task dies, so
    /// whoever drives the HTTP server's graceful shutdown must watch
    /// the receiver side as well as the OS signal.
    pub fn new(shutdown_tx: watch::Sender<bool>) -> Self {
        Self {
            shutdown_tx,
            handles: Vec::new(),
            abort_handles: Vec::new(),
            critical_exit: Arc::new(Mutex::new(None)),
        }
    }

    pub fn shutdown_rx(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    /// Resolves once shutdown has been requested — including when the
    /// request happened before this call. `changed()` on a receiver
    /// obtained here cannot express that case: `subscribe` marks the
    /// current value as seen, so a critical task that died before the
    /// caller subscribed would never wake it.
    pub fn shutdown_requested(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut rx = self.shutdown_tx.subscribe();
        async move {
            // Err means every sender is gone, which only happens on
            // the way out — treat it as "requested".
            let _ = rx.wait_for(|requested| *requested).await;
        }
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

    /// Spawn a task the process cannot run without. If it returns or
    /// panics while the shutdown watch still reads `false`, the
    /// supervisor requests shutdown and [`shutdown`](Self::shutdown)
    /// reports the failure, so the process exits non-zero instead of
    /// idling with the task's work silently undone. An exit after
    /// shutdown was requested is the normal case and is not reported.
    pub fn spawn_critical(
        &mut self,
        name: &str,
        fut: impl std::future::Future<Output = ()> + Send + 'static,
    ) {
        let inner = tokio::spawn(fut);
        // Registered so a shutdown-timeout abort reaches the real
        // task, not only the wrapper awaiting it.
        self.abort_handles
            .push((name.to_string(), inner.abort_handle()));
        let task_name = name.to_string();
        let shutdown_tx = self.shutdown_tx.clone();
        let critical_exit = self.critical_exit.clone();
        self.spawn(name, async move {
            let result = inner.await;
            if *shutdown_tx.borrow() {
                return;
            }
            match result {
                Ok(()) => error!(
                    task = task_name,
                    "Critical task exited before shutdown was requested; shutting down"
                ),
                Err(e) => error!(
                    task = task_name,
                    "Critical task died before shutdown was requested: {e}; shutting down"
                ),
            }
            critical_exit
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get_or_insert(task_name);
            let _ = shutdown_tx.send(true);
        });
    }

    /// Wait for every task to finish, aborting stragglers after
    /// `timeout`. Returns an error naming the critical task if one
    /// exited before shutdown was requested.
    pub async fn shutdown(self, timeout: Duration) -> anyhow::Result<()> {
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

        let failed = self
            .critical_exit
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        match failed {
            Some(name) => {
                anyhow::bail!("critical task `{name}` exited before shutdown was requested")
            }
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn wait_for_shutdown(mut rx: watch::Receiver<bool>) -> bool {
        tokio::time::timeout(Duration::from_secs(2), async move {
            while !*rx.borrow_and_update() {
                if rx.changed().await.is_err() {
                    return false;
                }
            }
            true
        })
        .await
        .unwrap_or(false)
    }

    #[tokio::test]
    async fn critical_task_early_exit_requests_shutdown_and_fails() {
        let (tx, rx) = watch::channel(false);
        let mut sup = RuntimeSupervisor::new(tx);
        sup.spawn_critical("pool", async {});
        assert!(
            wait_for_shutdown(rx).await,
            "early exit must request shutdown"
        );
        let err = sup
            .shutdown(Duration::from_secs(1))
            .await
            .expect_err("shutdown must report the early exit");
        assert!(err.to_string().contains("pool"), "{err}");
    }

    #[tokio::test]
    async fn critical_task_panic_requests_shutdown_and_fails() {
        let (tx, rx) = watch::channel(false);
        let mut sup = RuntimeSupervisor::new(tx);
        sup.spawn_critical("pool", async { panic!("boom") });
        assert!(wait_for_shutdown(rx).await, "panic must request shutdown");
        assert!(sup.shutdown(Duration::from_secs(1)).await.is_err());
    }

    #[tokio::test]
    async fn critical_task_exit_after_shutdown_request_is_clean() {
        let (tx, _rx) = watch::channel(false);
        let mut sup = RuntimeSupervisor::new(tx.clone());
        let mut watch_rx = sup.shutdown_rx();
        sup.spawn_critical("pool", async move {
            while !*watch_rx.borrow_and_update() {
                if watch_rx.changed().await.is_err() {
                    return;
                }
            }
        });
        let _ = tx.send(true);
        sup.shutdown(Duration::from_secs(1))
            .await
            .expect("exit after a requested shutdown is normal");
    }

    #[tokio::test]
    async fn shutdown_requested_resolves_for_a_death_before_the_call() {
        let (tx, rx) = watch::channel(false);
        let mut sup = RuntimeSupervisor::new(tx);
        sup.spawn_critical("pool", async {});
        assert!(
            wait_for_shutdown(rx).await,
            "early exit must request shutdown"
        );
        // The death is already recorded; a waiter created only now
        // must still resolve.
        tokio::time::timeout(Duration::from_secs(1), sup.shutdown_requested())
            .await
            .expect("a waiter created after the death must resolve");
        assert!(sup.shutdown(Duration::from_secs(1)).await.is_err());
    }

    #[tokio::test]
    async fn non_critical_task_exit_is_ignored() {
        let (tx, rx) = watch::channel(false);
        let mut sup = RuntimeSupervisor::new(tx);
        sup.spawn("cleanup", async {});
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!*rx.borrow(), "plain tasks never request shutdown");
        sup.shutdown(Duration::from_secs(1)).await.unwrap();
    }
}
