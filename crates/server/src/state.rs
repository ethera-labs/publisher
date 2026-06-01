//! Shared HTTP server state.

use std::sync::Arc;

use prometheus_client::registry::Registry;
use tokio::sync::Mutex;

use publisher_coordinator::coordinator::Coordinator;

#[derive(Debug, Clone)]
pub struct AppState {
    pub coordinator: Arc<Coordinator>,
    pub registry: Option<Arc<Mutex<Registry>>>,
}

impl AppState {
    pub fn new(coordinator: Arc<Coordinator>) -> Self {
        Self {
            coordinator,
            registry: None,
        }
    }

    pub fn with_registry(mut self, registry: Registry) -> Self {
        self.registry = Some(Arc::new(Mutex::new(registry)));
        self
    }
}
