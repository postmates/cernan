//! Sink equivalent of /dev/null.
use crate::sink::{Sink, Valve};

/// Null sink
///
/// This sink is intended for testing and demonstration. Every `metric::Event`
/// it receives will be deallocated.
pub struct Null {}

/// Configuration for the `Null` sink
#[derive(Clone, Debug, Deserialize)]
pub struct NullConfig {
    /// The sink's unique name in the routing topology.
    pub config_path: String,
}

impl NullConfig {
    /// Create a new `NullConfig`
    pub fn new(config_path: String) -> NullConfig {
        NullConfig { config_path }
    }
}

impl Sink<NullConfig> for Null {
    fn init(_config: NullConfig) -> Self {
        Null {}
    }

    fn valve_state(&self) -> Valve {
        Valve::Open
    }

    fn flush_interval(&self) -> Option<u64> {
        Some(1)
    }

    fn flush(&mut self) {
        // do nothing
    }

    fn shutdown(mut self) {
        self.flush();
    }
}
