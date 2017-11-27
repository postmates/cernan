use metric::{LogLine, Telemetry};
use sink::{Sink, Valve};
use std::sync;

/// Null sink
///
/// This sink is intended for testing and demonstration. Every `metric::Event`
/// it receives will be deallocated.
pub struct Null {}

impl Null {
    /// Create a new Null sink
    pub fn new(_config: &NullConfig) -> Null {
        Null {}
    }
}

/// Configuration for the `Null` sink
#[derive(Debug, Deserialize)]
pub struct NullConfig {
    /// The sink's unique name in the routing topology.
    pub config_path: String,
}

impl NullConfig {
    /// Create a new `NullConfig`
    pub fn new(config_path: String) -> NullConfig {
        NullConfig {
            config_path: config_path,
        }
    }
}

impl Sink for Null {
    fn valve_state(&self) -> Valve {
        Valve::Open
    }

    fn deliver(&mut self, _: sync::Arc<Option<Telemetry>>) -> () {
        // discard point
    }

    fn deliver_line(&mut self, _: sync::Arc<Option<LogLine>>) -> () {
        // discard point
    }

    fn flush_interval(&self) -> Option<u64> {
        Some(1)
    }


    fn flush(&mut self) {
        // do nothing
    }
}
