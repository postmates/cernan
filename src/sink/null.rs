use metric::{LogLine, Telemetry};
use sink::{Sink, Valve};
use std::sync;

pub struct Null {}

impl Null {
    pub fn new(_config: NullConfig) -> Null {
        Null {}
    }
}

#[derive(Debug, Deserialize)]
pub struct NullConfig {
    pub config_path: String,
}

impl NullConfig {
    pub fn new(config_path: String) -> NullConfig {
        NullConfig { config_path: config_path }
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
        None
    }


    fn flush(&mut self) {
        // do nothing
    }
}
