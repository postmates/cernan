use metric::{LogLine, Telemetry};
use sink::{Sink, Valve};
use std::sync;

pub struct Null {
}

impl Null {
    pub fn new(_config: NullConfig) -> Null {
        Null {}
    }
}

#[derive(Debug)]
pub struct NullConfig {
    pub config_path: String,
    pub flush_interval: u64,
}

impl NullConfig {
    pub fn new(config_path: String, flush_interval: u64) -> NullConfig {
        NullConfig { config_path: config_path, flush_interval: flush_interval }
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

    fn flush(&mut self) {
        // do nothing
    }
}
