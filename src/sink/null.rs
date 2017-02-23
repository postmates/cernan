use metric::{LogLine, Telemetry};
use sink::{Sink, Sink1, Valve};
use std::sync;

pub struct Null {
    pub config_path: String,
}

impl Null {
    pub fn new(config: NullConfig) -> Null {
        Null { config_path: config.config_path }
    }
}

#[derive(Debug)]
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

    fn flush(&mut self) {
        // do nothing
    }
}

impl Sink1 for Null {
    fn get_config_path(&self) -> &String {
        &self.config_path
    }
    fn get_forwards(&self) -> Vec<String> {
        Vec::new()
    }
}