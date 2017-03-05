use entry::{Entry, EntryConfig};
use hopper;
use metric::{Event, LogLine, Telemetry};
use sink::{Sink, Valve};
use std::sync;

pub struct Null {
    pub config: NullConfig,
}

impl Null {
    pub fn new(config: NullConfig) -> Null {
        Null { config: config }
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

impl EntryConfig for NullConfig {
    fn get_config_path(&self) -> &String {
        &self.config_path
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

impl Entry for Null {
    fn get_config(&self) -> &EntryConfig {
        &self.config
    }
    fn run1(&mut self, _forwards: Vec<hopper::Sender<Event>>, recv: hopper::Receiver<Event>) {
        self.run(recv)
    }
}