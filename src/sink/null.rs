use sink::{Sink, Valve};
use metric::{Metric, LogLine};

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
}

impl NullConfig {
    pub fn new(config_path: String) -> NullConfig {
        NullConfig { config_path: config_path }
    }
}

impl Sink for Null {
    fn deliver(&mut self, _: Metric) -> Valve<Metric> {
        // discard point
        Valve::Open
    }

    fn deliver_lines(&mut self, _: Vec<LogLine>) -> Valve<Vec<LogLine>> {
        // discard point
        Valve::Open
    }

    fn flush(&mut self) {
        // do nothing
    }
}
