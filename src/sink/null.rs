use sink::{Sink, Valve};
use metric::{Metric, LogLine};

pub struct Null {
}

impl Null {
    pub fn new(_config: NullConfig) -> Null {
        Null {}
    }
}

impl Default for Null {
    fn default() -> Self {
        Self::new(NullConfig::default())
    }
}

#[derive(Debug)]
pub struct NullConfig {
}

impl Default for NullConfig {
    fn default() -> NullConfig {
        NullConfig {}
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
