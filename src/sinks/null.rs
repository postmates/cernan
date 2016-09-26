use sink::Sink;
use metric::{Metric, LogLine};

pub struct Null {
}

impl Null {
    pub fn new() -> Null {
        Null {}
    }
}

impl Default for Null {
    fn default() -> Self {
        Self::new()
    }
}

impl Sink for Null {
    fn deliver(&mut self, _: Metric) {
        // discard point
    }

    fn deliver_lines(&mut self, _: Vec<LogLine>) {
        // discard point
    }

    fn flush(&mut self) {
        // do nothing
    }
}
