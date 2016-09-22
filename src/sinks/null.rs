use sink::Sink;
use metric::Metric;

pub struct Null {
}

impl Null {
    pub fn new() -> Null {
        Null { }
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

    fn flush(&mut self) {
        // do nothing
    }
}
