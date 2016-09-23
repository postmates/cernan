use metric::{Metric, Event};
use mpsc;

/// A 'sink' is a sink for metrics.
pub trait Sink {
    fn flush(&mut self) -> ();
    fn deliver(&mut self, point: Metric) -> ();
    fn run(&mut self, recv: mpsc::Receiver) {
        for event in recv {
            match event {
                Event::TimerFlush => self.flush(),
                Event::Graphite(metric) |
                Event::Statsd(metric) => self.deliver(metric),
            }
        }
        unreachable!();
    }
}
