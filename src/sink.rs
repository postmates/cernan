use metric::{Metric,Event};
use mpmc;

/// A 'sink' is a sink for metrics.
pub trait Sink {
    fn flush(&mut self) -> ();
    fn snapshot(&mut self) -> ();
    fn deliver(&mut self, point: Metric) -> ();
    fn run(&mut self, mut recv: mpmc::Receiver) {
        loop {
            match recv.recv() {
                Event::TimerFlush => self.flush(),
                Event::Snapshot => self.snapshot(),
                Event::Graphite(metric) => self.deliver(metric),
                Event::Statsd(metric) => self.deliver(metric),
            }
        }
    }
}
