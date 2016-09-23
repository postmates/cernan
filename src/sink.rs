use metric::{Metric, LogLine, Event};
use mpsc;

/// A 'sink' is a sink for metrics.
pub trait Sink {
    fn flush(&mut self) -> ();
    fn deliver(&mut self, point: Metric) -> ();
    fn deliver_lines(&mut self, lines: Vec<LogLine>) -> ();
    fn run(&mut self, recv: mpsc::Receiver) {
        for event in recv {
            match event {
                Event::TimerFlush => self.flush(),
                Event::Graphite(metric) |
                Event::Statsd(metric) => self.deliver(metric),
                Event::Log(lines) => self.deliver_lines(lines),
            }
        }
        unreachable!();
    }
}
