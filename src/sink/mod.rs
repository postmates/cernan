use metric::{Metric, LogLine, Event};
use mpsc;
use time;

/// A 'sink' is a sink for metrics.
pub trait Sink {
    fn flush(&mut self) -> ();
    fn deliver(&mut self, point: Metric) -> ();
    fn deliver_lines(&mut self, lines: Vec<LogLine>) -> ();
    fn run(&mut self, mut recv: mpsc::Receiver<Event>) {
        let mut attempts = 0;
        loop {
            time::delay(attempts);
            match recv.next() {
                None => attempts += 1,
                Some(event) => {
                    attempts = 0;
                    match event {
                        Event::TimerFlush => self.flush(),
                        Event::Graphite(metric) |
                        Event::Statsd(metric) => self.deliver(metric),
                        Event::Log(lines) => self.deliver_lines(lines),
                    }
                }
            }
        }
    }
}