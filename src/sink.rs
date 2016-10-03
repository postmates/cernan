use std::{cmp,thread,time};
use metric::{Metric, LogLine, Event};
use mpsc;

/// A 'sink' is a sink for metrics.
pub trait Sink {
    fn flush(&mut self) -> ();
    fn deliver(&mut self, point: Metric) -> ();
    fn deliver_lines(&mut self, lines: Vec<LogLine>) -> ();
    fn run(&mut self, mut recv: mpsc::Receiver<Event>) {
        let mut attempts = 0;
        loop {
            delay(attempts);
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

#[inline]
fn delay(attempts: u32) {
    if attempts > 0 {
        let max_delay : u32 = 60_000;
        let delay = cmp::min(max_delay, 2u32.pow(attempts));
        let sleep_time = time::Duration::from_millis(delay as u64);
        thread::sleep(sleep_time);
    }
}
