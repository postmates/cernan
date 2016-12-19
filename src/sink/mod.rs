use metric::{Metric, LogLine, Event};
use hopper;
use time;

mod console;
mod federation_transmitter;
mod firehose;
mod null;
mod wavefront;

pub use self::console::{Console, ConsoleConfig};
pub use self::federation_transmitter::{FederationTransmitter, FederationTransmitterConfig};
pub use self::firehose::{Firehose, FirehoseConfig};
pub use self::null::{Null, NullConfig};
pub use self::wavefront::{Wavefront, WavefrontConfig};

pub enum Valve<T> {
    Open,
    Closed(T),
}

/// A 'sink' is a sink for metrics.
pub trait Sink {
    fn flush(&mut self) -> ();
    fn deliver(&mut self, point: Metric) -> Valve<Metric>;
    fn deliver_line(&mut self, line: LogLine) -> Valve<LogLine>;
    fn run(&mut self, mut recv: hopper::Receiver<Event>) {
        let mut attempts = 0;
        loop {
            time::delay(attempts);
            match recv.next() {
                None => attempts += 1,
                Some(event) => {
                    attempts = 0;
                    match event {
                        Event::TimerFlush => self.flush(),
                        Event::Telemetry(mut metric) => {
                            let mut delivery_attempts = 0;
                            loop {
                                time::delay(delivery_attempts);
                                match self.deliver(metric) {
                                    Valve::Open => {
                                        // success, move on
                                        break;
                                    }
                                    Valve::Closed(m) => {
                                        metric = m;
                                        delivery_attempts += 1;
                                        continue;
                                    }
                                }
                            }
                        }

                        Event::Log(mut line) => {
                            let mut delivery_attempts = 0;
                            loop {
                                time::delay(delivery_attempts);
                                match self.deliver_line(line) {
                                    Valve::Open => {
                                        // success, move on
                                        break;
                                    }
                                    Valve::Closed(l) => {
                                        line = l;
                                        delivery_attempts += 1;
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
