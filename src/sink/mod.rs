//! A 'sink' is a final destination for telemetry and log lines. That is, a
//! 'sink' is that which is at the end of a `source -> filter -> filter ->
//! ... -> sink` chain. The sink has no obligations with regard to the telemetry
//! and log lines it receives, other than to receive them. Individual sinks make
//! different choices.

use hopper;
use metric::{Event, LogLine, Telemetry};
use std::sync;
use time;

mod console;
mod firehose;
mod null;
mod wavefront;
mod native;
mod influxdb;
mod prometheus;

pub use self::console::{Console, ConsoleConfig};
pub use self::firehose::{Firehose, FirehoseConfig};
pub use self::influxdb::{InfluxDB, InfluxDBConfig};
pub use self::native::{Native, NativeConfig};
pub use self::null::{Null, NullConfig};
pub use self::prometheus::{Prometheus, PrometheusConfig};
pub use self::wavefront::{Wavefront, WavefrontConfig};

pub enum Valve {
    Open,
    Closed,
}

/// A 'sink' is a sink for metrics.
pub trait Sink {
    fn flush(&mut self) -> ();
    fn valve_state(&self) -> Valve;
    fn deliver(&mut self, point: sync::Arc<Option<Telemetry>>) -> ();
    fn deliver_line(&mut self, line: sync::Arc<Option<LogLine>>) -> ();
    fn run(&mut self, recv: hopper::Receiver<Event>) {
        let mut attempts = 0;
        let mut recv = recv.into_iter();
        loop {
            time::delay(attempts);
            match recv.next() {
                None => attempts += 1,
                Some(event) => {
                    attempts = 0;
                    match self.valve_state() {
                        Valve::Open => {
                            match event {
                                Event::TimerFlush => self.flush(),
                                Event::Telemetry(metric) => {
                                    self.deliver(metric);
                                }

                                Event::Log(line) => {
                                    self.deliver_line(line);
                                }
                            }
                        }
                        Valve::Closed => {
                            attempts += 1;
                            continue;
                        }
                    }
                }
            }
        }
    }
}
