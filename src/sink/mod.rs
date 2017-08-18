//! A 'sink' is a final destination for telemetry and log lines. That is, a
//! 'sink' is that which is at the end of a `source -> filter -> filter ->
//! ... -> sink` chain. The sink has no obligations with regard to the telemetry
//! and log lines it receives, other than to receive them. Individual sinks make
//! different choices.

use hopper;
use metric::{Event, LogLine, Telemetry};
use std::sync;
use time;
use util::Valve;

mod console;
mod firehose;
mod null;
mod wavefront;
mod native;
mod influxdb;
mod prometheus;
mod elasticsearch;

pub use self::console::{Console, ConsoleConfig};
pub use self::elasticsearch::{Elasticsearch, ElasticsearchConfig};
pub use self::firehose::{Firehose, FirehoseConfig};
pub use self::influxdb::{InfluxDB, InfluxDBConfig};
pub use self::native::{Native, NativeConfig};
pub use self::null::{Null, NullConfig};
pub use self::prometheus::{Prometheus, PrometheusConfig};
pub use self::wavefront::{Wavefront, WavefrontConfig};

/// A 'sink' is a sink for metrics.
pub trait Sink {
    /// Lookup the `Sink`'s specific flush interval. This determines how often a
    /// sink will obey the periodic flush pulse.
    fn flush_interval(&self) -> Option<u64>;
    /// Perform the `Sink` specific flush. The rate at which this occurs is
    /// determined by the global `flush_interval` or the sink specific flush
    /// interval. Pulses occur at a rate of once per second, subject to
    /// communication delays in the routing topology.
    fn flush(&mut self) -> ();
    /// Lookup the `Sink` valve state. See `Valve` documentation for more
    /// information.
    fn valve_state(&self) -> Valve;
    /// Deliver a `Telemetry` to the `Sink`. Exact behaviour varies by
    /// implementation.
    fn deliver(&mut self, point: sync::Arc<Option<Telemetry>>) -> ();
    /// Deliver a `LogLine` to the `Sink`. Exact behaviour varies by
    /// implementation.
    fn deliver_line(&mut self, line: sync::Arc<Option<LogLine>>) -> ();
    /// The run-loop of the `Sink`. It's expect that few sinks will ever need to
    /// provide their own implementation. Please take care to obey `Valve`
    /// states and `flush_interval` configurations.
    fn run(&mut self, recv: hopper::Receiver<Event>) {
        let mut attempts = 0;
        let mut recv = recv.into_iter();
        let mut last_flush_idx = 0;
        loop {
            time::delay(attempts);
            match recv.next() {
                None => attempts += 1,
                Some(event) => {
                    match self.valve_state() {
                        Valve::Open => match event {
                            Event::TimerFlush(idx) => if idx > last_flush_idx {
                                if let Some(flush_interval) = self.flush_interval() {
                                    if idx % flush_interval == 0 {
                                        self.flush();
                                    }
                                }
                                last_flush_idx = idx;
                            },
                            Event::Telemetry(metric) => {
                                attempts = attempts.saturating_sub(1);
                                self.deliver(metric);
                            }

                            Event::Log(line) => {
                                attempts = attempts.saturating_sub(1);
                                self.deliver_line(line);
                            }
                        },
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
