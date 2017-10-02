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
pub mod wavefront;
mod native;
pub mod influxdb;
pub mod prometheus;
pub mod elasticsearch;

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
    ///
    /// If the value is `None` this is a signal that the sink will NEVER flush
    /// EXCEPT in the case where the sink's valve_state is Closed.
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
        // The run-loop of a sink is two nested loops. The outer loop pulls a
        // value from the hopper queue. If that value is Some the inner loop
        // tries to do something with it, only discarding it at such time as
        // it's been delivered to the Sink.
        loop {
            time::delay(attempts);
            let nxt = recv.next();
            if nxt.is_none() {
                attempts += 1;
                continue;
            }
            let event = nxt.unwrap();
            loop {
                // We have to be careful here not to dump a value until it's
                // already been delivered _and_ be sure we at least attempt to
                // make progress on delivery. There are two conditions we have
                // to look out for most carefully:
                //
                //  1. Is the valve_state closed?
                //  2. Does the flush_interval match our flush index?
                //
                // If the valve state is closed we attempt to flush the sink to
                // clear the valve, hold on to the value and loop around again
                // after a delay. If the flush_interval is Some and DOES match
                // then we flush. If the flush_interval is Some and DOES NOT
                // match then we do not flush. If the flush_interval is NONE
                // then we never flush.
                match self.valve_state() {
                    Valve::Open => match event {
                        Event::TimerFlush(idx) => {
                            // Flush timers are interesting. The timer thread
                            // sends a TimerFlush pulse once a second and it's
                            // possible that a sink will have multiple Sources /
                            // Filters pushing down into it. That means multiple
                            // TimerFlush values for the same time index.
                            //
                            // What we do to avoid duplicating time pulses is
                            // keep track of a 'last_flush_idx', our current
                            // time and only reset to a new time when the idx in
                            // the pulse is greater than the last one we've
                            // seen. If it's not, we ignore it.
                            if idx > last_flush_idx {
                                // Now, because sinks will not want to flush
                                // every timer pulse we query the flush_interval
                                // of the sink. If the interval and the idx
                                // match up, we flush. Else, not.
                                if let Some(flush_interval) = self.flush_interval() {
                                    if idx % flush_interval == 0 {
                                        self.flush();
                                    }
                                }
                                last_flush_idx = idx;
                            }
                            break;
                        }
                        Event::Telemetry(metric) => {
                            attempts = attempts.saturating_sub(1);
                            self.deliver(metric);
                            break;
                        }
                        Event::Log(line) => {
                            attempts = attempts.saturating_sub(1);
                            self.deliver_line(line);
                            break;
                        }
                    },
                    Valve::Closed => {
                        attempts += 1;
                        self.flush();
                        continue;
                    }
                }
            }
        }
    }
}
