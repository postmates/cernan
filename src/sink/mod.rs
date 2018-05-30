//! A 'sink' is a final destination for telemetry and log lines. That is, a
//! 'sink' is that which is at the end of a `source -> filter -> filter ->
//! ... -> sink` chain. The sink has no obligations with regard to the telemetry
//! and log lines it receives, other than to receive them. Individual sinks make
//! different choices.

use hopper;
use metric::{Encoding, Event, LogLine, Telemetry};
use std::marker::PhantomData;
use thread;
use time;
use util::Valve;

mod console;
mod null;
pub mod wavefront;
mod native;
pub mod influxdb;
pub mod prometheus;
pub mod elasticsearch;
pub mod kafka;

pub use self::console::{Console, ConsoleConfig};
pub use self::elasticsearch::{Elasticsearch, ElasticsearchConfig};
pub use self::influxdb::{InfluxDB, InfluxDBConfig};
pub use self::kafka::{Kafka, KafkaConfig};
pub use self::native::{Native, NativeConfig};
pub use self::null::{Null, NullConfig};
pub use self::prometheus::{Prometheus, PrometheusConfig};
pub use self::wavefront::{Wavefront, WavefrontConfig};

/// Generic interface used to capture global sink configuration
/// parameters as well as sink specific parameters.
///
/// Stored configuration is consumed when the sink is spawned,
/// resulting in a new thread executing the given sink.
pub struct RunnableSink<S, SConfig>
where
    S: Send + Sink<SConfig>,
    SConfig: 'static + Send + Clone,
{
    recv: hopper::Receiver<Event>,
    sources: Vec<String>,
    state: S,

    // Yes, compiler, we know that we aren't storing
    // anything of type SConfig.
    config: PhantomData<SConfig>,
}

impl<S, SConfig> RunnableSink<S, SConfig>
where
    S: 'static + Send + Sink<SConfig>,
    SConfig: 'static + Clone + Send,
{
    /// Generic constructor for RunnableSink - execution wrapper around objects
    /// implementing Sink.
    pub fn new(
        recv: hopper::Receiver<Event>,
        sources: Vec<String>,
        config: SConfig,
    ) -> RunnableSink<S, SConfig> {
        RunnableSink {
            recv: recv,
            sources: sources,
            state: S::init(config),
            config: PhantomData,
        }
    }

    /// Spawns / consumes the given stateful sink, returning the corresponding
    /// thread.
    pub fn run(self) -> thread::ThreadHandle {
        thread::spawn(move |_poll| {
            self.consume();
        })
    }

    fn consume(mut self) -> () {
        let mut attempts = 0;
        let mut recv = self.recv.into_iter();
        let mut last_flush_idx = 0;
        let mut total_shutdowns = 0;
        // The run-loop of a sink is two nested loops. The outer loop pulls a
        // value from the hopper queue. If that value is Some the inner loop
        // tries to do something with it, only discarding it at such time as
        // it's been delivered to the Sink.
        loop {
            let nxt = recv.next();
            if nxt.is_none() {
                time::delay(attempts);
                attempts += 1;
                continue;
            }
            attempts = 0;
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
                match self.state.valve_state() {
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
                                if let Some(flush_interval) =
                                    self.state.flush_interval()
                                {
                                    if idx % flush_interval == 0 {
                                        self.state.flush();
                                    }
                                }
                                last_flush_idx = idx;
                            }
                            break;
                        }
                        Event::Telemetry(metric) => {
                            self.state.deliver(metric);
                            break;
                        }
                        Event::Log(line) => {
                            self.state.deliver_line(line);
                            break;
                        }
                        Event::Raw {
                            order_by,
                            encoding,
                            bytes,
                        } => {
                            self.state.deliver_raw(order_by, encoding, bytes);
                            break;
                        }
                        Event::Shutdown => {
                            // Invariant - In order to ensure at least once delivery
                            // at the sink level, the following properties must hold:
                            //
                            //    1) An upstream source injects a Shutdown event after
                            //    all of its events have been processed.
                            //
                            // 2) Sources shutdown only after receiving Shutdown
                            // from each of its
                            // upstream sources/filters.
                            total_shutdowns += 1;
                            if total_shutdowns >= self.sources.len() {
                                trace!("Received shutdown from every configured source: {:?}", self.sources);
                                self.state.shutdown();
                                return;
                            }
                        }
                    },
                    Valve::Closed => {
                        self.state.flush();
                        continue;
                    }
                }
            }
        }
    }
}

/// A 'sink' is a sink for metrics.
pub trait Sink<SConfig>
where
    Self: 'static + Send + Sized,
    SConfig: 'static + Send + Clone,
{
    /// Generic constructor for sinks implementing this trait.
    fn new(
        recv: hopper::Receiver<Event>,
        sources: Vec<String>,
        config: SConfig,
    ) -> RunnableSink<Self, SConfig> {
        RunnableSink::<Self, SConfig>::new(recv, sources, config)
    }

    /// Constructs a new sink.
    fn init(config: SConfig) -> Self;

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
    fn valve_state(&self) -> Valve {
        // never close up shop
        Valve::Open
    }
    /// Deliver a `Telemetry` to the `Sink`. Exact behaviour varies by
    /// implementation.
    fn deliver(&mut self, _telem: Telemetry) -> () {
        // nothing, intentionally
    }
    /// Deliver a `LogLine` to the `Sink`. Exact behaviour varies by
    /// implementation.
    fn deliver_line(&mut self, _line: LogLine) -> () {
        // nothing, intentionally
    }
    /// Deliver a 'Raw' series of encoded bytes to the sink.
    fn deliver_raw(
        &mut self,
        _order_by: u64,
        _encoding: Encoding,
        _bytes: Vec<u8>,
    ) -> () {
        // Not all sinks accept raw events.  By default, we do nothing.
    }
    /// Provide a hook to shutdown a sink. This is necessary for sinks which
    /// have their own long-running threads.
    fn shutdown(self) -> ();
}
