use coco::Stack;
use filter;
use metric;
use metric::{AggregationMethod, Telemetry};
use sink;
use source;
use source::Source;
use std;
use std::sync;
use std::sync::atomic::Ordering;
use time;
use util;

lazy_static! {
    static ref Q: Stack<metric::Telemetry> = Stack::new();
}

/// 'Internal' is a Source which is meant to allow cernan to
/// self-telemeter. This is an improvement over past methods as an explicit
/// Source gives operators the ability to define a filter topology for such
/// telemetry and makes it easier for modules to report on themeselves.
pub struct Internal {
    chans: util::Channel,
    tags: sync::Arc<metric::TagMap>,
}

/// The configuration struct for 'Internal'
#[derive(Debug, Deserialize, Clone)]
pub struct InternalConfig {
    /// The configured name of Internal.
    pub config_path: Option<String>,
    /// The forwards which Internal will obey.
    pub forwards: Vec<String>,
    /// The default tags to apply to each Telemetry that comes through the
    /// queue.
    pub tags: metric::TagMap,
}

impl Default for InternalConfig {
    fn default() -> InternalConfig {
        InternalConfig {
            tags: metric::TagMap::default(),
            forwards: Vec::new(),
            config_path: Some("sources.internal".to_string()),
        }
    }
}

impl Internal {
    /// Create a new Internal
    pub fn new(chans: util::Channel, config: InternalConfig) -> Internal {
        Internal {
            chans: chans,
            tags: sync::Arc::new(config.tags),
        }
    }
}

/// Push telemetry into the Internal queue
///
/// Given a name, value, possible aggregation and possible metadata construct a
/// Telemetry with said aggregation and push into Internal's queue. This queue
/// will then be drained into operator configured forwards.
pub fn report_full_telemetry(
    name: &str,
    value: f64,
    metadata: Option<Vec<(&str, &str)>>,
) -> () {
    use metric::AggregationMethod;
    let mut telem = metric::Telemetry::new()
        .name(name)
        .value(value)
        .kind(AggregationMethod::Sum)
        .harden()
        .unwrap();
    telem = metadata
        .unwrap_or_default()
        .iter()
        .fold(telem, |acc, &(k, v)| acc.overlay_tag(k, v));
    Q.push(telem);
}

macro_rules! atom_non_zero_telem {
    ($name:expr, $atom:expr, $tags:expr, $chans:expr) => {
        let now = time::now();
        let value = $atom.swap(0, Ordering::Relaxed);
        if value != 0 {
            let telem = Telemetry::new()
                .name($name)
                .value(value as f64)
                .timestamp(now)
                .kind(AggregationMethod::Set)
                .harden()
                .unwrap()
                .overlay_tags_from_map(&$tags);
            util::send(&mut $chans, metric::Event::new_telemetry(telem));
        }
    }
}

/// Internal as Source
///
/// The 'run' of Internal will pull Telemetry off the internal queue, apply
/// Internal's configured tags and push said telemetry into operator configured
/// channels. If no channels are configured we toss the Telemetry onto the
/// floor.
impl Source for Internal {
    #[allow(cyclomatic_complexity)]
    fn run(&mut self) {
        let slp = std::time::Duration::from_millis(1_000);
        loop {
            std::thread::sleep(slp);
            if !self.chans.is_empty() {
                // source::graphite
                atom_non_zero_telem!(
                    "cernan.graphite.new_peer",
                    source::graphite::GRAPHITE_NEW_PEER,
                    self.tags,
                    self.chans
                );
                atom_non_zero_telem!(
                    "cernan.graphite.packet",
                    source::graphite::GRAPHITE_GOOD_PACKET,
                    self.tags,
                    self.chans
                );
                atom_non_zero_telem!(
                    "cernan.graphite.telemetry.received",
                    source::graphite::GRAPHITE_TELEM,
                    self.tags,
                    self.chans
                );
                atom_non_zero_telem!(
                    "cernan.graphite.bad_packet",
                    source::graphite::GRAPHITE_BAD_PACKET,
                    self.tags,
                    self.chans
                );
                // source::statsd
                atom_non_zero_telem!(
                    "cernan.statsd.packet",
                    source::statsd::STATSD_GOOD_PACKET,
                    self.tags,
                    self.chans
                );
                atom_non_zero_telem!(
                    "cernan.statsd.bad_packet",
                    source::statsd::STATSD_BAD_PACKET,
                    self.tags,
                    self.chans
                );
                // sink::elasticsearch
                atom_non_zero_telem!(
                    "cernan.sinks.elasticsearch.records.delivery",
                    sink::elasticsearch::ELASTIC_RECORDS_DELIVERY,
                    self.tags,
                    self.chans
                );
                atom_non_zero_telem!(
                    "cernan.sinks.elasticsearch.records.total_delivered",
                    sink::elasticsearch::ELASTIC_RECORDS_TOTAL_DELIVERED,
                    self.tags,
                    self.chans
                );
                atom_non_zero_telem!(
                    "cernan.sinks.elasticsearch.records.total_failed",
                    sink::elasticsearch::ELASTIC_RECORDS_TOTAL_FAILED,
                    self.tags,
                    self.chans
                );
                atom_non_zero_telem!(
                    "cernan.sinks.elasticsearch.error.attempts",
                    sink::elasticsearch::ELASTIC_ERROR_ATTEMPTS,
                    self.tags,
                    self.chans
                );
                // sink::wavefront
                atom_non_zero_telem!(
                    "cernan.sinks.wavefront.aggregation.histogram",
                    sink::wavefront::WAVEFRONT_AGGR_HISTO,
                    self.tags,
                    self.chans
                );
                atom_non_zero_telem!(
                    "cernan.sinks.wavefront.aggregation.sum",
                    sink::wavefront::WAVEFRONT_AGGR_SUM,
                    self.tags,
                    self.chans
                );
                atom_non_zero_telem!(
                    "cernan.sinks.wavefront.aggregation.set",
                    sink::wavefront::WAVEFRONT_AGGR_SET,
                    self.tags,
                    self.chans
                );
                atom_non_zero_telem!(
                    "cernan.sinks.wavefront.aggregation.summarize",
                    sink::wavefront::WAVEFRONT_AGGR_SUMMARIZE,
                    self.tags,
                    self.chans
                );
                atom_non_zero_telem!(
                    "cernan.sinks.wavefront.aggregation.summarize.total_percentiles",
                    sink::wavefront::WAVEFRONT_AGGR_TOT_PERCENT,
                    self.tags,
                    self.chans
                );
                atom_non_zero_telem!(
                    "cernan.sinks.wavefront.delivery_attempts",
                    sink::wavefront::WAVEFRONT_DELIVERY_ATTEMPTS,
                    self.tags,
                    self.chans
                );
                atom_non_zero_telem!(
                    "cernan.sinks.wavefront.value.closed",
                    sink::wavefront::WAVEFRONT_VALVE_CLOSED,
                    self.tags,
                    self.chans
                );
                atom_non_zero_telem!(
                    "cernan.sinks.wavefront.value.open",
                    sink::wavefront::WAVEFRONT_VALVE_OPEN,
                    self.tags,
                    self.chans
                );
                // sink::prometheus
                atom_non_zero_telem!(
                    "cernan.sinks.prometheus.aggregation.reportable",
                    sink::prometheus::PROMETHEUS_AGGR_REPORTABLE,
                    self.tags,
                    self.chans
                );
                atom_non_zero_telem!(
                    "cernan.sinks.prometheus.aggregation.remaining",
                    sink::prometheus::PROMETHEUS_AGGR_REMAINING,
                    self.tags,
                    self.chans
                );
                atom_non_zero_telem!(
                    "cernan.sinks.prometheus.write.binary",
                    sink::prometheus::PROMETHEUS_WRITE_BINARY,
                    self.tags,
                    self.chans
                );
                atom_non_zero_telem!(
                    "cernan.sinks.prometheus.write.text",
                    sink::prometheus::PROMETHEUS_WRITE_TEXT,
                    self.tags,
                    self.chans
                );
                atom_non_zero_telem!(
                    "cernan.sinks.prometheus.report_error",
                    sink::prometheus::PROMETHEUS_REPORT_ERROR,
                    self.tags,
                    self.chans
                );
                // sink::influxdb
                atom_non_zero_telem!(
                    "cernan.sinks.influxdb.delivery_attempts",
                    sink::influxdb::INFLUX_DELIVERY_ATTEMPTS,
                    self.tags,
                    self.chans
                );
                atom_non_zero_telem!(
                    "cernan.sinks.influxdb.success",
                    sink::influxdb::INFLUX_SUCCESS,
                    self.tags,
                    self.chans
                );
                atom_non_zero_telem!(
                    "cernan.sinks.influxdb.failure.client_error",
                    sink::influxdb::INFLUX_FAILURE_CLIENT,
                    self.tags,
                    self.chans
                );
                atom_non_zero_telem!(
                    "cernan.sinks.influxdb.failure.server_error",
                    sink::influxdb::INFLUX_FAILURE_SERVER,
                    self.tags,
                    self.chans
                );
                // filter::delay_filter
                atom_non_zero_telem!(
                    "cernan.filters.delay.telemetry.accept",
                    filter::delay_filter::DELAY_TELEM_ACCEPT,
                    self.tags,
                    self.chans
                );
                atom_non_zero_telem!(
                    "cernan.filters.delay.telemetry.reject",
                    filter::delay_filter::DELAY_TELEM_REJECT,
                    self.tags,
                    self.chans
                );
                atom_non_zero_telem!(
                    "cernan.filters.delay.log.reject",
                    filter::delay_filter::DELAY_LOG_REJECT,
                    self.tags,
                    self.chans
                );
                atom_non_zero_telem!(
                    "cernan.filters.delay.log.accept",
                    filter::delay_filter::DELAY_LOG_ACCEPT,
                    self.tags,
                    self.chans
                );
                while let Some(mut telem) = Q.pop() {
                    if !self.chans.is_empty() {
                        telem = telem.overlay_tags_from_map(&self.tags);
                        util::send(
                            &mut self.chans,
                            metric::Event::new_telemetry(telem),
                        );
                    } else {
                        // do nothing, intentionally
                    }
                }
            } else {
                // There are no chans available. In this case we want to drain
                // and deallocate any internal telemetry that may have made it
                // to Q.
                while let Some(_) = Q.pop() {
                    // do nothing, intentionally
                }
            }
        }
    }
}
