use crate::filter;
use crate::metric;
use crate::metric::{AggregationMethod, Telemetry};
use crate::sink;
use crate::source;
use crate::time;
use crate::util;
use coco::Stack;
use mio;
use std;
use std::sync::atomic::Ordering;

lazy_static! {
    static ref Q: Stack<metric::Telemetry> = Stack::new();
}

/// 'Internal' is a Source which is meant to allow cernan to
/// self-telemeter. This is an improvement over past methods as an explicit
/// Source gives operators the ability to define a filter topology for such
/// telemetry and makes it easier for modules to report on themeselves.
pub struct Internal;

/// The configuration struct for 'Internal'
#[derive(Debug, Deserialize, Clone)]
pub struct InternalConfig {
    /// The configured name of Internal.
    pub config_path: Option<String>,
    /// The forwards which Internal will obey.
    pub forwards: Vec<String>,
}

impl Default for InternalConfig {
    fn default() -> InternalConfig {
        InternalConfig {
            forwards: Vec::new(),
            config_path: Some("sources.internal".to_string()),
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
    use crate::metric::AggregationMethod;
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

macro_rules! atom_telem {
    ($name:expr, $atom:expr, $chans:expr) => {
        let now = time::now();
        let value = $atom.swap(0, Ordering::Relaxed);
        if value != 0 {
            let telem = Telemetry::new()
                .name($name)
                .value(value as f64)
                .timestamp(now)
                .kind(AggregationMethod::Sum)
                .harden()
                .unwrap();
            util::send(&mut $chans, metric::Event::new_telemetry(telem));
        }
    };
}

macro_rules! atom_set_telem {
    ($name:expr, $atom:expr, $chans:expr) => {
        let now = time::now();
        let value = $atom.swap(0, Ordering::Relaxed);
        if value != 0 {
            let telem = Telemetry::new()
                .name($name)
                .value(value as f64)
                .timestamp(now)
                .kind(AggregationMethod::Set)
                .harden()
                .unwrap();
            util::send(&mut $chans, metric::Event::new_telemetry(telem));
        }
    };
}

/// Internal as Source
///
/// The 'run' of Internal will pull Telemetry off the internal queue, apply
/// Internal's configured tags and push said telemetry into operator configured
/// channels. If no channels are configured we toss the Telemetry onto the
/// floor.
impl source::Source<InternalConfig> for Internal {
    /// Create a new Internal
    fn init(_config: InternalConfig) -> Self {
        Internal {}
    }

    #[allow(clippy::cyclomatic_complexity)]
    fn run(self, mut chans: util::Channel, poller: mio::Poll) -> () {
        let slp = std::time::Duration::from_millis(1_000);
        loop {
            let mut events = mio::Events::with_capacity(1024);
            match poller.poll(&mut events, Some(slp)) {
                Err(_) => error!("Failed to poll for system events"),
                // Internal source doesn't register any external evented sources.
                // Any event must be a system event which, at the time of this writing,
                // can only be a shutdown event.
                Ok(num_events) if num_events > 0 => {
                    util::send(&mut chans, metric::Event::Shutdown);
                    return;
                }
                Ok(_) => {
                    if !chans.is_empty() {
                        atom_telem!(
                            "cernan.util.hopper.error.full",
                            util::UTIL_SEND_HOPPER_ERROR_FULL,
                            chans
                        );
                        // source::graphite
                        atom_telem!(
                            "cernan.graphite.new_peer",
                            source::graphite::GRAPHITE_NEW_PEER,
                            chans
                        );
                        atom_telem!(
                            "cernan.graphite.packet",
                            source::graphite::GRAPHITE_GOOD_PACKET,
                            chans
                        );
                        atom_telem!(
                            "cernan.graphite.telemetry.received",
                            source::graphite::GRAPHITE_TELEM,
                            chans
                        );
                        atom_telem!(
                            "cernan.graphite.bad_packet",
                            source::graphite::GRAPHITE_BAD_PACKET,
                            chans
                        );
                        // source::statsd
                        atom_telem!(
                            "cernan.statsd.packet",
                            source::statsd::STATSD_GOOD_PACKET,
                            chans
                        );
                        atom_telem!(
                            "cernan.statsd.bad_packet",
                            source::statsd::STATSD_BAD_PACKET,
                            chans
                        );
                        // source::native
                        atom_telem!(
                            "cernan.native.payload.success",
                            source::native::NATIVE_PAYLOAD_SUCCESS_SUM,
                            chans
                        );
                        atom_telem!(
                            "cernan.native.payload.parse.failure",
                            source::native::NATIVE_PAYLOAD_PARSE_FAILURE_SUM,
                            chans
                        );
                        // source::avro
                        atom_telem!(
                            "cernan.avro.payload.success",
                            source::avro::AVRO_PAYLOAD_SUCCESS_SUM,
                            chans
                        );
                        atom_telem!(
                            "cernan.avro.payload.parse.failure",
                            source::avro::AVRO_PAYLOAD_PARSE_FAILURE_SUM,
                            chans
                        );
                        atom_telem!(
                            "cernan.avro.payload.io.failure",
                            source::avro::AVRO_PAYLOAD_IO_FAILURE_SUM,
                            chans
                        );
                        // sink::elasticsearch
                        atom_telem!(
                            "cernan.sinks.elasticsearch.records.delivery",
                            sink::elasticsearch::ELASTIC_RECORDS_DELIVERY,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.elasticsearch.internal.buffer_len",
                            sink::elasticsearch::ELASTIC_INTERNAL_BUFFER_LEN,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.elasticsearch.records.total_delivered",
                            sink::elasticsearch::ELASTIC_RECORDS_TOTAL_DELIVERED,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.elasticsearch.records.total_failed",
                            sink::elasticsearch::ELASTIC_RECORDS_TOTAL_FAILED,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.elasticsearch.error.unknown",
                            sink::elasticsearch::ELASTIC_ERROR_UNKNOWN,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.elasticsearch.error.bulk_action.index",
                            sink::elasticsearch::ELASTIC_BULK_ACTION_INDEX_ERR,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.elasticsearch.error.bulk_action.create",
                            sink::elasticsearch::ELASTIC_BULK_ACTION_CREATE_ERR,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.elasticsearch.error.bulk_action.update",
                            sink::elasticsearch::ELASTIC_BULK_ACTION_UPDATE_ERR,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.elasticsearch.error.bulk_action.delete",
                            sink::elasticsearch::ELASTIC_BULK_ACTION_DELETE_ERR,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.elasticsearch.error.api.index_not_found",
                            sink::elasticsearch::ELASTIC_ERROR_API_INDEX_NOT_FOUND,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.elasticsearch.error.api.parsing",
                            sink::elasticsearch::ELASTIC_ERROR_API_PARSING,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.elasticsearch.error.api.mapper_parsing",
                            sink::elasticsearch::ELASTIC_ERROR_API_MAPPER_PARSING,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.elasticsearch.error.api.action_request_validation",
                            sink::elasticsearch::ELASTIC_ERROR_API_ACTION_REQUEST_VALIDATION,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.elasticsearch.error.api.action_document_missing",
                            sink::elasticsearch::ELASTIC_ERROR_API_DOCUMENT_MISSING,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.elasticsearch.error.api.index_already_exists",
                            sink::elasticsearch::ELASTIC_ERROR_API_INDEX_ALREADY_EXISTS,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.elasticsearch.error.api.unknown",
                            sink::elasticsearch::ELASTIC_ERROR_API_UNKNOWN,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.elasticsearch.error.client",
                            sink::elasticsearch::ELASTIC_ERROR_CLIENT,
                            chans
                        );
                        // sink::wavefront
                        atom_telem!(
                            "cernan.sinks.wavefront.proxy.connection_attempts",
                            sink::wavefront::WAVEFRONT_CONNECT_ATTEMPTS,
                            chans
                        );
                        atom_set_telem!(
                            "cernan.sinks.wavefront.aggregations.total_stored",
                            sink::wavefront::WAVEFRONT_AGGR_STORED_VALUES,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.wavefront.aggregation.histogram",
                            sink::wavefront::WAVEFRONT_AGGR_HISTO,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.wavefront.aggregation.sum",
                            sink::wavefront::WAVEFRONT_AGGR_SUM,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.wavefront.aggregation.set",
                            sink::wavefront::WAVEFRONT_AGGR_SET,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.wavefront.aggregation.summarize",
                            sink::wavefront::WAVEFRONT_AGGR_SUMMARIZE,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.wavefront.aggregation.summarize.total_percentiles",
                            sink::wavefront::WAVEFRONT_AGGR_TOT_PERCENT,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.wavefront.delivery.success",
                            sink::wavefront::WAVEFRONT_DELIVERY_SUCCESS,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.wavefront.delivery.failure",
                            sink::wavefront::WAVEFRONT_DELIVERY_FAILURE,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.wavefront.valve.closed",
                            sink::wavefront::WAVEFRONT_VALVE_CLOSED,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.wavefront.valve.open",
                            sink::wavefront::WAVEFRONT_VALVE_OPEN,
                            chans
                        );
                        // sink::prometheus
                        atom_telem!(
                            "cernan.sinks.prometheus.aggregation.inside_baseball.windowed.purged",
                            sink::prometheus::PROMETHEUS_AGGR_WINDOWED_PURGED,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.prometheus.exposition.inside_baseball.delay.sum",
                            sink::prometheus::PROMETHEUS_RESPONSE_DELAY_SUM,
                            chans
                        );
                        atom_set_telem!(
                            "cernan.sinks.prometheus.aggregation.reportable",
                            sink::prometheus::PROMETHEUS_AGGR_REPORTABLE,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.prometheus.report.success",
                            sink::prometheus::PROMETHEUS_REPORT_SUCCESS,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.prometheus.report.error",
                            sink::prometheus::PROMETHEUS_REPORT_ERROR,
                            chans
                        );
                        // sink::influxdb
                        atom_telem!(
                            "cernan.sinks.influxdb.delivery_attempts",
                            sink::influxdb::INFLUX_DELIVERY_ATTEMPTS,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.influxdb.success",
                            sink::influxdb::INFLUX_SUCCESS,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.influxdb.failure.client_error",
                            sink::influxdb::INFLUX_FAILURE_CLIENT,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.influxdb.failure.server_error",
                            sink::influxdb::INFLUX_FAILURE_SERVER,
                            chans
                        );
                        // sink::kafka
                        atom_telem!(
                            "cernan.sinks.kafka.publish.success",
                            sink::kafka::KAFKA_PUBLISH_SUCCESS_SUM,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.kafka.publish.retry",
                            sink::kafka::KAFKA_PUBLISH_RETRY_SUM,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.kafka.publish.failure",
                            sink::kafka::KAFKA_PUBLISH_FAILURE_SUM,
                            chans
                        );
                        atom_telem!(
                            "cernan.sinks.kafka.publish.retry.failure",
                            sink::kafka::KAFKA_PUBLISH_RETRY_FAILURE_SUM,
                            chans
                        );
                        // filter::delay_filter
                        atom_telem!(
                            "cernan.filters.delay.telemetry.accept",
                            filter::delay_filter::DELAY_TELEM_ACCEPT,
                            chans
                        );
                        atom_telem!(
                            "cernan.filters.delay.telemetry.reject",
                            filter::delay_filter::DELAY_TELEM_REJECT,
                            chans
                        );
                        atom_telem!(
                            "cernan.filters.delay.log.reject",
                            filter::delay_filter::DELAY_LOG_REJECT,
                            chans
                        );
                        atom_telem!(
                            "cernan.filters.delay.log.accept",
                            filter::delay_filter::DELAY_LOG_ACCEPT,
                            chans
                        );
                        // filter::json_encode_filter
                        atom_telem!(
                            "cernan.filters.json_encode.log.processed",
                            filter::json_encode_filter::JSON_ENCODE_LOG_PROCESSED,
                            chans
                        );
                        atom_telem!(
                            "cernan.filters.json_encode.log.parsed",
                            filter::json_encode_filter::JSON_ENCODE_LOG_PARSED,
                            chans
                        );
                        while let Some(telem) = Q.pop() {
                            if !chans.is_empty() {
                                util::send(
                                    &mut chans,
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
    }
}
