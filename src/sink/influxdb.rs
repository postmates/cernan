//! `InfluxDB` is a telemetry database.

use metric::{TagIter, TagMap, Telemetry};
use quantiles::histogram::Bound;
use reqwest;
use sink::{Sink, Valve};
use source::flushes_per_second;
use std::cmp;
use std::string;
use std::sync::atomic::{AtomicUsize, Ordering};
use time;
use url::Url;

/// total delivery attempts made by this sink
pub static INFLUX_DELIVERY_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
/// total successful delivery attempts made by this sink
pub static INFLUX_SUCCESS: AtomicUsize = AtomicUsize::new(0);
/// total failed delivery attempts because of client error
pub static INFLUX_FAILURE_CLIENT: AtomicUsize = AtomicUsize::new(0);
/// total failed delivery attempts because of server error
pub static INFLUX_FAILURE_SERVER: AtomicUsize = AtomicUsize::new(0);

/// The `InfluxDB` structure
///
/// `InfluxDB` is a time-series database with nanosecond accuracy. This
/// structure holds all the information needed to communicate with it. See
/// `InfluxDBConfig` for configurable parameters.
pub struct InfluxDB {
    /// The store of Telemetry to be reported
    aggrs: Vec<Telemetry>,
    /// Number of failed delivery attempts by this sink. We keep track of this
    /// across flushes to avoid stampeding between flushes.
    delivery_attempts: u32,
    flush_interval: u64,
    client: reqwest::Client,
    uri: Url,
    tags: TagMap,
}

/// `InfluxDB` configuration
///
/// The cernan `InfluxDB` integration is done by HTTP/S. The options present
/// here assume that integration, as well as cernan inside-baseball.
#[derive(Clone, Debug, Deserialize)]
pub struct InfluxDBConfig {
    /// If secure, use HTTPS. Else, HTTP.
    pub secure: bool,
    /// The name of the database to connect to. This database MUST exist prior
    /// to cernan writing to it.
    pub db: String,
    /// The host machine toward which to report. May be an IP address or a DNS.
    pub host: String,
    /// The port of the host machine toward which to report.
    pub port: u16,
    /// The name of the influxdb sink in cernan.
    pub config_path: Option<String>,
    /// The default tags to apply to all telemetry flowing through the sink.
    pub tags: TagMap,
    /// The interval, in seconds, on which the `InfluxDB` sink will report.
    pub flush_interval: u64,
}

impl Default for InfluxDBConfig {
    fn default() -> Self {
        InfluxDBConfig {
            port: 8086,
            secure: true,
            host: "localhost".to_string(),
            db: "cernan".to_string(),
            config_path: None,
            tags: Default::default(),
            flush_interval: 60 * flushes_per_second(),
        }
    }
}

#[inline]
fn fmt_tags(tags: TagIter, s: &mut String) -> () {
    for (k, v) in tags {
        s.push_str(",");
        s.push_str(k);
        s.push_str("=");
        s.push_str(v);
    }
}

#[inline]
fn get_from_cache<T>(cache: &mut Vec<(T, String)>, val: T) -> &str
where
    T: cmp::PartialOrd + string::ToString + Copy,
{
    match cache.binary_search_by(|probe| probe.0.partial_cmp(&val).unwrap()) {
        Ok(idx) => &cache[idx].1,
        Err(idx) => {
            let str_val = val.to_string();
            cache.insert(idx, (val, str_val));
            get_from_cache(cache, val)
        }
    }
}

impl InfluxDB {
    /// Convert the slice into a payload that can be sent to InfluxDB
    fn format_stats(&self, buffer: &mut String, telems: &[Telemetry]) -> () {
        use metric::AggregationMethod;
        let mut time_cache: Vec<(u64, String)> = Vec::with_capacity(128);
        let mut value_cache: Vec<(f64, String)> = Vec::with_capacity(128);

        let mut tag_buf = String::with_capacity(1_024);
        for telem in telems.iter() {
            match telem.kind() {
                AggregationMethod::Sum => if let Some(val) = telem.sum() {
                    buffer.push_str(&telem.name);
                    fmt_tags(telem.tags(&self.tags), &mut tag_buf);
                    buffer.push_str(&tag_buf);
                    buffer.push_str(" ");
                    buffer.push_str("value=");
                    buffer.push_str(get_from_cache(&mut value_cache, val));
                    buffer.push_str(" ");
                    buffer.push_str(get_from_cache(
                        &mut time_cache,
                        telem.timestamp.saturating_mul(1_000_000_000) as u64,
                    ));
                    buffer.push_str("\n");
                    tag_buf.clear();
                },
                AggregationMethod::Set => if let Some(val) = telem.set() {
                    buffer.push_str(&telem.name);
                    fmt_tags(telem.tags(&self.tags), &mut tag_buf);
                    buffer.push_str(&tag_buf);
                    buffer.push_str(" ");
                    buffer.push_str("value=");
                    buffer.push_str(get_from_cache(&mut value_cache, val));
                    buffer.push_str(" ");
                    buffer.push_str(get_from_cache(
                        &mut time_cache,
                        telem.timestamp.saturating_mul(1_000_000_000) as u64,
                    ));
                    buffer.push_str("\n");
                    tag_buf.clear();
                },
                AggregationMethod::Histogram => if let Some(bin_iter) = telem.bins() {
                    for &(bound, count) in bin_iter {
                        let bound_name = match bound {
                            Bound::Finite(x) => format!("le_{}", x),
                            Bound::PosInf => "le_inf".to_string(),
                        };
                        buffer.push_str(&format!("{}.{}", &telem.name, bound_name));
                        fmt_tags(telem.tags(&self.tags), &mut tag_buf);
                        buffer.push_str(&tag_buf);
                        buffer.push_str(" ");
                        buffer.push_str("value=");
                        buffer
                            .push_str(get_from_cache(&mut value_cache, count as f64));
                        buffer.push_str(" ");
                        buffer.push_str(get_from_cache(
                            &mut time_cache,
                            telem.timestamp.saturating_mul(1_000_000_000) as u64,
                        ));
                        buffer.push_str("\n");
                        tag_buf.clear();
                    }
                },
                AggregationMethod::Summarize => for percentile in
                    &[0.25, 0.50, 0.75, 0.90, 0.99, 1.0]
                {
                    if let Some(val) = telem.query(*percentile) {
                        buffer.push_str(&format!("{}.{}", &telem.name, percentile));
                        fmt_tags(telem.tags(&self.tags), &mut tag_buf);
                        buffer.push_str(&tag_buf);
                        buffer.push_str(" ");
                        buffer.push_str("value=");
                        buffer.push_str(get_from_cache(&mut value_cache, val));
                        buffer.push_str(" ");
                        buffer.push_str(get_from_cache(
                            &mut time_cache,
                            telem.timestamp.saturating_mul(1_000_000_000) as u64,
                        ));
                        buffer.push_str("\n");
                        tag_buf.clear();
                    }
                },
            }
        }
    }

    #[cfg(test)]
    pub fn aggr_slice(&self) -> &[Telemetry] {
        &self.aggrs
    }
}

impl Sink<InfluxDBConfig> for InfluxDB {
    fn init(config: InfluxDBConfig) -> Self {
        let scheme = if config.secure { "https" } else { "http" };
        let uri = Url::parse(&format!(
            "{}://{}:{}/write?db={}",
            scheme, config.host, config.port, config.db
        )).expect("malformed url");

        let client = reqwest::Client::builder()
            .gzip(true)
            .build()
            .expect("could not create influxdb client");

        InfluxDB {
            aggrs: Vec::with_capacity(4048),
            delivery_attempts: 0,
            flush_interval: config.flush_interval,
            client,
            uri,
            tags: config.tags,
        }
    }

    fn flush_interval(&self) -> Option<u64> {
        Some(self.flush_interval)
    }

    fn flush(&mut self) {
        let mut buffer = String::with_capacity(4048);
        self.format_stats(&mut buffer, &self.aggrs);

        // report loop, infinite
        loop {
            // Report our delivery attempts and delay, potentially. As mentioned
            // in the documentation for the InfluxDB struct we want to avoid
            // stampeeding the database. If a flush fails X times then the next
            // flush will have delivery_attempts_1 = (X - 1) +
            // delivery_attempts_0 waits. The idea is that a failure that
            // happens to succeed may succeed against a degraded system; we
            // should not assume full health.
            INFLUX_DELIVERY_ATTEMPTS
                .fetch_add(self.delivery_attempts as usize, Ordering::Relaxed);
            time::delay(self.delivery_attempts);

            match self.client
                .post(self.uri.clone())
                .header(reqwest::header::Connection::keep_alive())
                .body(buffer.clone())
                .send()
            {
                Err(e) => debug!("hyper error doing POST: {:?}", e),
                Ok(resp) => {
                    // https://docs.influxdata.com/influxdb/v1.
                    // 2/guides/writing_data/#http-response-summary
                    if resp.status().is_success() {
                        INFLUX_SUCCESS.fetch_add(1, Ordering::Relaxed);
                        buffer.clear();
                        self.delivery_attempts =
                            self.delivery_attempts.saturating_sub(1);
                        break;
                    } else if resp.status().is_client_error() {
                        self.delivery_attempts =
                            self.delivery_attempts.saturating_add(1);
                        INFLUX_FAILURE_CLIENT.fetch_add(1, Ordering::Relaxed);
                    } else if resp.status().is_server_error() {
                        self.delivery_attempts =
                            self.delivery_attempts.saturating_add(1);
                        INFLUX_FAILURE_SERVER.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }

        self.aggrs.clear();
    }

    fn shutdown(mut self) -> () {
        self.flush();
    }

    fn deliver(&mut self, point: Telemetry) -> () {
        self.aggrs.push(point);
    }

    fn valve_state(&self) -> Valve {
        if self.aggrs.len() > 100_000 {
            Valve::Closed
        } else {
            Valve::Open
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::{TimeZone, Utc};
    use metric::{TagMap, Telemetry};
    use metric::AggregationMethod;
    use sink::Sink;

    #[test]
    fn test_format_influxdb() {
        let mut tags = TagMap::default();
        tags.insert("source".into(), "test-src".into());
        let mut custom_tags = TagMap::default();
        custom_tags.insert("filter".into(), "test-filter-mod".into());
        let config = InfluxDBConfig {
            db: "cernan".to_string(),
            host: "127.0.0.1".to_string(),
            secure: false,
            port: 1987,
            config_path: Some("sinks.influxdb".to_string()),
            tags: tags,
            flush_interval: 60 * flushes_per_second(),
        };
        let mut influxdb = InfluxDB::init(config);
        let dt_0 = Utc.ymd(1990, 6, 12).and_hms_milli(9, 10, 11, 00);
        let dt_1 = Utc.ymd(1990, 6, 12).and_hms_milli(9, 10, 12, 00);
        let dt_2 = Utc.ymd(1990, 6, 12).and_hms_milli(9, 10, 13, 00);
        influxdb.deliver(
            Telemetry::new()
                .name("test.counter")
                .value(-1.0)
                .timestamp(dt_0.timestamp())
                .kind(AggregationMethod::Sum)
                .harden()
                .unwrap()
                .overlay_tags_from_map(&custom_tags),
        );
        influxdb.deliver(
            Telemetry::new()
                .name("test.counter")
                .value(2.0)
                .timestamp(dt_0.timestamp())
                .kind(AggregationMethod::Sum)
                .harden()
                .unwrap(),
        );
        influxdb.deliver(
            Telemetry::new()
                .name("test.counter")
                .value(3.0)
                .timestamp(dt_1.timestamp())
                .kind(AggregationMethod::Sum)
                .harden()
                .unwrap(),
        );
        influxdb.deliver(
            Telemetry::new()
                .name("test.gauge")
                .value(3.211)
                .timestamp(dt_0.timestamp())
                .kind(AggregationMethod::Set)
                .harden()
                .unwrap(),
        );
        influxdb.deliver(
            Telemetry::new()
                .name("test.gauge")
                .value(4.322)
                .timestamp(dt_1.timestamp())
                .kind(AggregationMethod::Set)
                .harden()
                .unwrap(),
        );
        influxdb.deliver(
            Telemetry::new()
                .name("test.gauge")
                .value(5.433)
                .timestamp(dt_2.timestamp())
                .kind(AggregationMethod::Set)
                .harden()
                .unwrap(),
        );
        influxdb.deliver(
            Telemetry::new()
                .name("test.timer")
                .value(12.101)
                .timestamp(dt_0.timestamp())
                .kind(AggregationMethod::Summarize)
                .harden()
                .unwrap(),
        );
        influxdb.deliver(
            Telemetry::new()
                .name("test.timer")
                .value(1.101)
                .timestamp(dt_0.timestamp())
                .kind(AggregationMethod::Summarize)
                .harden()
                .unwrap(),
        );
        influxdb.deliver(
            Telemetry::new()
                .name("test.timer")
                .value(3.101)
                .timestamp(dt_0.timestamp())
                .kind(AggregationMethod::Summarize)
                .harden()
                .unwrap(),
        );
        influxdb.deliver(
            Telemetry::new()
                .name("test.raw")
                .value(1.0)
                .timestamp(dt_0.timestamp())
                .kind(AggregationMethod::Set)
                .harden()
                .unwrap(),
        );
        influxdb.deliver(
            Telemetry::new()
                .name("test.raw")
                .value(2.0)
                .timestamp(dt_1.timestamp())
                .kind(AggregationMethod::Set)
                .harden()
                .unwrap(),
        );
        let mut buffer = String::new();
        influxdb.format_stats(&mut buffer, influxdb.aggr_slice());
        let lines: Vec<&str> = buffer.lines().collect();

        println!("{:?}", lines);
        let expected = [
            "test.counter,filter=test-filter-mod,source=test-src value=-1 645181811000000000",
            "test.counter,source=test-src value=2 645181811000000000",
            "test.counter,source=test-src value=3 645181812000000000",
            "test.gauge,source=test-src value=3.211 645181811000000000",
            "test.gauge,source=test-src value=4.322 645181812000000000",
            "test.gauge,source=test-src value=5.433 645181813000000000",
            "test.timer.0.25,source=test-src value=12.101 645181811000000000",
            "test.timer.0.5,source=test-src value=12.101 645181811000000000",
            "test.timer.0.75,source=test-src value=12.101 645181811000000000",
            "test.timer.0.9,source=test-src value=12.101 645181811000000000",
            "test.timer.0.99,source=test-src value=12.101 645181811000000000",
            "test.timer.1,source=test-src value=12.101 645181811000000000",
            "test.timer.0.25,source=test-src value=1.101 645181811000000000",
            "test.timer.0.5,source=test-src value=1.101 645181811000000000",
            "test.timer.0.75,source=test-src value=1.101 645181811000000000",
            "test.timer.0.9,source=test-src value=1.101 645181811000000000",
            "test.timer.0.99,source=test-src value=1.101 645181811000000000",
            "test.timer.1,source=test-src value=1.101 645181811000000000",
            "test.timer.0.25,source=test-src value=3.101 645181811000000000",
            "test.timer.0.5,source=test-src value=3.101 645181811000000000",
            "test.timer.0.75,source=test-src value=3.101 645181811000000000",
            "test.timer.0.9,source=test-src value=3.101 645181811000000000",
            "test.timer.0.99,source=test-src value=3.101 645181811000000000",
            "test.timer.1,source=test-src value=3.101 645181811000000000",
            "test.raw,source=test-src value=1 645181811000000000",
            "test.raw,source=test-src value=2 645181812000000000",
        ];
        assert_eq!(expected.len(), lines.len());
        for line in &expected {
            println!("LINE: {:?}", line);
            assert!(lines.contains(line))
        }
    }
}
