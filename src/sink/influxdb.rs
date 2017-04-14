use hyper::Client;
use metric::{LogLine, TagMap, Telemetry};
use sink::{Sink, Valve};
use source::report_telemetry;
use std::cmp;
use std::string;
use std::sync;
use time;
use url::Url;

pub struct InfluxDB {
    secure: bool,
    db: String,
    host: String,
    port: u16,
    aggrs: Vec<Telemetry>,
    delivery_attempts: u32,
    flush_interval: u64,
}

#[derive(Debug)]
pub struct InfluxDBConfig {
    pub secure: bool,
    pub db: String,
    pub host: String,
    pub port: u16,
    pub config_path: String,
    pub tags: TagMap,
    pub flush_interval: u64,
}

#[inline]
fn fmt_tags(tags: &TagMap, s: &mut String) -> () {
    let mut iter = tags.iter();
    if let Some(&(ref fk, ref fv)) = iter.next() {
        s.push_str(fk);
        s.push_str("=");
        s.push_str(fv);
        for &(ref k, ref v) in iter {
            s.push_str(",");
            s.push_str(k);
            s.push_str("=");
            s.push_str(v);
        }
    }
}

#[inline]
fn get_from_cache<T>(cache: &mut Vec<(T, String)>, val: T) -> &str
    where T: cmp::PartialOrd + string::ToString + Copy
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
    pub fn new(config: InfluxDBConfig) -> InfluxDB {
        InfluxDB {
            secure: config.secure,
            db: config.db,
            host: config.host,
            port: config.port,
            aggrs: Vec::with_capacity(4048),
            delivery_attempts: 0,
            flush_interval: config.flush_interval,
        }
    }

    /// Convert the slice into a payload that can be sent to InfluxDB
    pub fn format_stats(&self, mut buffer: &mut String, telems: &[Telemetry]) -> () {
        let mut time_cache: Vec<(u64, String)> = Vec::with_capacity(128);
        let mut value_cache: Vec<(f64, String)> = Vec::with_capacity(128);

        let mut tag_buf = String::with_capacity(1_024);
        for telem in telems.iter() {
            if let Some(val) = telem.value() {
                buffer.push_str(&telem.name);
                buffer.push_str(",");
                fmt_tags(&telem.tags, &mut tag_buf);
                buffer.push_str(&tag_buf);
                buffer.push_str(" ");
                buffer.push_str("value=");
                buffer.push_str(get_from_cache(&mut value_cache, val));
                buffer.push_str(" ");
                buffer.push_str(get_from_cache(&mut time_cache, telem.timestamp_ns));
                buffer.push_str("\n");
                tag_buf.clear();
            }
        }
    }

    #[cfg(test)]
    pub fn aggr_slice(&self) -> &[Telemetry] {
        &self.aggrs
    }
}

impl Sink for InfluxDB {
    fn flush_interval(&self) -> Option<u64> {
        Some(self.flush_interval)
    }

    fn flush(&mut self) {
        let mut buffer = String::with_capacity(4048);
        self.format_stats(&mut buffer, &self.aggrs);

        loop {
            let client = Client::new();
            let scheme = if self.secure { "https" } else { "http" };
            let uri = Url::parse(&format!("{}://{}:{}/write?db={}",
                                          scheme,
                                          self.host,
                                          self.port,
                                          self.db))
                    .ok()
                    .expect("malformed url");

            report_telemetry("cernan.sinks.influxdb.delivery_attempts",
                             self.delivery_attempts as f64);
            if self.delivery_attempts > 0 {
                debug!("delivery attempts: {}", self.delivery_attempts);
            }
            time::delay(self.delivery_attempts);

            match client.post(uri).body(&buffer).send() {
                Err(e) => debug!("hyper error doing POST: {:?}", e),
                Ok(resp) => {
                    // https://docs.influxdata.com/influxdb/v1.
                    // 2/guides/writing_data/#http-response-summary
                    self.delivery_attempts = self.delivery_attempts.saturating_add(1) % 10;
                    if resp.status.is_success() {
                        report_telemetry("cernan.sinks.influxdb.success", 1.0);
                        buffer.clear();
                        self.delivery_attempts = 0;
                        break;
                    } else if resp.status.is_client_error() {
                        report_telemetry("cernan.sinks.influxdb.failure.client_error", 1.0);
                    } else if resp.status.is_server_error() {
                        report_telemetry("cernan.sinks.influxdb.failure.server_error", 1.0);
                    }
                }
            }
        }

        self.aggrs.clear();
    }

    fn deliver(&mut self, mut point: sync::Arc<Option<Telemetry>>) -> () {
        self.aggrs
            .push(sync::Arc::make_mut(&mut point).take().unwrap());
    }

    fn deliver_line(&mut self, _: sync::Arc<Option<LogLine>>) -> () {
        // nothing, intentionally
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
    extern crate quickcheck;

    use super::*;
    use chrono::{TimeZone, UTC};
    use metric::{TagMap, Telemetry};
    use sink::Sink;
    use std::sync::Arc;

    #[test]
    fn test_format_influxdb() {
        let mut tags = TagMap::default();
        tags.insert("source".into(), "test-src".into());
        let config = InfluxDBConfig {
            db: "cernan".to_string(),
            host: "127.0.0.1".to_string(),
            secure: false,
            port: 1987,
            config_path: "sinks.influxdb".to_string(),
            tags: tags.clone(),
            flush_interval: 60,
        };
        let mut influxdb = InfluxDB::new(config);
        let dt_0 = UTC.ymd(1990, 6, 12).and_hms_milli(9, 10, 11, 00);
        let dt_1 = UTC.ymd(1990, 6, 12).and_hms_milli(9, 10, 12, 00);
        let dt_2 = UTC.ymd(1990, 6, 12).and_hms_milli(9, 10, 13, 00);
        influxdb.deliver(Arc::new(Some(Telemetry::new("test.counter", -1.0)
                                           .timestamp_and_ns(dt_0.timestamp(),
                                                             dt_0.timestamp_subsec_nanos())
                                           .aggr_sum()
                                           .overlay_tags_from_map(&tags))));
        influxdb.deliver(Arc::new(Some(Telemetry::new("test.counter", 2.0)
                                           .timestamp_and_ns(dt_0.timestamp(),
                                                             dt_0.timestamp_subsec_nanos())
                                           .aggr_sum()
                                           .overlay_tags_from_map(&tags))));
        influxdb.deliver(Arc::new(Some(Telemetry::new("test.counter", 3.0)
                                           .timestamp_and_ns(dt_1.timestamp(),
                                                             dt_1.timestamp_subsec_nanos())
                                           .aggr_sum()
                                           .overlay_tags_from_map(&tags))));
        influxdb.deliver(Arc::new(Some(Telemetry::new("test.gauge", 3.211)
                                           .timestamp_and_ns(dt_0.timestamp(),
                                                             dt_0.timestamp_subsec_nanos())
                                           .aggr_set()
                                           .overlay_tags_from_map(&tags))));
        influxdb.deliver(Arc::new(Some(Telemetry::new("test.gauge", 4.322)
                                           .timestamp_and_ns(dt_1.timestamp(),
                                                             dt_1.timestamp_subsec_nanos())
                                           .aggr_set()
                                           .overlay_tags_from_map(&tags))));
        influxdb.deliver(Arc::new(Some(Telemetry::new("test.gauge", 5.433)
                                           .timestamp_and_ns(dt_2.timestamp(),
                                                             dt_2.timestamp_subsec_nanos())
                                           .aggr_set()
                                           .overlay_tags_from_map(&tags))));
        influxdb.deliver(Arc::new(Some(Telemetry::new("test.timer", 12.101)
                                           .timestamp_and_ns(dt_0.timestamp(),
                                                             dt_0.timestamp_subsec_nanos())
                                           .aggr_summarize()
                                           .overlay_tags_from_map(&tags))));
        influxdb.deliver(Arc::new(Some(Telemetry::new("test.timer", 1.101)
                                           .timestamp_and_ns(dt_0.timestamp(),
                                                             dt_0.timestamp_subsec_nanos())
                                           .aggr_summarize()
                                           .overlay_tags_from_map(&tags))));
        influxdb.deliver(Arc::new(Some(Telemetry::new("test.timer", 3.101)
                                           .timestamp_and_ns(dt_0.timestamp(),
                                                             dt_0.timestamp_subsec_nanos())
                                           .aggr_summarize()
                                           .overlay_tags_from_map(&tags))));
        influxdb.deliver(Arc::new(Some(Telemetry::new("test.raw", 1.0)
                                           .timestamp_and_ns(dt_0.timestamp(),
                                                             dt_0.timestamp_subsec_nanos())
                                           .aggr_set()
                                           .overlay_tags_from_map(&tags))));
        influxdb.deliver(Arc::new(Some(Telemetry::new("test.raw", 2.0)
                                           .timestamp_and_ns(dt_1.timestamp(),
                                                             dt_1.timestamp_subsec_nanos())
                                           .aggr_set()
                                           .overlay_tags_from_map(&tags))));
        let mut buffer = String::new();
        influxdb.format_stats(&mut buffer, influxdb.aggr_slice());
        let lines: Vec<&str> = buffer.lines().collect();

        println!("{:?}", lines);
        assert_eq!(11, lines.len());
        assert!(lines.contains(&"test.counter,source=test-src value=-1 645181811000000000"));
        assert!(lines.contains(&"test.counter,source=test-src value=2 645181811000000000"));
        assert!(lines.contains(&"test.counter,source=test-src value=3 645181812000000000"));
        assert!(lines.contains(&"test.gauge,source=test-src value=3.211 645181811000000000"));
        assert!(lines.contains(&"test.gauge,source=test-src value=4.322 645181812000000000"));
        assert!(lines.contains(&"test.gauge,source=test-src value=5.433 645181813000000000"));
        assert!(lines.contains(&"test.timer,source=test-src value=12.101 645181811000000000"));
        assert!(lines.contains(&"test.timer,source=test-src value=1.101 645181811000000000"));
        assert!(lines.contains(&"test.timer,source=test-src value=3.101 645181811000000000"));
        assert!(lines.contains(&"test.raw,source=test-src value=1 645181811000000000"));
        assert!(lines.contains(&"test.raw,source=test-src value=2 645181812000000000"));
    }
}
