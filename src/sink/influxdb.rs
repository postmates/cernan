use buckets::Buckets;
use metric::{LogLine, Metric, TagMap};
use sink::{Sink, Valve};
use std::cmp;
use std::net::{ToSocketAddrs, UdpSocket};
use std::string;
use std::sync;
use time;

pub struct InfluxDB {
    host: String,
    port: u16,
    aggrs: Buckets,
    delivery_attempts: u32,
    stats: String,
}

#[derive(Debug)]
pub struct InfluxDBConfig {
    pub bin_width: i64,
    pub host: String,
    pub port: u16,
    pub config_path: String,
    pub tags: TagMap,
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

#[inline]
fn ms_to_ns(ms_time: i64) -> i64 {
    ms_time * 1000000
}

impl InfluxDB {
    pub fn new(config: InfluxDBConfig) -> InfluxDB {
        InfluxDB {
            host: config.host,
            port: config.port,
            aggrs: Buckets::new(config.bin_width),
            delivery_attempts: 0,
            stats: String::with_capacity(8_192),
        }
    }

    /// Convert the buckets into a String that
    /// can be sent to the the wavefront proxy
    pub fn format_stats(&mut self) -> () {
        let mut time_cache: Vec<(i64, String)> = Vec::with_capacity(128);
        let mut count_cache: Vec<(usize, String)> = Vec::with_capacity(128);
        let mut value_cache: Vec<(f64, String)> = Vec::with_capacity(128);

        let flat_aggrs = self.aggrs
            .counters()
            .iter()
            .chain(self.aggrs.gauges().iter())
            .chain(self.aggrs.raws().iter());

        let mut tag_buf = String::with_capacity(1_024);
        for (key, vals) in flat_aggrs {
            for m in vals {
                if let Some(val) = m.value() {
                    self.stats.push_str(key);
                    self.stats.push_str(",");
                    fmt_tags(&m.tags, &mut tag_buf);
                    self.stats.push_str(&tag_buf);
                    self.stats.push_str(" ");
                    self.stats.push_str("value=");
                    self.stats.push_str(get_from_cache(&mut value_cache, val));
                    self.stats.push_str(",");
                    self.stats.push_str("count=");
                    self.stats.push_str(get_from_cache(&mut count_cache, m.count()));
                    self.stats.push_str(" ");
                    self.stats.push_str(get_from_cache(&mut time_cache, ms_to_ns(m.time)));
                    self.stats.push_str("\n");
                    tag_buf.clear();
                }
            }
        }

        let high_aggrs = self.aggrs.histograms().iter().chain(self.aggrs.timers().iter());

        for (key, hists) in high_aggrs {
            for hist in hists {
                let time = ms_to_ns(hist.time);
                let count = hist.count();

                self.stats.push_str(key);
                self.stats.push_str(",");
                fmt_tags(&hist.tags, &mut tag_buf);
                self.stats.push_str(&tag_buf);
                self.stats.push_str(" ");
                self.stats.push_str("min=");
                self.stats.push_str(get_from_cache(&mut value_cache, hist.query(0.0).unwrap()));
                self.stats.push_str(",");
                self.stats.push_str("max=");
                self.stats.push_str(get_from_cache(&mut value_cache, hist.query(1.0).unwrap()));
                self.stats.push_str(",");
                self.stats.push_str("25=");
                self.stats.push_str(get_from_cache(&mut value_cache, hist.query(0.25).unwrap()));
                self.stats.push_str(",");
                self.stats.push_str("50=");
                self.stats.push_str(get_from_cache(&mut value_cache, hist.query(0.5).unwrap()));
                self.stats.push_str(",");
                self.stats.push_str("75=");
                self.stats.push_str(get_from_cache(&mut value_cache, hist.query(0.75).unwrap()));
                self.stats.push_str(",");
                self.stats.push_str("90=");
                self.stats.push_str(get_from_cache(&mut value_cache, hist.query(0.90).unwrap()));
                self.stats.push_str(",");
                self.stats.push_str("95=");
                self.stats.push_str(get_from_cache(&mut value_cache, hist.query(0.95).unwrap()));
                self.stats.push_str(",");
                self.stats.push_str("99=");
                self.stats.push_str(get_from_cache(&mut value_cache, hist.query(0.99).unwrap()));
                self.stats.push_str(",");
                self.stats.push_str("999=");
                self.stats.push_str(get_from_cache(&mut value_cache, hist.query(0.999).unwrap()));
                self.stats.push_str(",");
                self.stats.push_str("count=");
                self.stats.push_str(get_from_cache(&mut count_cache, count));
                self.stats.push_str(" ");
                self.stats.push_str(get_from_cache(&mut time_cache, time));
                self.stats.push_str("\n");
                tag_buf.clear();
            }
        }
    }
}

impl Sink for InfluxDB {
    fn flush(&mut self) {
        loop {
            if self.delivery_attempts > 0 {
                debug!("delivery attempts: {}", self.delivery_attempts);
            }
            let addrs = (self.host.as_str(), self.port).to_socket_addrs();
            match addrs {
                Ok(srv) => {
                    let ips: Vec<_> = srv.collect();
                    for ip in ips {
                        time::delay(self.delivery_attempts);
                        match UdpSocket::bind("0.0.0.0:0") {
                            Ok(socket) => {
                                self.format_stats();
                                let res = socket.send_to(self.stats.as_bytes(), ip);
                                if res.is_ok() {
                                    self.aggrs.reset();
                                    self.stats.clear();
                                    self.delivery_attempts = 0;
                                    return;
                                } else {
                                    self.delivery_attempts = self.delivery_attempts
                                        .saturating_add(1);
                                }
                            }
                            Err(e) => {
                                info!("Unable to connect to db at {} using addr {} with error \
                                       {}",
                                      self.host,
                                      ip,
                                      e)
                            }
                        }
                    }
                }
                Err(e) => {
                    info!("Unable to perform DNS lookup on host {} with error {}",
                          self.host,
                          e);
                }
            }
        }
    }

    fn deliver(&mut self, mut point: sync::Arc<Option<Metric>>) -> () {
        self.aggrs.add(sync::Arc::make_mut(&mut point).take().unwrap());
    }

    fn deliver_line(&mut self, _: sync::Arc<Option<LogLine>>) -> () {
        // nothing, intentionally
    }

    fn valve_state(&self) -> Valve {
        if self.aggrs.count() > 10_000 {
            Valve::Closed
        } else {
            Valve::Open
        }
    }
}

#[cfg(test)]
mod test {
    extern crate quickcheck;

    use chrono::{TimeZone, UTC};
    use metric::{Metric, TagMap};
    use sink::Sink;
    use std::sync::Arc;
    use super::*;

    #[test]
    fn test_format_influxdb() {
        let mut tags = TagMap::default();
        tags.insert("source".into(), "test-src".into());
        let config = InfluxDBConfig {
            bin_width: 1,
            host: "127.0.0.1".to_string(),
            port: 1987,
            config_path: "sinks.influxdb".to_string(),
            tags: tags.clone(),
        };
        let mut influxdb = InfluxDB::new(config);
        let dt_0 = UTC.ymd(1990, 6, 12).and_hms_milli(9, 10, 11, 00).timestamp();
        let dt_1 = UTC.ymd(1990, 6, 12).and_hms_milli(9, 10, 12, 00).timestamp();
        let dt_2 = UTC.ymd(1990, 6, 12).and_hms_milli(9, 10, 13, 00).timestamp();
        influxdb.deliver(Arc::new(Some(Metric::new("test.counter", -1.0)
            .time(dt_0)
            .counter()
            .overlay_tags_from_map(&tags))));
        influxdb.deliver(Arc::new(Some(Metric::new("test.counter", 2.0)
            .time(dt_0)
            .counter()
            .overlay_tags_from_map(&tags))));
        influxdb.deliver(Arc::new(Some(Metric::new("test.counter", 3.0)
            .time(dt_1)
            .counter()
            .overlay_tags_from_map(&tags))));
        influxdb.deliver(Arc::new(Some(Metric::new("test.gauge", 3.211)
            .time(dt_0)
            .gauge()
            .overlay_tags_from_map(&tags))));
        influxdb.deliver(Arc::new(Some(Metric::new("test.gauge", 4.322)
            .time(dt_1)
            .gauge()
            .overlay_tags_from_map(&tags))));
        influxdb.deliver(Arc::new(Some(Metric::new("test.gauge", 5.433)
            .time(dt_2)
            .gauge()
            .overlay_tags_from_map(&tags))));
        influxdb.deliver(Arc::new(Some(Metric::new("test.timer", 12.101)
            .time(dt_0)
            .timer()
            .overlay_tags_from_map(&tags))));
        influxdb.deliver(Arc::new(Some(Metric::new("test.timer", 1.101)
            .time(dt_0)
            .timer()
            .overlay_tags_from_map(&tags))));
        influxdb.deliver(Arc::new(Some(Metric::new("test.timer", 3.101)
            .time(dt_0)
            .timer()
            .overlay_tags_from_map(&tags))));
        influxdb.deliver(Arc::new(Some(Metric::new("test.raw", 1.0)
            .time(dt_0)
            .overlay_tags_from_map(&tags)
            .created_time(dt_0))));
        influxdb.deliver(Arc::new(Some(Metric::new("test.raw", 2.0)
            .time(dt_1)
            .overlay_tags_from_map(&tags)
            .created_time(dt_0))));
        influxdb.format_stats();
        let lines: Vec<&str> = influxdb.stats.lines().collect();

        println!("{:?}", lines);
        assert_eq!(8, lines.len());
        assert!(lines.contains(&"test.counter,source=test-src value=1,count=2 645181811000000"));
        assert!(lines.contains(&"test.counter,source=test-src value=3,count=1 645181812000000"));
        assert!(lines.contains(&"test.gauge,source=test-src value=3.211,count=1 645181811000000"));
        assert!(lines.contains(&"test.gauge,source=test-src value=4.322,count=1 645181812000000"));
        assert!(lines.contains(&"test.gauge,source=test-src value=5.433,count=1 645181813000000"));
        assert!(lines.contains(&"test.raw,source=test-src value=1,count=1 645181811000000"));
        assert!(lines.contains(&"test.raw,source=test-src value=2,count=1 645181812000000"));
        assert!(lines.contains(&"test.timer,source=test-src \
                                 min=1.101,max=12.101,25=1.101,50=3.101,75=3.101,90=12.101,\
                                 95=12.101,99=12.101,999=12.101,count=3 645181811000000"));
    }
}
