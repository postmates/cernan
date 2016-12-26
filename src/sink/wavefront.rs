use buckets::Buckets;
use metric::{LogLine, Metric, TagMap};
use sink::{Sink, Valve};
use std::cmp;
use std::io::Write as IoWrite;
use std::net::TcpStream;
use std::net::ToSocketAddrs;
use std::string;
use std::sync;
use time;

pub struct Wavefront {
    host: String,
    port: u16,
    aggrs: Buckets,
    delivery_attempts: u32,
    pub stats: String,
}

#[derive(Debug)]
pub struct WavefrontConfig {
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
            s.push_str(" ");
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

impl Wavefront {
    pub fn new(config: WavefrontConfig) -> Wavefront {
        Wavefront {
            host: config.host,
            port: config.port,
            aggrs: Buckets::new(config.bin_width),
            delivery_attempts: 0,
            stats: String::with_capacity(8_192),
        }
    }

    /// Convert the buckets into a String that
    /// can be sent to the the wavefront proxy
    pub fn format_stats(&mut self, _: i64) -> () {
        let mut time_cache: Vec<(i64, String)> = Vec::with_capacity(128);
        let mut count_cache: Vec<(usize, String)> = Vec::with_capacity(128);
        let mut value_cache: Vec<(f64, String)> = Vec::with_capacity(128);

        let flat_aggrs = self.aggrs
            .counters()
            .iter()
            .chain(self.aggrs.gauges().iter())
            .chain(self.aggrs.delta_gauges().iter())
            .chain(self.aggrs.raws().iter());

        let mut tag_buf = String::with_capacity(1_024);
        for (key, vals) in flat_aggrs {
            for m in vals {
                if let Some(v) = m.value() {
                    self.stats.push_str(key);
                    self.stats.push_str(" ");
                    self.stats.push_str(get_from_cache(&mut value_cache, v));
                    self.stats.push_str(" ");
                    self.stats.push_str(get_from_cache(&mut time_cache, m.time));
                    self.stats.push_str(" ");
                    fmt_tags(&m.tags, &mut tag_buf);
                    self.stats.push_str(&tag_buf);
                    self.stats.push_str("\n");

                    tag_buf.clear();
                }
            }
        }

        let high_aggrs = self.aggrs.histograms().iter().chain(self.aggrs.timers().iter());

        for (key, hists) in high_aggrs {
            for hist in hists {
                fmt_tags(&hist.tags, &mut tag_buf);
                for tup in &[("min", 0.0),
                             ("max", 1.0),
                             ("2", 0.02),
                             ("9", 0.09),
                             ("25", 0.25),
                             ("50", 0.5),
                             ("75", 0.75),
                             ("90", 0.90),
                             ("91", 0.91),
                             ("95", 0.95),
                             ("98", 0.98),
                             ("99", 0.99),
                             ("999", 0.999)] {
                    let stat: &str = tup.0;
                    let quant: f64 = tup.1;
                    self.stats.push_str(key);
                    self.stats.push_str(".");
                    self.stats.push_str(stat);
                    self.stats.push_str(" ");
                    self.stats
                        .push_str(get_from_cache(&mut value_cache, hist.query(quant).unwrap()));
                    self.stats.push_str(" ");
                    self.stats.push_str(get_from_cache(&mut time_cache, hist.time));
                    self.stats.push_str(" ");
                    self.stats.push_str(&tag_buf);
                    self.stats.push_str("\n");
                }
                let count = hist.count();
                self.stats.push_str(key);
                self.stats.push_str(".count");
                self.stats.push_str(" ");
                self.stats.push_str(get_from_cache(&mut count_cache, count));
                self.stats.push_str(" ");
                self.stats.push_str(get_from_cache(&mut time_cache, hist.time));
                self.stats.push_str(" ");
                self.stats.push_str(&tag_buf);
                self.stats.push_str("\n");

                tag_buf.clear();
            }
        }
    }
}

impl Sink for Wavefront {
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
                        match TcpStream::connect(ip) {
                            Ok(mut stream) => {
                                self.format_stats(time::now());
                                let res = stream.write(self.stats.as_bytes());
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
                                info!("Unable to connect to proxy at {} using addr {} with error \
                                       {}",
                                      self.host,
                                      ip,
                                      e)
                            }
                        }
                        time::delay(self.delivery_attempts);
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
    fn test_format_wavefront() {
        let mut tags = TagMap::default();
        tags.insert("source".into(), "test-src".into());
        let config = WavefrontConfig {
            bin_width: 1,
            host: "127.0.0.1".to_string(),
            port: 1987,
            config_path: "sinks.wavefront".to_string(),
            tags: tags.clone(),
        };
        let mut wavefront = Wavefront::new(config);
        let dt_0 = UTC.ymd(1990, 6, 12).and_hms_milli(9, 10, 11, 00).timestamp();
        let dt_1 = UTC.ymd(1990, 6, 12).and_hms_milli(9, 10, 12, 00).timestamp();
        let dt_2 = UTC.ymd(1990, 6, 12).and_hms_milli(9, 10, 13, 00).timestamp();
        wavefront.deliver(Arc::new(Some(Metric::new("test.counter", -1.0)
            .time(dt_0)
            .counter()
            .overlay_tags_from_map(&tags))));
        wavefront.deliver(Arc::new(Some(Metric::new("test.counter", 2.0)
            .time(dt_0)
            .counter()
            .overlay_tags_from_map(&tags))));
        wavefront.deliver(Arc::new(Some(Metric::new("test.counter", 3.0)
            .time(dt_1)
            .counter()
            .overlay_tags_from_map(&tags))));
        wavefront.deliver(Arc::new(Some(Metric::new("test.gauge", 3.211)
            .time(dt_0)
            .gauge()
            .overlay_tags_from_map(&tags))));
        wavefront.deliver(Arc::new(Some(Metric::new("test.gauge", 4.322)
            .time(dt_1)
            .gauge()
            .overlay_tags_from_map(&tags))));
        wavefront.deliver(Arc::new(Some(Metric::new("test.gauge", 5.433)
            .time(dt_2)
            .gauge()
            .overlay_tags_from_map(&tags))));
        wavefront.deliver(Arc::new(Some(Metric::new("test.timer", 12.101)
            .time(dt_0)
            .timer()
            .overlay_tags_from_map(&tags))));
        wavefront.deliver(Arc::new(Some(Metric::new("test.timer", 1.101)
            .time(dt_0)
            .timer()
            .overlay_tags_from_map(&tags))));
        wavefront.deliver(Arc::new(Some(Metric::new("test.timer", 3.101)
            .time(dt_0)
            .timer()
            .overlay_tags_from_map(&tags))));
        wavefront.deliver(Arc::new(Some(Metric::new("test.raw", 1.0)
            .time(dt_0)
            .overlay_tags_from_map(&tags)
            .created_time(dt_0))));
        wavefront.deliver(Arc::new(Some(Metric::new("test.raw", 2.0)
            .time(dt_1)
            .overlay_tags_from_map(&tags)
            .created_time(dt_0))));
        wavefront.format_stats(dt_2);
        let lines: Vec<&str> = wavefront.stats.lines().collect();

        println!("{:?}", lines);
        assert!(lines.contains(&"test.counter 1 645181811 source=test-src"));
        assert!(lines.contains(&"test.counter 3 645181812 source=test-src"));
        assert!(lines.contains(&"test.gauge 3.211 645181811 source=test-src"));
        assert!(lines.contains(&"test.gauge 4.322 645181812 source=test-src"));
        assert!(lines.contains(&"test.gauge 5.433 645181813 source=test-src"));
        assert!(lines.contains(&"test.timer.min 1.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.max 12.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.2 1.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.9 1.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.25 1.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.50 3.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.75 3.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.90 12.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.91 12.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.95 12.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.98 12.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.99 12.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.999 12.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.count 3 645181811 source=test-src"));
        assert!(lines.contains(&"test.raw 1 645181811 source=test-src"));
    }
}
