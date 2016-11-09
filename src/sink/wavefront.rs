use std::net::TcpStream;
use std::fmt::Write;
use std::io::Write as IoWrite;
use metric::{Metric, LogLine, TagMap};
use buckets::Buckets;
use sink::Sink;
use std::net::ToSocketAddrs;
use time;

pub struct Wavefront {
    host: String,
    port: u16,
    aggrs: Buckets,
}

#[derive(Debug)]
pub struct WavefrontConfig {
    pub bin_width: i64,
    pub host: String,
    pub port: u16,
}

impl Default for WavefrontConfig {
    fn default() -> WavefrontConfig {
        WavefrontConfig {
            bin_width: 1,
            host: String::from("localhost"),
            port: 2878,
        }
    }
}

#[inline]
fn fmt_tags(tags: &TagMap) -> String {
    let mut s = String::new();
    let mut iter = tags.iter();
    if let Some((fk, fv)) = iter.next() {
        write!(s, "{}={}", fk, fv).unwrap();
        for (k, v) in iter {
            write!(s, " {}={}", k, v).unwrap();
        }
    }
    s
}

impl Wavefront {
    pub fn new(config: WavefrontConfig) -> Wavefront {
        Wavefront {
            host: config.host,
            port: config.port,
            aggrs: Buckets::new(config.bin_width),
        }
    }

    /// Convert the buckets into a String that
    /// can be sent to the the wavefront proxy
    pub fn format_stats(&mut self) -> String {
        let mut stats = String::new();

        let flat_aggrs = self.aggrs
            .counters()
            .iter()
            .chain(self.aggrs.gauges().iter())
            .chain(self.aggrs.raws().iter());

        for (key, vals) in flat_aggrs {
            for m in vals {
                if let Some(v) = m.value() {
                    write!(stats, "{} {} {} {}\n", key, v, m.time, fmt_tags(&m.tags)).unwrap();
                }
            }
        }

        let high_aggrs = self.aggrs.histograms().iter().chain(self.aggrs.timers().iter());

        for (key, hists) in high_aggrs {
            for hist in hists {
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
                    write!(stats,
                           "{}.{} {} {} {}\n",
                           key,
                           stat,
                           hist.query(quant).unwrap(),
                           hist.time,
                           fmt_tags(&hist.tags))
                        .unwrap()
                }
                let count = hist.count();
                write!(stats,
                       "{}.count {} {} {}\n",
                       key,
                       count,
                       hist.time,
                       fmt_tags(&hist.tags))
                    .unwrap();
            }
        }

        stats
    }
}

impl Sink for Wavefront {
    fn flush(&mut self) {
        let mut attempts = 0;
        loop {
            if attempts > 0 {
                debug!("delivery attempts: {}", attempts);
            }
            time::delay(attempts);
            let addrs = (self.host.as_str(), self.port).to_socket_addrs();
            match addrs {
                Ok(srv) => {
                    let ips: Vec<_> = srv.collect();
                    for ip in ips {
                        match TcpStream::connect(ip) {
                            Ok(mut stream) => {
                                let res = stream.write(self.format_stats().as_bytes());
                                if res.is_ok() {
                                    trace!("flushed to wavefront!");
                                    self.aggrs.reset();
                                    return;
                                } else {
                                    attempts += 1;
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

    fn deliver(&mut self, point: Metric) {
        self.aggrs.add(point);
    }

    fn deliver_lines(&mut self, _: Vec<LogLine>) {
        // nothing, intentionally
    }
}

#[cfg(test)]
mod test {
    extern crate quickcheck;

    use metric::{Metric, TagMap};
    use sink::Sink;
    use chrono::{UTC, TimeZone};
    use super::*;

    #[test]
    fn test_format_wavefront() {
        let mut tags = TagMap::default();
        tags.insert("source".into(), "test-src".into());
        let config = WavefrontConfig::default();
        let mut wavefront = Wavefront::new(config);
        let dt_0 = UTC.ymd(1990, 6, 12).and_hms_milli(9, 10, 11, 00).timestamp();
        let dt_1 = UTC.ymd(1990, 6, 12).and_hms_milli(9, 10, 12, 00).timestamp();
        let dt_2 = UTC.ymd(1990, 6, 12).and_hms_milli(9, 10, 13, 00).timestamp();
        wavefront.deliver(Metric::new("test.counter", -1.0)
            .time(dt_0)
            .counter()
            .overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.counter", 2.0)
            .time(dt_0)
            .counter()
            .overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.counter", 3.0)
            .time(dt_1)
            .counter()
            .overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.gauge", 3.211)
            .time(dt_0)
            .gauge()
            .overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.gauge", 4.322)
            .time(dt_1)
            .gauge()
            .overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.gauge", 5.433)
            .time(dt_2)
            .gauge()
            .overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.timer", 12.101)
            .time(dt_0)
            .timer()
            .overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.timer", 1.101)
            .time(dt_0)
            .timer()
            .overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.timer", 3.101)
            .time(dt_0)
            .timer()
            .overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.raw", 1.0).time(dt_0).overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.raw", 2.0).time(dt_1).overlay_tags_from_map(&tags));
        let result = wavefront.format_stats();
        let lines: Vec<&str> = result.lines().collect();

        println!("{:?}", lines);
        assert_eq!(21, lines.len());
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
