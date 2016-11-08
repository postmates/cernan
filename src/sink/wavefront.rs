use std::net::{SocketAddr, TcpStream};
use std::fmt::Write;
use std::io::Write as IoWrite;
use metric::{Metric, LogLine, TagMap};
use buckets::Buckets;
use sink::Sink;
use dns_lookup;
use time;

pub struct Wavefront {
    addr: SocketAddr,
    aggrs: Buckets,
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
    pub fn new(host: &str, port: u16, bin_width: i64) -> Wavefront {
        match dns_lookup::lookup_host(host) {
            Ok(mut lh) => {
                let ip = lh.next().expect("No IPs associated with host").unwrap();
                let addr = SocketAddr::new(ip, port);
                Wavefront {
                    addr: addr,
                    aggrs: Buckets::new(bin_width),
                }
            }
            Err(_) => panic!("Could not lookup host"),
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
            match TcpStream::connect(self.addr) {
                Ok(mut stream) => {
                    let res = stream.write(self.format_stats().as_bytes());
                    if res.is_ok() {
                        trace!("flushed to wavefront!");
                        self.aggrs.reset();
                        break;
                    } else {
                        attempts += 1;
                    }
                }
                Err(e) => {
                    info!("unable to connect to proxy at addr {} with error {}",
                          self.addr,
                          e)
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
        let mut wavefront = Wavefront::new("localhost", 2003, 1);
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
