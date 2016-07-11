use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};
use std::str::FromStr;
use std::fmt::Write;
use std::io::Write as IoWrite;
use chrono;
use metric::Metric;
use buckets::Buckets;
use backend::Backend;
use std::rc::Rc;

pub struct Wavefront {
    addr: SocketAddrV4,
    tags: String,
    mk_aggrs: bool,
    // The wavefront implementation keeps an aggregate, depricated, and ships
    // exact points. The aggregate is 'aggrs' and the exact points is 'points'.
    aggrs: Buckets,
    points: Vec<Rc<Metric>>,
}

impl Wavefront {
    /// Create a Wavefront formatter
    ///
    /// # Examples
    ///
    /// ```
    /// let wave = Wavefront::new(host, port, source);
    /// ```
    pub fn new(host: &str, port: u16, skip_aggrs: bool, tags: String) -> Wavefront {
        let ip = Ipv4Addr::from_str(host).unwrap();
        let addr = SocketAddrV4::new(ip, port);
        Wavefront {
            addr: addr,
            tags: tags,
            mk_aggrs: !skip_aggrs,
            aggrs: Buckets::new(),
            points: Vec::new(),
        }
    }

    /// Convert the buckets into a String that
    /// can be sent to the the wavefront proxy
    pub fn format_stats(&self, curtime: Option<i64>) -> String {
        let start = match curtime {
            Some(x) => x,
            None => chrono::UTC::now().timestamp(),
        };
        let mut stats = String::new();

        if self.mk_aggrs {
            for (key, value) in self.aggrs.counters().iter() {
                write!(stats, "{} {} {} {}\n", key, value, start, self.tags).unwrap();
            }

            for (key, value) in self.aggrs.gauges().iter() {
                write!(stats, "{} {} {} {}\n", key, value, start, self.tags).unwrap();
            }

            for (key, value) in self.aggrs.histograms().iter() {
                for tup in &[("min", 0.0),
                             ("max", 1.0),
                             ("50", 0.5),
                             ("90", 0.9),
                             ("99", 0.99),
                             ("999", 0.999)] {
                    let stat: &str = tup.0;
                    let quant: f64 = tup.1;
                    write!(stats,
                           "{}.{} {} {} {}\n",
                           key,
                           stat,
                           value.query(quant).unwrap().1,
                           start,
                           self.tags)
                        .unwrap();
                }
            }

            for (key, value) in self.aggrs.timers().iter() {
                for tup in &[("min", 0.0),
                             ("max", 1.0),
                             ("50", 0.5),
                             ("90", 0.9),
                             ("99", 0.99),
                             ("999", 0.999)] {
                    let stat: &str = tup.0;
                    let quant: f64 = tup.1;
                    write!(stats,
                           "{}.{} {} {} {}\n",
                           key,
                           stat,
                           value.query(quant).unwrap().1,
                           start,
                           self.tags)
                        .unwrap();
                }
            }
        }

        for m in &self.points {
            write!(stats,
                   "{} {} {} {}\n",
                   m.name,
                   m.value,
                   m.time.timestamp(),
                   self.tags)
                .unwrap();
        }

        stats
    }
}

impl Backend for Wavefront {
    fn flush(&mut self) {
        debug!("wavefront flush");
        let stats = self.format_stats(None);
        debug!("wavefront - {}", stats);
        self.points.clear();
        let mut stream = TcpStream::connect(self.addr).unwrap();
        let _ = stream.write(stats.as_bytes());
    }

    fn deliver(&mut self, point: Rc<Metric>) {
        debug!("wavefront deliver");
        if self.mk_aggrs {
            self.aggrs.add(&point);
        }
        self.points.push(point);
    }
}

#[cfg(test)]
mod test {
    use metric::{Metric, MetricKind};
    use backend::Backend;
    use chrono::{UTC, TimeZone};
    use std::rc::Rc;
    use string_cache::Atom;
    use super::*;

    #[test]
    fn test_format_wavefront() {
        let mut wavefront = Wavefront::new("127.0.0.1", 2003, false, "source=test-src".to_string());
        let dt = UTC.ymd(1990, 6, 12).and_hms_milli(9, 10, 11, 12);
        wavefront.deliver(Rc::new(Metric::new_with_time(Atom::from("test.counter"),
                                                        1.0,
                                                        Some(dt),
                                                        MetricKind::Counter(1.0))));
        wavefront.deliver(Rc::new(Metric::new_with_time(Atom::from("test.gauge"),
                                                        3.211,
                                                        Some(dt),
                                                        MetricKind::Gauge)));
        wavefront.deliver(Rc::new(Metric::new_with_time(Atom::from("test.timer"),
                                                        12.101,
                                                        Some(dt),
                                                        MetricKind::Timer)));
        wavefront.deliver(Rc::new(Metric::new_with_time(Atom::from("test.timer"),
                                                        1.101,
                                                        Some(dt),
                                                        MetricKind::Timer)));
        wavefront.deliver(Rc::new(Metric::new_with_time(Atom::from("test.timer"),
                                                        3.101,
                                                        Some(dt),
                                                        MetricKind::Timer)));
        let result = wavefront.format_stats(Some(10101));
        let lines: Vec<&str> = result.lines().collect();

        println!("{:?}", lines);
        assert_eq!(13, lines.len());
        assert_eq!(lines[0], "test.counter 1 10101 source=test-src");
        assert_eq!(lines[1], "test.gauge 3.211 10101 source=test-src");
        assert_eq!(lines[2], "test.timer.min 1.101 10101 source=test-src");
        assert_eq!(lines[3], "test.timer.max 12.101 10101 source=test-src");
        assert_eq!(lines[4], "test.timer.50 3.101 10101 source=test-src");
        assert_eq!(lines[5], "test.timer.90 12.101 10101 source=test-src");
        assert_eq!(lines[6], "test.timer.99 12.101 10101 source=test-src");
        assert_eq!(lines[7], "test.timer.999 12.101 10101 source=test-src");
        assert_eq!(lines[8], "test.counter 1 645181811 source=test-src");
        assert_eq!(lines[9], "test.gauge 3.211 645181811 source=test-src");
        assert_eq!(lines[10], "test.timer 12.101 645181811 source=test-src");
        assert_eq!(lines[11], "test.timer 1.101 645181811 source=test-src");
        assert_eq!(lines[12], "test.timer 3.101 645181811 source=test-src");
    }

    #[test]
    fn test_format_wavefront_skip_aggrs() {
        let mut wavefront = Wavefront::new("127.0.0.1", 2003, true, "source=test-src".to_string());
        let dt = UTC.ymd(1990, 6, 12).and_hms_milli(9, 10, 11, 12);
        wavefront.deliver(Rc::new(Metric::new_with_time(Atom::from("test.counter"),
                                                        1.0,
                                                        Some(dt),
                                                        MetricKind::Counter(1.0))));
        wavefront.deliver(Rc::new(Metric::new_with_time(Atom::from("test.gauge"),
                                                        3.211,
                                                        Some(dt),
                                                        MetricKind::Gauge)));
        wavefront.deliver(Rc::new(Metric::new_with_time(Atom::from("test.timer"),
                                                        12.101,
                                                        Some(dt),
                                                        MetricKind::Timer)));
        wavefront.deliver(Rc::new(Metric::new_with_time(Atom::from("test.timer"),
                                                        1.101,
                                                        Some(dt),
                                                        MetricKind::Timer)));
        wavefront.deliver(Rc::new(Metric::new_with_time(Atom::from("test.timer"),
                                                        3.101,
                                                        Some(dt),
                                                        MetricKind::Timer)));
        let result = wavefront.format_stats(Some(10101));
        let lines: Vec<&str> = result.lines().collect();

        println!("{:?}", lines);
        assert_eq!(5, lines.len());
        assert_eq!(lines[0], "test.counter 1 645181811 source=test-src");
        assert_eq!(lines[1], "test.gauge 3.211 645181811 source=test-src");
        assert_eq!(lines[2], "test.timer 12.101 645181811 source=test-src");
        assert_eq!(lines[3], "test.timer 1.101 645181811 source=test-src");
        assert_eq!(lines[4], "test.timer 3.101 645181811 source=test-src");
    }

}
