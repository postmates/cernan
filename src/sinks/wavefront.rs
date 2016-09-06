use std::net::{SocketAddr, TcpStream};
use std::fmt::Write;
use std::io::Write as IoWrite;
use chrono;
use metric::{Metric, MetricQOS};
use buckets::Buckets;
use sink::Sink;
use std::sync::Arc;
use dns_lookup;

pub struct Wavefront {
    addr: SocketAddr,
    tags: String,
    aggrs: Buckets,
    qos: MetricQOS,
    snapshots: Vec<String>,
    tot_snapshots: u64,
}

impl Wavefront {
    /// Create a Wavefront formatter
    ///
    /// # Examples
    ///
    /// ```
    /// let wave = Wavefront::new(host, port, source);
    /// ```
    pub fn new(host: &str, port: u16, tags: String, qos: MetricQOS) -> Wavefront {
        match dns_lookup::lookup_host(host) {
            Ok(mut lh) => {
                let ip = lh.next().expect("No IPs associated with host").unwrap();
                let addr = SocketAddr::new(ip, port);
                Wavefront {
                    addr: addr,
                    tags: tags,
                    aggrs: Buckets::new(),
                    qos: qos,
                    snapshots: Vec::new(),
                    tot_snapshots: 0,
                }
            }
            Err(_) => panic!("Could not lookup host"),
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

        trace!("TOTSNP: {:?}, QOS: {:?}", self.tot_snapshots, self.qos);

        if (self.tot_snapshots % self.qos.counter) == 0 {
            for (key, value) in self.aggrs.counters().iter() {
                write!(stats, "{} {} {} {}\n", key, value / (self.qos.counter as f64), start, self.tags).unwrap();
            }
        }

        if (self.tot_snapshots % self.qos.gauge) == 0 {
            for (key, value) in self.aggrs.gauges().iter() {
                write!(stats, "{} {} {} {}\n", key, value, start, self.tags).unwrap();
            }
        }

        if (self.tot_snapshots % self.qos.histogram) == 0 {
            for (key, value) in self.aggrs.histograms().iter() {
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
                           value.query(quant).unwrap().1,
                           start,
                           self.tags)
                        .unwrap();
                }
                let count = value.count();
                write!(stats, "{}.count {} {} {}\n", key, count, start, self.tags).unwrap();
            }
        }

        if (self.tot_snapshots % self.qos.timer) == 0 {
            for (key, value) in self.aggrs.timers().iter() {
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
                           value.query(quant).unwrap().1,
                           start,
                           self.tags)
                        .unwrap();
                }
                let count = value.count();
                write!(stats, "{}.count {} {} {}\n", key, count, start, self.tags).unwrap();
            }
        }

        stats
    }
}

impl Sink for Wavefront {
    fn snapshot(&mut self) {
        self.tot_snapshots = self.tot_snapshots.wrapping_add(1);
        let stats = self.format_stats(None);
        if stats.len() > 0 {
            debug!("wavefront - {}", stats);
            self.snapshots.push(stats);
            trace!("snapshots : {:?}", self.snapshots);
            self.aggrs.reset();
        }
    }

    fn flush(&mut self) {
        if self.snapshots.len() > 0 {
            match TcpStream::connect(self.addr) {
                Ok(mut stream) => {
                    debug!("wavefront flush");
                    if self.snapshots
                        .iter()
                        .map(|s| stream.write(s.as_bytes()))
                        .all(|res| res.is_ok()) {
                        trace!("flushed to wavefront!");
                        self.snapshots.clear();
                    }
                }
                Err(e) => debug!("Unable to connect: {}", e),
            }
        }
    }

    fn deliver(&mut self, point: Arc<Metric>) {
        debug!("wavefront deliver");
        self.aggrs.add(&point);
    }
}

#[cfg(test)]
mod test {
    use metric::{Metric, MetricKind, MetricQOS};
    use sink::Sink;
    use chrono::{UTC, TimeZone};
    use std::sync::Arc;
    use string_cache::Atom;
    use super::*;

    #[test]
    fn test_format_wavefront() {
        let qos = MetricQOS::default();
        let mut wavefront = Wavefront::new("localhost", 2003, "source=test-src".to_string(), qos);
        let dt = UTC.ymd(1990, 6, 12).and_hms_milli(9, 10, 11, 12).timestamp();
        wavefront.deliver(Arc::new(Metric::new_with_time(Atom::from("test.counter"),
                                                         1.0,
                                                         Some(dt),
                                                         MetricKind::Counter(1.0))));
        wavefront.deliver(Arc::new(Metric::new_with_time(Atom::from("test.gauge"),
                                                         3.211,
                                                         Some(dt),
                                                         MetricKind::Gauge)));
        wavefront.deliver(Arc::new(Metric::new_with_time(Atom::from("test.timer"),
                                                         12.101,
                                                         Some(dt),
                                                         MetricKind::Timer)));
        wavefront.deliver(Arc::new(Metric::new_with_time(Atom::from("test.timer"),
                                                         1.101,
                                                         Some(dt),
                                                         MetricKind::Timer)));
        wavefront.deliver(Arc::new(Metric::new_with_time(Atom::from("test.timer"),
                                                         3.101,
                                                         Some(dt),
                                                         MetricKind::Timer)));
        let result = wavefront.format_stats(Some(10101));
        let lines: Vec<&str> = result.lines().collect();

        println!("{:?}", lines);
        assert_eq!(16, lines.len());
        assert_eq!(lines[0], "test.counter 1 10101 source=test-src");
        assert_eq!(lines[1], "test.gauge 3.211 10101 source=test-src");
        assert_eq!(lines[2], "test.timer.min 1.101 10101 source=test-src");
        assert_eq!(lines[3], "test.timer.max 12.101 10101 source=test-src");
        assert_eq!(lines[4], "test.timer.2 1.101 10101 source=test-src");
        assert_eq!(lines[5], "test.timer.9 1.101 10101 source=test-src");
        assert_eq!(lines[6], "test.timer.25 1.101 10101 source=test-src");
        assert_eq!(lines[7], "test.timer.50 3.101 10101 source=test-src");
        assert_eq!(lines[8], "test.timer.75 3.101 10101 source=test-src");
        assert_eq!(lines[9], "test.timer.90 12.101 10101 source=test-src");
        assert_eq!(lines[10], "test.timer.91 12.101 10101 source=test-src");
        assert_eq!(lines[11], "test.timer.95 12.101 10101 source=test-src");
        assert_eq!(lines[12], "test.timer.98 12.101 10101 source=test-src");
        assert_eq!(lines[13], "test.timer.99 12.101 10101 source=test-src");
        assert_eq!(lines[14], "test.timer.999 12.101 10101 source=test-src");
        assert_eq!(lines[15], "test.timer.count 3 10101 source=test-src");
    }
}
