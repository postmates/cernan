use super::super::backend::Backend;
use super::super::buckets::Buckets;
use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};
use std::str::FromStr;
use std::fmt::Write;
use std::io::Write as IoWrite;
use time;

#[derive(Debug)]
pub struct Wavefront {
    addr: SocketAddrV4,
    tags: String,
}

impl Wavefront {
    /// Create a Wavefront formatter
    ///
    /// # Examples
    ///
    /// ```
    /// let wave = Wavefront::new(host, port, source);
    /// ```
    pub fn new(host: &str, port: u16, tags: String) -> Wavefront {
        let ip = Ipv4Addr::from_str(&host).unwrap();
        let addr = SocketAddrV4::new(ip, port);
        Wavefront {
            addr: addr,
            tags: tags,
        }
    }

    /// Convert the buckets into a String that
    /// can be sent to the the wavefront proxy
    pub fn format_stats(&self, buckets: &Buckets, curtime: Option<i64>) -> String {
        let start = match curtime {
            Some(x) => x,
            None => time::get_time().sec,
        };

        let mut stats = String::new();
        write!(stats,
               "{} {} {} {}\n",
               "cernan.bad_messages",
               buckets.bad_messages(),
               start,
               self.tags)
            .unwrap();
        write!(stats,
               "{} {} {} {}\n",
               "cernan.total_messages",
               buckets.total_messages(),
               start,
               self.tags)
            .unwrap();

        for (key, value) in buckets.counters().iter() {
            write!(stats, "{} {} {} {}\n", key, value, start, self.tags).unwrap();
        }

        for (key, value) in buckets.gauges().iter() {
            write!(stats, "{} {} {} {}\n", key, value, start, self.tags).unwrap();
        }

        for (key, value) in buckets.histograms().iter() {
            for tup in [("min", 0.0),
                        ("max", 1.0),
                        ("50", 0.5),
                        ("90", 0.9),
                        ("99", 0.99),
                        ("999", 0.999)]
                .iter() {
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

        for (key, value) in buckets.timers().iter() {
            for tup in [("min", 0.0),
                        ("max", 1.0),
                        ("50", 0.5),
                        ("90", 0.9),
                        ("99", 0.99),
                        ("999", 0.999)]
                .iter() {
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

        stats
    }
}

impl Backend for Wavefront {
    fn flush(&mut self, buckets: &Buckets) {
        let stats = self.format_stats(&buckets, Some(time::get_time().sec));

        let mut stream = TcpStream::connect(self.addr).unwrap();
        let _ = stream.write(stats.as_bytes());
    }
}

#[cfg(test)]
mod test {
    use super::super::super::metric::{Metric, MetricKind};
    use super::super::super::buckets::Buckets;
    use super::*;

    fn make_buckets() -> Buckets {
        let mut buckets = Buckets::new();
        let m1 = Metric::new("test.counter", 1.0, MetricKind::Counter(1.0));
        let m2 = Metric::new("test.gauge", 3.211, MetricKind::Gauge);

        let m3 = Metric::new("test.timer", 12.101, MetricKind::Timer);
        let m4 = Metric::new("test.timer", 1.101, MetricKind::Timer);
        let m5 = Metric::new("test.timer", 3.101, MetricKind::Timer);
        buckets.add(&m1);
        buckets.add(&m2);
        buckets.add(&m3);
        buckets.add(&m4);
        buckets.add(&m5);
        buckets
    }

    #[test]
    fn test_format_wavefront_buckets_no_timers() {
        let buckets = make_buckets();
        let wavefront = Wavefront::new("127.0.0.1", 2003, "source=test-src".to_string());
        let result = wavefront.format_stats(&buckets, Some(10101));
        let lines: Vec<&str> = result.lines().collect();

        println!("{:?}", lines);
        assert_eq!(10, lines.len());
        assert!(lines[0].contains("cernan.bad_messages 0 10101 source=test-src"));
        assert!(lines[1].contains("cernan.total_messages 5 10101 source=test-src"));
        assert!(lines[2].contains("test.counter 1 10101 source=test-src"));
        assert!(lines[3].contains("test.gauge 3.211 10101 source=test-src"));
        assert!(lines[4].contains("test.timer.min 1.101 10101 source=test-src"));
        assert!(lines[5].contains("test.timer.max 12.101 10101 source=test-src"));
        assert!(lines[6].contains("test.timer.50 3.101 10101 source=test-src"));
        assert!(lines[7].contains("test.timer.90 12.101 10101 source=test-src"));
        assert!(lines[8].contains("test.timer.99 12.101 10101 source=test-src"));
        assert!(lines[9].contains("test.timer.999 12.101 10101 source=test-src"));
    }
}
