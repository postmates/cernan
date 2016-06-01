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
    source: String,
}

impl Wavefront {
    /// Create a Wavefront formatter
    ///
    /// # Examples
    ///
    /// ```
    /// let wave = Wavefront::new(host, port, source);
    /// ```
    pub fn new(host: &str, port: u16, source: &str) -> Wavefront {
        let ip = Ipv4Addr::from_str(&host).unwrap();
        let addr = SocketAddrV4::new(ip, port);
        Wavefront {
            addr: addr,
            source: String::from_str(source).unwrap(),
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
               "{} {} {} source={}\n",
               "cernan.bad_messages",
               buckets.bad_messages(),
               start,
               self.source)
            .unwrap();
        write!(stats,
               "{} {} {} source={}\n",
               "cernan.total_messages",
               buckets.total_messages(),
               start,
               self.source)
            .unwrap();

        for (key, value) in buckets.counters().iter() {
            write!(stats,
                   "{} {} {} source={}\n",
                   key,
                   value,
                   start,
                   self.source)
                .unwrap();
        }

        for (key, value) in buckets.gauges().iter() {
            write!(stats,
                   "{} {} {} source={}\n",
                   key,
                   value,
                   start,
                   self.source)
                .unwrap();
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
        let wavefront = Wavefront::new("127.0.0.1", 2003, "test-src");
        let result = wavefront.format_stats(&buckets, Some(10101));
        let lines: Vec<&str> = result.lines().collect();

        assert_eq!(4, lines.len());
        println!("{:?}", lines);
        assert!(lines[0].contains("cernan.bad_messages 0 10101 source=test-src"));
        assert!(lines[1].contains("cernan.total_messages 5 10101 source=test-src"));
        assert!(lines[2].contains("test.counter 1 10101 source=test-src"));
        assert!(lines[3].contains("test.gauge 3.211 10101 source=test-src"));
    }
}
