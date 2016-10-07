use std::net::{SocketAddr, TcpStream};
use std::fmt::Write;
use std::io::Write as IoWrite;
use metric::{Metric, LogLine, MetricQOS};
use buckets::Buckets;
use sink::Sink;
use dns_lookup;
use time;

pub struct Wavefront {
    addr: SocketAddr,
    tags: String,
    aggrs: Buckets,
    qos: MetricQOS,
}

impl Wavefront {
    pub fn new(host: &str, port: u16, tags: String, qos: MetricQOS) -> Wavefront {
        match dns_lookup::lookup_host(host) {
            Ok(mut lh) => {
                let ip = lh.next().expect("No IPs associated with host").unwrap();
                let addr = SocketAddr::new(ip, port);
                Wavefront {
                    addr: addr,
                    tags: tags,
                    aggrs: Buckets::default(),
                    qos: qos,
                }
            }
            Err(_) => panic!("Could not lookup host"),
        }
    }

    /// Convert the buckets into a String that
    /// can be sent to the the wavefront proxy
    pub fn format_stats(&mut self) -> String {
        let mut stats = String::new();

        for (key, value) in self.aggrs.counters().iter() {
            for m in value {
                if (m.time % (self.qos.counter as i64)) == 0 {
                    write!(stats, "{} {} {} {}\n", key, m.value, m.time, self.tags).unwrap();
                }
            }
        }

        for (key, value) in self.aggrs.gauges().iter() {
            for m in value {
                if (m.time % (self.qos.gauge as i64)) == 0 {
                    write!(stats, "{} {} {} {}\n", key, m.value, m.time, self.tags).unwrap();
                }
            }
        }

        for (key, hists) in self.aggrs.histograms().iter() {
            for &(smpl_time, ref hist) in hists {
                if (smpl_time % (self.qos.histogram as i64)) == 0 {
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
                               hist.query(quant).unwrap().1,
                               smpl_time,
                               self.tags)
                            .unwrap()
                    }
                    let count = hist.count();
                    write!(stats, "{}.count {} {} {}\n", key, count, smpl_time, self.tags).unwrap();
                }
            }
        }

        for (key, tmrs) in self.aggrs.timers().iter() {
            for &(smpl_time, ref tmr) in tmrs {
                if (smpl_time % (self.qos.timer as i64)) == 0 {
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
                               tmr.query(quant).unwrap().1,
                               smpl_time,
                               self.tags)
                            .unwrap()
                    }
                    let count = tmr.count();
                    write!(stats, "{}.count {} {} {}\n", key, count, smpl_time, self.tags).unwrap();
                }
            }
        }

        // Raw points have no QOS as we can make no valid aggregation of them.
        for (key, value) in self.aggrs.raws().iter() {
            for m in value {
                write!(stats, "{} {} {} {}\n", key, m.value, m.time, self.tags).unwrap();
            }
        }

        stats
    }
}

impl Sink for Wavefront {
    fn flush(&mut self) {
        let mut attempts = 0;
        loop {
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
                Err(e) => debug!("Unable to connect: {}", e),
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
    use metric::{Metric, MetricKind, MetricQOS};
    use sink::Sink;
    use chrono::{UTC, TimeZone};
    use super::*;

    #[test]
    fn test_histogram_qos_ellision() {
        let mut qos = MetricQOS::default();
        qos.histogram = 2;

        let mut wavefront = Wavefront::new("localhost", 2003, "source=test-src".to_string(), qos);
        let dt_0 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 11, 0).timestamp();
        let dt_1 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 12, 0).timestamp();
        let dt_2 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 13, 0).timestamp();

        wavefront.deliver(Metric::new_with_time(String::from("test.histogram"),
                                                1.0,
                                                Some(dt_0),
                                                MetricKind::Histogram,
                                                None));
        wavefront.deliver(Metric::new_with_time(String::from("test.histogram"),
                                                1.0,
                                                Some(dt_1),
                                                MetricKind::Histogram,
                                                None));
        wavefront.deliver(Metric::new_with_time(String::from("test.histogram"),
                                                1.0,
                                                Some(dt_2),
                                                MetricKind::Histogram,
                                                None));

        let result = wavefront.format_stats();
        let lines: Vec<&str> = result.lines().collect();
        println!("{:?}", lines);
        assert_eq!(14, lines.len());
        // time dt_0
        assert!(!lines.contains(&"test.histogram.min 1 645185471 source=test-src"));
        assert!(!lines.contains(&"test.histogram.max 1 645185471 source=test-src"));
        assert!(!lines.contains(&"test.histogram.2 1 645185471 source=test-src"));
        assert!(!lines.contains(&"test.histogram.9 1 645185471 source=test-src"));
        assert!(!lines.contains(&"test.histogram.25 1 645185471 source=test-src"));
        assert!(!lines.contains(&"test.histogram.50 1 645185471 source=test-src"));
        assert!(!lines.contains(&"test.histogram.75 1 645185471 source=test-src"));
        assert!(!lines.contains(&"test.histogram.90 1 645185471 source=test-src"));
        assert!(!lines.contains(&"test.histogram.91 1 645185471 source=test-src"));
        assert!(!lines.contains(&"test.histogram.95 1 645185471 source=test-src"));
        assert!(!lines.contains(&"test.histogram.98 1 645185471 source=test-src"));
        assert!(!lines.contains(&"test.histogram.99 1 645185471 source=test-src"));
        assert!(!lines.contains(&"test.histogram.999 1 645185471 source=test-src"));
        assert!(!lines.contains(&"test.histogram.count 1 645185471 source=test-src"));
        // time dt_1
        assert!(lines.contains(&"test.histogram.min 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.histogram.max 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.histogram.2 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.histogram.9 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.histogram.25 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.histogram.50 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.histogram.75 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.histogram.90 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.histogram.91 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.histogram.95 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.histogram.98 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.histogram.99 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.histogram.999 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.histogram.count 1 645185472 source=test-src"));
        // time dt_2
        assert!(!lines.contains(&"test.histogram.min 1 645185473 source=test-src"));
        assert!(!lines.contains(&"test.histogram.max 1 645185473 source=test-src"));
        assert!(!lines.contains(&"test.histogram.2 1 645185473 source=test-src"));
        assert!(!lines.contains(&"test.histogram.9 1 645185473 source=test-src"));
        assert!(!lines.contains(&"test.histogram.25 1 645185473 source=test-src"));
        assert!(!lines.contains(&"test.histogram.50 1 645185473 source=test-src"));
        assert!(!lines.contains(&"test.histogram.75 1 645185473 source=test-src"));
        assert!(!lines.contains(&"test.histogram.90 1 645185473 source=test-src"));
        assert!(!lines.contains(&"test.histogram.91 1 645185473 source=test-src"));
        assert!(!lines.contains(&"test.histogram.95 1 645185473 source=test-src"));
        assert!(!lines.contains(&"test.histogram.98 1 645185473 source=test-src"));
        assert!(!lines.contains(&"test.histogram.99 1 645185473 source=test-src"));
        assert!(!lines.contains(&"test.histogram.999 1 645185473 source=test-src"));
        assert!(!lines.contains(&"test.histogram.count 1 645185473 source=test-src"));
    }

    #[test]
    fn test_timer_qos_ellision() {
        let mut qos = MetricQOS::default();
        qos.timer = 2;

        let mut wavefront = Wavefront::new("localhost", 2003, "source=test-src".to_string(), qos);
        let dt_0 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 11, 0).timestamp();
        let dt_1 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 12, 0).timestamp();
        let dt_2 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 13, 0).timestamp();

        wavefront.deliver(Metric::new_with_time(String::from("test.timer"),
                                                1.0,
                                                Some(dt_0),
                                                MetricKind::Timer,
                                                None));
        wavefront.deliver(Metric::new_with_time(String::from("test.timer"),
                                                1.0,
                                                Some(dt_1),
                                                MetricKind::Timer,
                                                None));
        wavefront.deliver(Metric::new_with_time(String::from("test.timer"),
                                                1.0,
                                                Some(dt_2),
                                                MetricKind::Timer,
                                                None));

        let result = wavefront.format_stats();
        let lines: Vec<&str> = result.lines().collect();
        println!("{:?}", lines);
        assert_eq!(14, lines.len());
        // time dt_1
        assert!(lines.contains(&"test.timer.min 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.timer.max 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.timer.2 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.timer.9 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.timer.25 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.timer.50 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.timer.75 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.timer.90 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.timer.91 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.timer.95 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.timer.98 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.timer.99 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.timer.999 1 645185472 source=test-src"));
        assert!(lines.contains(&"test.timer.count 1 645185472 source=test-src"));
    }

    #[test]
    fn test_gauge_qos_ellision() {
        let mut qos = MetricQOS::default();
        qos.gauge = 2;

        let mut wavefront = Wavefront::new("localhost", 2003, "source=test-src".to_string(), qos);
        let dt_0 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 11, 0).timestamp();
        let dt_1 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 12, 0).timestamp();
        let dt_2 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 13, 0).timestamp();
        let dt_3 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 14, 0).timestamp();
        let dt_4 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 15, 0).timestamp();

        wavefront.deliver(Metric::new_with_time(String::from("test.gauge"),
                                                1.0,
                                                Some(dt_0),
                                                MetricKind::Gauge,
                                                None));
        wavefront.deliver(Metric::new_with_time(String::from("test.gauge"),
                                                2.0,
                                                Some(dt_1),
                                                MetricKind::Gauge,
                                                None));
        wavefront.deliver(Metric::new_with_time(String::from("test.gauge"),
                                                3.0,
                                                Some(dt_2),
                                                MetricKind::Gauge,
                                                None));
        wavefront.deliver(Metric::new_with_time(String::from("test.gauge"),
                                                4.0,
                                                Some(dt_3),
                                                MetricKind::Gauge,
                                                None));
        wavefront.deliver(Metric::new_with_time(String::from("test.some_other_gauge"),
                                                1.0,
                                                Some(dt_3),
                                                MetricKind::Gauge,
                                                None));
        wavefront.deliver(Metric::new_with_time(String::from("test.gauge"),
                                                5.0,
                                                Some(dt_4),
                                                MetricKind::Gauge,
                                                None));
        let result = wavefront.format_stats();
        let lines: Vec<&str> = result.lines().collect();

        assert_eq!(645185471, dt_0); // elided
        assert_eq!(645185472, dt_1); // exist
        assert_eq!(645185473, dt_2); // elided
        assert_eq!(645185474, dt_3); // exist
        assert_eq!(645185475, dt_4); // elided

        println!("{:?}", lines);
        assert_eq!(3, lines.len());
        assert!(lines.contains(&"test.gauge 2 645185472 source=test-src"));
        assert!(lines.contains(&"test.gauge 4 645185474 source=test-src"));
        assert!(lines.contains(&"test.some_other_gauge 1 645185474 source=test-src"));
    }

    #[test]
    fn test_counter_qos_ellision() {
        let mut qos = MetricQOS::default();
        qos.counter = 2;

        let mut wavefront = Wavefront::new("localhost", 2003, "source=test-src".to_string(), qos);
        let dt_0 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 11, 0).timestamp();
        let dt_1 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 12, 0).timestamp();
        let dt_2 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 13, 0).timestamp();
        let dt_3 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 14, 0).timestamp();
        let dt_4 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 15, 0).timestamp();

        wavefront.deliver(Metric::new_with_time(String::from("test.counter"),
                                                1.0,
                                                Some(dt_0),
                                                MetricKind::Counter(1.0),
                                                None));
        wavefront.deliver(Metric::new_with_time(String::from("test.counter"),
                                                2.0,
                                                Some(dt_1),
                                                MetricKind::Counter(1.0),
                                                None));
        wavefront.deliver(Metric::new_with_time(String::from("test.counter"),
                                                3.0,
                                                Some(dt_2),
                                                MetricKind::Counter(1.0),
                                                None));
        wavefront.deliver(Metric::new_with_time(String::from("test.counter"),
                                                4.0,
                                                Some(dt_3),
                                                MetricKind::Counter(1.0),
                                                None));
        wavefront.deliver(Metric::new_with_time(String::from("test.some_other_counter"),
                                                1.0,
                                                Some(dt_3),
                                                MetricKind::Counter(1.0),
                                                None));
        wavefront.deliver(Metric::new_with_time(String::from("test.counter"),
                                                5.0,
                                                Some(dt_4),
                                                MetricKind::Counter(1.0),
                                                None));
        let result = wavefront.format_stats();
        let lines: Vec<&str> = result.lines().collect();

        assert_eq!(645185471, dt_0); // elided
        assert_eq!(645185472, dt_1); // exist
        assert_eq!(645185473, dt_2); // elided
        assert_eq!(645185474, dt_3); // exist
        assert_eq!(645185475, dt_4); // elided

        println!("{:?}", lines);
        assert_eq!(3, lines.len());
        assert!(lines.contains(&"test.counter 2 645185472 source=test-src"));
        assert!(lines.contains(&"test.counter 4 645185474 source=test-src"));
        assert!(lines.contains(&"test.some_other_counter 1 645185474 source=test-src"));
    }

    #[test]
    fn test_format_wavefront() {
        let qos = MetricQOS::default();
        let mut wavefront = Wavefront::new("localhost", 2003, "source=test-src".to_string(), qos);
        let dt_0 = UTC.ymd(1990, 6, 12).and_hms_milli(9, 10, 11, 12).timestamp();
        let dt_1 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 12, 13).timestamp();
        wavefront.deliver(Metric::new_with_time(String::from("test.counter"),
                                                -1.0,
                                                Some(dt_0),
                                                MetricKind::Counter(1.0),
                                                None));
        wavefront.deliver(Metric::new_with_time(String::from("test.counter"),
                                                2.0,
                                                Some(dt_0),
                                                MetricKind::Counter(1.0),
                                                None));
        wavefront.deliver(Metric::new_with_time(String::from("test.counter"),
                                                3.0,
                                                Some(dt_1),
                                                MetricKind::Counter(1.0),
                                                None));
        wavefront.deliver(Metric::new_with_time(String::from("test.gauge"),
                                                3.211,
                                                Some(dt_0),
                                                MetricKind::Gauge,
                                                None));
        wavefront.deliver(Metric::new_with_time(String::from("test.timer"),
                                                12.101,
                                                Some(dt_0),
                                                MetricKind::Timer,
                                                None));
        wavefront.deliver(Metric::new_with_time(String::from("test.timer"),
                                                1.101,
                                                Some(dt_0),
                                                MetricKind::Timer,
                                                None));
        wavefront.deliver(Metric::new_with_time(String::from("test.timer"),
                                                3.101,
                                                Some(dt_0),
                                                MetricKind::Timer,
                                                None));
        wavefront.deliver(Metric::new_with_time(String::from("test.raw"),
                                                1.0,
                                                Some(dt_0),
                                                MetricKind::Raw,
                                                None));
        wavefront.deliver(Metric::new_with_time(String::from("test.raw"),
                                                2.0,
                                                Some(dt_1),
                                                MetricKind::Raw,
                                                None));
        let result = wavefront.format_stats();
        let lines: Vec<&str> = result.lines().collect();

        println!("{:?}", lines);
        assert_eq!(19, lines.len());
        assert!(lines.contains(&"test.counter 1 645181811 source=test-src"));
        assert!(lines.contains(&"test.counter 3 645185472 source=test-src"));
        assert!(lines.contains(&"test.gauge 3.211 645181811 source=test-src"));
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
