use std::net::{SocketAddr, TcpStream};
use std::fmt::Write;
use std::io::Write as IoWrite;
use chrono;
use metric::{Metric, LogLine, MetricQOS};
use buckets::Buckets;
use sink::Sink;
use dns_lookup;

pub struct Wavefront {
    addr: SocketAddr,
    tags: String,
    aggrs: Buckets,
    qos: MetricQOS,

    timer_last_sample: i64,
    reset_timer: bool,
    histogram_last_sample: i64,
    reset_histogram: bool,
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
                    timer_last_sample: 0,
                    reset_timer: false,
                    histogram_last_sample: 0,
                    reset_histogram: false,
                }
            }
            Err(_) => panic!("Could not lookup host"),
        }
    }

    /// Convert the buckets into a String that
    /// can be sent to the the wavefront proxy
    pub fn format_stats(&mut self, curtime: Option<i64>) -> String {
        let start = match curtime {
            Some(x) => x,
            None => chrono::UTC::now().timestamp(),
        };
        let mut stats = String::new();

        for (key, value) in self.aggrs.counters().iter() {
            let mut counter_last_sample = 0;
            for m in value {
                if (m.time - counter_last_sample) >= (self.qos.counter as i64) {
                    write!(stats, "{} {} {} {}\n", key, m.value, m.time, self.tags).unwrap();
                    counter_last_sample = m.time;
                }
            }
        }

        for (key, value) in self.aggrs.gauges().iter() {
            let mut gauge_last_sample = 0;
            for m in value {
                if (m.time - gauge_last_sample) >= (self.qos.gauge as i64) {
                    write!(stats, "{} {} {} {}\n", key, m.value, m.time, self.tags).unwrap();
                    gauge_last_sample = m.time;
                }
            }
        }

        if (start - self.histogram_last_sample) >= (self.qos.histogram as i64) {
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
            self.histogram_last_sample = start;
            self.reset_histogram = true;
        }

        if (start - self.timer_last_sample) >= (self.qos.timer as i64) {
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
            self.timer_last_sample = start;
            self.reset_timer = true;
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
        match TcpStream::connect(self.addr) {
            Ok(mut stream) => {
                let res = stream.write(self.format_stats(None).as_bytes());
                if res.is_ok() {
                    trace!("flushed to wavefront!");
                    self.aggrs.reset();
                    if self.reset_histogram {
                        self.aggrs.reset_histograms();
                    }
                    if self.reset_timer {
                        self.aggrs.reset_timers();
                    }
                }
            }
            Err(e) => debug!("Unable to connect: {}", e),
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
    use string_cache::Atom;
    use super::*;

    #[test]
    fn test_histogram_qos_ellision() {
        let mut qos = MetricQOS::default();
        qos.histogram = 2;

        let mut wavefront = Wavefront::new("localhost", 2003, "source=test-src".to_string(), qos);
        let dt_0 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 11, 0).timestamp();
        let dt_1 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 12, 0).timestamp();
        let dt_2 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 13, 0).timestamp();

        wavefront.deliver(Metric::new_with_time(Atom::from("test.histogram"),
                                                1.0,
                                                Some(dt_0),
                                                MetricKind::Histogram,
                                                None));

        // time dt_0
        let result_0 = wavefront.format_stats(Some(dt_0));
        let lines_0: Vec<&str> = result_0.lines().collect();
        println!("{:?}", lines_0);
        assert_eq!(14, lines_0.len());
        assert_eq!(lines_0[0], "test.histogram.min 1 645185471 source=test-src");
        assert_eq!(lines_0[1], "test.histogram.max 1 645185471 source=test-src");
        assert_eq!(lines_0[2], "test.histogram.2 1 645185471 source=test-src");
        assert_eq!(lines_0[3], "test.histogram.9 1 645185471 source=test-src");
        assert_eq!(lines_0[4], "test.histogram.25 1 645185471 source=test-src");
        assert_eq!(lines_0[5], "test.histogram.50 1 645185471 source=test-src");
        assert_eq!(lines_0[6], "test.histogram.75 1 645185471 source=test-src");
        assert_eq!(lines_0[7], "test.histogram.90 1 645185471 source=test-src");
        assert_eq!(lines_0[8], "test.histogram.91 1 645185471 source=test-src");
        assert_eq!(lines_0[9], "test.histogram.95 1 645185471 source=test-src");
        assert_eq!(lines_0[10], "test.histogram.98 1 645185471 source=test-src");
        assert_eq!(lines_0[11], "test.histogram.99 1 645185471 source=test-src");
        assert_eq!(lines_0[12],
                   "test.histogram.999 1 645185471 source=test-src");
        assert_eq!(lines_0[13],
                   "test.histogram.count 1 645185471 source=test-src");

        // time dt_1
        let result_1 = wavefront.format_stats(Some(dt_1));
        let lines_1: Vec<&str> = result_1.lines().collect();
        println!("{:?}", lines_1);
        assert_eq!(0, lines_1.len());

        // time dt_2
        let result_2 = wavefront.format_stats(Some(dt_2));
        let lines_2: Vec<&str> = result_2.lines().collect();
        println!("{:?}", lines_2);
        assert_eq!(14, lines_2.len());
        assert_eq!(lines_2[0], "test.histogram.min 1 645185473 source=test-src");
        assert_eq!(lines_2[1], "test.histogram.max 1 645185473 source=test-src");
        assert_eq!(lines_2[2], "test.histogram.2 1 645185473 source=test-src");
        assert_eq!(lines_2[3], "test.histogram.9 1 645185473 source=test-src");
        assert_eq!(lines_2[4], "test.histogram.25 1 645185473 source=test-src");
        assert_eq!(lines_2[5], "test.histogram.50 1 645185473 source=test-src");
        assert_eq!(lines_2[6], "test.histogram.75 1 645185473 source=test-src");
        assert_eq!(lines_2[7], "test.histogram.90 1 645185473 source=test-src");
        assert_eq!(lines_2[8], "test.histogram.91 1 645185473 source=test-src");
        assert_eq!(lines_2[9], "test.histogram.95 1 645185473 source=test-src");
        assert_eq!(lines_2[10], "test.histogram.98 1 645185473 source=test-src");
        assert_eq!(lines_2[11], "test.histogram.99 1 645185473 source=test-src");
        assert_eq!(lines_2[12],
                   "test.histogram.999 1 645185473 source=test-src");
        assert_eq!(lines_2[13],
                   "test.histogram.count 1 645185473 source=test-src");
    }

    #[test]
    fn test_timer_qos_ellision() {
        let mut qos = MetricQOS::default();
        qos.timer = 2;

        let mut wavefront = Wavefront::new("localhost", 2003, "source=test-src".to_string(), qos);
        let dt_0 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 11, 0).timestamp();
        let dt_1 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 12, 0).timestamp();
        let dt_2 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 13, 0).timestamp();

        wavefront.deliver(Metric::new_with_time(Atom::from("test.timer"),
                                                1.0,
                                                Some(dt_0),
                                                MetricKind::Timer,
                                                None));

        // time dt_0
        let result_0 = wavefront.format_stats(Some(dt_0));
        let lines_0: Vec<&str> = result_0.lines().collect();
        println!("{:?}", lines_0);
        assert_eq!(14, lines_0.len());
        assert_eq!(lines_0[0], "test.timer.min 1 645185471 source=test-src");
        assert_eq!(lines_0[1], "test.timer.max 1 645185471 source=test-src");
        assert_eq!(lines_0[2], "test.timer.2 1 645185471 source=test-src");
        assert_eq!(lines_0[3], "test.timer.9 1 645185471 source=test-src");
        assert_eq!(lines_0[4], "test.timer.25 1 645185471 source=test-src");
        assert_eq!(lines_0[5], "test.timer.50 1 645185471 source=test-src");
        assert_eq!(lines_0[6], "test.timer.75 1 645185471 source=test-src");
        assert_eq!(lines_0[7], "test.timer.90 1 645185471 source=test-src");
        assert_eq!(lines_0[8], "test.timer.91 1 645185471 source=test-src");
        assert_eq!(lines_0[9], "test.timer.95 1 645185471 source=test-src");
        assert_eq!(lines_0[10], "test.timer.98 1 645185471 source=test-src");
        assert_eq!(lines_0[11], "test.timer.99 1 645185471 source=test-src");
        assert_eq!(lines_0[12], "test.timer.999 1 645185471 source=test-src");
        assert_eq!(lines_0[13], "test.timer.count 1 645185471 source=test-src");

        // time dt_1
        let result_1 = wavefront.format_stats(Some(dt_1));
        let lines_1: Vec<&str> = result_1.lines().collect();
        println!("{:?}", lines_1);
        assert_eq!(0, lines_1.len());

        // time dt_2
        let result_2 = wavefront.format_stats(Some(dt_2));
        let lines_2: Vec<&str> = result_2.lines().collect();
        println!("{:?}", lines_2);
        assert_eq!(14, lines_2.len());
        assert_eq!(lines_2[0], "test.timer.min 1 645185473 source=test-src");
        assert_eq!(lines_2[1], "test.timer.max 1 645185473 source=test-src");
        assert_eq!(lines_2[2], "test.timer.2 1 645185473 source=test-src");
        assert_eq!(lines_2[3], "test.timer.9 1 645185473 source=test-src");
        assert_eq!(lines_2[4], "test.timer.25 1 645185473 source=test-src");
        assert_eq!(lines_2[5], "test.timer.50 1 645185473 source=test-src");
        assert_eq!(lines_2[6], "test.timer.75 1 645185473 source=test-src");
        assert_eq!(lines_2[7], "test.timer.90 1 645185473 source=test-src");
        assert_eq!(lines_2[8], "test.timer.91 1 645185473 source=test-src");
        assert_eq!(lines_2[9], "test.timer.95 1 645185473 source=test-src");
        assert_eq!(lines_2[10], "test.timer.98 1 645185473 source=test-src");
        assert_eq!(lines_2[11], "test.timer.99 1 645185473 source=test-src");
        assert_eq!(lines_2[12], "test.timer.999 1 645185473 source=test-src");
        assert_eq!(lines_2[13], "test.timer.count 1 645185473 source=test-src");
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

        wavefront.deliver(Metric::new_with_time(Atom::from("test.gauge"),
                                                1.0,
                                                Some(dt_0),
                                                MetricKind::Gauge,
                                                None));
        wavefront.deliver(Metric::new_with_time(Atom::from("test.some_other_gauge"),
                                                1.0,
                                                Some(dt_0),
                                                MetricKind::Gauge,
                                                None));
        wavefront.deliver(Metric::new_with_time(Atom::from("test.gauge"),
                                                2.0,
                                                Some(dt_1),
                                                MetricKind::Gauge,
                                                None));
        wavefront.deliver(Metric::new_with_time(Atom::from("test.gauge"),
                                                3.0,
                                                Some(dt_2),
                                                MetricKind::Gauge,
                                                None));
        wavefront.deliver(Metric::new_with_time(Atom::from("test.gauge"),
                                                4.0,
                                                Some(dt_3),
                                                MetricKind::Gauge,
                                                None));
        wavefront.deliver(Metric::new_with_time(Atom::from("test.gauge"),
                                                5.0,
                                                Some(dt_4),
                                                MetricKind::Gauge,
                                                None));
        let result = wavefront.format_stats(Some(10101));
        let lines: Vec<&str> = result.lines().collect();

        assert_eq!(645185471, dt_0); // exist
        assert_eq!(645185472, dt_1); // elided
        assert_eq!(645185473, dt_2); // exist
        assert_eq!(645185474, dt_3); // elided
        assert_eq!(645185475, dt_4); // exist

        println!("{:?}", lines);
        assert_eq!(4, lines.len());
        assert_eq!(lines[0],
                   "test.some_other_gauge 1 645185471 source=test-src");
        assert_eq!(lines[1], "test.gauge 1 645185471 source=test-src");
        assert_eq!(lines[2], "test.gauge 3 645185473 source=test-src");
        assert_eq!(lines[3], "test.gauge 5 645185475 source=test-src");
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

        wavefront.deliver(Metric::new_with_time(Atom::from("test.counter"),
                                                1.0,
                                                Some(dt_0),
                                                MetricKind::Counter(1.0),
                                                None));
        wavefront.deliver(Metric::new_with_time(Atom::from("test.some_other_counter"),
                                                1.0,
                                                Some(dt_0),
                                                MetricKind::Counter(1.0),
                                                None));
        wavefront.deliver(Metric::new_with_time(Atom::from("test.counter"),
                                                2.0,
                                                Some(dt_1),
                                                MetricKind::Counter(1.0),
                                                None));
        wavefront.deliver(Metric::new_with_time(Atom::from("test.counter"),
                                                3.0,
                                                Some(dt_2),
                                                MetricKind::Counter(1.0),
                                                None));
        wavefront.deliver(Metric::new_with_time(Atom::from("test.counter"),
                                                4.0,
                                                Some(dt_3),
                                                MetricKind::Counter(1.0),
                                                None));
        wavefront.deliver(Metric::new_with_time(Atom::from("test.counter"),
                                                5.0,
                                                Some(dt_4),
                                                MetricKind::Counter(1.0),
                                                None));
        let result = wavefront.format_stats(Some(10101));
        let lines: Vec<&str> = result.lines().collect();

        assert_eq!(645185471, dt_0); // exist
        assert_eq!(645185472, dt_1); // elided
        assert_eq!(645185473, dt_2); // exist
        assert_eq!(645185474, dt_3); // elided
        assert_eq!(645185475, dt_4); // exist

        println!("{:?}", lines);
        assert_eq!(4, lines.len());
        assert_eq!(lines[0],
                   "test.some_other_counter 1 645185471 source=test-src");
        assert_eq!(lines[1], "test.counter 1 645185471 source=test-src");
        assert_eq!(lines[2], "test.counter 3 645185473 source=test-src");
        assert_eq!(lines[3], "test.counter 5 645185475 source=test-src");
    }

    #[test]
    fn test_format_wavefront() {
        let qos = MetricQOS::default();
        let mut wavefront = Wavefront::new("localhost", 2003, "source=test-src".to_string(), qos);
        let dt_0 = UTC.ymd(1990, 6, 12).and_hms_milli(9, 10, 11, 12).timestamp();
        let dt_1 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 12, 13).timestamp();
        wavefront.deliver(Metric::new_with_time(Atom::from("test.counter"),
                                                -1.0,
                                                Some(dt_0),
                                                MetricKind::Counter(1.0),
                                                None));
        wavefront.deliver(Metric::new_with_time(Atom::from("test.counter"),
                                                2.0,
                                                Some(dt_0),
                                                MetricKind::Counter(1.0),
                                                None));
        wavefront.deliver(Metric::new_with_time(Atom::from("test.counter"),
                                                3.0,
                                                Some(dt_1),
                                                MetricKind::Counter(1.0),
                                                None));
        wavefront.deliver(Metric::new_with_time(Atom::from("test.gauge"),
                                                3.211,
                                                Some(dt_0),
                                                MetricKind::Gauge,
                                                None));
        wavefront.deliver(Metric::new_with_time(Atom::from("test.timer"),
                                                12.101,
                                                Some(dt_0),
                                                MetricKind::Timer,
                                                None));
        wavefront.deliver(Metric::new_with_time(Atom::from("test.timer"),
                                                1.101,
                                                Some(dt_0),
                                                MetricKind::Timer,
                                                None));
        wavefront.deliver(Metric::new_with_time(Atom::from("test.timer"),
                                                3.101,
                                                Some(dt_0),
                                                MetricKind::Timer,
                                                None));
        wavefront.deliver(Metric::new_with_time(Atom::from("test.raw"),
                                                1.0,
                                                Some(dt_0),
                                                MetricKind::Raw,
                                                None));
        wavefront.deliver(Metric::new_with_time(Atom::from("test.raw"),
                                                2.0,
                                                Some(dt_1),
                                                MetricKind::Raw,
                                                None));
        let result = wavefront.format_stats(Some(10101));
        let lines: Vec<&str> = result.lines().collect();

        println!("{:?}", lines);
        assert_eq!(19, lines.len());
        assert_eq!(lines[0], "test.counter 1 645181811 source=test-src");
        assert_eq!(lines[1], "test.counter 3 645185472 source=test-src");
        assert_eq!(lines[2], "test.gauge 3.211 645181811 source=test-src");
        assert_eq!(lines[3], "test.timer.min 1.101 10101 source=test-src");
        assert_eq!(lines[4], "test.timer.max 12.101 10101 source=test-src");
        assert_eq!(lines[5], "test.timer.2 1.101 10101 source=test-src");
        assert_eq!(lines[6], "test.timer.9 1.101 10101 source=test-src");
        assert_eq!(lines[7], "test.timer.25 1.101 10101 source=test-src");
        assert_eq!(lines[8], "test.timer.50 3.101 10101 source=test-src");
        assert_eq!(lines[9], "test.timer.75 3.101 10101 source=test-src");
        assert_eq!(lines[10], "test.timer.90 12.101 10101 source=test-src");
        assert_eq!(lines[11], "test.timer.91 12.101 10101 source=test-src");
        assert_eq!(lines[12], "test.timer.95 12.101 10101 source=test-src");
        assert_eq!(lines[13], "test.timer.98 12.101 10101 source=test-src");
        assert_eq!(lines[14], "test.timer.99 12.101 10101 source=test-src");
        assert_eq!(lines[15], "test.timer.999 12.101 10101 source=test-src");
        assert_eq!(lines[16], "test.timer.count 3 10101 source=test-src");
        assert_eq!(lines[17], "test.raw 1 645181811 source=test-src");
    }
}
