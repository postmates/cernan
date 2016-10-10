use std::net::{SocketAddr, TcpStream};
use std::fmt::Write;
use std::io::Write as IoWrite;
use metric::{Metric, LogLine, MetricQOS};
use buckets::Buckets;
use sink::Sink;
use dns_lookup;
use time;
use rand;
use rand::Rng;

pub struct Wavefront {
    addr: SocketAddr,
    tags: String,
    aggrs: Buckets,
    qos: MetricQOS,
    interval: i64,
}

impl Wavefront {
    pub fn new(host: &str, port: u16, tags: String, qos: MetricQOS, interval: i64) -> Wavefront {
        match dns_lookup::lookup_host(host) {
            Ok(mut lh) => {
                let ip = lh.next().expect("No IPs associated with host").unwrap();
                let addr = SocketAddr::new(ip, port);
                Wavefront {
                    addr: addr,
                    tags: tags,
                    aggrs: Buckets::default(),
                    qos: qos,
                    interval: interval,
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
            for &(_, ref m) in sample(self.interval, self.qos.counter as i64, value) {
                write!(stats, "{} {} {} {}\n", key, m.value, m.time, self.tags).unwrap();
            }
        }

        for (key, value) in self.aggrs.gauges().iter() {
            for &(_, ref m) in sample(self.interval, self.qos.gauge as i64, value) {
                write!(stats, "{} {} {} {}\n", key, m.value, m.time, self.tags).unwrap();
            }
        }

        for (key, hists) in self.aggrs.histograms().iter() {
            for &(smpl_time, ref hist) in sample(self.interval, self.qos.histogram as i64, hists) {
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
                write!(stats,
                       "{}.count {} {} {}\n",
                       key,
                       count,
                       smpl_time,
                       self.tags)
                    .unwrap();
            }
        }

        for (key, tmrs) in self.aggrs.timers().iter() {
            for &(smpl_time, ref tmr) in sample(self.interval, self.qos.timer as i64, tmrs) {
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
                write!(stats,
                       "{}.count {} {} {}\n",
                       key,
                       count,
                       smpl_time,
                       self.tags)
                    .unwrap();
            }
        }

        // Raw points have no QOS as we can make no valid aggregation of them.
        for (key, value) in self.aggrs.raws().iter() {
            for &(_, ref m) in value {
                write!(stats, "{} {} {} {}\n", key, m.value, m.time, self.tags).unwrap();
            }
        }

        let now = time::now();
        write!(stats,
               "{} {} {} {}\n",
               "cernan.wavefront.qos.timer",
               self.qos.timer,
               now,
               self.tags)
            .unwrap();
        write!(stats,
               "{} {} {} {}\n",
               "cernan.wavefront.qos.gauge",
               self.qos.gauge,
               now,
               self.tags)
            .unwrap();
        write!(stats,
               "{} {} {} {}\n",
               "cernan.wavefront.qos.histogram",
               self.qos.histogram,
               now,
               self.tags)
            .unwrap();
        write!(stats,
               "{} {} {} {}\n",
               "cernan.wavefront.qos.counter",
               self.qos.counter,
               now,
               self.tags)
            .unwrap();

        stats
    }
}

pub fn sample<T>(interval: i64, qos: i64, vals: &Vec<(i64, T)>) -> Vec<&(i64, T)> {
    assert!(qos <= interval);
    let samples: usize = (interval as usize) / (qos as usize);
    let mut max = vals.len();
    let mut res = Vec::with_capacity(max);
    for v in vals {
        res.push(v);
    }
    if qos == 1 {
        return res;
    } else if max <= samples {
        return res;
    }
    let mut base_idx = 0;
    let mut idx = 0;
    let mut cur_base_smpl_time = res[0].0;
    let mut rng = rand::thread_rng();
    let mut has_sampled = false;
    while idx < max {
        if (res[idx].0 - cur_base_smpl_time) >= interval {
            // We've found another interval, time to potentially downsample past
            // all points from base_idx to idx, exclusive of idx.
            if (idx - base_idx) > samples {
                // This interval has more points than allowed by the sample
                // limit, so we downsample.
                let mut must_go = (idx - base_idx) - samples;
                while must_go > 0 {
                    has_sampled = true;
                    let del_idx = rng.gen_range(base_idx, idx);
                    res.remove(del_idx);
                    if idx > 0 {
                        idx -= 1;
                    }
                    max -= 1;
                    must_go -= 1;
                }
                base_idx = idx;
                cur_base_smpl_time = res[idx].0;
            } else {
                // We're below the sample limit, so we adjust ourselves to be
                // over the new interval.
                base_idx = idx;
                cur_base_smpl_time = res[idx].0;
                idx += 1;
            }
        } else {
            idx += 1;
        }
    }
    // It's possible that all our points will sit inside the same interval but
    // that there will still be too many of them. This clause catches that.
    if !has_sampled && res.len() > samples {
        let mut must_go = res.len() - samples;
        while must_go > 0 {
            let del_idx = rng.gen_range(0, res.len());
            res.remove(del_idx);
            must_go -= 1;
        }
    }
    res
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
    fn test_sampling() {
        let interval = 5;
        let qos = 2;
        // With interval 5 and qos 2 this means that for every interval there
        // will be at most 2 points in it.
        let samples = vec![(0, "a"), (1, "b"), (2, "c") /* interval 1 */,
                           (5, "d") /* interval 2 */, (10, "e"),
                           (11, "f") /* interval 3 */, (15, "g")];
        let downsamples = sample(interval, qos, &samples);
        assert_eq!(6, downsamples.len());
        assert!(downsamples.contains(&&(5, "d")));
        assert!(downsamples.contains(&&(10, "e")));
        assert!(downsamples.contains(&&(11, "f")));
        assert!(downsamples.contains(&&(15, "g")));
    }

    #[test]
    fn test_sample_even_if_interval_not_passed() {
        let interval = 6;
        let qos = 2;
        let samples = vec![(0, "a"), (1, "b"), (2, "c"), (3, "d"), (4, "e"), (5, "f")];
        let downsamples = sample(interval, qos, &samples);
        assert_eq!(3, downsamples.len());
    }

    #[test]
    fn test_sampling_empty() {
        let interval = 5;
        let qos = 5;
        // With interval 5 and qos 2 this means that for every interval there
        // will be at most 2 points in it.
        let samples = vec![];
        let downsamples: Vec<&(i64, &str)> = sample(interval, qos, &samples);
        assert!(downsamples.is_empty());
    }

    #[test]
    fn test_sampling_no_sampling() {
        let interval = 5;
        let qos = 1;
        // With interval 5 and qos 1 this means that for every interval there
        // will be at most 2 points in it.
        let samples: Vec<(i64, &str)> = vec![// interval 1
                                             (0, "a"),
                                             (1, "b"),
                                             (2, "c"),
                                             // interval 2
                                             (5, "d"),
                                             // interval 3
                                             (10, "e"),
                                             (11, "f"),
                                             // interval 4
                                             (15, "g")];
        let downsamples = sample(interval, qos, &samples);
        assert_eq!(7, downsamples.len());
        assert!(downsamples.contains(&&(0, "a")));
        assert!(downsamples.contains(&&(1, "b")));
        assert!(downsamples.contains(&&(2, "c")));
        assert!(downsamples.contains(&&(5, "d")));
        assert!(downsamples.contains(&&(10, "e")));
        assert!(downsamples.contains(&&(11, "f")));
        assert!(downsamples.contains(&&(15, "g")));
    }

    #[test]
    fn test_sampling_extreme() {
        let interval = 5;
        let qos = 5;
        // With interval 5 and qos 1 this means that for every interval there
        // will be at most 2 points in it.
        let samples: Vec<(i64, &str)> = vec![// interval 1
                                             (0, "a"),
                                             (1, "b"),
                                             (2, "c"),
                                             // interval 2
                                             (5, "d"),
                                             // interval 3
                                             (10, "e"),
                                             (11, "f"),
                                             // interval 4
                                             (15, "g")];
        let downsamples = sample(interval, qos, &samples);
        println!("DOWNSAMPLES: {:?}", downsamples);
        assert_eq!(4, downsamples.len());
    }

    #[test]
    fn test_histogram_qos_ellision() {
        let mut qos = MetricQOS::default();
        qos.histogram = 2;
        let flush = 6;

        let mut wavefront =
            Wavefront::new("localhost", 2003, "source=test-src".to_string(), qos, flush);
        let dt_0 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 11, 0).timestamp();
        let dt_1 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 12, 0).timestamp();
        let dt_2 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 13, 0).timestamp();
        let dt_3 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 14, 0).timestamp();
        let dt_4 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 15, 0).timestamp();

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
        wavefront.deliver(Metric::new_with_time(String::from("test.histogram"),
                                                1.0,
                                                Some(dt_3),
                                                MetricKind::Histogram,
                                                None));
        wavefront.deliver(Metric::new_with_time(String::from("test.histogram"),
                                                1.0,
                                                Some(dt_4),
                                                MetricKind::Histogram,
                                                None));


        let result = wavefront.format_stats();
        let lines: Vec<&str> = result.lines().collect();
        println!("{:?}", lines);
        assert_eq!(46, lines.len());
    }

    #[test]
    fn test_timer_qos_ellision() {
        let mut qos = MetricQOS::default();
        qos.timer = 2;
        let flush = 6;

        let mut wavefront =
            Wavefront::new("localhost", 2003, "source=test-src".to_string(), qos, flush);
        let dt_0 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 11, 0).timestamp();
        let dt_1 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 12, 0).timestamp();
        let dt_2 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 13, 0).timestamp();
        let dt_3 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 14, 0).timestamp();
        let dt_4 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 15, 0).timestamp();

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
        wavefront.deliver(Metric::new_with_time(String::from("test.timer"),
                                                1.0,
                                                Some(dt_3),
                                                MetricKind::Timer,
                                                None));
        wavefront.deliver(Metric::new_with_time(String::from("test.timer"),
                                                1.0,
                                                Some(dt_4),
                                                MetricKind::Timer,
                                                None));

        let result = wavefront.format_stats();
        let lines: Vec<&str> = result.lines().collect();
        println!("{:?}", lines);
        assert_eq!(46, lines.len());
    }

    #[test]
    fn test_gauge_qos_ellision() {
        let mut qos = MetricQOS::default();
        qos.gauge = 2;
        let flush = 6;

        let mut wavefront =
            Wavefront::new("localhost", 2003, "source=test-src".to_string(), qos, flush);
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

        println!("{:?}", lines);
        assert_eq!(8, lines.len());
    }

    #[test]
    fn test_counter_qos_ellision() {
        let mut qos = MetricQOS::default();
        qos.counter = 2;
        let flush = 6;

        let mut wavefront =
            Wavefront::new("localhost", 2003, "source=test-src".to_string(), qos, flush);
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

        println!("{:?}", lines);
        assert_eq!(8, lines.len());
    }

    #[test]
    fn test_format_wavefront() {
        let qos = MetricQOS::default();
        let mut wavefront =
            Wavefront::new("localhost", 2003, "source=test-src".to_string(), qos, 60);
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
        assert_eq!(23, lines.len());
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
