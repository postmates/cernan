use std::net::{SocketAddr, TcpStream};
use std::fmt::Write;
use std::io::Write as IoWrite;
use metric::{Metric, LogLine, MetricQOS, TagMap};
use buckets::Buckets;
use sink::Sink;
use dns_lookup;
use time;
use rand;
use rand::Rng;

pub struct Wavefront {
    addr: SocketAddr,
    aggrs: Buckets,
    qos: MetricQOS,
    tags: TagMap,
    interval: i64,
}

#[inline]
fn fmt_tags(tags: &TagMap) -> String {
    let mut s = String::new();
    let mut iter = tags.iter();
    if let Some((fk,fv)) = iter.next() {
        write!(s, "{}={}", fk, fv).unwrap();
        for (k, v) in iter {
            write!(s, " {}={}", k, v).unwrap();
        }
    }
    s
}

impl Wavefront {
    pub fn new(host: &str, port: u16, tags: TagMap, qos: MetricQOS, interval: i64) -> Wavefront {
        match dns_lookup::lookup_host(host) {
            Ok(mut lh) => {
                let ip = lh.next().expect("No IPs associated with host").unwrap();
                let addr = SocketAddr::new(ip, port);
                Wavefront {
                    addr: addr,
                    aggrs: Buckets::default(),
                    qos: qos,
                    tags: tags,
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
                write!(stats, "{} {} {} {}\n", key, m.value, m.time, fmt_tags(&m.tags)).unwrap();
            }
        }

        for (key, value) in self.aggrs.gauges().iter() {
            for &(_, ref m) in sample(self.interval, self.qos.gauge as i64, value) {
                write!(stats, "{} {} {} {}\n", key, m.value, m.time, fmt_tags(&m.tags)).unwrap();
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
                           fmt_tags(&self.tags))
                        .unwrap()
                }
                let count = hist.count();
                write!(stats,
                       "{}.count {} {} {}\n",
                       key,
                       count,
                       smpl_time,
                       fmt_tags(&self.tags))
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
                           fmt_tags(&self.tags))
                        .unwrap()
                }
                let count = tmr.count();
                write!(stats,
                       "{}.count {} {} {}\n",
                       key,
                       count,
                       smpl_time,
                       fmt_tags(&self.tags))
                    .unwrap();
            }
        }

        // Raw points have no QOS as we can make no valid aggregation of them.
        for (key, value) in self.aggrs.raws().iter() {
            for &(_, ref m) in value {
                write!(stats, "{} {} {} {}\n", key, m.value, m.time, fmt_tags(&m.tags)).unwrap();
            }
        }

        let now = time::now();
        write!(stats,
               "{} {} {} {}\n",
               "cernan.wavefront.qos.timer",
               self.qos.timer,
               now,
               fmt_tags(&self.tags))
            .unwrap();
        write!(stats,
               "{} {} {} {}\n",
               "cernan.wavefront.qos.gauge",
               self.qos.gauge,
               now,
               fmt_tags(&self.tags))
            .unwrap();
        write!(stats,
               "{} {} {} {}\n",
               "cernan.wavefront.qos.histogram",
               self.qos.histogram,
               now,
               fmt_tags(&self.tags))
            .unwrap();
        write!(stats,
               "{} {} {} {}\n",
               "cernan.wavefront.qos.counter",
               self.qos.counter,
               now,
               fmt_tags(&self.tags))
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
    if max <= samples {
        return res;
    }
    let mut base_idx = 0;
    let mut idx = 0;
    let mut cur_base_smpl_time = res[0].0;
    let mut rng = rand::thread_rng();
    while idx < max {
        if (res[idx].0 - cur_base_smpl_time) >= interval {
            // We've found another interval, time to potentially downsample past
            // all points from base_idx to idx, exclusive of idx.
            if (idx - base_idx) > samples {
                // This interval has more points than allowed by the sample
                // limit, so we downsample.
                let mut must_go = (idx - base_idx.saturating_sub(1)) - samples;
                while must_go > 0 {
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
    // It's possible the last interval has more points than needed. We cover
    // that here.
    let mut must_go = idx.saturating_sub(base_idx).saturating_sub(samples);
    while must_go > 0 {
        let del_idx = rng.gen_range(base_idx, res.len());
        res.remove(del_idx);
        must_go -= 1;
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
    extern crate quickcheck;

    use metric::{Metric, MetricQOS, TagMap};
    use sink::Sink;
    use chrono::{UTC, TimeZone};
    use super::*;

    use self::quickcheck::{TestResult, QuickCheck};

    #[test]
    fn windows_obey_sample_limit() {
        fn inner(interval: u8, qos: u8, mut vals: Vec<(i64, u64)>) -> TestResult {
            if qos == 0 {
                return TestResult::discard();
            } else if interval == 0 {
                return TestResult::discard();
            } else if vals.len() == 0 {
                return TestResult::discard();
            } else if qos > interval {
                return TestResult::discard();
            }

            let interval = interval as i64;
            let qos = qos as i64;
            let sample_size = interval / qos;

            vals.sort_by_key(|&(t, _)| t);
            let smpl = sample(interval, qos, &vals);
            assert!(smpl.len() <= vals.len());

            // compute the total intervals
            let mut total_intervals = 1;
            let mut low_side_bound = vals[0].0;
            for &(t, _) in &vals {
                if (low_side_bound - t).abs() >= interval {
                    total_intervals += 1;
                    low_side_bound = t;
                }
            }

            // compute the bounds
            let mut bounds = Vec::new();
            let mut low_bound = vals[0].0;
            for &(t, _) in &vals {
                if (low_bound - t).abs() >= interval {
                    // new interval
                    bounds.push(low_bound);
                    low_bound = t;
                }
            }
            bounds.push(low_bound);

            // assert sample strides
            let mut bound_idx = 0;
            let mut cur_samples_per_interval = 0;
            for &&(t, _) in &smpl {
                debug_assert!(bound_idx < bounds.len(),
                              "bounds: {:?} | vals: {:?}",
                              bounds,
                              vals);
                if bounds[bound_idx] <= t {
                    // inside current interval
                    cur_samples_per_interval += 1;
                } else {
                    // new interval
                    assert!(cur_samples_per_interval <= sample_size);
                    cur_samples_per_interval = 0;
                    bound_idx += 1;
                }
            }

            debug_assert!(smpl.len() <= (sample_size * total_intervals) as usize,
                          "smpl.len() = {} | sample_size = {} | total_intervals = {} | vals = \
                           {:?} | smpl = {:?}",
                          smpl.len(),
                          sample_size,
                          total_intervals,
                          vals,
                          smpl);

            TestResult::passed()
        }
        QuickCheck::new()
            .tests(100)
            .max_tests(1000)
            .quickcheck(inner as fn(u8, u8, Vec<(i64, u64)>) -> TestResult);
    }

    #[test]
    fn test_sampling() {
        let interval = 5;
        let qos = 2;
        // With interval 5 and qos 2 this means that for every interval there
        // will be at most 2 points in it.
        let samples = vec![// interval 1
                           (0, "a"),
                           (1, "b"),
                           (2, "c"),
                           // interval 2
                           (5, "d"),
                           // interval 3
                           (10, "e"),
                           (11, "f"),
                           (15, "g")];
        let downsamples = sample(interval, qos, &samples);
        assert_eq!(6, downsamples.len());
        assert!(downsamples.contains(&&(5, "d")));
        assert!(downsamples.contains(&&(10, "e")));
        assert!(downsamples.contains(&&(11, "f")));
        assert!(downsamples.contains(&&(15, "g")));
    }

    #[test]
    fn test_sampling_sixty_fifteen() {
        let interval = 60;
        let qos = 15;
        // With interval 60 and qos 15 over two intervals we would expect to see
        // 8 points total.
        let mut samples = Vec::new();
        for i in 0..120 {
            samples.push((i, 0));
        }
        let downsamples = sample(interval, qos, &samples);
        assert_eq!(8, downsamples.len());
    }

    #[test]
    fn test_sampling_all_redundant() {
        let interval = 2;
        let qos = 1;
        let samples = vec![(0, 0), (0, 0), (0, 0)];
        let downsamples = sample(interval, qos, &samples);
        assert_eq!(downsamples.len(), 2);
        assert!(downsamples.contains(&&(0, 0)));
    }

    #[test]
    fn test_sampling_zero_thirtyone_thirtysix() {
        let interval = 1;
        let qos = 1;
        let samples = vec![(0, 0), (31, 0), (36, 0), (36, 0)];
        let downsamples = sample(interval, qos, &samples);
        println!("DOWNSAMPLES: {:?}", downsamples);
        assert_eq!(downsamples.len(), 3);
        assert!(downsamples.contains(&&(0, 0)));
        assert!(downsamples.contains(&&(31, 0)));
        assert!(downsamples.contains(&&(36, 0)));
    }

    #[test]
    fn test_sampling_double_zero() {
        let interval = 4;
        let qos = 2;
        let samples = vec![// interval 1
                           (0, 0),
                           (0, 0),
                           // interval 2
                           (70, 0),
                           (70, 0),
                           (73, 0),
                           (74, 0),
                           // interval 3
                           (76, 0)];
        let downsamples = sample(interval, qos, &samples);
        println!("DOWNSAMPLES: {:?}", downsamples);
        assert_eq!(downsamples.len(), 5);
        assert!(downsamples.contains(&&(0, 0)));
        assert!(downsamples.contains(&&(76, 0)));
    }

    #[test]
    fn test_sampling_six_interval() {
        let interval = 6;
        let qos = 3;
        let samples = vec![// interval 1
                           (0, 0),
                           // interval 2
                           (70, 0),
                           (71, 0),
                           (75, 0),
                           // interval 3
                           (77, 0),
                           (77, 73),
                           (83, 0)];
        let downsamples = sample(interval, qos, &samples);
        println!("DOWNSAMPLES: {:?}", downsamples);
        assert_eq!(downsamples.len(), 5);
        assert!(downsamples.contains(&&(0, 0)));
    }

    #[test]
    fn test_sampling_zero_seventies() {
        let interval = 3;
        let qos = 2;
        let samples = vec![(0, 0), (74, 0), (75, 0), (77, 0)];
        let downsamples = sample(interval, qos, &samples);
        println!("DOWNSAMPLES: {:?}", downsamples);
        assert_eq!(downsamples.len(), 2);
        assert!(downsamples.contains(&&(0, 0)));
    }

    #[test]
    fn test_sampling_zero_two_two() {
        let interval = 2;
        let qos = 1;
        let samples = vec![(0, 0), (2, 0), (2, 0), (2, 0)];
        let downsamples = sample(interval, qos, &samples);
        println!("DOWNSAMPLES: {:?}", downsamples);
        assert_eq!(downsamples.len(), 3);
    }

    #[test]
    fn test_sampling_neg_double_seven() {
        let interval = 6;
        let qos = 3;
        let samples = vec![// interval 1
                           (0, 0),
                           // interval 2
                           (6, 0),
                           (11, 0),
                           (11, 0),
                           // interval 3
                           (12, 0),
                           (16, 0),
                           (18, 0)];
        let downsamples = sample(interval, qos, &samples);
        println!("DOWNSAMPLES: {:?}", downsamples);
        assert_eq!(downsamples.len(), 5);
    }

    #[test]
    fn test_sampling_triple_zero() {
        let interval = 4;
        let qos = 2;
        let samples = vec![(0, 0), (0, 0), (0, 0), (4, 0)];
        let downsamples = sample(interval, qos, &samples);
        println!("DOWNSAMPLES: {:?}", downsamples);
        assert_eq!(downsamples.len(), 3);
    }

    #[test]
    fn test_sampling_one_one_two_intervals() {
        let interval = 1;
        let qos = 1;
        let samples = vec![(-1, 0), (0, 0), (0, 0)];
        let downsamples = sample(interval, qos, &samples);
        println!("DOWNSAMPLES: {:?}", downsamples);
        assert_eq!(downsamples.len(), 2);
        assert!(downsamples.contains(&&(0, 0)));
        assert!(downsamples.contains(&&(-1, 0)));
    }

    #[test]
    fn test_sampling_one_one_no_overlap() {
        let interval = 1;
        let qos = 1;
        let samples = vec![(0, 0), (2, 0)];
        let downsamples = sample(interval, qos, &samples);
        assert_eq!(downsamples.len(), 2);
        assert!(downsamples.contains(&&(0, 0)));
        assert!(downsamples.contains(&&(2, 0)));
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
                                             (15, "g")];
        let downsamples = sample(interval, qos, &samples);
        println!("DOWNSAMPLES: {:?}", downsamples);
        assert_eq!(3, downsamples.len());
    }

    #[test]
    fn test_histogram_qos_ellision() {
        let mut qos = MetricQOS::default();
        qos.histogram = 2;
        let flush = 6;

        let mut wavefront =
            Wavefront::new("localhost", 2003, TagMap::default(), qos, flush);
        let dt_0 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 11, 0).timestamp();
        let dt_1 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 12, 0).timestamp();
        let dt_2 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 13, 0).timestamp();
        let dt_3 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 14, 0).timestamp();
        let dt_4 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 15, 0).timestamp();

        for dt in &[dt_0, dt_1, dt_2, dt_3, dt_4] {
            wavefront.deliver(Metric::new("test.histogram", 1.0).time(*dt).histogram());
        }

        let result = wavefront.format_stats();
        let lines: Vec<&str> = result.lines().collect();
        println!("{:?}", lines);
        assert_eq!(46, lines.len());
    }

    #[test]
    fn test_sixty_fifteen_histogram_qos_ellision() {
        let mut qos = MetricQOS::default();
        qos.histogram = 15;
        let flush = 60;

        let mut wavefront =
            Wavefront::new("localhost", 2003, TagMap::default(), qos, flush);
        for i in 0..122 {
            wavefront.deliver(Metric::new("test.histogram", 1.0).time(i).histogram());
        }

        let result = wavefront.format_stats();
        let lines: Vec<&str> = result.lines().collect();
        println!("{:?}", lines);
        assert_eq!(130, lines.len()); // 130 = 8*14 + 14 + 4
                                      //       int01  int2 meta
    }

    #[test]
    fn test_timer_qos_ellision() {
        let mut qos = MetricQOS::default();
        qos.timer = 2;
        let flush = 6;

        let mut wavefront =
            Wavefront::new("localhost", 2003, TagMap::default(), qos, flush);
        let dt_0 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 11, 0).timestamp();
        let dt_1 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 12, 0).timestamp();
        let dt_2 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 13, 0).timestamp();
        let dt_3 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 14, 0).timestamp();
        let dt_4 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 15, 0).timestamp();

        for dt in &[dt_0, dt_1, dt_2, dt_3, dt_4] {
            wavefront.deliver(Metric::new("test.timer", 1.0).time(*dt).timer());
        }

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
            Wavefront::new("localhost", 2003, TagMap::default(), qos, flush);
        let dt_0 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 11, 0).timestamp();
        let dt_1 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 12, 0).timestamp();
        let dt_2 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 13, 0).timestamp();
        let dt_3 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 14, 0).timestamp();
        let dt_4 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 15, 0).timestamp();

        for (i, dt) in vec![dt_0, dt_1, dt_2, dt_3, dt_4].iter().enumerate() {
            wavefront.deliver(Metric::new("test.gauge", i as f64).time(*dt).gauge());
        }
        wavefront.deliver(Metric::new("test.some_other_gauge", 1.0).time(dt_3).gauge());

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
            Wavefront::new("localhost", 2003, TagMap::default(), qos, flush);
        let dt_0 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 11, 0).timestamp();
        let dt_1 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 12, 0).timestamp();
        let dt_2 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 13, 0).timestamp();
        let dt_3 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 14, 0).timestamp();
        let dt_4 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 15, 0).timestamp();

        for (i, dt) in vec![dt_0, dt_1, dt_2, dt_3, dt_4].iter().enumerate() {
            wavefront.deliver(Metric::new("test.counter", i as f64).time(*dt).counter(1.0));
        }
        wavefront.deliver(Metric::new("test.some_other_counter", 1.0).time(dt_3).counter(1.0));

        let result = wavefront.format_stats();
        let lines: Vec<&str> = result.lines().collect();

        println!("{:?}", lines);
        assert_eq!(8, lines.len());
    }

    #[test]
    fn test_format_wavefront() {
        let qos = MetricQOS::default();
        let mut tags = TagMap::default();
        tags.insert("source".into(), "test-src".into());
        let mut wavefront =
            Wavefront::new("localhost", 2003, tags.clone(), qos, 60);
        let dt_0 = UTC.ymd(1990, 6, 12).and_hms_milli(9, 10, 11, 12).timestamp();
        let dt_1 = UTC.ymd(1990, 6, 12).and_hms_milli(10, 11, 12, 13).timestamp();
        wavefront.deliver(Metric::new("test.counter", -1.0).time(dt_0).counter(1.0).overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.counter", 2.0).time(dt_0).counter(1.0).overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.counter", 3.0).time(dt_1).counter(1.0).overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.gauge", 3.211).time(dt_0).gauge().overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.timer", 12.101).time(dt_0).timer().overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.timer", 1.101).time(dt_0).timer().overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.timer", 3.101).time(dt_0).timer().overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.raw", 1.0).time(dt_0).overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.raw", 2.0).time(dt_1).overlay_tags_from_map(&tags));
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
