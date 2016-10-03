use time::now;
use std::str::FromStr;

include!(concat!(env!("OUT_DIR"), "/serde_types.rs"));

impl LogLine {
    pub fn new(path: String, value: String) -> LogLine {
        LogLine {
            path: path,
            value: value,
            time: now(),
        }
    }
}

#[derive(PartialEq, Clone, Debug)]
pub struct MetricQOS {
    pub counter: u64,
    pub gauge: u64,
    pub timer: u64,
    pub histogram: u64,
    pub raw: u64,
}

impl Default for MetricQOS {
    fn default() -> MetricQOS {
        MetricQOS {
            counter: 1,
            gauge: 1,
            timer: 1,
            histogram: 1,
            raw: 1,
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum MetricSign {
    Positive,
    Negative,
}

impl Metric {
    pub fn counter(name: &str) -> Metric {
        Metric {
            name: String::from(name),
            value: 1.0,
            kind: MetricKind::Counter(1.0),
            time: now(),
        }
    }

    /// Create a new metric
    ///
    /// Uses the Into trait to allow both str and String types.
    pub fn new(name: String,
               raw_value: f64,
               raw_kind: MetricKind,
               sign: Option<MetricSign>)
               -> Metric {
        let kind = match raw_kind {
            MetricKind::Gauge => {
                match sign {
                    Some(MetricSign::Positive) |
                    Some(MetricSign::Negative) => MetricKind::DeltaGauge,
                    None => raw_kind,
                }
            }
            _ => raw_kind,
        };

        let value = match sign {
            None |
            Some(MetricSign::Positive) => raw_value,
            Some(MetricSign::Negative) => -1.0 * raw_value,
        };

        Metric {
            name: name,
            value: value,
            kind: kind,
            time: now(),
        }
    }

    pub fn new_with_time(name: String,
                         value: f64,
                         time: Option<i64>,
                         kind: MetricKind,
                         _: Option<MetricSign>)
                         -> Metric {
        Metric {
            name: name,
            value: value,
            kind: kind,
            time: time.unwrap_or(now()),
        }
    }

    /// Valid message formats are:
    ///
    /// - `<str:metric_name>:<f64:value>|<str:type>`
    /// - `<str:metric_name>:<f64:value>|c|@<f64:sample_rate>`
    ///
    /// Multiple metrics can be sent in a single UDP packet
    /// separated by newlines.
    pub fn parse_statsd(source: &str) -> Option<Vec<Metric>> {
        let mut res = Vec::new();
        let mut iter = source.lines();
        loop {
            let mut offset = 0;
            match iter.next() {
                Some(src) => {
                    let len = src.len();
                    match (&src[offset..]).find(':') {
                        Some(colon_idx) => {
                            let name = &src[offset..(offset+colon_idx)];
                            if name.is_empty() { return None };
                            offset += colon_idx + 1;
                            if offset >= len { return None };
                            match (&src[offset..]).find('|') {
                                Some(pipe_idx) => {
                                    let sign = match &src[offset..(offset+1)] {
                                        "+" => Some(MetricSign::Positive),
                                        "-" => Some(MetricSign::Negative),
                                        _ => None
                                    };
                                    let val = match f64::from_str(&src[offset..(offset+pipe_idx)]) {
                                        Ok(f) => f,
                                        Err(_) => return None
                                    };
                                    offset += pipe_idx + 1;
                                    if offset >= len { return None };
                                    let kind = match (&src[offset..]).find('@') {
                                        Some(sample_idx) => {
                                            match &src[offset..(offset+sample_idx)] {
                                                "g" => MetricKind::Gauge,
                                                "ms" => MetricKind::Timer,
                                                "h" => MetricKind::Histogram,
                                                "c" => {
                                                    let sample = match f64::from_str(&src[(offset+sample_idx+1)..]) {
                                                        Ok(f) => f,
                                                        Err(_) => return None
                                                    };
                                                    MetricKind::Counter(sample)
                                                }
                                                _ => return None
                                            }
                                        }
                                        None => {
                                            match &src[offset..] {
                                                "g" | "g\n" => MetricKind::Gauge,
                                                "ms" | "ms\n" => MetricKind::Timer,
                                                "h" | "h\n" => MetricKind::Histogram,
                                                "c" | "c\n" => MetricKind::Counter(1.0),
                                                _ => return None
                                            }
                                        }
                                    };

                                    let m = match sign {
                                        None => Metric::new(String::from(name), val, kind, None),
                                        Some(MetricSign::Positive) => {
                                            match kind {
                                                MetricKind::Gauge => Metric::new(String::from(name), val, MetricKind::DeltaGauge, None),
                                                _ => Metric::new(String::from(name), val, kind, None),
                                            }
                                        },
                                        Some(MetricSign::Negative) => {
                                            match kind {
                                                MetricKind::Gauge => Metric::new(String::from(name), val, MetricKind::DeltaGauge, None),
                                                _ => Metric::new(String::from(name), val, kind, None),
                                            }
                                        }
                                    };
                                    res.push(m);
                                }
                                None => return None
                            }
                        }
                        None => return None
                    }
                }
                None => break
            }
        }
        if res.is_empty() {
            None
        } else {
            Some(res)
        }
    }

    pub fn parse_graphite(source: &str) -> Option<Vec<Metric>> {
        let mut res = Vec::new();
        let mut iter = source.split_whitespace();
        loop {
            match iter.next() {
                Some(name) => {
                    match iter.next() {
                        Some(val) => {
                            match iter.next() {
                                Some(time) => {
                                    let parsed_val = match f64::from_str(val) {
                                        Ok(f) => f,
                                        Err(_) => return None,
                                    };
                                    let parsed_time = match i64::from_str(time) {
                                        Ok(t) => t,
                                        Err(_) => return None,
                                    };
                                    res.push(Metric::new_with_time(String::from(name),
                                                                   parsed_val,
                                                                   Some(parsed_time),
                                                                   MetricKind::Raw, None))
                                }
                                None => return None
                            }
                        }
                        None => return None
                    }
                }
                None => break
            }
        }
        if res.is_empty() { return None }
        Some(res)
    }
}

#[cfg(test)]
mod tests {
    extern crate rand;
    extern crate quickcheck;

    use metric::{Metric, MetricKind, MetricSign, MetricQOS, Event};
    use self::quickcheck::{Arbitrary, Gen};
    use chrono::{UTC, TimeZone};
    use self::rand::{Rand, Rng};

    impl Rand for MetricSign {
        fn rand<R: Rng>(rng: &mut R) -> MetricSign {
            let i: usize = rng.gen_range(0, 2);
            match i % 2 {
                0 => MetricSign::Positive,
                1 => MetricSign::Negative,
                _ => unreachable!(),
            }
        }
    }

    impl Rand for MetricKind {
        fn rand<R: Rng>(rng: &mut R) -> MetricKind {
            let i: usize = rng.gen();
            match i % 6 {
                0 => MetricKind::Counter(rng.gen_range(-2.0, 2.0)),
                1 => MetricKind::Gauge,
                2 => MetricKind::DeltaGauge,
                3 => MetricKind::Timer,
                4 => MetricKind::Histogram,
                _ => MetricKind::Raw,
            }
        }
    }

    impl Rand for Metric {
        fn rand<R: Rng>(rng: &mut R) -> Metric {
            let name: String = rng.gen_ascii_chars().take(2).collect();
            let val: f64 = rng.gen();
            let kind: MetricKind = rng.gen();
            let sign: Option<MetricSign> = rng.gen();
            let time: i64 = rng.gen_range(0, 100);
            Metric::new_with_time(name, val, Some(time), kind, sign)
        }
    }

    impl Rand for MetricQOS {
        fn rand<R: Rng>(rng: &mut R) -> MetricQOS {
            MetricQOS {
                counter: rng.gen_range(1, 60),
                gauge: rng.gen_range(1, 60),
                timer: rng.gen_range(1, 60),
                histogram: rng.gen_range(1, 60),
                raw: rng.gen_range(1, 60),
            }
        }
    }

    impl Rand for Event {
        fn rand<R: Rng>(rng: &mut R) -> Event {
            let i: usize = rng.gen();
            match i % 4 {
                0 => Event::TimerFlush,
                _ => Event::Statsd(rng.gen()),
            }
        }
    }

    impl Arbitrary for MetricQOS {
        fn arbitrary<G: Gen>(g: &mut G) -> MetricQOS {
            g.gen()
        }
    }

    impl Arbitrary for Metric {
        fn arbitrary<G: Gen>(g: &mut G) -> Metric {
            g.gen()
        }
    }

    impl Arbitrary for Event {
        fn arbitrary<G: Gen>(g: &mut G) -> Event {
            g.gen()
        }
    }

    #[test]
    fn test_parse_graphite() {
        let pyld = "fst 1 101\nsnd -2.0 202\nthr 3 303\nfth@fth 4 404\nfv%fv 5 505\ns-th 6 606\n";
        let prs = Metric::parse_graphite(pyld);

        assert!(prs.is_some());
        let prs_pyld = prs.unwrap();

        assert_eq!(prs_pyld[0].kind, MetricKind::Raw);
        assert_eq!(prs_pyld[0].name, "fst");
        assert_eq!(prs_pyld[0].value, 1.0);
        assert_eq!(prs_pyld[0].time, UTC.timestamp(101, 0).timestamp());

        assert_eq!(prs_pyld[1].kind, MetricKind::Raw);
        assert_eq!(prs_pyld[1].name, "snd");
        assert_eq!(prs_pyld[1].value, -2.0);
        assert_eq!(prs_pyld[1].time, UTC.timestamp(202, 0).timestamp());

        assert_eq!(prs_pyld[2].kind, MetricKind::Raw);
        assert_eq!(prs_pyld[2].name, "thr");
        assert_eq!(prs_pyld[2].value, 3.0);
        assert_eq!(prs_pyld[2].time, UTC.timestamp(303, 0).timestamp());

        assert_eq!(prs_pyld[3].kind, MetricKind::Raw);
        assert_eq!(prs_pyld[3].name, "fth@fth");
        assert_eq!(prs_pyld[3].value, 4.0);
        assert_eq!(prs_pyld[3].time, UTC.timestamp(404, 0).timestamp());

        assert_eq!(prs_pyld[4].kind, MetricKind::Raw);
        assert_eq!(prs_pyld[4].name, "fv%fv");
        assert_eq!(prs_pyld[4].value, 5.0);
        assert_eq!(prs_pyld[4].time, UTC.timestamp(505, 0).timestamp());

        assert_eq!(prs_pyld[5].kind, MetricKind::Raw);
        assert_eq!(prs_pyld[5].name, "s-th");
        assert_eq!(prs_pyld[5].value, 6.0);
        assert_eq!(prs_pyld[5].time, UTC.timestamp(606, 0).timestamp());
    }

    #[test]
    fn test_parse_metric_via_api() {
        let pyld = "zrth:0|g\nfst:-1.1|ms\nsnd:+2.2|g\nthd:3.3|h\nfth:4|c\nfvth:5.5|c@2\nsxth:-6.\
                    6|g\nsvth:+7.77|g\n";
        let prs = Metric::parse_statsd(pyld);

        assert!(prs.is_some());
        let prs_pyld = prs.unwrap();

        assert_eq!(prs_pyld[0].kind, MetricKind::Gauge);
        assert_eq!(prs_pyld[0].name, "zrth");
        assert_eq!(prs_pyld[0].value, 0.0);

        assert_eq!(prs_pyld[1].kind, MetricKind::Timer);
        assert_eq!(prs_pyld[1].name, "fst");
        assert_eq!(prs_pyld[1].value, -1.1);

        assert_eq!(prs_pyld[2].kind, MetricKind::DeltaGauge);
        assert_eq!(prs_pyld[2].name, "snd");
        assert_eq!(prs_pyld[2].value, 2.2);

        assert_eq!(prs_pyld[3].kind, MetricKind::Histogram);
        assert_eq!(prs_pyld[3].name, "thd");
        assert_eq!(prs_pyld[3].value, 3.3);

        assert_eq!(prs_pyld[4].kind, MetricKind::Counter(1.0));
        assert_eq!(prs_pyld[4].name, "fth");
        assert_eq!(prs_pyld[4].value, 4.0);

        assert_eq!(prs_pyld[5].kind, MetricKind::Counter(2.0));
        assert_eq!(prs_pyld[5].name, "fvth");
        assert_eq!(prs_pyld[5].value, 5.5);

        assert_eq!(prs_pyld[6].kind, MetricKind::DeltaGauge);
        assert_eq!(prs_pyld[6].name, "sxth");
        assert_eq!(prs_pyld[6].value, -6.6);

        assert_eq!(prs_pyld[7].kind, MetricKind::DeltaGauge);
        assert_eq!(prs_pyld[7].name, "svth");
        assert_eq!(prs_pyld[7].value, 7.77);
    }

    #[test]
    fn test_metric_equal_in_name() {
        let res = Metric::parse_statsd("A=:1|ms\n").unwrap();

        assert_eq!("A=", res[0].name);
        assert_eq!(1.0, res[0].value);
        assert_eq!(MetricKind::Timer, res[0].kind);
    }

    #[test]
    fn test_metric_slash_in_name() {
        let res = Metric::parse_statsd("A/:1|ms\n").unwrap();

        assert_eq!("A/", res[0].name);
        assert_eq!(1.0, res[0].value);
        assert_eq!(MetricKind::Timer, res[0].kind);
    }

    #[test]
    fn test_metric_sample_gauge() {
        let res = Metric::parse_statsd("foo:1|g@0.22\nbar:101|g@2\n").unwrap();
        //                              0         A     F
        assert_eq!("foo", res[0].name);
        assert_eq!(1.0, res[0].value);
        assert_eq!(MetricKind::Gauge, res[0].kind);

        assert_eq!("bar", res[1].name);
        assert_eq!(101.0, res[1].value);
        assert_eq!(MetricKind::Gauge, res[1].kind);
    }

    #[test]
    fn test_metric_parse_invalid_no_name() {
        assert_eq!(None, Metric::parse_statsd(""));
    }

    #[test]
    fn test_metric_parse_invalid_no_value() {
        assert_eq!(None, Metric::parse_statsd("foo:"));
    }

    #[test]
    fn test_metric_multiple() {
        let res = Metric::parse_statsd("a.b:12.1|g\nb_c:13.2|c\n").unwrap();
        assert_eq!(2, res.len());

        assert_eq!("a.b", res[0].name);
        assert_eq!(12.1, res[0].value);

        assert_eq!("b_c", res[1].name);
        assert_eq!(13.2, res[1].value);
    }

    #[test]
    fn test_metric_optional_final_newline() {
        let res = Metric::parse_statsd("a.b:12.1|g\nb_c:13.2|c").unwrap();
        assert_eq!(2, res.len());

        assert_eq!("a.b", res[0].name);
        assert_eq!(12.1, res[0].value);

        assert_eq!("b_c", res[1].name);
        assert_eq!(13.2, res[1].value);
    }

    #[test]
    fn test_metric_invalid() {
        let invalid = vec!["", "metric", "metric|11:", "metric|12", "metric:13|", ":|@", ":1.0|c"];
        for input in invalid.iter() {
            let result = Metric::parse_statsd(*input);
            assert!(result.is_none());
        }
    }
}
