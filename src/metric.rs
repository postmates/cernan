use time;
use std::str::FromStr;

include!(concat!(env!("OUT_DIR"), "/serde_types.rs"));

impl LogLine {
    pub fn new(path: String, value: String) -> LogLine {
        LogLine {
            path: path,
            value: value,
            time: time::now(),
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

#[derive(Debug)]
pub struct MetricBuilder {
    name: String,
    value: Option<f64>,
    kind: Option<MetricKind>,
    sign: Option<MetricSign>,
    time: Option<i64>,
}

impl MetricBuilder {
    /// Make a builder for metrics
    ///
    /// This function returns a MetricBuidler with a name set. A metric must
    /// have _at least_ a name and a value but values may be delayed behind
    /// names.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::MetricBuilder;
    ///
    /// assert!(MetricBuilder::new("foo").build().is_err());
    /// assert!(MetricBuilder::new("foo").value(1.1).build().is_ok());
    /// ```
    pub fn new<S>(name: S) -> MetricBuilder where S: Into<String> {
        MetricBuilder {
            name: name.into(),
            value: None,
            kind: None,
            time: None,
            sign: None,
        }
    }

    /// Add a value to a MetricBuilder
    ///
    /// Values are mandatory for a Metric but may not be available at the same
    /// time as names. Values _must_ carry their proper sign. `positive` and
    /// `negative` functions _will not_ modify value sign. These functions
    /// modify the interpretation of a MetricKind::Gauge.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::MetricBuilder;
    ///
    /// let mp = MetricBuilder::new("foo").value(1.1).positive().build().unwrap();
    /// let mn = MetricBuilder::new("foo").value(1.1).negative().build().unwrap();
    ///
    /// assert_eq!(1.1, mp.value);
    /// assert_eq!(1.1, mn.value);
    /// ```
    pub fn value(mut self, value: f64) -> MetricBuilder {
        self.value = Some(value);
        self
    }

    /// Adjust MetricKind to Counter
    ///
    /// This sets the ultimate metric's MetricKind to Counter. A sample
    /// rate--measured in per-second units--is required.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::{MetricBuilder, MetricKind};
    ///
    /// let mr = MetricBuilder::new("foo").value(1.1).build().unwrap();
    /// let mc = MetricBuilder::new("foo").value(1.1).counter(1.0).build().unwrap();
    ///
    /// assert_eq!(MetricKind::Raw, mr.kind);
    /// assert_eq!(MetricKind::Counter(1.0), mc.kind);
    /// ```
    pub fn counter(mut self, sample: f64) -> MetricBuilder {
        self.kind = Some(MetricKind::Counter(sample));
        self
    }

    /// Adjust MetricKind to Gauge
    ///
    /// This sets the ultimate metric's MetricKind to gauge-type. There are two
    /// manner of gauge, DeltaGauge and Gauge. This function will set the manner
    /// to Gauge, though `postive` and `negative` may adjust this.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::{MetricBuilder, MetricKind};
    ///
    /// let mr = MetricBuilder::new("foo").value(1.1).build().unwrap();
    /// let mg = MetricBuilder::new("foo").value(1.1).gauge().build().unwrap();
    ///
    /// assert_eq!(MetricKind::Raw, mr.kind);
    /// assert_eq!(MetricKind::Gauge, mg.kind);
    /// ```
    pub fn gauge(mut self) -> MetricBuilder {
        self.kind = Some(MetricKind::Gauge);
        self
    }

    /// Adjust MetricKind to Timer
    ///
    /// This sets the ultimate metric's MetricKind to Timer.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::{MetricBuilder, MetricKind};
    ///
    /// let mr = MetricBuilder::new("foo").value(1.1).build().unwrap();
    /// let mt = MetricBuilder::new("foo").value(1.1).timer().build().unwrap();
    ///
    /// assert_eq!(MetricKind::Raw, mr.kind);
    /// assert_eq!(MetricKind::Timer, mt.kind);
    /// ```
    pub fn timer(mut self) -> MetricBuilder {
        self.kind = Some(MetricKind::Timer);
        self
    }

    /// Adjust MetricKind to Histogram
    ///
    /// This sets the ultimate metric's MetricKind to Histogram.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::{MetricBuilder, MetricKind};
    ///
    /// let mr = MetricBuilder::new("foo").value(1.1).build().unwrap();
    /// let mt = MetricBuilder::new("foo").value(1.1).histogram().build().unwrap();
    ///
    /// assert_eq!(MetricKind::Raw, mr.kind);
    /// assert_eq!(MetricKind::Histogram, mt.kind);
    /// ```
    pub fn histogram(mut self) -> MetricBuilder {
        self.kind = Some(MetricKind::Histogram);
        self
    }

    /// Adjust Metric time
    ///
    /// This sets the metric time to the specified value, taken to be UTC
    /// seconds since the Unix Epoch. If this is not set the metric will default
    /// to `cernan::time::now()`.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::MetricBuilder;
    ///
    /// let m = MetricBuilder::new("foo").value(1.1).time(10101).build().unwrap();
    ///
    /// assert_eq!(10101, m.time);
    /// ```
    pub fn time(mut self, time: i64) -> MetricBuilder {
        self.time = Some(time);
        self
    }

    /// Adjust Metric with Gauge kind to be DeltaGauge
    ///
    /// This sets the metric's kind to DeltaGauge if the kind has been otherwise
    /// set to Gauge. The ordering does not matter. This will not affect other
    /// metric types. A distinction is made between `positive` and `negative` to
    /// aid debugging.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::{MetricBuilder,MetricKind};
    ///
    /// let mr = MetricBuilder::new("foo").value(1.1).positive().build().unwrap();
    /// let mg = MetricBuilder::new("bar").value(1.1).gauge().build().unwrap();
    /// let mdg = MetricBuilder::new("bar").value(1.1).gauge().positive().build().unwrap();
    ///
    /// assert_eq!(MetricKind::Raw, mr.kind);
    /// assert_eq!(MetricKind::Gauge, mg.kind);
    /// assert_eq!(MetricKind::DeltaGauge, mdg.kind);
    /// ```
    pub fn positive(mut self) -> MetricBuilder {
        self.sign = Some(MetricSign::Positive);
        self
    }

    /// Adjust Metric with Gauge kind to be DeltaGauge
    ///
    /// This sets the metric's kind to DeltaGauge if the kind has been otherwise
    /// set to Gauge. The ordering does not matter. This will not affect other
    /// metric types. A distinction is made between `positive` and `negative` to
    /// aid debugging.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::{MetricBuilder,MetricKind};
    ///
    /// let mr = MetricBuilder::new("foo").value(1.1).positive().build().unwrap();
    /// let mg = MetricBuilder::new("bar").value(1.1).gauge().build().unwrap();
    /// let mdg = MetricBuilder::new("bar").value(1.1).gauge().negative().build().unwrap();
    ///
    /// assert_eq!(MetricKind::Raw, mr.kind);
    /// assert_eq!(MetricKind::Gauge, mg.kind);
    /// assert_eq!(MetricKind::DeltaGauge, mdg.kind);
    /// ```
    pub fn negative(mut self) -> MetricBuilder {
        self.sign = Some(MetricSign::Negative);
        self
    }

    /// Build a Metric
    ///
    /// This function will return Err if a value has not been
    /// provided. Otherwise, everything will pretty well go through smooth.
    ///
    /// See other functions on this impl for examples.
    pub fn build(self) -> Result<Metric, &'static str> {
        if !self.value.is_some() {
            return Err("must have a value");
        }
        let raw_value = self.value.unwrap();

        let kind = if let Some(k) = self.kind {
            match k {
                MetricKind::Gauge => if self.sign.is_some() {
                    MetricKind::DeltaGauge
                } else {
                    MetricKind::Gauge
                },
                _ => k,
            }
        } else {
            MetricKind::Raw
        };

        Ok(Metric {
            name: self.name,
            time: self.time.unwrap_or(time::now()),
            value: raw_value,
            kind: kind,
            tags: vec![],
        })
    }
}

impl Metric {
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
                            let mut metric_builder = MetricBuilder::new(name);
                            offset += colon_idx + 1;
                            if offset >= len { return None };
                            match (&src[offset..]).find('|') {
                                Some(pipe_idx) => {
                                    metric_builder = match &src[offset..(offset+1)] {
                                        "+" => metric_builder.positive(),
                                        "-" => metric_builder.negative(),
                                        _ => metric_builder,
                                    };
                                    let val = match f64::from_str(&src[offset..(offset+pipe_idx)]) {
                                        Ok(f) => f,
                                        Err(_) => return None
                                    };
                                    metric_builder = metric_builder.value(val);
                                    offset += pipe_idx + 1;
                                    if offset >= len { return None };
                                    metric_builder = match (&src[offset..]).find('@') {
                                        Some(sample_idx) => {
                                            match &src[offset..(offset+sample_idx)] {
                                                "g" => metric_builder.gauge(),
                                                "ms" => metric_builder.timer(),
                                                "h" => metric_builder.histogram(),
                                                "c" => {
                                                    let sample = match f64::from_str(&src[(offset+sample_idx+1)..]) {
                                                        Ok(f) => f,
                                                        Err(_) => return None
                                                    };
                                                    metric_builder.counter(sample)
                                                }
                                                _ => return None
                                            }
                                        }
                                        None => {
                                            match &src[offset..] {
                                                "g" | "g\n" => metric_builder.gauge(),
                                                "ms" | "ms\n" => metric_builder.timer(),
                                                "h" | "h\n" => metric_builder.histogram(),
                                                "c" | "c\n" => metric_builder.counter(1.0),
                                                _ => return None
                                            }
                                        }
                                    };

                                    res.push(metric_builder.build().unwrap());
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
                                    res.push(MetricBuilder::new(String::from(name)).value(parsed_val).time(parsed_time).build().unwrap());
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

    use metric::{Metric, MetricBuilder, MetricKind, MetricSign, MetricQOS, Event};
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
            let mut mb = MetricBuilder::new(name).value(val).time(time);
            mb = match kind {
                MetricKind::Gauge => mb.gauge(),
                MetricKind::Timer => mb.timer(),
                MetricKind::Histogram => mb.histogram(),
                MetricKind::Counter(smpl) => mb.counter(smpl),
                MetricKind::DeltaGauge => mb.gauge(),
                MetricKind::Raw => mb,
            };
            mb = match sign {
                None => mb,
                Some(MetricSign::Positive) => mb.positive(),
                Some(MetricSign::Negative) => mb.negative(),
            };
            mb.build().unwrap()
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
    fn test_negative_timer() {
        let m = MetricBuilder::new("timer").value(-1.0).timer().build().unwrap();

        assert_eq!(m.kind, MetricKind::Timer);
        assert_eq!(m.value, -1.0);
        assert_eq!(m.name, "timer");
    }

    #[test]
    fn test_parse_negative_timer() {
        let prs = Metric::parse_statsd("fst:-1.1|ms\n");

        assert!(prs.is_some());
        let prs_pyld = prs.unwrap();

        assert_eq!(prs_pyld[0].kind, MetricKind::Timer);
        assert_eq!(prs_pyld[0].name, "fst");
        assert_eq!(prs_pyld[0].value, -1.1);
    }

    #[test]
    fn test_postive_delta_gauge() {
        let m = MetricBuilder::new("dgauge").value(1.0).positive().gauge().build().unwrap();

        assert_eq!(m.kind, MetricKind::DeltaGauge);
        assert_eq!(m.value, 1.0);
        assert_eq!(m.name, "dgauge");
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
