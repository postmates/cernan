use time;
use std::str::FromStr;
use quantiles::CKMS;

include!(concat!(env!("OUT_DIR"), "/metric_types.rs"));

use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use fnv::FnvHasher;
use std::cmp::Ordering;
use std::ops::AddAssign;
use std::fmt;

pub type TagMap = HashMap<String, String, BuildHasherDefault<FnvHasher>>;

impl LogLine {
    pub fn new<S>(path: S, value: String, tags: TagMap) -> LogLine
        where S: Into<String>
    {
        LogLine {
            path: path.into(),
            value: value,
            time: time::now(),
            tags: tags,
        }
    }
}

fn cmp(left: &TagMap, right: &TagMap) -> Option<Ordering> {
    if left.len() != right.len() {
        left.len().partial_cmp(&right.len())
    } else {
        let mut l: Vec<(&String, &String)> = left.iter().collect();
        l.sort();
        let mut r: Vec<(&String, &String)> = right.iter().collect();
        r.sort();
        l.partial_cmp(&r)
    }
}

impl PartialEq for MetricKind {
    fn eq(&self, other: &MetricKind) -> bool {
        match (self, other) {
            (&MetricKind::Gauge, &MetricKind::Gauge) |
            (&MetricKind::Gauge, &MetricKind::DeltaGauge) |
            (&MetricKind::DeltaGauge, &MetricKind::DeltaGauge) |
            (&MetricKind::DeltaGauge, &MetricKind::Gauge) |
            (&MetricKind::Counter, &MetricKind::Counter) |
            (&MetricKind::Timer, &MetricKind::Timer) |
            (&MetricKind::Histogram, &MetricKind::Histogram) |
            (&MetricKind::Raw, &MetricKind::Raw) => true,
            _ => false,
        }
    }
}

impl PartialOrd for MetricKind {
    fn partial_cmp(&self, other: &MetricKind) -> Option<Ordering> {
        match (self, other) {
            (&MetricKind::Counter, &MetricKind::DeltaGauge) |
            (&MetricKind::Counter, &MetricKind::Gauge) |
            (&MetricKind::Counter, &MetricKind::Histogram) |
            (&MetricKind::Counter, &MetricKind::Raw) |
            (&MetricKind::Counter, &MetricKind::Timer) |
            (&MetricKind::Histogram, &MetricKind::Raw) |
            (&MetricKind::DeltaGauge, &MetricKind::Histogram) |
            (&MetricKind::DeltaGauge, &MetricKind::Raw) |
            (&MetricKind::DeltaGauge, &MetricKind::Timer) |
            (&MetricKind::Gauge, &MetricKind::Histogram) |
            (&MetricKind::Gauge, &MetricKind::Raw) |
            (&MetricKind::Gauge, &MetricKind::Timer) |
            (&MetricKind::Timer, &MetricKind::Histogram) |
            (&MetricKind::Timer, &MetricKind::Raw) => Some(Ordering::Less),

            (&MetricKind::Counter, &MetricKind::Counter) |
            (&MetricKind::DeltaGauge, &MetricKind::DeltaGauge) |
            (&MetricKind::DeltaGauge, &MetricKind::Gauge) |
            (&MetricKind::Gauge, &MetricKind::DeltaGauge) |
            (&MetricKind::Gauge, &MetricKind::Gauge) |
            (&MetricKind::Histogram, &MetricKind::Histogram) |
            (&MetricKind::Raw, &MetricKind::Raw) |
            (&MetricKind::Timer, &MetricKind::Timer) => Some(Ordering::Equal),

            (&MetricKind::DeltaGauge, &MetricKind::Counter) |
            (&MetricKind::Gauge, &MetricKind::Counter) |
            (&MetricKind::Histogram, &MetricKind::Counter) |
            (&MetricKind::Histogram, &MetricKind::DeltaGauge) |
            (&MetricKind::Histogram, &MetricKind::Gauge) |
            (&MetricKind::Histogram, &MetricKind::Timer) |
            (&MetricKind::Raw, &MetricKind::Counter) |
            (&MetricKind::Raw, &MetricKind::DeltaGauge) |
            (&MetricKind::Raw, &MetricKind::Gauge) |
            (&MetricKind::Raw, &MetricKind::Histogram) |
            (&MetricKind::Raw, &MetricKind::Timer) |
            (&MetricKind::Timer, &MetricKind::Counter) |
            (&MetricKind::Timer, &MetricKind::DeltaGauge) |
            (&MetricKind::Timer, &MetricKind::Gauge) => Some(Ordering::Greater),
        }
    }
}

impl AddAssign for Metric {
    fn add_assign(&mut self, rhs: Metric) {
        match rhs.kind {
            MetricKind::DeltaGauge => {
                self.kind = rhs.kind;
                self.value += rhs.value;
            }
            MetricKind::Gauge => {
                self.kind = rhs.kind;
                self.value = rhs.value;
            }
            _ => {
                self.value += rhs.value;
                assert_eq!(self.kind, rhs.kind);
            }
        }
    }
}

impl fmt::Debug for Metric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "Metric {{ kind: {:#?}, name: {}, time: {}, tags: {:?}, value: {:?} }}",
               self.kind,
               self.name,
               self.time,
               self.tags,
               self.value())
    }
}

impl PartialOrd for Metric {
    fn partial_cmp(&self, other: &Metric) -> Option<Ordering> {
        match self.kind.partial_cmp(&other.kind) {
            Some(Ordering::Equal) => {
                match self.name.partial_cmp(&other.name) {
                    Some(Ordering::Equal) => {
                        match self.time.partial_cmp(&other.time) {
                            Some(Ordering::Equal) => cmp(&self.tags, &other.tags),
                            other => other,
                        }
                    }
                    other => other,
                }
            }
            other => other,
        }
    }
}

impl Metric {
    /// Make a builder for metrics
    ///
    /// This function returns a MetricBuidler with a name set. A metric must
    /// have _at least_ a name and a value but values may be delayed behind
    /// names.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::{Metric,MetricKind};
    ///
    /// let m = Metric::new("foo", 1.1);
    ///
    /// assert_eq!(m.kind, MetricKind::Raw);
    /// assert_eq!(m.name, "foo");
    /// assert_eq!(m.value(), Some(1.1));
    /// ```
    pub fn new<S>(name: S, value: f64) -> Metric
        where S: Into<String>
    {
        let mut ckms = CKMS::new(0.001);
        ckms.insert(value);
        Metric {
            kind: MetricKind::Raw,
            name: name.into(),
            tags: TagMap::default(),
            created_time: time::now(),
            time: time::now(),
            value: ckms,
        }
    }

    /// Overlay a specific key / value pair in self's tags
    ///
    /// This insert a key / value pair into the metric's tag map. If the key was
    /// already present in the tag map the value will be replaced, else it will
    /// be inserted.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::Metric;
    ///
    /// let mut m = Metric::new("foo", 1.1);
    ///
    /// assert!(m.tags.is_empty());
    ///
    /// m = m.overlay_tag("foo", "bar");
    /// assert_eq!(Some(&"bar".into()), m.tags.get("foo".into()));
    ///
    /// m = m.overlay_tag("foo", "22");
    /// assert_eq!(Some(&"22".into()), m.tags.get("foo".into()));
    /// ```
    pub fn overlay_tag<S>(mut self, key: S, val: S) -> Metric
        where S: Into<String>
    {
        self.tags.insert(key.into(), val.into());
        self
    }

    /// Overlay self's tags with a TagMap
    ///
    /// This inserts a map of key / value pairs over the top of metric's
    /// existing tag map. Any new keys will be inserted while existing keys will
    /// be overwritten.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::{Metric,TagMap};
    ///
    /// let mut m = Metric::new("foo", 1.1);
    ///
    /// assert!(m.tags.is_empty());
    ///
    /// m = m.overlay_tag("foo", "22");
    /// assert_eq!(Some(&"22".into()), m.tags.get("foo".into()));
    ///
    /// let mut tag_map = TagMap::default();
    /// tag_map.insert("foo".into(), "bar".into());
    /// tag_map.insert("oof".into(), "rab".into());
    ///
    /// m = m.overlay_tags_from_map(&tag_map);
    /// assert_eq!(Some(&"bar".into()), m.tags.get("foo".into()));
    /// assert_eq!(Some(&"rab".into()), m.tags.get("oof".into()));
    /// ```
    pub fn overlay_tags_from_map(mut self, map: &TagMap) -> Metric {
        for (k, v) in map.iter() {
            self.tags.insert(k.clone(), v.clone());
        }
        self
    }

    /// Merge a TagMap into self's tags
    ///
    /// This inserts a map of key / values pairs into metric's existing map,
    /// inserting keys if and only if the key does not already exist
    /// in-map. This is the information-preserving partner to
    /// overlay_tags_from_map.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::{Metric,TagMap};
    ///
    /// let mut m = Metric::new("foo", 1.1);
    ///
    /// assert!(m.tags.is_empty());
    ///
    /// m = m.overlay_tag("foo", "22");
    /// assert_eq!(Some(&"22".into()), m.tags.get("foo".into()));
    ///
    /// let mut tag_map = TagMap::default();
    /// tag_map.insert("foo".into(), "bar".into());
    /// tag_map.insert("oof".into(), "rab".into());
    ///
    /// m = m.merge_tags_from_map(&tag_map);
    /// assert_eq!(Some(&"22".into()), m.tags.get("foo".into()));
    /// assert_eq!(Some(&"rab".into()), m.tags.get("oof".into()));
    /// ```
    pub fn merge_tags_from_map(mut self, map: &TagMap) -> Metric {
        for (k, v) in map.iter() {
            self.tags.entry(k.clone()).or_insert(v.clone());
        }
        self
    }

    /// Replace a Metric's value
    ///
    /// While it is sometimes still necessary to fiddle with the value of a
    /// Metric directly--I'm looking at you, buckets.rs--ideally we'd use a safe
    /// API to do that fiddling.
    ///
    /// This function merely replaces the value. We could get fancy in the
    /// future and do the Kind appropriate computation...
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::Metric;
    ///
    /// let m = Metric::new("foo", 1.1).set_value(10.10);
    ///
    /// assert_eq!(m.value(), Some(10.10));
    /// ```
    pub fn set_value(mut self, value: f64) -> Metric {
        self.value = CKMS::new(0.001);
        self.value.insert(value);
        self
    }

    pub fn insert_value(mut self, value: f64) -> Metric {
        self.value.insert(value);
        self
    }

    pub fn value(&self) -> Option<f64> {
        match self.kind {
            MetricKind::Gauge | MetricKind::Raw => self.value.last(),
            MetricKind::DeltaGauge | MetricKind::Counter => self.value.sum(),
            MetricKind::Timer | MetricKind::Histogram => self.value.query(1.0).map(|x| x.1),
        }
    }

    pub fn within(&self, span: i64, other: &Metric) -> Ordering {
        match self.kind.partial_cmp(&other.kind) {
            Some(Ordering::Equal) => {
                match self.name.partial_cmp(&other.name) {
                    Some(Ordering::Equal) => {
                        match cmp(&self.tags, &other.tags) {
                            Some(Ordering::Equal) => {
                                let lhs_bin = self.time / span;
                                let rhs_bin = other.time / span;
                                lhs_bin.cmp(&rhs_bin)
                            }
                            other => other.unwrap(),
                        }
                    }
                    other => other.unwrap(),
                }
            }
            other => other.unwrap(),
        }
    }

    /// Adjust MetricKind to Counter
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::{Metric, MetricKind};
    ///
    /// let m = Metric::new("foo", 1.1).counter();
    ///
    /// assert_eq!(MetricKind::Counter, m.kind);
    /// ```
    pub fn counter(mut self) -> Metric {
        self.kind = MetricKind::Counter;
        self
    }

    /// Adjust MetricKind to Gauge
    ///
    /// If the kind has previously been set to DeltaGauge the kind will not be
    /// reset to Gauge.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::{Metric, MetricKind};
    ///
    /// let m = Metric::new("foo", 1.1).gauge();
    ///
    /// assert_eq!(MetricKind::Gauge, m.kind);
    /// ```
    pub fn gauge(mut self) -> Metric {
        self.kind = match self.kind {
            MetricKind::DeltaGauge => MetricKind::DeltaGauge,
            _ => MetricKind::Gauge,
        };
        self
    }

    /// Adjust MetricKind to DeltaGauge
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::{Metric, MetricKind};
    ///
    /// let m = Metric::new("foo", 1.1).delta_gauge();
    ///
    /// assert_eq!(MetricKind::DeltaGauge, m.kind);
    /// ```
    pub fn delta_gauge(mut self) -> Metric {
        self.kind = MetricKind::DeltaGauge;
        self
    }

    /// Adjust MetricKind to Timer
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::{Metric, MetricKind};
    ///
    /// let m = Metric::new("foo", 1.1).delta_gauge();
    ///
    /// assert_eq!(MetricKind::DeltaGauge, m.kind);
    /// ```
    pub fn timer(mut self) -> Metric {
        self.kind = MetricKind::Timer;
        self
    }

    /// Adjust MetricKind to Histogram
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::{Metric, MetricKind};
    ///
    /// let m = Metric::new("foo", 1.1).histogram();
    ///
    /// assert_eq!(MetricKind::Histogram, m.kind);
    /// ```
    pub fn histogram(mut self) -> Metric {
        self.kind = MetricKind::Histogram;
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
    /// use cernan::metric::Metric;
    ///
    /// let m = Metric::new("foo", 1.1).time(10101);
    ///
    /// assert_eq!(10101, m.time);
    /// ```
    pub fn time(mut self, time: i64) -> Metric {
        self.time = time;
        self
    }

    pub fn created_time(mut self, time: i64) -> Metric {
        self.created_time = time;
        self
    }

    pub fn query(&self, prcnt: f64) -> Option<f64> {
        self.value.query(prcnt).map(|x| x.1)
    }

    pub fn count(&self) -> usize {
        self.value.count()
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
                            let name = &src[offset..(offset + colon_idx)];
                            if name.is_empty() {
                                return None;
                            };
                            offset += colon_idx + 1;
                            if offset >= len {
                                return None;
                            };
                            match (&src[offset..]).find('|') {
                                Some(pipe_idx) => {
                                    let val = match f64::from_str(&src[offset..(offset +
                                                                                pipe_idx)]) {
                                        Ok(f) => f,
                                        Err(_) => return None,
                                    };
                                    let mut metric = Metric::new(name, val);
                                    metric = match &src[offset..(offset + 1)] {
                                        "+" | "-" => metric.delta_gauge(),
                                        _ => metric,
                                    };
                                    offset += pipe_idx + 1;
                                    if offset >= len {
                                        return None;
                                    };
                                    metric = match (&src[offset..]).find('@') {
                                        Some(sample_idx) => {
                                            match &src[offset..(offset + sample_idx)] {
                                                "g" => metric.gauge(),
                                                "ms" => metric.timer(),
                                                "h" => metric.histogram(),
                                                "c" => {
                                                    let sample =
                                                        match f64::from_str(&src[(offset +
                                                                                  sample_idx +
                                                                                  1)..]) {
                                                            Ok(f) => f,
                                                            Err(_) => return None,
                                                        };
                                                    metric = metric.counter();
                                                    metric.set_value(val * (1.0 / sample))
                                                }
                                                _ => return None,
                                            }
                                        }
                                        None => {
                                            match &src[offset..] {
                                                "g" | "g\n" => metric.gauge(),
                                                "ms" | "ms\n" => metric.timer(),
                                                "h" | "h\n" => metric.histogram(),
                                                "c" | "c\n" => metric.counter(),
                                                _ => return None,
                                            }
                                        }
                                    };

                                    res.push(metric);
                                }
                                None => return None,
                            }
                        }
                        None => return None,
                    }
                }
                None => break,
            }
        }
        if res.is_empty() { None } else { Some(res) }
    }

    pub fn parse_graphite(source: &str) -> Option<Vec<Metric>> {
        let mut res = Vec::new();
        let mut iter = source.split_whitespace();
        while let Some(name) = iter.next() {
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
                            res.push(Metric::new(name, parsed_val).time(parsed_time));
                        }
                        None => return None,
                    }
                }
                None => return None,
            }
        }
        if res.is_empty() {
            return None;
        }
        Some(res)
    }
}

#[cfg(test)]
mod tests {
    extern crate rand;
    extern crate quickcheck;

    use metric::{Metric, MetricKind, Event};
    use self::quickcheck::{Arbitrary, Gen, TestResult, QuickCheck};
    use chrono::{UTC, TimeZone};
    use self::rand::{Rand, Rng};
    use std::cmp::Ordering;

    #[test]
    fn partial_ord_distinct() {
        let mc = Metric::new("l6", 0.7913855).counter().time(47);
        let mg = Metric::new("l6", 0.9683).gauge().time(47);

        assert_eq!(Some(Ordering::Less), mc.partial_cmp(&mg));
    }

    #[test]
    fn partial_ord_gauges() {
        let mdg = Metric::new("l6", 0.7913855).delta_gauge().time(47);
        let mg = Metric::new("l6", 0.9683).gauge().time(47);

        assert_eq!(Some(Ordering::Equal), mg.partial_cmp(&mdg));
        assert_eq!(Some(Ordering::Equal), mdg.partial_cmp(&mg));
    }

    impl Rand for MetricKind {
        fn rand<R: Rng>(rng: &mut R) -> MetricKind {
            let i: usize = rng.gen();
            match i % 6 {
                0 => MetricKind::Counter,
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
            let time: i64 = rng.gen_range(0, 100);
            let mut mb = Metric::new(name, val).time(time);
            mb = match kind {
                MetricKind::Gauge => mb.gauge(),
                MetricKind::Timer => mb.timer(),
                MetricKind::Histogram => mb.histogram(),
                MetricKind::Counter => mb.counter(),
                MetricKind::DeltaGauge => mb.delta_gauge(),
                MetricKind::Raw => mb,
            };
            mb
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

    impl Arbitrary for MetricKind {
        fn arbitrary<G: Gen>(g: &mut G) -> MetricKind {
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
    fn test_metric_within() {
        fn inner(span: i64, lhs: Metric, rhs: Metric) -> TestResult {
            if lhs.kind != rhs.kind {
                return TestResult::discard();
            } else if lhs.name != rhs.name {
                return TestResult::discard();
            } else if span < 1 {
                return TestResult::discard();
            }
            let order = (lhs.time / span).cmp(&(rhs.time / span));
            assert_eq!(order, lhs.within(span, &rhs));
            TestResult::passed()
        }
        QuickCheck::new()
            .tests(10000)
            .max_tests(100000)
            .quickcheck(inner as fn(i64, Metric, Metric) -> TestResult);
    }

    #[test]
    fn test_metric_add_assign() {
        fn inner(lhs: f64, rhs: f64, kind: MetricKind) -> TestResult {
            let mut mlhs = Metric::new("foo", lhs);
            let mut mrhs = Metric::new("foo", rhs);
            mlhs = match kind {
                MetricKind::Gauge => mlhs.gauge(),
                MetricKind::DeltaGauge => mlhs.delta_gauge(),
                MetricKind::Histogram => mlhs.histogram(),
                MetricKind::Timer => mlhs.timer(),
                MetricKind::Raw => mlhs,
                MetricKind::Counter => mlhs.counter(),
            };
            mrhs = match kind {
                MetricKind::Gauge => mrhs.gauge(),
                MetricKind::DeltaGauge => mrhs.delta_gauge(),
                MetricKind::Histogram => mrhs.histogram(),
                MetricKind::Timer => mrhs.timer(),
                MetricKind::Raw => mrhs,
                MetricKind::Counter => mrhs.counter(),
            };
            mlhs += mrhs;
            if let Some(val) = mlhs.value() {
                let expected = match kind {
                    MetricKind::Gauge | MetricKind::Raw => rhs,
                    MetricKind::DeltaGauge | MetricKind::Counter => lhs + rhs,
                    MetricKind::Timer | MetricKind::Histogram => lhs.max(rhs),
                };
                match val.partial_cmp(&expected) {
                    Some(Ordering::Equal) => return TestResult::passed(),
                    _ => return TestResult::failed(),
                }
            } else {
                return TestResult::failed();
            }
        }
        QuickCheck::new()
            .tests(100)
            .max_tests(1000)
            .quickcheck(inner as fn(f64, f64, MetricKind) -> TestResult);
    }

    #[test]
    fn test_parse_graphite() {
        let pyld = "fst 1 101\nsnd -2.0 202\nthr 3 303\nfth@fth 4 404\nfv%fv 5 505\ns-th 6 606\n";
        let prs = Metric::parse_graphite(pyld);

        assert!(prs.is_some());
        let prs_pyld = prs.unwrap();

        assert_eq!(prs_pyld[0].kind, MetricKind::Raw);
        assert_eq!(prs_pyld[0].name, "fst");
        assert_eq!(prs_pyld[0].value(), Some(1.0));
        assert_eq!(prs_pyld[0].time, UTC.timestamp(101, 0).timestamp());

        assert_eq!(prs_pyld[1].kind, MetricKind::Raw);
        assert_eq!(prs_pyld[1].name, "snd");
        assert_eq!(prs_pyld[1].value(), Some(-2.0));
        assert_eq!(prs_pyld[1].time, UTC.timestamp(202, 0).timestamp());

        assert_eq!(prs_pyld[2].kind, MetricKind::Raw);
        assert_eq!(prs_pyld[2].name, "thr");
        assert_eq!(prs_pyld[2].value(), Some(3.0));
        assert_eq!(prs_pyld[2].time, UTC.timestamp(303, 0).timestamp());

        assert_eq!(prs_pyld[3].kind, MetricKind::Raw);
        assert_eq!(prs_pyld[3].name, "fth@fth");
        assert_eq!(prs_pyld[3].value(), Some(4.0));
        assert_eq!(prs_pyld[3].time, UTC.timestamp(404, 0).timestamp());

        assert_eq!(prs_pyld[4].kind, MetricKind::Raw);
        assert_eq!(prs_pyld[4].name, "fv%fv");
        assert_eq!(prs_pyld[4].value(), Some(5.0));
        assert_eq!(prs_pyld[4].time, UTC.timestamp(505, 0).timestamp());

        assert_eq!(prs_pyld[5].kind, MetricKind::Raw);
        assert_eq!(prs_pyld[5].name, "s-th");
        assert_eq!(prs_pyld[5].value(), Some(6.0));
        assert_eq!(prs_pyld[5].time, UTC.timestamp(606, 0).timestamp());
    }

    #[test]
    fn test_negative_timer() {
        let m = Metric::new("timer", -1.0).timer();

        assert_eq!(m.kind, MetricKind::Timer);
        assert_eq!(m.query(1.0), Some(-1.0));
        assert_eq!(m.name, "timer");
    }

    #[test]
    fn test_parse_negative_timer() {
        let prs = Metric::parse_statsd("fst:-1.1|ms\n");

        assert!(prs.is_some());
        let prs_pyld = prs.unwrap();

        assert_eq!(prs_pyld[0].kind, MetricKind::Timer);
        assert_eq!(prs_pyld[0].name, "fst");
        assert_eq!(prs_pyld[0].query(1.0), Some(-1.1));
    }

    #[test]
    fn test_postive_delta_gauge() {
        let m = Metric::new("dgauge", 1.0).delta_gauge();

        assert_eq!(m.kind, MetricKind::DeltaGauge);
        assert_eq!(m.value(), Some(1.0));
        assert_eq!(m.name, "dgauge");
    }

    #[test]
    fn test_parse_metric_via_api() {
        let pyld = "zrth:0|g\nfst:-1.1|ms\nsnd:+2.2|g\nthd:3.3|h\nfth:4|c\nfvth:5.5|c@0.1\nsxth:\
                    -6.6|g\nsvth:+7.77|g\n";
        let prs = Metric::parse_statsd(pyld);

        assert!(prs.is_some());
        let prs_pyld = prs.unwrap();

        assert_eq!(prs_pyld[0].kind, MetricKind::Gauge);
        assert_eq!(prs_pyld[0].name, "zrth");
        assert_eq!(prs_pyld[0].value(), Some(0.0));

        assert_eq!(prs_pyld[1].kind, MetricKind::Timer);
        assert_eq!(prs_pyld[1].name, "fst");
        assert_eq!(prs_pyld[1].query(1.0), Some(-1.1));

        assert_eq!(prs_pyld[2].kind, MetricKind::DeltaGauge);
        assert_eq!(prs_pyld[2].name, "snd");
        assert_eq!(prs_pyld[2].value(), Some(2.2));

        assert_eq!(prs_pyld[3].kind, MetricKind::Histogram);
        assert_eq!(prs_pyld[3].name, "thd");
        assert_eq!(prs_pyld[3].query(1.0), Some(3.3));

        assert_eq!(prs_pyld[4].kind, MetricKind::Counter);
        assert_eq!(prs_pyld[4].name, "fth");
        assert_eq!(prs_pyld[4].value(), Some(4.0));

        assert_eq!(prs_pyld[5].kind, MetricKind::Counter);
        assert_eq!(prs_pyld[5].name, "fvth");
        assert_eq!(prs_pyld[5].value(), Some(55.0));

        assert_eq!(prs_pyld[6].kind, MetricKind::DeltaGauge);
        assert_eq!(prs_pyld[6].name, "sxth");
        assert_eq!(prs_pyld[6].value(), Some(-6.6));

        assert_eq!(prs_pyld[7].kind, MetricKind::DeltaGauge);
        assert_eq!(prs_pyld[7].name, "svth");
        assert_eq!(prs_pyld[7].value(), Some(7.77));
    }

    #[test]
    fn test_metric_equal_in_name() {
        let res = Metric::parse_statsd("A=:1|ms\n").unwrap();

        assert_eq!("A=", res[0].name);
        assert_eq!(Some(1.0), res[0].query(1.0));
        assert_eq!(MetricKind::Timer, res[0].kind);
    }

    #[test]
    fn test_metric_slash_in_name() {
        let res = Metric::parse_statsd("A/:1|ms\n").unwrap();

        assert_eq!("A/", res[0].name);
        assert_eq!(Some(1.0), res[0].query(1.0));
        assert_eq!(MetricKind::Timer, res[0].kind);
    }

    #[test]
    fn test_metric_sample_gauge() {
        let res = Metric::parse_statsd("foo:1|g@0.22\nbar:101|g@2\n").unwrap();
        //                              0         A     F
        assert_eq!("foo", res[0].name);
        assert_eq!(Some(1.0), res[0].query(1.0));
        assert_eq!(MetricKind::Gauge, res[0].kind);

        assert_eq!("bar", res[1].name);
        assert_eq!(Some(101.0), res[1].query(1.0));
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
        assert_eq!(Some(12.1), res[0].value());

        assert_eq!("b_c", res[1].name);
        assert_eq!(Some(13.2), res[1].value());
    }

    #[test]
    fn test_metric_optional_final_newline() {
        let res = Metric::parse_statsd("a.b:12.1|g\nb_c:13.2|c").unwrap();
        assert_eq!(2, res.len());

        assert_eq!("a.b", res[0].name);
        assert_eq!(Some(12.1), res[0].value());

        assert_eq!("b_c", res[1].name);
        assert_eq!(Some(13.2), res[1].value());
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
