use quantiles::ckms::CKMS;
use std::sync;
use time;

mod tagmap;

pub use self::tagmap::cmp;

include!(concat!(env!("OUT_DIR"), "/metric_types.rs"));

use std::cmp::{Ordering, PartialOrd};
use std::fmt;
use std::ops::AddAssign;

pub type TagMap = self::tagmap::TagMap<String, String>;

impl LogLine {
    pub fn new<S>(path: S, value: S) -> LogLine
        where S: Into<String>
    {
        LogLine {
            path: path.into(),
            value: value.into(),
            time: time::now(),
            tags: Default::default(),
        }
    }

    pub fn time(mut self, time: i64) -> LogLine {
        self.time = time;
        self
    }

    pub fn overlay_tag<S>(mut self, key: S, val: S) -> LogLine
        where S: Into<String>
    {
        self.tags.insert(key.into(), val.into());
        self
    }

    pub fn overlay_tags_from_map(mut self, map: &TagMap) -> LogLine {
        for &(ref k, ref v) in map.iter() {
            self.tags.insert(k.clone(), v.clone());
        }
        self
    }
}

impl AddAssign for MetricValue {
    fn add_assign(&mut self, rhs: MetricValue) {
        match rhs.kind {
            MetricValueKind::Single => self.insert(rhs.single.expect("EMPTY SINGLE ADD_ASSIGN")),
            MetricValueKind::Many => self.merge(rhs.many.expect("EMPTY MANY ADD_ASSIGN")), 
        }
    }
}

impl MetricValue {
    fn new(value: f64) -> MetricValue {
        MetricValue {
            kind: MetricValueKind::Single,
            single: Some(value),
            many: None,
        }
    }

    fn into_vec(self) -> Vec<f64> {
        match self.kind {
            MetricValueKind::Single => vec![self.single.unwrap()],
            MetricValueKind::Many => self.many.unwrap().into_vec(),
        }
    }

    fn insert(&mut self, value: f64) -> () {
        match self.kind {
            MetricValueKind::Single => {
                let mut ckms = CKMS::new(0.001);
                ckms.insert(self.single.expect("NOT SINGLE IN METRICVALUE INSERT"));
                ckms.insert(value);
                self.many = Some(ckms);
                self.single = None;
                self.kind = MetricValueKind::Many;
            }
            MetricValueKind::Many => {
                match self.many.as_mut() {
                    None => {}
                    Some(ckms) => ckms.insert(value),
                };
            }
        }
    }

    fn merge(&mut self, mut value: CKMS<f64>) -> () {
        match self.kind {
            MetricValueKind::Single => {
                value.insert(self.single.expect("NOT SINGLE IN METRICVALUE MERGE"));
                self.many = Some(value);
                self.single = None;
                self.kind = MetricValueKind::Many;
            }
            MetricValueKind::Many => {
                match self.many.as_mut() {
                    None => {}
                    Some(ckms) => *ckms += value,
                };
            }
        }
    }

    fn last(&self) -> Option<f64> {
        match self.kind {
            MetricValueKind::Single => self.single,
            MetricValueKind::Many => {
                match self.many {
                    Some(ref ckms) => ckms.last(),
                    None => None,
                }
            }
        }
    }

    fn sum(&self) -> Option<f64> {
        match self.kind {
            MetricValueKind::Single => self.single,
            MetricValueKind::Many => {
                match self.many {
                    Some(ref ckms) => ckms.sum(),
                    None => None,
                }
            }
        }
    }

    fn count(&self) -> usize {
        match self.kind {
            MetricValueKind::Single => 1,
            MetricValueKind::Many => {
                match self.many {
                    Some(ref ckms) => ckms.count(),
                    None => 0,
                }
            }
        }
    }

    fn query(&self, query: f64) -> Option<(usize, f64)> {
        match self.kind {
            MetricValueKind::Single => {
                Some((1 as usize, self.single.expect("NOT SINGLE IN METRICVALUE QUERY")))
            }
            MetricValueKind::Many => {
                match self.many {
                    Some(ref ckms) => ckms.query(query),
                    None => None,
                }
            }
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

impl Default for Metric {
    fn default() -> Metric {
        Metric {
            kind: MetricKind::Raw,
            name: String::from(""),
            tags: sync::Arc::new(TagMap::default()),
            created_time: time::now(),
            time: time::now(),
            value: MetricValue::new(0.0),
        }
    }
}

impl Event {
    #[inline]
    pub fn new_telemetry(metric: Metric) -> Event {
        Event::Telemetry(sync::Arc::new(Some(metric)))
    }

    #[inline]
    pub fn new_log(log: LogLine) -> Event {
        Event::Log(sync::Arc::new(Some(log)))
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
        let val = MetricValue::new(value);
        Metric {
            kind: MetricKind::Raw,
            name: name.into(),
            tags: sync::Arc::new(TagMap::default()),
            created_time: time::now(),
            time: time::now(),
            value: val,
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
    /// assert_eq!(Some(&"bar".into()), m.tags.get(&String::from("foo")));
    ///
    /// m = m.overlay_tag("foo", "22");
    /// assert_eq!(Some(&"22".into()), m.tags.get(&String::from("foo")));
    /// ```
    pub fn overlay_tag<S>(mut self, key: S, val: S) -> Metric
        where S: Into<String>
    {
        sync::Arc::make_mut(&mut self.tags).insert(key.into(), val.into());
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
    /// assert_eq!(Some(&"22".into()), m.tags.get(&String::from("foo")));
    ///
    /// let mut tag_map = TagMap::default();
    /// tag_map.insert("foo".into(), "bar".into());
    /// tag_map.insert("oof".into(), "rab".into());
    ///
    /// m = m.overlay_tags_from_map(&tag_map);
    /// assert_eq!(Some(&"bar".into()), m.tags.get(&String::from("foo")));
    /// assert_eq!(Some(&"rab".into()), m.tags.get(&String::from("oof")));
    /// ```
    pub fn overlay_tags_from_map(mut self, map: &TagMap) -> Metric {
        for &(ref k, ref v) in map.iter() {
            sync::Arc::make_mut(&mut self.tags).insert(k.clone(), v.clone());
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
    /// assert_eq!(Some(&"22".into()), m.tags.get(&String::from("foo")));
    ///
    /// let mut tag_map = TagMap::default();
    /// tag_map.insert("foo".into(), "bar".into());
    /// tag_map.insert("oof".into(), "rab".into());
    ///
    /// m = m.merge_tags_from_map(&tag_map);
    /// assert_eq!(Some(&"22".into()), m.tags.get(&String::from("foo")));
    /// assert_eq!(Some(&"rab".into()), m.tags.get(&String::from("oof")));
    /// ```
    pub fn merge_tags_from_map(mut self, map: &TagMap) -> Metric {
        sync::Arc::make_mut(&mut self.tags).merge(map);
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
        self.value = MetricValue::new(value);
        self
    }

    pub fn set_name<S>(mut self, name: S) -> Metric
        where S: Into<String>
    {
        self.name = name.into();
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

    pub fn into_vec(self) -> Vec<f64> {
        self.value.clone().into_vec()
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
}

#[cfg(test)]
mod tests {
    extern crate rand;
    extern crate quickcheck;

    use metric::{Event, Metric, MetricKind};
    use self::quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
    use self::rand::{Rand, Rng};
    use std::cmp::Ordering;
    use std::sync::Arc;

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

        assert_eq!(Some(Ordering::Less), mg.partial_cmp(&mdg));
        assert_eq!(Some(Ordering::Greater), mdg.partial_cmp(&mg));
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
            match i % 3 {
                0 => Event::TimerFlush,
                _ => Event::Telemetry(Arc::new(Some(rng.gen()))),
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
    fn test_negative_timer() {
        let m = Metric::new("timer", -1.0).timer();

        assert_eq!(m.kind, MetricKind::Timer);
        assert_eq!(m.query(1.0), Some(-1.0));
        assert_eq!(m.name, "timer");
    }

    #[test]
    fn test_postive_delta_gauge() {
        let m = Metric::new("dgauge", 1.0).delta_gauge();

        assert_eq!(m.kind, MetricKind::DeltaGauge);
        assert_eq!(m.value(), Some(1.0));
        assert_eq!(m.name, "dgauge");
    }
}
