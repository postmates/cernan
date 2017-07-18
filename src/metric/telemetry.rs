use metric::TagMap;
use metric::tagmap::cmp;
use quantiles::ckms::CKMS;
use std::cmp;
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::AddAssign;
use std::sync;
use time;

#[derive(PartialEq, Serialize, Deserialize, Clone)]
pub struct Telemetry {
    pub name: String,
    value: Value,
    pub persist: bool,
    pub aggr_method: AggregationMethod,
    pub tags: sync::Arc<TagMap>,
    pub timestamp: i64, // seconds, see #166
    pub timestamp_ns: u64,
}

impl Hash for Telemetry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.tags.hash(state);
        self.aggr_method.hash(state);
    }
}

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub enum ValueKind {
    Single,
    Many,
}

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct Value {
    kind: ValueKind,
    single: Option<f64>,
    many: Option<CKMS<f64>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd, Eq, Hash)]
pub enum AggregationMethod {
    Sum,
    Set,
    Summarize,
}

impl AddAssign for Telemetry {
    fn add_assign(&mut self, rhs: Telemetry) {
        // When we add two telemetries together what we gotta do is make sure
        // that if one side or the other is persisted then the resulting
        // Telemetry is persisted.
        self.persist = rhs.persist;
        self.value += rhs.value;
        self.aggr_method = rhs.aggr_method;
    }
}

impl fmt::Debug for Telemetry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Telemetry {{ aggr_method: {:#?}, name: {}, timestamp: {}, \
             value: {:?} }}",
            self.aggr_method,
            self.name,
            self.timestamp,
            self.value()
        )
    }
}

impl PartialOrd for Telemetry {
    fn partial_cmp(&self, other: &Telemetry) -> Option<cmp::Ordering> {
        match self.name.partial_cmp(&other.name) {
            Some(cmp::Ordering::Equal) => {
                match self.timestamp.partial_cmp(&other.timestamp) {
                    Some(cmp::Ordering::Equal) => cmp(&self.tags, &other.tags),
                    other => other,
                }
            }
            other => other,
        }
    }
}

impl Default for Telemetry {
    fn default() -> Telemetry {
        Telemetry {
            name: String::from(""),
            value: Value::new(Default::default()),
            persist: false,
            aggr_method: AggregationMethod::Summarize,
            tags: sync::Arc::new(TagMap::default()),
            timestamp: time::now(),
            timestamp_ns: time::now_ns(),
        }
    }
}

impl Telemetry {
    /// Make a builder for metrics
    ///
    /// This function returns a TelemetryBuidler with a name set. A metric must
    /// have _at least_ a name and a value but values may be delayed behind
    /// names.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::{Telemetry,AggregationMethod};
    ///
    /// let m = Telemetry::new("foo", 1.1);
    ///
    /// assert_eq!(m.aggr_method, AggregationMethod::Summarize);
    /// assert_eq!(m.name, "foo");
    /// assert_eq!(m.value(), Some(1.1));
    /// ```
    pub fn new<S>(name: S, value: f64) -> Telemetry
    where
        S: Into<String>,
    {
        let val = Value::new(value);
        Telemetry {
            aggr_method: AggregationMethod::Summarize,
            name: name.into(),
            tags: sync::Arc::new(TagMap::default()),
            timestamp: time::now(),
            timestamp_ns: time::now_ns(),
            value: val,
            persist: false,
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
    /// use cernan::metric::Telemetry;
    ///
    /// let mut m = Telemetry::new("foo", 1.1);
    ///
    /// assert!(m.tags.is_empty());
    ///
    /// m = m.overlay_tag("foo", "bar");
    /// assert_eq!(Some(&"bar".into()), m.tags.get(&String::from("foo")));
    ///
    /// m = m.overlay_tag("foo", "22");
    /// assert_eq!(Some(&"22".into()), m.tags.get(&String::from("foo")));
    /// ```
    pub fn overlay_tag<S>(mut self, key: S, val: S) -> Telemetry
    where
        S: Into<String>,
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
    /// use cernan::metric::{Telemetry,TagMap};
    ///
    /// let mut m = Telemetry::new("foo", 1.1);
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
    pub fn overlay_tags_from_map(mut self, map: &TagMap) -> Telemetry {
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
    /// use cernan::metric::{Telemetry,TagMap};
    ///
    /// let mut m = Telemetry::new("foo", 1.1);
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
    pub fn merge_tags_from_map(mut self, map: &TagMap) -> Telemetry {
        sync::Arc::make_mut(&mut self.tags).merge(map);
        self
    }

    /// Replace a Telemetry's value
    ///
    /// While it is sometimes still necessary to fiddle with the value of a
    /// Telemetry directly--I'm looking at you, buckets.rs--ideally we'd use a
    /// safe
    /// API to do that fiddling.
    ///
    /// This function merely replaces the value. We could get fancy in the
    /// future and do the Kind appropriate computation...
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::Telemetry;
    ///
    /// let m = Telemetry::new("foo", 1.1).set_value(10.10);
    ///
    /// assert_eq!(m.value(), Some(10.10));
    /// ```
    pub fn set_value(mut self, value: f64) -> Telemetry {
        self.value = Value::new(value);
        self
    }

    pub fn set_name<S>(mut self, name: S) -> Telemetry
    where
        S: Into<String>,
    {
        self.name = name.into();
        self
    }

    pub fn insert_value(mut self, value: f64) -> Telemetry {
        self.value.insert(value);
        self
    }

    pub fn value(&self) -> Option<f64> {
        match self.aggr_method {
            AggregationMethod::Set => self.value.last(),
            AggregationMethod::Sum => self.value.sum(),
            AggregationMethod::Summarize => self.value.query(1.0).map(|x| x.1),
        }
    }

    pub fn into_vec(self) -> Vec<f64> {
        self.value.clone().into_vec()
    }

    pub fn within(&self, span: i64, other: &Telemetry) -> cmp::Ordering {
        match self.name.partial_cmp(&other.name) {
            Some(cmp::Ordering::Equal) => {
                match cmp(&self.tags, &other.tags) {
                    Some(cmp::Ordering::Equal) => {
                        let lhs_bin = self.timestamp / span;
                        let rhs_bin = other.timestamp / span;
                        lhs_bin.cmp(&rhs_bin)
                    }
                    other => other.unwrap(),
                }
            }
            other => other.unwrap(),
        }
    }

    pub fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.name.hash(&mut hasher);
        self.tags.hash(&mut hasher);
        self.aggr_method.hash(&mut hasher);
        hasher.finish()
    }

    pub fn persist(mut self) -> Telemetry {
        self.persist = true;
        self
    }

    pub fn ephemeral(mut self) -> Telemetry {
        self.persist = false;
        self
    }

    /// Set Telemetry aggregation to SET
    ///
    /// This function sets the telemetry aggregation method to set, meaning our
    /// view into its values will be that of the last value inserted.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::Telemetry;
    ///
    /// let m = Telemetry::new("foo", 1.1).timestamp(10101).aggr_set();
    /// assert!(m.is_set());
    /// ```
    pub fn aggr_set(mut self) -> Telemetry {
        self.aggr_method = AggregationMethod::Set;
        self
    }

    pub fn is_set(&self) -> bool {
        self.aggr_method == AggregationMethod::Set
    }

    /// Set Telemetry aggregation to SUM
    ///
    /// This function sets the telemetry aggregation method to sum, meaning our
    /// view into its values will be a summation of them in order of insertion.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::Telemetry;
    ///
    /// let m = Telemetry::new("foo", 1.1).timestamp(10101).aggr_sum();
    /// assert!(m.is_sum());
    /// ```
    pub fn aggr_sum(mut self) -> Telemetry {
        self.aggr_method = AggregationMethod::Sum;
        self
    }

    pub fn is_sum(&self) -> bool {
        self.aggr_method == AggregationMethod::Sum
    }

    /// Set Telemetry aggregation to SUMMARIZE
    ///
    /// This function sets the telemetry aggregation method to summarize,
    /// meaning our view into its values will be a quantile structure over all
    /// values inserted.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::Telemetry;
    ///
    /// let m = Telemetry::new("foo", 1.1).timestamp(10101).aggr_summarize();
    /// assert!(m.is_summarize());
    /// ```
    pub fn aggr_summarize(mut self) -> Telemetry {
        self.aggr_method = AggregationMethod::Summarize;
        self
    }

    pub fn is_summarize(&self) -> bool {
        self.aggr_method == AggregationMethod::Summarize
    }

    /// Adjust Telemetry time
    ///
    /// This sets the metric time to the specified value, taken to be UTC
    /// seconds since the Unix Epoch. If this is not set the metric will default
    /// to `cernan::time::now()`.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::Telemetry;
    ///
    /// let m = Telemetry::new("foo", 1.1).timestamp(10101);
    ///
    /// assert_eq!(10101, m.timestamp);
    /// ```
    pub fn timestamp(mut self, time: i64) -> Telemetry {
        self.timestamp = time;
        self
    }

    pub fn timestamp_ns(mut self, time_ns: u64) -> Telemetry {
        self.timestamp_ns = time_ns;
        self
    }

    pub fn timestamp_and_ns(mut self, sec: i64, ns: u32) -> Telemetry {
        self.timestamp = sec;
        let seconds = (sec as u64) * 1_000_000_000;
        self.timestamp_ns = seconds.saturating_add(ns as u64);
        self
    }

    pub fn query(&self, prcnt: f64) -> Option<f64> {
        self.value.query(prcnt).map(|x| x.1)
    }

    pub fn count(&self) -> usize {
        self.value.count()
    }

    pub fn mean(&self) -> f64 {
        self.value.mean().unwrap()
    }

    pub fn sum(&self) -> f64 {
        self.value.sum().unwrap()
    }

    #[cfg(test)]
    pub fn ckms(&self) -> CKMS<f64> {
        match self.value.kind {
            ValueKind::Single => {
                let mut ckms = CKMS::new(0.001);
                ckms.insert(self.value.single.unwrap());
                ckms
            }
            ValueKind::Many => self.value.many.clone().unwrap(),
        }
    }
}


impl AddAssign for Value {
    fn add_assign(&mut self, rhs: Value) {
        match rhs.kind {
            ValueKind::Single => {
                self.insert(rhs.single.expect("EMPTY SINGLE ADD_ASSIGN"))
            }
            ValueKind::Many => self.merge(rhs.many.expect("EMPTY MANY ADD_ASSIGN")),
        }
    }
}

impl Value {
    fn new(value: f64) -> Value {
        Value {
            kind: ValueKind::Single,
            single: Some(value),
            many: None,
        }
    }

    fn into_vec(self) -> Vec<f64> {
        match self.kind {
            ValueKind::Single => vec![self.single.unwrap()],
            ValueKind::Many => self.many.unwrap().into_vec(),
        }
    }

    fn insert(&mut self, value: f64) -> () {
        match self.kind {
            ValueKind::Single => {
                let mut ckms = CKMS::new(0.001);
                ckms.insert(self.single.expect("NOT SINGLE IN METRICVALUE INSERT"));
                ckms.insert(value);
                self.many = Some(ckms);
                self.single = None;
                self.kind = ValueKind::Many;
            }
            ValueKind::Many => {
                match self.many.as_mut() {
                    None => {}
                    Some(ckms) => ckms.insert(value),
                };
            }
        }
    }

    fn merge(&mut self, mut value: CKMS<f64>) -> () {
        match self.kind {
            ValueKind::Single => {
                value.insert(self.single.expect("NOT SINGLE IN METRICVALUE MERGE"));
                self.many = Some(value);
                self.single = None;
                self.kind = ValueKind::Many;
            }
            ValueKind::Many => {
                match self.many.as_mut() {
                    None => {}
                    Some(ckms) => *ckms += value,
                };
            }
        }
    }

    fn last(&self) -> Option<f64> {
        match self.kind {
            ValueKind::Single => self.single,
            ValueKind::Many => {
                match self.many {
                    Some(ref ckms) => ckms.last(),
                    None => None,
                }
            }
        }
    }

    fn sum(&self) -> Option<f64> {
        match self.kind {
            ValueKind::Single => self.single,
            ValueKind::Many => {
                match self.many {
                    Some(ref ckms) => ckms.sum(),
                    None => None,
                }
            }
        }
    }

    fn count(&self) -> usize {
        match self.kind {
            ValueKind::Single => 1,
            ValueKind::Many => {
                match self.many {
                    Some(ref ckms) => ckms.count(),
                    None => 0,
                }
            }
        }
    }

    fn mean(&self) -> Option<f64> {
        match self.kind {
            ValueKind::Single => self.single,
            ValueKind::Many => {
                match self.many {
                    Some(ref x) => x.cma(),
                    None => None,
                }
            }
        }
    }

    fn query(&self, query: f64) -> Option<(usize, f64)> {
        match self.kind {
            ValueKind::Single => {
                Some((1, self.single.expect("NOT SINGLE IN METRICVALUE QUERY")))
            }
            ValueKind::Many => {
                match self.many {
                    Some(ref ckms) => ckms.query(query),
                    None => None,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use metric::{AggregationMethod, Event, Telemetry};
    use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
    use std::cmp;
    use std::sync::Arc;

    #[test]
    fn partial_ord_equal() {
        let mc = Telemetry::new("l6", 0.7913855).aggr_sum().timestamp(47);
        let mg = Telemetry::new("l6", 0.9683).aggr_set().timestamp(47);

        assert_eq!(Some(cmp::Ordering::Equal), mc.partial_cmp(&mg));
    }

    #[test]
    fn partial_ord_distinct() {
        let mc = Telemetry::new("l6", 0.7913855).aggr_sum().timestamp(7);
        let mg = Telemetry::new("l6", 0.9683).aggr_set().timestamp(47);

        assert_eq!(Some(cmp::Ordering::Less), mc.partial_cmp(&mg));
    }

    #[test]
    fn partial_ord_gauges() {
        let mdg = Telemetry::new("l6", 0.7913855).aggr_set().persist().timestamp(47);
        let mg = Telemetry::new("l6", 0.9683).aggr_set().timestamp(47);

        assert_eq!(Some(cmp::Ordering::Equal), mg.partial_cmp(&mdg));
        assert_eq!(Some(cmp::Ordering::Equal), mdg.partial_cmp(&mg));
    }

    impl Arbitrary for AggregationMethod {
        fn arbitrary<G>(g: &mut G) -> Self
        where
            G: Gen,
        {
            let i: usize = g.gen_range(0, 3);
            match i {
                0 => AggregationMethod::Sum,
                1 => AggregationMethod::Set,
                _ => AggregationMethod::Summarize,
            }
        }
    }

    impl Arbitrary for Telemetry {
        fn arbitrary<G>(g: &mut G) -> Self
        where
            G: Gen,
        {
            let name_len = g.gen_range(0, 64);
            let name: String = g.gen_iter::<char>().take(name_len).collect();
            let val: f64 = g.gen();
            let kind: AggregationMethod = AggregationMethod::arbitrary(g);
            let persist: bool = g.gen();
            let time: i64 = g.gen_range(0, 100);
            let time_ns: u64 = (time as u64) * 1_000_000_000;
            let mut mb =
                Telemetry::new(name, val).timestamp(time).timestamp_ns(time_ns);
            mb = match kind {
                AggregationMethod::Set => mb.aggr_set(),
                AggregationMethod::Sum => mb.aggr_sum(),
                AggregationMethod::Summarize => mb.aggr_summarize(),
            };
            if persist {
                mb.persist()
            } else {
                mb
            }
        }
    }

    impl Arbitrary for Event {
        fn arbitrary<G>(g: &mut G) -> Self
        where
            G: Gen,
        {
            let i: usize = g.gen();
            match i % 3 {
                0 => Event::TimerFlush(g.gen()),
                _ => Event::Telemetry(Arc::new(Some(Arbitrary::arbitrary(g)))),
            }
        }
    }

    #[test]
    fn test_metric_within() {
        fn inner(span: i64, lhs: Telemetry, rhs: Telemetry) -> TestResult {
            if lhs.aggr_method != rhs.aggr_method {
                return TestResult::discard();
            } else if lhs.name != rhs.name {
                return TestResult::discard();
            } else if span < 1 {
                return TestResult::discard();
            }
            let order = (lhs.timestamp / span).cmp(&(rhs.timestamp / span));
            assert_eq!(order, lhs.within(span, &rhs));
            TestResult::passed()
        }
        QuickCheck::new()
            .tests(10000)
            .max_tests(100000)
            .quickcheck(inner as fn(i64, Telemetry, Telemetry) -> TestResult);
    }

    #[test]
    fn test_metric_add_assign() {
        fn inner(lhs: f64, rhs: f64, kind: AggregationMethod) -> TestResult {
            let mut mlhs = Telemetry::new("foo", lhs);
            let mut mrhs = Telemetry::new("foo", rhs);
            mlhs = match kind {
                AggregationMethod::Sum => mlhs.aggr_sum(),
                AggregationMethod::Set => mlhs.aggr_set(),
                AggregationMethod::Summarize => mlhs.aggr_summarize(),
            };
            mrhs = match kind {
                AggregationMethod::Sum => mrhs.aggr_sum(),
                AggregationMethod::Set => mrhs.aggr_set(),
                AggregationMethod::Summarize => mrhs.aggr_summarize(),
            };
            mlhs += mrhs;
            if let Some(val) = mlhs.value() {
                let expected = match kind {
                    AggregationMethod::Set => rhs,
                    AggregationMethod::Sum => lhs + rhs,
                    AggregationMethod::Summarize => lhs.max(rhs),
                };
                match val.partial_cmp(&expected) {
                    Some(cmp::Ordering::Equal) => return TestResult::passed(),
                    _ => return TestResult::failed(),
                }
            } else {
                return TestResult::failed();
            }
        }
        QuickCheck::new()
            .tests(100)
            .max_tests(1000)
            .quickcheck(inner as fn(f64, f64, AggregationMethod) -> TestResult);
    }

    #[test]
    fn test_negative_timer() {
        let m = Telemetry::new("timer", -1.0).aggr_summarize();

        assert_eq!(m.aggr_method, AggregationMethod::Summarize);
        assert_eq!(m.query(1.0), Some(-1.0));
        assert_eq!(m.name, "timer");
    }

    #[test]
    fn test_postive_delta_gauge() {
        let m = Telemetry::new("dgauge", 1.0).persist().aggr_set();

        assert_eq!(m.aggr_method, AggregationMethod::Set);
        assert_eq!(m.value(), Some(1.0));
        assert_eq!(m.name, "dgauge");
    }
}
