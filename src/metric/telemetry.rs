use metric::TagMap;
use metric::tagmap::cmp;
use std::cmp;
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::AddAssign;
use std::sync;
use time;
use quantiles::histogram::Histogram;
use quantiles::ckms::CKMS;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, PartialOrd, Eq, Hash)]
pub enum AggregationMethod {
    Sum,
    Set,
    Summarize,
    Histogram,
}

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
enum Value {
    Set(f64),
    Sum(f64),
    Histogram(Histogram<f64>),
    Quantiles(CKMS<f64>),
}

pub struct SoftTelemetry {
    name: Option<String>,
    initial_value: Option<f64>,
    thawed_value: Option<Value>,
    kind: Option<AggregationMethod>,
    error: Option<f64>,       // only needed for Summarize
    bounds: Option<Vec<f64>>, // only needed for Histogram
    timestamp: Option<i64>,
    tags: Option<sync::Arc<TagMap>>,
    persist: Option<bool>,
}

#[derive(PartialEq, Serialize, Deserialize, Clone)]
pub struct Telemetry {
    pub name: String,
    value: Value,
    pub persist: bool,
    pub tags: sync::Arc<TagMap>,
    pub timestamp: i64,
}

impl AddAssign for Telemetry {
    fn add_assign(&mut self, rhs: Telemetry) {
        // We only add Telemetry that are of the same aggregation kind. We
        // silently ignore incompatibility because AddAssign may not fail.
        match (self.value, rhs.value) {
            (Value::Set(_), Value::Set(y)) => {
                self.value = Value::Set(y);
            },
            (Value::Sum(x), Value::Sum(y)) => {
                self.value = Value::Sum(x+y);
            },
            (Value::Quantiles(x), Value::Quantiles(y)) => {
                x += y;
                self.value = Value::Quantiles(x)
            },
            (Value::Histogram(x), Value::Histogram(y)) => {
                x += y;
                self.value = Value::Histogram(x)
            },
            (_, _) => { return },
        }
        // When we add two telemetries together what we gotta do is make sure
        // that if one side or the other is persisted then the resulting
        // Telemetry is persisted.
        self.persist = rhs.persist;
    }
}

impl fmt::Debug for Telemetry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Telemetry {{ aggr_method: {:#?}, name: {}, timestamp: {}, \
             value: {:?} }}",
            self.kind(),
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
            value: Value::Set(0.0),
            persist: false,
            tags: sync::Arc::new(TagMap::default()),
            timestamp: time::now(),
        }
    }
}

impl SoftTelemetry {
    /// Set the name of the Telemetry
    ///
    /// The likelyhood is that there will be many Telemetry with the same
    /// name. We might do fancy tricks with this in mind but, then again, we
    /// might not.
    pub fn name<S>(self, name: S) -> SoftTelemetry
    where
        S: Into<String>,
    {
        self.name = Some(name.into());
        self
    }

    /// Set the initial value of Telemetry
    ///
    /// This value primes the pump of the Telemetry. There'll be more come in
    /// but we've got to know where to start.
    pub fn value(self, val: f64) -> SoftTelemetry {
        self.thawed_value = None;
        self.initial_value = Some(val);
        self
    }

    /// Set the kind of Telemetry aggregation
    ///
    /// Telemetry provide different views into the stored data. The kind
    /// controls, well, what kind of view is going to be used.
    pub fn kind(self, aggr: AggregationMethod) -> SoftTelemetry {
        self.kind = Some(aggr);
        self
    }

    /// Set the error for quantile calculation
    ///
    /// This is only necessary if the kind has been set to
    /// AggregationMethod::Summarize. It is an error to set this if the
    /// aggregation method is not as previously specified.
    pub fn error(self, error: f64) -> SoftTelemetry {
        self.error = Some(error);
        self
    }

    /// Set the bounds for histogram calculation
    ///
    /// This is only necessary if the kind has been set to
    /// AggregationMethod::Histogram. It is an error to set this if the
    /// aggregation method is not as previously specified.
    pub fn bounds(self, bounds: Vec<f64>) -> SoftTelemetry {
        bounds.sort_by(|a, b| a.partial_cmp(b).unwrap());
        self.bounds = Some(bounds);
        self
    }

    /// Set the timestamp of the Telemetry
    ///
    /// This is the instant of time in seconds that the Telemetry is considered
    /// to have happened.
    pub fn timestamp(self, ts: i64) -> SoftTelemetry {
        self.timestamp = Some(ts);
        self
    }

    /// Set the tags of the Telemetry
    ///
    /// These are the tags associated with the Telemetry and help us determine
    /// origin of report etc, depending on what the user has configured.
    pub fn tags(self, tags: sync::Arc<TagMap>) -> SoftTelemetry {
        self.tags = Some(tags);
        self
    }

    /// Set the persist of the Telemetry
    ///
    /// This flag determines if the Telemetry persists across time bins. This is
    /// a flag for aggregation implementation and may be ignored. If this is not
    /// specified the Telemetry is considered to not persist.
    pub fn persist(self, persist: bool) -> SoftTelemetry {
        self.persist = Some(persist);
        self
    }

    pub fn harden(self) -> Result<Telemetry, Error> {
        if self.initial_value.is_some() && self.thawed_value.is_some() {
            return Err(Error::CannotHaveTwoValues);
        }
        let name = if let Some(name) = self.name {
            name
        } else {
            return Err(Error::NoName);
        };
        let initial_value = if let Some(initial_value) = self.initial_value {
            initial_value
        } else {
            return Err(Error::NoInitialValue);
        };
        let kind = if let Some(kind) = self.kind {
            kind
        } else {
            return Err(Error::NoKind);
        };
        let timestamp = if let Some(timestamp) = self.timestamp {
            timestamp
        } else {
            return Err(Error::NoTimestamp);
        };
        let tags = if let Some(tags) = self.tags {
            tags
        } else {
            return Err(Error::NoTags);
        };
        let persist = if let Some(persist) = self.persist {
            persist
        } else {
            false
        };
        match kind {
            AggregationMethod::Summarize => {
                if self.bounds.is_some() {
                    return Err(Error::CannotSetBounds);
                }
                let error = if let Some(error) = self.error {
                    if error >= 1.0 {
                        return Err(Error::SummarizeErrorTooLarge);
                    }
                    error
                } else {
                    return Err(Error::NoErrorForSummarize);
                };
                let mut ckms = CKMS::new(error);
                ckms.insert(initial_value);
                Ok(Telemetry {
                    name: name,
                    value: Value::Quantiles(ckms),
                    persist: persist,
                    tags: tags,
                    timestamp: timestamp,
                })
            }
            AggregationMethod::Histogram => {
                if self.error.is_some() {
                    return Err(Error::CannotSetError);
                }
                let bounds = if let Some(bounds) = self.bounds {
                    bounds
                } else {
                    return Err(Error::NoBoundsForSummarize);
                };
                let mut histo = Histogram::new(bounds).unwrap();
                histo.insert(initial_value);
                Ok(Telemetry {
                    name: name,
                    value: Value::Histogram(histo),
                    persist: persist,
                    tags: tags,
                    timestamp: timestamp,
                })
            }
            AggregationMethod::Set => {
                if self.error.is_some() {
                    return Err(Error::CannotSetError);
                }
                if self.bounds.is_some() {
                    return Err(Error::CannotSetBounds);
                }
                Ok(Telemetry {
                    name: name,
                    value: Value::Set(initial_value),
                    persist: persist,
                    tags: tags,
                    timestamp: timestamp,
                })
            }
            AggregationMethod::Sum => {
                if self.error.is_some() {
                    return Err(Error::CannotSetError);
                }
                if self.bounds.is_some() {
                    return Err(Error::CannotSetBounds);
                }
                Ok(Telemetry {
                    name: name,
                    value: Value::Sum(initial_value),
                    persist: persist,
                    tags: tags,
                    timestamp: timestamp,
                })
            }
        }
    }
}

#[derive(Debug)]
pub enum Error {
    CannotHaveTwoValues,
    CannotSetBounds,
    CannotSetError,
    NoBoundsForSummarize,
    NoErrorForSummarize,
    NoInitialValue,
    NoKind,
    NoName,
    NoTags,
    NoTimestamp,
    SummarizeBoundsTooLarge,
    SummarizeErrorTooLarge,
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
    /// assert_eq!(m.aggregation(), AggregationMethod::Set);
    /// assert_eq!(m.name, "foo");
    /// assert_eq!(m.value(), Some(1.1));
    /// ```
    pub fn new() -> SoftTelemetry {
        SoftTelemetry {
            name: None,
            initial_value: None,
            thawed_value: None,
            kind: None,
            error: None,
            bounds: None,
            timestamp: None,
            tags: None,
            persist: None,
        }
    }

    pub fn thaw(self) -> SoftTelemetry {
        let kind = self.kind();
        SoftTelemetry {
            name: Some(self.name),
            initial_value: None,
            thawed_value: Some(self.value),
            kind: Some(kind),
            error: None,
            bounds: None,
            timestamp: Some(self.timestamp),
            tags: Some(self.tags),
            persist: Some(self.persist),
        }
    }

    pub fn kind(&self) -> AggregationMethod {
        match self.value {
            Value::Set(_) => AggregationMethod::Set,
            Value::Sum(_) => AggregationMethod::Sum,
            Value::Histogram(_) => AggregationMethod::Histogram,
            Value::Quantiles(_) => AggregationMethod::Summarize,
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

    pub fn insert(mut self, value: f64) -> Telemetry {
        match self.value {
            Value::Set(_) => {
                self.value = Value::Set(value);
            },
            Value::Sum(x) => {
                self.value = Value::Sum(x + value);
            },
            Value::Histogram(ref mut histo) => {
                histo.insert(value);
            },
            Value::Quantiles(ref mut ckms) => {
                ckms.insert(value);
            }
        }
        self
    }

    pub fn sum(&self) -> Option<f64> {
        match self.value {
            Value::Sum(x) => Some(x),
            _ => None
        }
    }

    pub fn set(&self) -> Option<f64> {
        match self.value {
            Value::Set(x) => Some(x),
            _ => None
        }
    }

    pub fn query(&self, prcnt: f64) -> Option<f64> {
        match self.value {
            Value::Quantiles(ckms) => { ckms.query(prcnt).map(|x| x.1) },
            _ => None
        }
    }

    /// Sum of all samples inserted into this Telemetry 
    pub fn samples_sum(&self) -> f64 {
        unimplemented!();
    }
    
    pub fn count(&self) -> usize {
        self.value.count()
    }

    pub fn mean(&self) -> f64 {
        self.value.mean().unwrap()
    }

    // pub fn value(&self) -> Option<f64> {
    //     self.value.value()
    // }

    // pub fn into_vec(self) -> Vec<f64> {
    //     self.value.clone().into_vec()
    // }

    pub fn samples(&self) -> Vec<f64> {
        match self.value {
            Value::Set(x) => vec![x],
            Value::Sum(x) => vec![x],
            Value::Quantiles(ckms) => ckms.clone().into_vec(),
            Value::Histogram(histo) => histo.counts(),
        }
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

    pub fn is_zeroed(&self) -> bool {
        match self.value() {
            None => false,
            Some(n) => !(n == 0.0),
        }
    }

    pub fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.name.hash(&mut hasher);
        self.tags.hash(&mut hasher);
        self.value.kind().hash(&mut hasher);
        hasher.finish()
    }

    pub fn is_set(&self) -> bool {
        self.value.kind() == AggregationMethod::Set
    }

    pub fn is_histogram(&self) -> bool {
        self.value.kind() == AggregationMethod::Histogram
    }

    pub fn aggregation(&self) -> AggregationMethod {
        self.value.kind()
    }

    pub fn is_sum(&self) -> bool {
        self.value.kind() == AggregationMethod::Sum
    }

    pub fn is_summarize(&self) -> bool {
        self.value.kind() == AggregationMethod::Summarize
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
            if lhs.aggregation() != rhs.aggregation() {
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
            let old_mlhs = mlhs.clone();
            let old_mrhs = mrhs.clone();
            mlhs += mrhs;
            if let Some(val) = mlhs.value() {
                let expected = match kind {
                    AggregationMethod::Set => rhs,
                    AggregationMethod::Sum => lhs + rhs,
                    AggregationMethod::Summarize => lhs.max(rhs),
                };
                // println!("VAL: {:?} | EXPECTED: {:?}", val, expected);
                match val.partial_cmp(&expected) {
                    Some(cmp::Ordering::Equal) => return TestResult::passed(),
                    _ => {
                        println!(
                            "\n\nMLHS: {:?} | MRHS: {:?} | RES: {:?}\nEXPECTED: {:?} | VAL: {:?}",
                            old_mlhs,
                            old_mrhs,
                            mlhs,
                            expected,
                            val
                        );
                        return TestResult::failed();
                    }
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

        assert_eq!(m.aggregation(), AggregationMethod::Summarize);
        assert_eq!(m.query(1.0), Some(-1.0));
        assert_eq!(m.name, "timer");
    }

    #[test]
    fn test_postive_delta_gauge() {
        let m = Telemetry::new("dgauge", 1.0).persist().aggr_set();

        assert_eq!(m.aggregation(), AggregationMethod::Set);
        assert_eq!(m.value(), Some(1.0));
        assert_eq!(m.name, "dgauge");
    }
}
