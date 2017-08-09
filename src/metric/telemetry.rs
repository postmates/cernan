use metric::TagMap;
use metric::tagmap::cmp;
use quantiles::ckms::CKMS;
use quantiles::histogram::{Histogram, Iter};
#[cfg(test)]
use quantiles::histogram::Bound;
use std::cmp;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::mem;
use std::ops::AddAssign;
use std::sync;
use time;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, PartialOrd, Eq, Hash)]
pub enum AggregationMethod {
    Sum,
    Set,
    Summarize,
    Histogram,
}

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub enum Value {
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

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct Telemetry {
    pub name: String,
    value: Option<Value>,
    pub persist: bool,
    pub tags: sync::Arc<TagMap>,
    pub timestamp: i64,
}

impl Value {
    #[cfg(test)]
    pub fn is_same_kind(&self, rhs: &Value) -> bool {
        match (self, rhs) {
            (&Value::Set(_), &Value::Set(_)) => true,
            (&Value::Sum(_), &Value::Sum(_)) => true,
            (&Value::Histogram(_), &Value::Histogram(_)) => true,
            (&Value::Quantiles(_), &Value::Quantiles(_)) => true,
            _ => false,
        }
    }

    pub fn add(self, rhs: Value) -> Value {
        match self {
            Value::Set(x) => match rhs {
                Value::Set(y) => Value::Set(y),
                Value::Sum(y) => Value::Sum(x + y),
                Value::Histogram(mut y) => {
                    y.insert(x);
                    Value::Histogram(y)
                }
                Value::Quantiles(mut y) => {
                    y.insert(x);
                    Value::Quantiles(y)
                }
            },
            Value::Sum(x) => match rhs {
                Value::Set(y) => Value::Set(y),
                Value::Sum(y) => Value::Sum(x + y),
                Value::Histogram(mut y) => {
                    y.insert(x);
                    Value::Histogram(y)
                }
                Value::Quantiles(mut y) => {
                    y.insert(x);
                    Value::Quantiles(y)
                }
            },
            Value::Histogram(mut x) => match rhs {
                Value::Set(y) => Value::Set(y),
                Value::Sum(y) => Value::Sum(x.sum().unwrap_or(0.0) + y),
                Value::Histogram(y) => {
                    x += y;
                    Value::Histogram(x)
                }
                Value::Quantiles(y) => Value::Quantiles(y),
            },
            Value::Quantiles(mut x) => match rhs {
                Value::Set(y) => Value::Set(y),
                Value::Sum(y) => Value::Sum(x.sum().unwrap_or(0.0) + y),
                Value::Histogram(mut y) => {
                    for v in x.into_vec() {
                        y.insert(v);
                    }
                    Value::Histogram(y)
                }
                Value::Quantiles(y) => {
                    x += y;
                    Value::Quantiles(x)
                }
            },
        }
    }

    pub fn sum(&self) -> Option<f64> {
        match *self {
            Value::Sum(x) => Some(x),
            _ => None,
        }
    }

    pub fn set(&self) -> Option<f64> {
        match *self {
            Value::Set(x) => Some(x),
            _ => None,
        }
    }

    pub fn query(&self, prcnt: f64) -> Option<f64> {
        match *self {
            Value::Quantiles(ref ckms) => ckms.query(prcnt).map(|x| x.1),
            _ => None,
        }
    }

    pub fn bins(&self) -> Option<Iter<f64>> {
        match *self {
            Value::Histogram(ref histo) => Some(histo.iter()),
            _ => None,
        }
    }

    pub fn count(&self) -> usize {
        match *self {
            Value::Set(_) | Value::Sum(_) => 1,
            Value::Histogram(ref histo) => histo.count(),
            Value::Quantiles(ref ckms) => ckms.count(),
        }
    }

    pub fn mean(&self) -> f64 {
        match *self {
            Value::Set(_) | Value::Sum(_) => 1.0,
            Value::Histogram(ref histo) => if let Some(sum) = histo.sum() {
                let count = histo.count();
                assert!(count > 0);
                sum / (count as f64)
            } else {
                0.0
            },
            Value::Quantiles(ref ckms) => ckms.cma().unwrap_or(0.0),
        }
    }

    pub fn kind(&self) -> AggregationMethod {
        match *self {
            Value::Set(_) => AggregationMethod::Set,
            Value::Sum(_) => AggregationMethod::Sum,
            Value::Histogram(_) => AggregationMethod::Histogram,
            Value::Quantiles(_) => AggregationMethod::Summarize,
        }
    }

    #[cfg(test)]
    pub fn into_vec(self) -> Option<Vec<(Bound<f64>, usize)>> {
        match self {
            Value::Histogram(h) => Some(h.into_vec()),
            _ => None,
        }
    }
}

impl AddAssign for Telemetry {
    fn add_assign(&mut self, rhs: Telemetry) {
        let value = mem::replace(&mut self.value, Default::default());
        self.value = Some(value.unwrap().add(rhs.value.unwrap()));
        // When we add two telemetries together what we gotta do is make sure
        // that if one side or the other is persisted then the resulting
        // Telemetry is persisted.
        self.persist = rhs.persist;
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
            value: Some(Value::Set(0.0)),
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
    pub fn name<S>(mut self, name: S) -> SoftTelemetry
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
    pub fn value(mut self, val: f64) -> SoftTelemetry {
        self.thawed_value = None;
        self.initial_value = Some(val);
        self
    }

    /// Set the kind of Telemetry aggregation
    ///
    /// Telemetry provide different views into the stored data. The kind
    /// controls, well, what kind of view is going to be used.
    pub fn kind(mut self, aggr: AggregationMethod) -> SoftTelemetry {
        self.kind = Some(aggr);
        self
    }

    /// Set the error for quantile calculation
    ///
    /// This is only necessary if the kind has been set to
    /// AggregationMethod::Summarize. It is an error to set this if the
    /// aggregation method is not as previously specified.
    pub fn error(mut self, error: f64) -> SoftTelemetry {
        self.error = Some(error);
        self
    }

    /// Set the bounds for histogram calculation
    ///
    /// This is only necessary if the kind has been set to
    /// AggregationMethod::Histogram. It is an error to set this if the
    /// aggregation method is not as previously specified.
    pub fn bounds(mut self, mut bounds: Vec<f64>) -> SoftTelemetry {
        bounds.sort_by(|a, b| a.partial_cmp(b).unwrap());
        self.bounds = Some(bounds);
        self
    }

    /// Set the timestamp of the Telemetry
    ///
    /// This is the instant of time in seconds that the Telemetry is considered
    /// to have happened.
    pub fn timestamp(mut self, ts: i64) -> SoftTelemetry {
        self.timestamp = Some(ts);
        self
    }

    /// Set the tags of the Telemetry
    ///
    /// These are the tags associated with the Telemetry and help us determine
    /// origin of report etc, depending on what the user has configured.
    pub fn tags(mut self, tags: sync::Arc<TagMap>) -> SoftTelemetry {
        self.tags = Some(tags);
        self
    }

    /// Set the persist of the Telemetry
    ///
    /// This flag determines if the Telemetry persists across time bins. This is
    /// a flag for aggregation implementation and may be ignored. If this is not
    /// specified the Telemetry is considered to not persist.
    pub fn persist(mut self, persist: bool) -> SoftTelemetry {
        self.persist = Some(persist);
        self
    }

    pub fn harden(self) -> Result<Telemetry, Error> {
        if self.initial_value.is_some() && self.thawed_value.is_some() {
            return Err(Error::CannotHaveTwoValues);
        }
        if self.initial_value.is_none() && self.thawed_value.is_none() {
            return Err(Error::NoValue);
        }
        let name = if let Some(name) = self.name {
            name
        } else {
            return Err(Error::NoName);
        };
        let kind = if let Some(kind) = self.kind {
            kind
        } else {
            AggregationMethod::Set
        };
        let timestamp = if let Some(timestamp) = self.timestamp {
            timestamp
        } else {
            time::now()
        };
        let tags = if let Some(tags) = self.tags {
            tags
        } else {
            sync::Arc::new(TagMap::default())
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
                    0.001
                };
                let value = match (self.initial_value, self.thawed_value) {
                    (Some(iv), None) => {
                        let mut ckms = CKMS::new(error);
                        ckms.insert(iv);
                        Value::Quantiles(ckms)
                    }
                    (None, Some(tv)) => tv,
                    _ => unreachable!(),
                };
                Ok(Telemetry {
                    name: name,
                    value: Some(value),
                    persist: persist,
                    tags: tags,
                    timestamp: timestamp,
                })
            }
            AggregationMethod::Histogram => {
                if self.error.is_some() {
                    return Err(Error::CannotSetError);
                }
                let value = match (self.initial_value, self.thawed_value) {
                    (Some(iv), None) => {
                        let bounds = if let Some(bounds) = self.bounds {
                            bounds
                        } else {
                            vec![1.0, 10.0, 100.0, 1000.0]
                        };
                        let mut histo = Histogram::new(bounds).unwrap();
                        histo.insert(iv);
                        Value::Histogram(histo)
                    }
                    (None, Some(tv)) => tv,
                    _ => unreachable!(),
                };
                Ok(Telemetry {
                    name: name,
                    value: Some(value),
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
                let value = match (self.initial_value, self.thawed_value) {
                    (Some(iv), None) => Value::Set(iv),
                    (None, Some(tv)) => tv,
                    _ => unreachable!(),
                };
                Ok(Telemetry {
                    name: name,
                    value: Some(value),
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
                let value = match (self.initial_value, self.thawed_value) {
                    (Some(iv), None) => Value::Sum(iv),
                    (None, Some(tv)) => tv,
                    _ => unreachable!(),
                };
                Ok(Telemetry {
                    name: name,
                    value: Some(value),
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
    NoInitialValue,
    NoName,
    NoValue,
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
    /// let m = Telemetry::new().name("foo").value(1.1).harden().unwrap();
    ///
    /// assert_eq!(m.kind(), AggregationMethod::Set);
    /// assert_eq!(m.name, "foo");
    /// assert_eq!(m.set(), Some(1.1));
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
            thawed_value: Some(self.value.unwrap()),
            kind: Some(kind),
            error: None,
            bounds: None,
            timestamp: Some(self.timestamp),
            tags: Some(self.tags),
            persist: Some(self.persist),
        }
    }

    pub fn kind(&self) -> AggregationMethod {
        if let Some(ref v) = self.value {
            v.kind()
        } else {
            unreachable!()
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
    /// let mut m = Telemetry::new().name("foo").value(1.1).harden().unwrap();
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
    /// let mut m = Telemetry::new().name("foo").value(1.1).harden().unwrap();
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
    /// let mut m = Telemetry::new().name("foo").value(1.1).harden().unwrap();
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
            Some(Value::Set(_)) => {
                self.value = Some(Value::Set(value));
            }
            Some(Value::Sum(x)) => {
                self.value = Some(Value::Sum(x + value));
            }
            Some(Value::Histogram(ref mut histo)) => {
                histo.insert(value);
            }
            Some(Value::Quantiles(ref mut ckms)) => {
                ckms.insert(value);
            }
            None => unreachable!(),
        }
        self
    }

    pub fn sum(&self) -> Option<f64> {
        if let Some(ref v) = self.value {
            v.sum()
        } else {
            None
        }
    }

    pub fn set(&self) -> Option<f64> {
        if let Some(ref v) = self.value {
            v.set()
        } else {
            None
        }
    }

    pub fn query(&self, prcnt: f64) -> Option<f64> {
        if let Some(ref v) = self.value {
            v.query(prcnt)
        } else {
            None
        }
    }

    pub fn bins(&self) -> Option<Iter<f64>> {
        if let Some(ref v) = self.value {
            v.bins()
        } else {
            None
        }
    }

    /// Sum of all samples inserted into this Telemetry
    pub fn samples_sum(&self) -> f64 {
        unimplemented!();
    }

    pub fn count(&self) -> usize {
        if let Some(ref v) = self.value {
            v.count()
        } else {
            0
        }
    }

    pub fn mean(&self) -> f64 {
        if let Some(ref v) = self.value {
            v.mean()
        } else {
            0.0
        }
    }

    // TODO this function should be removed entirely in favor of the known-type
    // functions: set, sum, etc etc
    #[cfg(test)]
    pub fn value(&self) -> Option<f64> {
        match self.value {
            Some(Value::Set(x)) => Some(x),
            Some(Value::Sum(x)) => Some(x),
            Some(Value::Quantiles(ref ckms)) => ckms.query(1.0).map(|x| x.1),
            Some(Value::Histogram(ref histo)) => histo.sum(),
            None => unreachable!(),
        }
    }

    pub fn samples(&self) -> Vec<f64> {
        match self.value {
            Some(Value::Set(x)) => vec![x],
            Some(Value::Sum(x)) => vec![x],
            Some(Value::Quantiles(ref ckms)) => ckms.clone().into_vec(),
            Some(Value::Histogram(ref histo)) => {
                histo.clone().into_vec().iter().map(|x| x.1 as f64).collect()
            }
            None => unreachable!(),
        }
    }

    pub fn within(&self, span: i64, other: &Telemetry) -> cmp::Ordering {
        match self.name.partial_cmp(&other.name) {
            Some(cmp::Ordering::Equal) => match cmp(&self.tags, &other.tags) {
                Some(cmp::Ordering::Equal) => {
                    let lhs_bin = self.timestamp / span;
                    let rhs_bin = other.timestamp / span;
                    lhs_bin.cmp(&rhs_bin)
                }
                other => other.unwrap(),
            },
            other => other.unwrap(),
        }
    }

    pub fn is_zeroed(&self) -> bool {
        match self.value {
            Some(Value::Set(x)) => x == 0.0,
            Some(Value::Sum(x)) => x == 0.0,
            Some(Value::Histogram(ref histo)) => histo.count() == 0,
            Some(Value::Quantiles(ref ckms)) => ckms.count() == 0,
            None => unreachable!(),
        }
    }

    pub fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.name.hash(&mut hasher);
        self.tags.hash(&mut hasher);
        self.kind().hash(&mut hasher);
        hasher.finish()
    }

    pub fn name_tag_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.name.hash(&mut hasher);
        self.tags.hash(&mut hasher);
        hasher.finish()
    }

    pub fn is_set(&self) -> bool {
        self.kind() == AggregationMethod::Set
    }

    pub fn is_histogram(&self) -> bool {
        self.kind() == AggregationMethod::Histogram
    }

    pub fn aggregation(&self) -> AggregationMethod {
        self.kind()
    }

    pub fn is_sum(&self) -> bool {
        self.kind() == AggregationMethod::Sum
    }

    pub fn is_summarize(&self) -> bool {
        self.kind() == AggregationMethod::Summarize
    }

    #[cfg(test)]
    pub fn priv_value(&self) -> Value {
        self.value.clone().unwrap()
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
    /// let m = Telemetry::new().name("foo").value(1.1).harden().unwrap().
    /// timestamp(10101);
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
        let mc = Telemetry::new()
            .name("l6")
            .value(0.7913855)
            .kind(AggregationMethod::Sum)
            .harden()
            .unwrap()
            .timestamp(47);
        let mg = Telemetry::new()
            .name("l6")
            .value(0.9683)
            .kind(AggregationMethod::Set)
            .harden()
            .unwrap()
            .timestamp(47);

        assert_eq!(Some(cmp::Ordering::Equal), mc.partial_cmp(&mg));
    }

    #[test]
    fn partial_ord_distinct() {
        let mc = Telemetry::new()
            .name("l6")
            .value(0.7913855)
            .kind(AggregationMethod::Sum)
            .harden()
            .unwrap()
            .timestamp(7);
        let mg = Telemetry::new()
            .name("l6")
            .value(0.9683)
            .kind(AggregationMethod::Set)
            .harden()
            .unwrap()
            .timestamp(47);

        assert_eq!(Some(cmp::Ordering::Less), mc.partial_cmp(&mg));
    }

    #[test]
    fn partial_ord_gauges() {
        let mdg = Telemetry::new()
            .name("l6")
            .value(0.7913855)
            .kind(AggregationMethod::Set)
            .persist(true)
            .harden()
            .unwrap()
            .timestamp(47);
        let mg = Telemetry::new()
            .name("l6")
            .value(0.9683)
            .kind(AggregationMethod::Set)
            .harden()
            .unwrap()
            .timestamp(47);

        assert_eq!(Some(cmp::Ordering::Equal), mg.partial_cmp(&mdg));
        assert_eq!(Some(cmp::Ordering::Equal), mdg.partial_cmp(&mg));
    }

    impl Arbitrary for AggregationMethod {
        fn arbitrary<G>(g: &mut G) -> Self
        where
            G: Gen,
        {
            let i: usize = g.gen_range(0, 4);
            match i {
                0 => AggregationMethod::Sum,
                1 => AggregationMethod::Set,
                2 => AggregationMethod::Summarize,
                _ => AggregationMethod::Histogram,
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
            let mut mb = Telemetry::new().name(name).value(val).timestamp(time);
            mb = match kind {
                AggregationMethod::Set => mb.kind(AggregationMethod::Set),
                AggregationMethod::Sum => mb.kind(AggregationMethod::Sum),
                AggregationMethod::Summarize => mb.kind(AggregationMethod::Summarize),
                AggregationMethod::Histogram => mb.kind(AggregationMethod::Histogram)
                    .bounds(vec![1.0, 10.0, 100.0, 1000.0]),
            };
            mb = if persist { mb.persist(true) } else { mb };
            mb.harden().unwrap()
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
            if lhs.kind() != rhs.aggregation() {
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
            let mut mlhs =
                Telemetry::new().name("foo").value(lhs).kind(kind).harden().unwrap();
            let mrhs =
                Telemetry::new().name("foo").value(rhs).kind(kind).harden().unwrap();
            let old_mlhs = mlhs.clone();
            let old_mrhs = mrhs.clone();
            mlhs += mrhs;
            if let Some(val) = mlhs.value() {
                let expected = match kind {
                    AggregationMethod::Set => rhs,
                    AggregationMethod::Sum => lhs + rhs,
                    AggregationMethod::Summarize => lhs.max(rhs),
                    AggregationMethod::Histogram => lhs + rhs,
                };
                // println!("VAL: {:?} | EXPECTED: {:?}", val, expected);
                match val.partial_cmp(&expected) {
                    Some(cmp::Ordering::Equal) => return TestResult::passed(),
                    _ => {
                        println!(
                            "\n\nLHS: {:?}\nRHS: {:?}\nMLHS: {:?}\nMRHS: {:?}\nRES: {:?}\nEXPECTED: {:?}\nVAL: {:?}",
                            lhs,
                            rhs,
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
        let m = Telemetry::new()
            .name("timer")
            .value(-1.0)
            .kind(AggregationMethod::Summarize)
            .harden()
            .unwrap();

        assert_eq!(m.kind(), AggregationMethod::Summarize);
        assert_eq!(m.query(1.0), Some(-1.0));
        assert_eq!(m.name, "timer");
    }

    #[test]
    fn test_postive_delta_gauge() {
        let m = Telemetry::new()
            .name("dgauge")
            .value(1.0)
            .persist(true)
            .kind(AggregationMethod::Set)
            .harden()
            .unwrap();

        assert_eq!(m.kind(), AggregationMethod::Set);
        assert_eq!(m.value(), Some(1.0));
        assert_eq!(m.name, "dgauge");
    }
}
