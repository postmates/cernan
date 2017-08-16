//! Buckets are the primary internal storage type.
//!
//! Each bucket contains a set of hashmaps containing
//! each set of metrics received by clients.

use metric::Telemetry;
use std::iter::Iterator;
use std::ops::IndexMut;
use time;

/// Buckets stores all metrics until they are flushed.
#[derive(Clone)]
pub struct Buckets {
    keys: Vec<u64>,
    values: Vec<Vec<Telemetry>>,
    count: u64,
    bin_width: i64,
}

impl Default for Buckets {
    /// Create a default Buckets
    ///
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::buckets::Buckets;
    ///
    /// let bucket = Buckets::default();
    /// assert_eq!(0, bucket.len());
    /// ```
    fn default() -> Buckets {
        Buckets {
            keys: Default::default(),
            values: Default::default(),
            count: 0,
            bin_width: 1,
        }
    }
}

/// Iteration struct for the bucket. Created by `Buckets.iter()`.
pub struct Iter<'a> {
    buckets: &'a Buckets,
    key_index: usize,
    value_index: Option<usize>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a Telemetry;

    fn next(&mut self) -> Option<&'a Telemetry> {
        while self.key_index < self.buckets.keys.len() {
            if let Some(value_index) = self.value_index {
                if value_index < self.buckets.values[self.key_index].len() {
                    let v = &self.buckets.values[self.key_index][value_index];
                    self.value_index = Some(value_index + 1);
                    return Some(v);
                } else {
                    self.value_index = None;
                    self.key_index += 1;
                }
            } else if !self.buckets.values[self.key_index].is_empty() {
                let v = &self.buckets.values[self.key_index][0];
                self.value_index = Some(1);
                return Some(v);
            } else {
                return None;
            }
        }
        None
    }
}

impl Buckets {
    /// Create a new bucket. The `bin_width` controls the aggregation width of
    /// the bucket. If `bin_width` is set to N then two `Telemetry` will be
    /// considered as happening at the 'same' time if their timestamps are
    /// within N seconds of one another.
    pub fn new(bin_width: i64) -> Buckets {
        let mut b = Buckets::default();
        b.bin_width = bin_width;
        b
    }

    /// Resets appropriate aggregates
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate cernan;
    ///
    /// let metric =
    /// cernan::metric::Telemetry::new().name("foo").value(1.0).kind(cernan::
    /// metric::AggregationMethod::Sum).harden().unwrap();
    /// let mut buckets = cernan::buckets::Buckets::default();
    ///
    /// assert_eq!(true, buckets.is_empty());
    ///
    /// buckets.add(metric);
    /// assert_eq!(false, buckets.is_empty());
    /// //buckets.reset();
    /// //assert_eq!(true, buckets.is_empty());
    /// ```
    pub fn reset(&mut self) {
        self.count = 0;
        for v in &mut self.values {
            if v.is_empty() {
                continue;
            }
            let len = v.len();
            if v[len - 1].persist {
                v.swap(0, len - 1);
                v.truncate(1);
                v[0].timestamp = time::now();
                self.count = self.count.saturating_add(1);
            } else {
                v.clear()
            }
        }
    }

    /// Determine if a bucket has no stored points.
    ///
    /// # Examples
    /// ```
    /// extern crate cernan;
    ///
    /// let bucket = cernan::buckets::Buckets::default();
    /// assert!(bucket.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Adds a metric to the bucket storage.
    ///
    /// # Examples
    /// ```
    /// extern crate cernan;
    ///
    /// let metric =
    /// cernan::metric::Telemetry::new().name("foo").value(1.0).kind(cernan::
    /// metric::AggregationMethod::Sum).harden().unwrap();
    /// let mut bucket = cernan::buckets::Buckets::default();
    /// bucket.add(metric);
    /// ```
    pub fn add(&mut self, value: Telemetry) {
        let hsh = match self.keys.binary_search_by(
            |probe| probe.partial_cmp(&value.name_tag_hash()).unwrap(),
        ) {
            Ok(hsh_idx) => self.values.index_mut(hsh_idx),
            Err(hsh_idx) => {
                self.keys.insert(hsh_idx, value.name_tag_hash());
                self.values.insert(hsh_idx, Vec::with_capacity(128));
                self.values.index_mut(hsh_idx)
            }
        };
        let bin_width = self.bin_width;

        match hsh.binary_search_by(|probe| probe.within(bin_width, &value)) {
            Ok(idx) => hsh[idx] += value,
            Err(idx) => {
                self.count = self.count.saturating_add(1);
                if value.persist && idx > 0 {
                    let mut cur: Telemetry = hsh[idx - 1]
                        .clone()
                        .timestamp(value.timestamp)
                        .thaw()
                        .persist(true)
                        .harden()
                        .unwrap();
                    cur += value;
                    hsh.insert(idx, cur)
                } else {
                    hsh.insert(idx, value)
                }
            }
        }
    }

    #[cfg(test)]
    pub fn get(&self, key: u64) -> Option<&[Telemetry]> {
        use std::ops::Index;
        match self.keys.iter().position(|k| *k == key) {
            Some(idx) => Some(self.values.index(idx)),
            None => None,
        }
    }

    /// Determine the number of `Telemetry` stored in the bucket.
    ///
    /// This function returns the total number of `Telemetry` stored in the
    /// bucket. This is distinct from the value returned by `Buckets::len` which
    /// measures the total number of `Telemetry` _names_ received by the bucket.
    pub fn count(&self) -> usize {
        self.count as usize
    }

    /// Determine the number of `Telemetry` names stored in the bucket.
    ///
    /// This function returns the total number of `Telemetry` names stored in
    /// the bucket. This is distinct from the value returned by `Buckets::count`
    /// which measures the total number of `Telemetry` received by the bucket.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Create an iterator for the bucket. See documentation on `buckets::Iter`
    /// for more details.
    pub fn iter(&mut self) -> Iter {
        Iter {
            buckets: self,
            key_index: 0,
            value_index: None,
        }
    }
}

// Tests
//
#[cfg(test)]
mod test {
    use super::*;
    use chrono::{TimeZone, Utc};
    use metric::AggregationMethod;
    use metric::Telemetry;
    use quickcheck::{QuickCheck, TestResult};
    use std::cmp::Ordering;
    use std::collections::{HashMap, HashSet};
    use std::ops::Add;

    fn within(width: i64, lhs: i64, rhs: i64) -> bool {
        (lhs / width) == (rhs / width)
    }

    #[test]
    fn fitness_for_statsd_gauge() {
        // This test is intended to demonstrate that buckets can act as an
        // etsy/statsd compatible aggregation backend. In particular, we need to
        // demonstrate that:
        //
        //  * it is possible to switch a SET|SUM from persist to ephemeral
        //  * a SET|SUM with persist will survive across resets
        //  * an ephemeral SET|SUM will have its value inherited by a persist SET|SUM
        //  * an ephemeral SET|SUM will not inherit the value of a persist SET|SUM
        let bin_width = 1;
        let m0 = Telemetry::new()
            .name("lO")
            .value(1.0)
            .timestamp(0)
            .kind(AggregationMethod::Set)
            .persist(false)
            .harden()
            .unwrap();
        let m1 = Telemetry::new()
            .name("lO")
            .value(2.0)
            .timestamp(1)
            .kind(AggregationMethod::Sum)
            .persist(true)
            .harden()
            .unwrap();
        let m2 = Telemetry::new()
            .name("lO")
            .value(0.0)
            .timestamp(1)
            .kind(AggregationMethod::Set)
            .persist(false)
            .harden()
            .unwrap();

        let mut bkt = Buckets::new(bin_width);
        let id = m0.name_tag_hash();

        bkt.add(m0);
        bkt.add(m1);

        //  * it is possible to switch a SET|SUM from persist to ephemeral
        //  * an ephemeral SET|SUM will have its value inherited by a persist SET|SUM
        let aggrs = bkt.clone();
        let res = aggrs.get(id).unwrap();
        assert_eq!(0, res[0].timestamp);
        assert_eq!(Some(1.0), res[0].set());
        assert_eq!(1, res[1].timestamp);
        assert_eq!(Some(3.0), res[1].sum());

        //  * a SET|SUM with persist will survive across resets
        bkt.reset();
        assert!(!bkt.is_empty());

        //  * an ephemeral SET|SUM will not inherit the value of a persist SET|SUM
        let aggrs = bkt.clone();
        let res = aggrs.get(id).unwrap();
        bkt.add(m2.timestamp(res[0].timestamp));
        let aggrs = bkt.clone();
        let res = aggrs.get(id).unwrap();
        assert_eq!(Some(0.0), res[0].set());

        bkt.reset();

        assert!(bkt.is_empty());
    }

    #[test]
    fn raw_test_variable() {
        let bin_width = 66;
        let m0 = Telemetry::new()
            .name("lO")
            .value(0.807)
            .timestamp(18)
            .kind(AggregationMethod::Set)
            .harden()
            .unwrap();
        let m4 = Telemetry::new()
            .name("lO")
            .value(0.361)
            .timestamp(75)
            .kind(AggregationMethod::Set)
            .harden()
            .unwrap();
        let m7 = Telemetry::new()
            .name("lO")
            .value(0.291)
            .timestamp(42)
            .kind(AggregationMethod::Set)
            .harden()
            .unwrap();

        let mut bkt = Buckets::new(bin_width);
        let id = m0.name_tag_hash();

        bkt.add(m0);
        bkt.add(m4);
        bkt.add(m7);

        let raws = bkt.get(id).unwrap();
        println!("RAWS: {:?}", raws);
        let ref res = raws[0];
        assert_eq!(18, res.timestamp);
        assert_eq!(Some(0.291), res.value());
    }

    #[test]
    fn test_gauge_small_bin_width() {
        let bin_width = 1;
        let m0 = Telemetry::new()
            .name("lO")
            .value(3.211)
            .timestamp(645181811)
            .kind(AggregationMethod::Set)
            .harden()
            .unwrap();
        let m1 = Telemetry::new()
            .name("lO")
            .value(4.322)
            .timestamp(645181812)
            .kind(AggregationMethod::Set)
            .harden()
            .unwrap();
        let m2 = Telemetry::new()
            .name("lO")
            .value(5.433)
            .timestamp(645181813)
            .kind(AggregationMethod::Set)
            .harden()
            .unwrap();

        let mut bkt = Buckets::new(bin_width);
        let id = m0.name_tag_hash();

        bkt.add(m0);
        bkt.add(m1);
        bkt.add(m2);

        let v = bkt.get(id).unwrap();
        assert_eq!(3, v.len());
    }

    #[test]
    fn variable_size_bins() {
        fn inner(bin_width: u16, ms: Vec<Telemetry>) -> TestResult {
            let bin_width: i64 = bin_width as i64;
            if bin_width == 0 {
                return TestResult::discard();
            }

            let mut bucket = Buckets::new(bin_width);
            for m in ms.clone() {
                bucket.add(m);
            }

            let mut mp: Vec<Telemetry> = Vec::new();
            let fill_ms = ms.clone();
            for m in fill_ms {
                match mp.binary_search_by(|probe| probe.within(bin_width, &m)) {
                    Ok(idx) => mp[idx] += m,
                    Err(idx) => {
                        if m.persist && idx > 0 && mp[idx - 1].name() == m.name() {
                            let mut cur = mp[idx - 1]
                                .clone()
                                .timestamp(m.timestamp)
                                .thaw()
                                .persist(true)
                                .harden()
                                .unwrap();
                            cur += m;
                            mp.insert(idx, cur)
                        } else {
                            mp.insert(idx, m)
                        }
                    }
                }
            }

            for v in bucket.iter() {
                let ref kind = v.kind();
                let time = v.timestamp;
                let mut found_one = false;
                for m in &mp {
                    if (m.name() == v.name()) && (&m.kind() == kind) &&
                        within(bin_width, m.timestamp, time)
                    {
                        assert_eq!(Ordering::Equal, m.within(bin_width, &v));
                        found_one = true;
                        debug_assert!(
                            v.value() == m.value(),
                            "{:?} != {:?} |::|::| MODEL: {:?} |::|::|
    SUT: {:?}",
                            v.value(),
                            m.value(),
                            mp,
                            ms
                        );
                    }
                }
                debug_assert!(
                    found_one,
                    "DID NOT FIND ONE FOR {:?} |::|::| MODEL: {:?}
    |::|::| SUT: \
                                   {:?}",
                    v,
                    mp,
                    ms
                );
            }
            TestResult::passed()
        }
        QuickCheck::new().quickcheck(inner as fn(u16, Vec<Telemetry>) -> TestResult);
    }

    #[test]
    fn test_add_gauge_metric_distinct_tags() {
        let mut buckets = Buckets::default();
        let m0 = Telemetry::new()
            .name("some.metric")
            .value(1.0)
            .kind(AggregationMethod::Set)
            .timestamp(10)
            .harden()
            .unwrap()
            .overlay_tag("foo", "bar");
        let m1 = Telemetry::new()
            .name("some.metric")
            .value(1.0)
            .kind(AggregationMethod::Set)
            .timestamp(10)
            .harden()
            .unwrap()
            .overlay_tag("foo", "bingo");

        buckets.add(m0.clone());
        buckets.add(m1.clone());

        assert_eq!(2, buckets.count());
    }

    #[test]
    fn test_add_counter_metric() {
        let mut buckets = Buckets::default();
        let metric = Telemetry::new()
            .name("some.metric")
            .value(1.0)
            .kind(AggregationMethod::Sum)
            .harden()
            .unwrap();
        buckets.add(metric.clone());

        let id = metric.name_tag_hash();

        assert_eq!(Some(1.0), buckets.get(id).unwrap()[0].value());

        // Increment counter
        buckets.add(metric);
        assert_eq!(Some(2.0), buckets.get(id).unwrap()[0].value());
        assert_eq!(1, buckets.count());
    }

    #[test]
    fn test_reset_add_counter_metric() {
        let mut buckets = Buckets::default();
        let m0 = Telemetry::new()
            .name("some.metric")
            .value(1.0)
            .kind(AggregationMethod::Sum)
            .harden()
            .unwrap()
            .timestamp(101);
        let id = m0.name_tag_hash();
        let m1 = m0.clone();

        buckets.add(m0);

        assert_eq!(Some(1.0), buckets.get(id).unwrap()[0].value());

        // Increment counter
        buckets.add(m1);
        assert_eq!(Some(2.0), buckets.get(id).unwrap()[0].value());
        assert_eq!(1, buckets.count());

        buckets.reset();
        assert_eq!(true, buckets.is_empty());
    }

    #[test]
    fn test_true_histogram() {
        let mut buckets = Buckets::default();

        let dt_0 = Utc.ymd(1996, 10, 7).and_hms_milli(10, 11, 11, 0).timestamp();
        let dt_1 = Utc.ymd(1996, 10, 7).and_hms_milli(10, 11, 12, 0).timestamp();
        let dt_2 = Utc.ymd(1996, 10, 7).and_hms_milli(10, 11, 13, 0).timestamp();

        let m0 = Telemetry::new()
            .name("some.metric")
            .value(1.0)
            .kind(AggregationMethod::Summarize)
            .harden()
            .unwrap()
            .timestamp(dt_0);
        let m1 = Telemetry::new()
            .name("some.metric")
            .value(2.0)
            .kind(AggregationMethod::Summarize)
            .harden()
            .unwrap()
            .timestamp(dt_1);
        let m2 = Telemetry::new()
            .name("some.metric")
            .value(3.0)
            .kind(AggregationMethod::Summarize)
            .harden()
            .unwrap()
            .timestamp(dt_2);

        let id = m0.name_tag_hash();

        buckets.add(m0.clone());
        buckets.add(m1.clone());
        buckets.add(m2.clone());

        let hists = buckets;
        assert!(hists.get(id).is_some());
        let ref hist = hists.get(id).unwrap();

        assert_eq!(3, hist.len());

        let ref ckms0 = hist[0];
        assert_eq!(1, ckms0.count());
        assert_eq!(Some(1.0), ckms0.query(1.0));
        assert_eq!(Some(1.0), ckms0.query(0.0));

        let ref ckms1 = hist[1];
        assert_eq!(1, ckms1.count());
        assert_eq!(Some(2.0), ckms1.query(1.0));
        assert_eq!(Some(2.0), ckms1.query(0.0));

        let ref ckms2 = hist[2];
        assert_eq!(1, ckms2.count());
        assert_eq!(Some(3.0), ckms2.query(1.0));
        assert_eq!(Some(3.0), ckms2.query(0.0));
    }

    #[test]
    fn test_add_histogram_metric_reset() {
        let mut buckets = Buckets::default();
        let metric = Telemetry::new()
            .name("some.metric")
            .value(1.0)
            .kind(AggregationMethod::Summarize)
            .harden()
            .unwrap();
        buckets.add(metric.clone());

        buckets.reset();
        assert!(buckets.is_empty());
    }

    #[test]
    fn test_add_timer_metric_reset() {
        let mut buckets = Buckets::default();
        let metric = Telemetry::new()
            .name("some.metric")
            .value(1.0)
            .kind(AggregationMethod::Summarize)
            .harden()
            .unwrap();
        buckets.add(metric.clone());

        buckets.reset();
        assert!(buckets.is_empty());
    }

    #[test]
    fn test_add_gauge_metric() {
        let mut buckets = Buckets::default();
        let metric = Telemetry::new()
            .name("some.metric")
            .value(11.5)
            .kind(AggregationMethod::Set)
            .harden()
            .unwrap();
        let id = metric.name_tag_hash();
        buckets.add(metric);

        assert_eq!(Some(11.5), buckets.get(id).unwrap()[0].value());
        assert_eq!(1, buckets.count());
    }

    #[test]
    fn test_add_delta_gauge_metric() {
        // A gauge that transitions from gauge to delta gauge does _not_ retain
        // its previous value. Other statsd aggregations require that you reset
        // the value to zero before moving to a delta gauge. Cernan does not
        // require this explicit reset, only the +/- on the metric value.
        let mut buckets = Buckets::default();
        let metric = Telemetry::new()
            .name("some.metric")
            .value(100.0)
            .timestamp(0)
            .kind(AggregationMethod::Set)
            .harden()
            .unwrap();
        let id = metric.name_tag_hash();
        buckets.add(metric);
        let delta_metric = Telemetry::new()
            .name("some.metric")
            .value(-11.5)
            .timestamp(0)
            .kind(AggregationMethod::Sum)
            .persist(true)
            .harden()
            .unwrap();
        buckets.add(delta_metric);

        assert_eq!(Some(88.5), buckets.get(id).unwrap()[0].value());
        assert_eq!(1, buckets.count());
        buckets.reset();
        assert_eq!(1, buckets.count());
    }

    #[test]
    fn test_reset_add_delta_gauge_metric() {
        let mut buckets = Buckets::default();
        let metric = Telemetry::new()
            .name("some.metric")
            .value(100.0)
            .kind(AggregationMethod::Set)
            .harden()
            .unwrap();
        let id = metric.name_tag_hash();
        buckets.add(metric);
        let delta_metric = Telemetry::new()
            .name("some.metric")
            .value(-11.5)
            .kind(AggregationMethod::Sum)
            .persist(true)
            .harden()
            .unwrap();
        buckets.add(delta_metric);
        let reset_metric = Telemetry::new()
            .name("some.metric")
            .value(2007.3)
            .kind(AggregationMethod::Set)
            .harden()
            .unwrap();
        buckets.add(reset_metric);

        assert_eq!(Some(2007.3), buckets.get(id).unwrap()[0].value());
        assert_eq!(1, buckets.count());
    }

    #[test]
    fn test_add_timer_metric() {
        let mut buckets = Buckets::default();
        let metric = Telemetry::new()
            .name("some.metric")
            .value(11.5)
            .kind(AggregationMethod::Summarize)
            .harden()
            .unwrap();
        let id = metric.name_tag_hash();
        buckets.add(metric);
        assert_eq!(Some(11.5), buckets.get(id).expect("hwhap")[0].query(0.0));

        let metric_two = Telemetry::new()
            .name("some.metric")
            .value(99.5)
            .kind(AggregationMethod::Summarize)
            .harden()
            .unwrap();
        buckets.add(metric_two);

        let metric_three = Telemetry::new()
            .name("other.metric")
            .value(811.5)
            .kind(AggregationMethod::Summarize)
            .harden()
            .unwrap();
        let id_3 = metric_three.name_tag_hash();
        buckets.add(metric_three);
        assert_eq!(Some(811.5), buckets.get(id_3).expect("hwhap")[0].query(0.0));
    }

    #[test]
    fn test_raws_store_one_point_per_second() {
        let mut buckets = Buckets::default();
        let dt_0 = Utc.ymd(2016, 9, 13).and_hms_milli(11, 30, 0, 0).timestamp();
        let dt_1 = Utc.ymd(2016, 9, 13).and_hms_milli(11, 30, 1, 0).timestamp();

        let m0 = Telemetry::new()
            .name("some.metric")
            .value(1.0)
            .timestamp(dt_0)
            .kind(AggregationMethod::Set)
            .harden()
            .unwrap();
        let id = m0.name_tag_hash();
        buckets.add(m0);
        buckets.add(
            Telemetry::new()
                .name("some.metric")
                .value(2.0)
                .timestamp(dt_0)
                .kind(AggregationMethod::Set)
                .harden()
                .unwrap(),
        );
        buckets.add(
            Telemetry::new()
                .name("some.metric")
                .value(3.0)
                .timestamp(dt_0)
                .kind(AggregationMethod::Set)
                .harden()
                .unwrap(),
        );
        buckets.add(
            Telemetry::new()
                .name("some.metric")
                .value(4.0)
                .timestamp(dt_1)
                .kind(AggregationMethod::Set)
                .harden()
                .unwrap(),
        );

        let metrics = buckets.get(id).unwrap();
        assert_eq!(2, metrics.len());

        assert_eq!(dt_0, metrics[0].timestamp);
        assert_eq!(Some(3.0), metrics[0].value());

        assert_eq!(dt_1, metrics[1].timestamp);
        assert_eq!(Some(4.0), metrics[1].value());
    }

    #[test]
    fn unique_names_preserved() {
        fn qos_ret(ms: Vec<Telemetry>) -> TestResult {
            let mut bucket = Buckets::default();

            for m in ms.clone() {
                bucket.add(m);
            }

            let cnts: HashSet<String> =
                ms.iter().fold(HashSet::default(), |mut acc, ref m| {
                    acc.insert(m.name().as_ref().clone());
                    acc
                });
            let b_cnts: HashSet<String> =
                bucket.iter().fold(HashSet::default(), |mut acc, v| {
                    acc.insert(v.name().as_ref().clone());
                    acc
                });
            assert_eq!(cnts, b_cnts);

            TestResult::passed()
        }
        QuickCheck::new().quickcheck(qos_ret as fn(Vec<Telemetry>) -> TestResult);
    }

    #[test]
    fn correct_bin_count() {
        fn inner(ms: Vec<Telemetry>) -> TestResult {
            let mut bucket = Buckets::new(1);

            for m in ms.clone() {
                bucket.add(m);
            }

            let mut coll: HashMap<String, HashSet<i64>> = HashMap::new();
            for m in ms {
                let name = m.name().as_ref().to_string();
                let time = m.timestamp;

                let entry = coll.entry(name).or_insert(HashSet::new());
                (*entry).insert(time);
            }

            let mut expected_count = 0;

            for (_, val) in coll.iter() {
                expected_count += val.len()
            }
            assert_eq!(expected_count, bucket.count());
            TestResult::passed()
        }
        QuickCheck::new().quickcheck(inner as fn(Vec<Telemetry>) -> TestResult);
    }

    #[test]
    fn test_reset_clears_space() {
        fn qos_ret(ms: Vec<Telemetry>) -> TestResult {
            let mut bucket = Buckets::default();

            for m in ms {
                bucket.add(m.thaw().persist(false).harden().unwrap());
            }
            bucket.reset();

            assert_eq!(0, bucket.count());
            TestResult::passed()
        }
        QuickCheck::new().quickcheck(qos_ret as fn(Vec<Telemetry>) -> TestResult);
    }

    #[test]
    fn test_reset_maintains_persist() {
        use std::collections::HashSet;
        fn inner(cycles: u8, mut ms: Vec<Telemetry>) -> TestResult {
            ms.sort_by(|x, y| x.timestamp.cmp(&y.timestamp));

            let mut bucket = Buckets::default();
            let mut persists = HashSet::new();

            for m in ms {
                bucket.add(m);
            }
            for t in bucket.iter() {
                if t.persist {
                    persists.insert(t.name_tag_hash());
                } else {
                    persists.remove(&t.name_tag_hash());
                }
            }

            for _ in 0..(cycles as usize) {
                bucket.reset();
                assert_eq!(persists.len(), bucket.count());
            }
            TestResult::passed()
        }
        QuickCheck::new().quickcheck(inner as fn(u8, Vec<Telemetry>) -> TestResult);
    }

    #[test]
    fn test_reset_clears_not_all_space_if_persistent() {
        fn qos_ret(ms: Vec<Telemetry>) -> TestResult {
            if ms.is_empty() {
                return TestResult::discard();
            }
            let mut bucket = Buckets::default();

            for m in ms {
                bucket.add(m.thaw().persist(true).harden().unwrap());
            }
            bucket.reset();

            assert!(!bucket.is_empty());
            TestResult::passed()
        }
        QuickCheck::new().quickcheck(qos_ret as fn(Vec<Telemetry>) -> TestResult);
    }

    #[test]
    fn test_gauge_insertion() {
        let mut buckets = Buckets::default();

        let m0 = Telemetry::new()
            .name("test.gauge_0")
            .value(1.0)
            .kind(AggregationMethod::Set)
            .harden()
            .unwrap();
        let m1 = Telemetry::new()
            .name("test.gauge_1")
            .value(1.0)
            .kind(AggregationMethod::Set)
            .harden()
            .unwrap();
        buckets.add(m0.clone());
        buckets.add(m1.clone());

        assert_eq!(m0, buckets.get(m0.name_tag_hash()).unwrap()[0]);
        assert_eq!(m1, buckets.get(m1.name_tag_hash()).unwrap()[0]);
    }

    #[test]
    fn test_counter_insertion() {
        let mut buckets = Buckets::default();

        let m0 = Telemetry::new()
            .name("test.counter_0")
            .value(1.0)
            .kind(AggregationMethod::Sum)
            .harden()
            .unwrap();
        let m1 = Telemetry::new()
            .name("test.counter_1")
            .value(1.0)
            .kind(AggregationMethod::Sum)
            .harden()
            .unwrap();
        buckets.add(m0.clone());
        buckets.add(m1.clone());

        assert_eq!(m0, buckets.get(m0.name_tag_hash()).unwrap()[0]);
        assert_eq!(m1, buckets.get(m1.name_tag_hash()).unwrap()[0]);
    }

    #[test]
    fn test_all_insertions() {
        use metric::Value;

        fn qos_ret(ms: Vec<Telemetry>) -> TestResult {
            let mut bucket = Buckets::default();

            for m in ms.clone() {
                bucket.add(m)
            }

            let mut cnts: HashMap<String, Vec<(i64, Value)>> = HashMap::default();
            for m in ms {
                let c = cnts.entry(m.name().as_ref().to_string()).or_insert(vec![]);
                match c.binary_search_by_key(&m.timestamp, |&(a, _)| a) {
                    Ok(idx) => {
                        let val = c[idx].1.clone();
                        c[idx].1 = val.add(m.priv_value());
                    }
                    Err(idx) => if m.persist && idx > 0 {
                        let mut val = c[idx - 1].clone().1;
                        val = val.add(m.priv_value());
                        c.insert(idx, (m.timestamp, val))
                    } else {
                        c.insert(idx, (m.timestamp, m.priv_value()))
                    },
                }
            }
            let len_cnts = cnts.values().fold(0, |acc, ref x| acc + x.len());

            assert_eq!(len_cnts, bucket.count());

            for val in bucket.iter() {
                if let Some(c_vs) = cnts.get(val.name().as_ref()) {
                    match c_vs.binary_search_by_key(
                        &val.timestamp,
                        |&(c_ts, _)| c_ts,
                    ) {
                        Ok(idx) => {
                            let c_v = &c_vs[idx];

                            let lhs = c_v.1.clone();
                            let rhs = val.priv_value();
                            assert!(lhs.is_same_kind(&rhs));
                            assert!((lhs.mean() - rhs.mean()).abs() < 0.0001);
                            assert_eq!(lhs.count(), rhs.count());
                            match lhs.kind() {
                                AggregationMethod::Set => assert!(
                                    (lhs.set().unwrap() - rhs.set().unwrap()).abs() <
                                        0.0001
                                ),
                                AggregationMethod::Sum => assert!(
                                    (lhs.sum().unwrap() - rhs.sum().unwrap()).abs() <
                                        0.0001
                                ),
                                AggregationMethod::Summarize => {
                                    for prcnt in &[0.5, 0.75, 0.99, 0.999] {
                                        assert!(
                                            (lhs.query(*prcnt).unwrap() -
                                                rhs.query(*prcnt).unwrap())
                                                .abs() <
                                                0.0001
                                        )
                                    }
                                }
                                AggregationMethod::Histogram => {
                                    let l_bins = lhs.into_vec();
                                    let r_bins = rhs.into_vec();
                                    assert_eq!(l_bins, r_bins);
                                }
                            }
                        }
                        Err(_) => return TestResult::failed(),
                    }

                } else {
                    return TestResult::failed();
                }
            }

            TestResult::passed()
        }
        QuickCheck::new()
            .tests(1000)
            .max_tests(10000)
            .quickcheck(qos_ret as fn(Vec<Telemetry>) -> TestResult);
    }
}
