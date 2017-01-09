//! Buckets are the primary internal storage type.
//!
//! Each bucket contains a set of hashmaps containing
//! each set of metrics received by clients.

use metric::Telemetry;
use seahash::SeaHasher;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::iter::Iterator;
use std::ops::{Index, IndexMut};
use time;

pub type HashMapFnv<K, V> = HashMap<K, V, BuildHasherDefault<SeaHasher>>;
pub type AggrMap = HashMapFnv<String, Vec<Telemetry>>;

/// Buckets stores all metrics until they are flushed.
#[derive(Clone)]
pub struct Buckets {
    keys: Vec<String>,
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

pub struct BucketsIterator<'a> {
    buckets: &'a Buckets,
    index: usize,
}

impl<'a> IntoIterator for &'a Buckets {
    type Item = &'a [Telemetry];
    type IntoIter = BucketsIterator<'a>;
    fn into_iter(self) -> Self::IntoIter {
        BucketsIterator {
            buckets: self,
            index: 0,
        }
    }
}

impl<'a> Iterator for BucketsIterator<'a> {
    type Item = &'a [Telemetry];
    fn next(&mut self) -> Option<&'a [Telemetry]> {
        let result = if self.index < self.buckets.keys.len() {
            let v = &self.buckets.values[self.index][..];
            Some(v)
        } else {
            None
        };
        self.index += 1;
        result
    }
}

impl Buckets {
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
    /// let metric = cernan::metric::Telemetry::new("foo", 1.0).aggr_sum();
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

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Adds a metric to the bucket storage.
    ///
    /// # Examples
    /// ```
    /// extern crate cernan;
    ///
    /// let metric = cernan::metric::Telemetry::new("foo", 1.0).aggr_sum();
    /// let mut bucket = cernan::buckets::Buckets::default();
    /// bucket.add(metric);
    /// ```
    pub fn add(&mut self, value: Telemetry) {
        let mut hsh = match self.keys
            .binary_search_by(|probe| probe.partial_cmp(&value.name).unwrap()) {
            Ok(hsh_idx) => self.values.index_mut(hsh_idx),
            Err(hsh_idx) => {
                self.keys.insert(hsh_idx, value.name.to_owned());
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
                        .persist();
                    cur += value;
                    hsh.insert(idx, cur)
                } else {
                    hsh.insert(idx, value)
                }
            }
        }
    }

    pub fn get(&self, key: &str) -> Option<&[Telemetry]> {
        match self.keys.iter().position(|k| k == key) {
            Some(idx) => Some(self.values.index(idx)),
            None => None,
        }
    }

    pub fn count(&self) -> usize {
        self.count as usize
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }
}

// Tests
//
#[cfg(test)]
mod test {
    extern crate quickcheck;

    use chrono::{TimeZone, UTC};
    use metric::Telemetry;
    use quantiles::ckms::CKMS;
    use self::quickcheck::{QuickCheck, TestResult};
    use std::cmp::Ordering;
    use std::collections::{HashMap, HashSet};
    use super::*;

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
        let m0 = Telemetry::new("lO", 1.0).timestamp(0).aggr_set().ephemeral();
        let m1 = Telemetry::new("lO", 2.0).timestamp(1).aggr_sum().persist();
        let m2 = Telemetry::new("lO", 0.0).timestamp(1).aggr_set().ephemeral();

        let mut bkt = Buckets::new(bin_width);
        bkt.add(m0);
        bkt.add(m1);

        //  * it is possible to switch a SET|SUM from persist to ephemeral
        //  * an ephemeral SET|SUM will have its value inherited by a persist SET|SUM
        let aggrs = bkt.clone();
        let res = aggrs.get("lO").unwrap();
        assert_eq!(0, res[0].timestamp);
        assert_eq!(Some(1.0), res[0].value());
        assert_eq!(1, res[1].timestamp);
        assert_eq!(Some(3.0), res[1].value());

        //  * a SET|SUM with persist will survive across resets
        bkt.reset();
        assert!(!bkt.is_empty());

        //  * an ephemeral SET|SUM will not inherit the value of a persist SET|SUM
        let aggrs = bkt.clone();
        let res = aggrs.get("lO").unwrap();
        bkt.add(m2.timestamp(res[0].timestamp));
        let aggrs = bkt.clone();
        let res = aggrs.get("lO").unwrap();
        assert_eq!(Some(0.0), res[0].value());

        bkt.reset();

        assert!(bkt.is_empty());
    }

    #[test]
    fn raw_test_variable() {
        let bin_width = 66;
        let m0 = Telemetry::new("lO", 0.807).timestamp(18).aggr_set();
        let m4 = Telemetry::new("lO", 0.361).timestamp(75).aggr_set();
        let m7 = Telemetry::new("lO", 0.291).timestamp(42).aggr_set();

        let mut bkt = Buckets::new(bin_width);
        bkt.add(m0);
        bkt.add(m4);
        bkt.add(m7);

        let raws = bkt.get("lO").unwrap();
        println!("RAWS: {:?}", raws);
        let ref res = raws[0];
        assert_eq!(18, res.timestamp);
        assert_eq!(Some(0.291), res.value());
    }

    #[test]
    fn test_gauge_small_bin_width() {
        let bin_width = 1;
        let m0 = Telemetry::new("lO", 3.211).timestamp(645181811).aggr_set();
        let m1 = Telemetry::new("lO", 4.322).timestamp(645181812).aggr_set();
        let m2 = Telemetry::new("lO", 5.433).timestamp(645181813).aggr_set();

        let mut bkt = Buckets::new(bin_width);
        bkt.add(m0);
        bkt.add(m1);
        bkt.add(m2);

        let v = bkt.get("lO").unwrap();
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
                        if m.persist && idx > 0 && mp[idx - 1].name == m.name {
                            let mut cur = mp[idx - 1].clone().timestamp(m.timestamp).persist();
                            cur += m;
                            mp.insert(idx, cur)
                        } else {
                            mp.insert(idx, m)
                        }
                    }
                }
            }

            for vs in bucket.into_iter() {
                for v in vs {
                    let ref kind = v.aggr_method;
                    let time = v.timestamp;
                    let mut found_one = false;
                    for m in &mp {
                        if (m.name == v.name) && (&m.aggr_method == kind) &&
                           within(bin_width, m.timestamp, time) {
                            assert_eq!(Ordering::Equal, m.within(bin_width, v));
                            found_one = true;
                            debug_assert!(v.value() == m.value(),
                                          "{:?} != {:?} |::|::| MODEL: {:?} |::|::|
    SUT: {:?}",
                                          v.value(),
                                          m.value(),
                                          mp,
                                          ms);
                        }
                    }
                    debug_assert!(found_one,
                                  "DID NOT FIND ONE FOR {:?} |::|::| MODEL: {:?}
    |::|::| SUT: \
                                   {:?}",
                                  v,
                                  mp,
                                  ms);
                }
            }
            TestResult::passed()
        }
        QuickCheck::new().quickcheck(inner as fn(u16, Vec<Telemetry>) -> TestResult);
    }

    #[test]
    fn test_add_gauge_metric_distinct_tags() {
        let mut buckets = Buckets::default();
        let m0 =
            Telemetry::new("some.metric", 1.0).aggr_set().timestamp(10).overlay_tag("foo", "bar");
        let m1 =
            Telemetry::new("some.metric", 1.0).aggr_set().timestamp(10).overlay_tag("foo", "bingo");

        buckets.add(m0.clone());
        buckets.add(m1.clone());

        let res = buckets.get("some.metric");
        assert!(res.is_some());
        let res = res.unwrap();
        assert_eq!(Some(&m0), res.get(0));
        assert_eq!(Some(&m1), res.get(1));
    }

    #[test]
    fn test_add_counter_metric() {
        let mut buckets = Buckets::default();
        let metric = Telemetry::new("some.metric", 1.0).aggr_sum();
        buckets.add(metric.clone());

        let rmname = String::from("some.metric");
        assert_eq!(Some(1.0), buckets.get(&rmname).unwrap()[0].value());

        // Increment counter
        buckets.add(metric);
        assert_eq!(Some(2.0), buckets.get(&rmname).unwrap()[0].value());
        assert_eq!(1, buckets.count());
    }

    #[test]
    fn test_reset_add_counter_metric() {
        let mut buckets = Buckets::default();
        let m0 = Telemetry::new("some.metric", 1.0).aggr_sum().timestamp(101);
        let m1 = m0.clone();

        buckets.add(m0);

        let rmname = String::from("some.metric");
        assert_eq!(Some(1.0), buckets.get(&rmname).unwrap()[0].value());

        // Increment counter
        buckets.add(m1);
        assert_eq!(Some(2.0), buckets.get(&rmname).unwrap()[0].value());
        assert_eq!(1, buckets.count());

        buckets.reset();
        assert_eq!(true, buckets.is_empty());
    }

    #[test]
    fn test_true_histogram() {
        let mut buckets = Buckets::default();

        let dt_0 = UTC.ymd(1996, 10, 7).and_hms_milli(10, 11, 11, 0).timestamp();
        let dt_1 = UTC.ymd(1996, 10, 7).and_hms_milli(10, 11, 12, 0).timestamp();
        let dt_2 = UTC.ymd(1996, 10, 7).and_hms_milli(10, 11, 13, 0).timestamp();

        let name = String::from("some.metric");
        let m0 = Telemetry::new("some.metric", 1.0).timestamp(dt_0).aggr_summarize();
        let m1 = Telemetry::new("some.metric", 2.0).timestamp(dt_1).aggr_summarize();
        let m2 = Telemetry::new("some.metric", 3.0).timestamp(dt_2).aggr_summarize();

        buckets.add(m0.clone());
        buckets.add(m1.clone());
        buckets.add(m2.clone());

        let hists = buckets;
        assert!(hists.get(&name).is_some());
        let ref hist = hists.get(&name).unwrap();

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
        let metric = Telemetry::new("some.metric", 1.0).aggr_summarize();
        buckets.add(metric.clone());

        buckets.reset();
        assert!(buckets.is_empty());
    }

    #[test]
    fn test_add_timer_metric_reset() {
        let mut buckets = Buckets::default();
        let metric = Telemetry::new("some.metric", 1.0).aggr_summarize();
        buckets.add(metric.clone());

        buckets.reset();
        assert!(buckets.is_empty());
    }

    #[test]
    fn test_add_gauge_metric() {
        let mut buckets = Buckets::default();
        let rmname = String::from("some.metric");
        let metric = Telemetry::new("some.metric", 11.5).aggr_set();
        buckets.add(metric);
        assert_eq!(Some(11.5), buckets.get(&rmname).unwrap()[0].value());
        assert_eq!(1, buckets.count());
    }

    #[test]
    fn test_add_delta_gauge_metric() {
        // A gauge that transitions from gauge to delta gauge does _not_ retain
        // its previous value. Other statsd aggregations require that you reset
        // the value to zero before moving to a delta gauge. Cernan does not
        // require this explicit reset, only the +/- on the metric value.
        let mut buckets = Buckets::default();
        let rmname = String::from("some.metric");
        let metric = Telemetry::new("some.metric", 100.0).timestamp(0).aggr_set();
        buckets.add(metric);
        let delta_metric = Telemetry::new("some.metric", -11.5).timestamp(0).aggr_sum().persist();
        buckets.add(delta_metric);
        assert_eq!(Some(88.5), buckets.get(&rmname).unwrap()[0].value());
        assert_eq!(1, buckets.count());
        buckets.reset();
        assert_eq!(1, buckets.count());
    }

    #[test]
    fn test_reset_add_delta_gauge_metric() {
        let mut buckets = Buckets::default();
        let rmname = String::from("some.metric");
        let metric = Telemetry::new("some.metric", 100.0).aggr_set();
        buckets.add(metric);
        let delta_metric = Telemetry::new("some.metric", -11.5).aggr_sum().persist();
        buckets.add(delta_metric);
        let reset_metric = Telemetry::new("some.metric", 2007.3).aggr_set();
        buckets.add(reset_metric);
        assert_eq!(Some(2007.3), buckets.get(&rmname).unwrap()[0].value());
        assert_eq!(1, buckets.count());
    }

    #[test]
    fn test_add_timer_metric() {
        let mut buckets = Buckets::default();
        let rmname = String::from("some.metric");
        let metric = Telemetry::new("some.metric", 11.5).aggr_sum();
        buckets.add(metric);
        assert_eq!(Some(11.5),
                   buckets.get(&rmname).expect("hwhap")[0].query(0.0));

        let metric_two = Telemetry::new("some.metric", 99.5).aggr_sum();
        buckets.add(metric_two);

        let romname = String::from("other.metric");
        let metric_three = Telemetry::new("other.metric", 811.5).aggr_sum();
        buckets.add(metric_three);
        assert_eq!(Some(811.5),
                   buckets.get(&romname).expect("hwhap")[0].query(0.0));
    }

    #[test]
    fn test_raws_store_one_point_per_second() {
        let mut buckets = Buckets::default();
        let dt_0 = UTC.ymd(2016, 9, 13).and_hms_milli(11, 30, 0, 0).timestamp();
        let dt_1 = UTC.ymd(2016, 9, 13).and_hms_milli(11, 30, 1, 0).timestamp();

        buckets.add(Telemetry::new("some.metric", 1.0).timestamp(dt_0).aggr_set());
        buckets.add(Telemetry::new("some.metric", 2.0).timestamp(dt_0).aggr_set());
        buckets.add(Telemetry::new("some.metric", 3.0).timestamp(dt_0).aggr_set());
        buckets.add(Telemetry::new("some.metric", 4.0).timestamp(dt_1).aggr_set());

        let mname = String::from("some.metric");
        let metrics = buckets.get(&mname).unwrap();
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

            let cnts: HashSet<String> = ms.iter().fold(HashSet::default(), |mut acc, ref m| {
                acc.insert(m.name.clone());
                acc
            });
            let b_cnts: HashSet<String> = bucket.into_iter()
                .fold(HashSet::default(), |mut acc, v| {
                    acc.insert(v[0].name.clone());
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
                let name = m.name;
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
                bucket.add(m.ephemeral());
            }
            bucket.reset();

            assert_eq!(0, bucket.count());
            TestResult::passed()
        }
        QuickCheck::new().quickcheck(qos_ret as fn(Vec<Telemetry>) -> TestResult);
    }

    #[test]
    fn test_reset_clears_not_all_space_if_persistent() {
        fn qos_ret(ms: Vec<Telemetry>) -> TestResult {
            if ms.is_empty() {
                return TestResult::discard();
            }
            let mut bucket = Buckets::default();

            for m in ms {
                bucket.add(m.persist());
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

        let m0 = Telemetry::new("test.gauge_0", 1.0).aggr_set();
        let m1 = Telemetry::new("test.gauge_1", 1.0).aggr_set();
        buckets.add(m0.clone());
        buckets.add(m1.clone());

        assert_eq!(m0, buckets.get(&String::from("test.gauge_0")).unwrap()[0]);
        assert_eq!(m1, buckets.get(&String::from("test.gauge_1")).unwrap()[0]);
    }

    #[test]
    fn test_counter_insertion() {
        let mut buckets = Buckets::default();

        let m0 = Telemetry::new("test.counter_0", 1.0).aggr_sum();
        let m1 = Telemetry::new("test.counter_1", 1.0).aggr_sum();
        buckets.add(m0.clone());
        buckets.add(m1.clone());

        assert_eq!(m0, buckets.get(&String::from("test.counter_0")).unwrap()[0]);
        assert_eq!(m1, buckets.get(&String::from("test.counter_1")).unwrap()[0]);
    }

    #[test]
    fn test_all_insertions() {
        fn qos_ret(ms: Vec<Telemetry>) -> TestResult {
            let mut bucket = Buckets::default();

            for m in ms.clone() {
                bucket.add(m)
            }

            let mut cnts: HashMap<String, Vec<(i64, CKMS<f64>)>> = HashMap::default();
            for m in ms {
                let c = cnts.entry(m.name.clone()).or_insert(vec![]);
                match c.binary_search_by_key(&m.timestamp, |&(a, _)| a) {
                    Ok(idx) => c[idx].1 += m.ckms(),
                    Err(idx) => {
                        if m.persist && idx > 0 {
                            let mut val = c[idx - 1].clone().1;
                            val += m.ckms();
                            c.insert(idx, (m.timestamp, val))
                        } else {
                            c.insert(idx, (m.timestamp, m.ckms()))
                        }
                    }
                }
            }

            for vals in bucket.into_iter() {
                let mut v: Vec<Telemetry> = vals.to_vec();
                v.sort_by_key(|ref m| m.timestamp);

                let ref cnts_v = *cnts.get(&v[0].name).unwrap();
                assert_eq!(cnts_v.len(), v.len());

                for (i, c_v) in cnts_v.iter().enumerate() {
                    assert_eq!((c_v.0, c_v.1.clone()), (v[i].timestamp, v[i].ckms()));
                }
            }

            TestResult::passed()
        }
        QuickCheck::new().quickcheck(qos_ret as fn(Vec<Telemetry>) -> TestResult);
    }
}
