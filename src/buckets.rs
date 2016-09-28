//! Buckets are the primary internal storage type.
//!
//! Each bucket contains a set of hashmaps containing
//! each set of metrics received by clients.

use metric::{Metric, MetricKind};
use quantiles::CKMS;
use string_cache::Atom;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use fnv::FnvHasher;

pub type HashMapFnv<K, V> = HashMap<K, V, BuildHasherDefault<FnvHasher>>;

/// Buckets stores all metrics until they are flushed.
pub struct Buckets {
    counters: HashMapFnv<Atom, Vec<Metric>>,
    gauges: HashMapFnv<Atom, Vec<Metric>>,
    raws: HashMapFnv<Atom, Vec<Metric>>,

    timers: HashMapFnv<Atom, CKMS<f64>>,
    histograms: HashMapFnv<Atom, CKMS<f64>>,
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
    /// assert_eq!(0, bucket.counters().len());
    /// ```
    fn default() -> Buckets {
        Buckets {
            counters: HashMapFnv::default(),
            gauges: HashMapFnv::default(),
            raws: HashMapFnv::default(),
            timers: HashMapFnv::default(),
            histograms: HashMapFnv::default(),
        }
    }
}

impl Buckets {
    /// Resets appropriate aggregates
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate cernan;
    /// extern crate string_cache;
    ///
    /// let metric = cernan::metric::Metric::parse_statsd("foo:1|c").unwrap();
    /// let mut buckets = cernan::buckets::Buckets::default();
    /// let rname = string_cache::Atom::from("foo");
    ///
    /// assert_eq!(true, buckets.counters().is_empty());
    ///
    /// buckets.add(metric[0].clone());
    /// assert_eq!(false, buckets.counters().is_empty());
    /// buckets.reset();
    /// assert_eq!(true, buckets.counters().is_empty());
    /// ```
    pub fn reset(&mut self) {
        self.counters.clear();
        self.raws.clear();
        for (_, v) in self.gauges.iter_mut() {
            let len = v.len();
            if len > 0 {
                v.swap(0, len - 1);
                v.truncate(1);
            }
        }
    }

    pub fn reset_histograms(&mut self) {
        self.histograms.clear();
    }

    pub fn reset_timers(&mut self) {
        self.timers.clear();
    }

    /// Adds a metric to the bucket storage.
    ///
    /// # Examples
    /// ```
    /// extern crate cernan;
    ///
    /// let metric = cernan::metric::Metric::parse_statsd("foo:1|c").unwrap();
    /// let mut bucket = cernan::buckets::Buckets::default();
    /// bucket.add(metric[0].clone());
    /// ```
    pub fn add(&mut self, value: Metric) {
        let name = value.name.to_owned();
        match value.kind {
            MetricKind::Counter(rate) => {
                let counter = self.counters.entry(name).or_insert(vec![]);
                match (*counter).binary_search_by_key(&value.time, |m| m.time) {
                    Ok(idx) => {
                        (*counter)[idx].value += value.value * (1.0 / rate);
                    }
                    Err(idx) => {
                        let mut v = value;
                        v.value *= 1.0 / rate;
                        (*counter).insert(idx, v);
                    }
                }
            }
            MetricKind::DeltaGauge => {
                let gauge = self.gauges.entry(name).or_insert(vec![]);
                match (*gauge).binary_search_by_key(&value.time, |m| m.time) {
                    Ok(idx) => (*gauge)[idx].value += value.value,
                    Err(idx) => (*gauge).insert(idx, value),
                }
            }
            MetricKind::Gauge => {
                let gauge = self.gauges.entry(name).or_insert(vec![]);
                match (*gauge).binary_search_by_key(&value.time, |m| m.time) {
                    Ok(idx) => (*gauge)[idx].value = value.value,
                    Err(idx) => (*gauge).insert(idx, value),
                }
            }
            MetricKind::Raw => {
                let raw = self.raws.entry(name).or_insert(vec![]);
                // We only want to keep one raw point per second per name. If
                // someone pushes more than one point in a second interval we'll
                // overwrite the value of the metric already in-vector.
                match (*raw).binary_search_by_key(&value.time, |m| m.time) {
                    Ok(idx) => (*raw)[idx].value = value.value,
                    Err(idx) => (*raw).insert(idx, value),
                }
            }
            MetricKind::Histogram => {
                let hist = self.histograms.entry(name).or_insert(CKMS::<f64>::new(0.001));
                (*hist).insert(value.value);
            }
            MetricKind::Timer => {
                let tm = self.timers.entry(name).or_insert(CKMS::<f64>::new(0.001));
                (*tm).insert(value.value);
            }
        }
    }

    /// Get the counters as a borrowed reference.
    pub fn counters(&self) -> &HashMapFnv<Atom, Vec<Metric>> {
        &self.counters
    }

    /// Get the gauges as a borrowed reference.
    pub fn gauges(&self) -> &HashMapFnv<Atom, Vec<Metric>> {
        &self.gauges
    }

    /// Get the gauges as a borrowed reference.
    pub fn raws(&self) -> &HashMapFnv<Atom, Vec<Metric>> {
        &self.raws
    }

    /// Get the histograms as a borrowed reference.
    pub fn histograms(&self) -> &HashMapFnv<Atom, CKMS<f64>> {
        &self.histograms
    }

    /// Get the timers as a borrowed reference.
    pub fn timers(&self) -> &HashMapFnv<Atom, CKMS<f64>> {
        &self.timers
    }
}

// Tests
//
#[cfg(test)]
mod test {
    extern crate quickcheck;

    use super::*;
    use self::quickcheck::{TestResult, QuickCheck};
    use metric::{Metric, MetricKind, MetricSign};
    use string_cache::Atom;
    use std::collections::{HashSet, HashMap};
    use chrono::{UTC, TimeZone};

    #[test]
    fn test_add_increments_total_messages() {
        let mut buckets = Buckets::default();
        // duff value to ensure it changes.
        let metric = Metric::new(Atom::from("some.metric"),
                                 1.0,
                                 MetricKind::Counter(1.0),
                                 None);
        buckets.add(metric);
    }

    #[test]
    fn test_add_counter_metric() {
        let mut buckets = Buckets::default();
        let mname = Atom::from("some.metric");
        let metric = Metric::new(mname, 1.0, MetricKind::Counter(1.0), None);
        buckets.add(metric.clone());

        let rmname = Atom::from("some.metric");
        assert!(buckets.counters.contains_key(&rmname),
                "Should contain the metric key");
        assert_eq!(1.0, buckets.counters.get_mut(&rmname).unwrap()[0].value);

        // Increment counter
        buckets.add(metric);
        assert_eq!(2.0, buckets.counters.get_mut(&rmname).unwrap()[0].value);
        assert_eq!(1, buckets.counters().len());
        assert_eq!(0, buckets.gauges().len());
    }

    #[test]
    fn test_add_counter_metric_reset() {
        let mut buckets = Buckets::default();
        let mname = Atom::from("some.metric");
        let metric = Metric::new(mname, 1.0, MetricKind::Counter(1.0), None);
        buckets.add(metric.clone());

        let rmname = Atom::from("some.metric");
        assert!(buckets.counters.contains_key(&rmname),
                "Should contain the metric key");
        assert_eq!(1.0, buckets.counters.get_mut(&rmname).unwrap()[0].value);

        // Increment counter
        buckets.add(metric);
        assert_eq!(2.0, buckets.counters.get_mut(&rmname).unwrap()[0].value);
        assert_eq!(1, buckets.counters().len());

        buckets.reset();
        assert_eq!(None, buckets.counters.get_mut(&rmname));
    }

    #[test]
    fn test_add_histogram_metric_reset() {
        let mut buckets = Buckets::default();
        let name = Atom::from("some.metric");
        let mname = name.clone();
        let metric = Metric::new(mname, 1.0, MetricKind::Histogram, None);
        buckets.add(metric.clone());

        buckets.reset_histograms();
        assert!(buckets.histograms.get_mut(&name).is_none());
    }

    #[test]
    fn test_add_timer_metric_reset() {
        let mut buckets = Buckets::default();
        let name = Atom::from("some.metric");
        let mname = name.clone();
        let metric = Metric::new(mname, 1.0, MetricKind::Timer, None);
        buckets.add(metric.clone());

        buckets.reset_timers();
        assert!(buckets.histograms.get_mut(&name).is_none());
    }

    #[test]
    fn test_add_counter_metric_sampled() {
        let mut buckets = Buckets::default();
        let metric = Metric::new(Atom::from("some.metric"),
                                 1.0,
                                 MetricKind::Counter(0.1),
                                 None);

        let rmname = Atom::from("some.metric");

        buckets.add(metric);
        assert_eq!(10.0, buckets.counters.get_mut(&rmname).unwrap()[0].value);

        let metric_two = Metric::new(Atom::from("some.metric"),
                                     1.0,
                                     MetricKind::Counter(0.5),
                                     None);
        buckets.add(metric_two);
        assert_eq!(12.0, buckets.counters.get_mut(&rmname).unwrap()[0].value);
    }

    #[test]
    fn test_add_gauge_metric() {
        let mut buckets = Buckets::default();
        let rmname = Atom::from("some.metric");
        let metric = Metric::new(Atom::from("some.metric"), 11.5, MetricKind::Gauge, None);
        buckets.add(metric);
        assert!(buckets.gauges.contains_key(&rmname),
                "Should contain the metric key");
        assert_eq!(11.5, buckets.gauges.get_mut(&rmname).unwrap()[0].value);
        assert_eq!(1, buckets.gauges().len());
        assert_eq!(0, buckets.counters().len());
    }

    #[test]
    fn test_add_delta_gauge_metric() {
        let mut buckets = Buckets::default();
        let rmname = Atom::from("some.metric");
        let metric = Metric::new(Atom::from("some.metric"), 100.0, MetricKind::Gauge, None);
        buckets.add(metric);
        let delta_metric = Metric::new(Atom::from("some.metric"),
                                       11.5,
                                       MetricKind::Gauge,
                                       Some(MetricSign::Negative));
        buckets.add(delta_metric);
        assert!(buckets.gauges.contains_key(&rmname),
                "Should contain the metric key");
        assert_eq!(88.5, buckets.gauges.get_mut(&rmname).unwrap()[0].value);
        assert_eq!(1, buckets.gauges().len());
        assert_eq!(0, buckets.counters().len());
    }

    #[test]
    fn test_add_delta_gauge_metric_reset() {
        let mut buckets = Buckets::default();
        let rmname = Atom::from("some.metric");
        let metric = Metric::new(Atom::from("some.metric"), 100.0, MetricKind::Gauge, None);
        buckets.add(metric);
        let delta_metric = Metric::new(Atom::from("some.metric"),
                                       11.5,
                                       MetricKind::Gauge,
                                       Some(MetricSign::Negative));
        buckets.add(delta_metric);
        let reset_metric = Metric::new(Atom::from("some.metric"), 2007.3, MetricKind::Gauge, None);
        buckets.add(reset_metric);
        assert!(buckets.gauges.contains_key(&rmname),
                "Should contain the metric key");
        assert_eq!(2007.3, buckets.gauges.get_mut(&rmname).unwrap()[0].value);
        assert_eq!(1, buckets.gauges().len());
        assert_eq!(0, buckets.counters().len());
    }

    #[test]
    fn test_add_timer_metric() {
        let mut buckets = Buckets::default();
        let rmname = Atom::from("some.metric");
        let metric = Metric::new(Atom::from("some.metric"), 11.5, MetricKind::Timer, None);
        buckets.add(metric);
        assert!(buckets.timers.contains_key(&rmname),
                "Should contain the metric key");

        assert_eq!(Some((1, 11.5)),
                   buckets.timers.get_mut(&rmname).expect("hwhap").query(0.0));

        let metric_two = Metric::new(Atom::from("some.metric"), 99.5, MetricKind::Timer, None);
        buckets.add(metric_two);

        let romname = Atom::from("other.metric");
        let metric_three = Metric::new(Atom::from("other.metric"), 811.5, MetricKind::Timer, None);
        buckets.add(metric_three);
        assert!(buckets.timers.contains_key(&romname),
                "Should contain the metric key");
        assert!(buckets.timers.contains_key(&romname),
                "Should contain the metric key");

        // assert_eq!(Some(&mut vec![11.5, 99.5]), buckets.timers.get_mut("some.metric"));
        // assert_eq!(Some(&mut vec![811.5]), buckets.timers.get_mut("other.metric"));
    }

    #[test]
    fn test_raws_store_one_point_per_second() {
        let mut buckets = Buckets::default();
        let dt_0 = UTC.ymd(2016, 9, 13).and_hms_milli(11, 30, 0, 0).timestamp();
        let dt_1 = UTC.ymd(2016, 9, 13).and_hms_milli(11, 30, 1, 0).timestamp();

        buckets.add(Metric::new_with_time(Atom::from("some.metric"),
                                          1.0,
                                          Some(dt_0),
                                          MetricKind::Raw,
                                          None));
        buckets.add(Metric::new_with_time(Atom::from("some.metric"),
                                          2.0,
                                          Some(dt_0),
                                          MetricKind::Raw,
                                          None));
        buckets.add(Metric::new_with_time(Atom::from("some.metric"),
                                          3.0,
                                          Some(dt_0),
                                          MetricKind::Raw,
                                          None));
        buckets.add(Metric::new_with_time(Atom::from("some.metric"),
                                          4.0,
                                          Some(dt_1),
                                          MetricKind::Raw,
                                          None));

        let mname = Atom::from("some.metric");
        assert!(buckets.raws.contains_key(&mname),
                "Should contain the metric key");

        assert_eq!(Some(&mut vec![
            Metric::new_with_time(Atom::from("some.metric"), 3.0, Some(dt_0), MetricKind::Raw, None),
            Metric::new_with_time(Atom::from("some.metric"), 4.0, Some(dt_1), MetricKind::Raw, None),
        ]),
                   buckets.raws.get_mut(&mname));
    }

    #[test]
    fn unique_names_preserved_counters() {
        fn qos_ret(ms: Vec<Metric>) -> TestResult {
            let mut bucket = Buckets::default();

            for m in ms.clone() {
                bucket.add(m);
            }

            let cnts: HashSet<Atom> = ms.iter().fold(HashSet::default(), |mut acc, ref m| {
                match m.kind {
                    MetricKind::Counter(_) => {
                        acc.insert(m.name.clone());
                        acc
                    }
                    _ => acc,
                }
            });
            let b_cnts: HashSet<Atom> =
                bucket.counters().iter().fold(HashSet::default(), |mut acc, (k, _)| {
                    acc.insert(k.clone());
                    acc
                });
            assert_eq!(cnts, b_cnts);

            TestResult::passed()
        }
        QuickCheck::new()
            .tests(100)
            .max_tests(1000)
            .quickcheck(qos_ret as fn(Vec<Metric>) -> TestResult);
    }

    #[test]
    fn unique_names_preserved_gauges() {
        fn qos_ret(ms: Vec<Metric>) -> TestResult {
            let mut bucket = Buckets::default();

            for m in ms.clone() {
                bucket.add(m);
            }

            let gauges: HashSet<Atom> = ms.iter().fold(HashSet::default(), |mut acc, ref m| {
                match m.kind {
                    MetricKind::Gauge | MetricKind::DeltaGauge => {
                        acc.insert(m.name.clone());
                        acc
                    }
                    _ => acc,
                }
            });
            let b_gauges: HashSet<Atom> =
                bucket.gauges().iter().fold(HashSet::default(), |mut acc, (k, _)| {
                    acc.insert(k.clone());
                    acc
                });
            assert_eq!(gauges, b_gauges);

            TestResult::passed()
        }
        QuickCheck::new()
            .tests(100)
            .max_tests(1000)
            .quickcheck(qos_ret as fn(Vec<Metric>) -> TestResult);
    }

    #[test]
    fn unique_names_preserved_histograms() {
        fn qos_ret(ms: Vec<Metric>) -> TestResult {
            let mut bucket = Buckets::default();

            for m in ms.clone() {
                bucket.add(m);
            }

            let hist: HashSet<Atom> = ms.iter().fold(HashSet::default(), |mut acc, ref m| {
                match m.kind {
                    MetricKind::Histogram => {
                        acc.insert(m.name.clone());
                        acc
                    }
                    _ => acc,
                }
            });
            let b_hist: HashSet<Atom> =
                bucket.histograms().iter().fold(HashSet::default(), |mut acc, (k, _)| {
                    acc.insert(k.clone());
                    acc
                });
            assert_eq!(hist, b_hist);

            TestResult::passed()
        }
        QuickCheck::new()
            .tests(100)
            .max_tests(1000)
            .quickcheck(qos_ret as fn(Vec<Metric>) -> TestResult);
    }

    #[test]
    fn unique_names_preserved_timers() {
        fn qos_ret(ms: Vec<Metric>) -> TestResult {
            let mut bucket = Buckets::default();

            for m in ms.clone() {
                bucket.add(m);
            }

            let tm: HashSet<Atom> = ms.iter().fold(HashSet::default(), |mut acc, ref m| {
                match m.kind {
                    MetricKind::Timer => {
                        acc.insert(m.name.clone());
                        acc
                    }
                    _ => acc,
                }
            });
            let b_tm: HashSet<Atom> =
                bucket.timers().iter().fold(HashSet::default(), |mut acc, (k, _)| {
                    acc.insert(k.clone());
                    acc
                });
            assert_eq!(tm, b_tm);

            TestResult::passed()
        }
        QuickCheck::new()
            .tests(100)
            .max_tests(1000)
            .quickcheck(qos_ret as fn(Vec<Metric>) -> TestResult);
    }

    #[test]
    fn test_reset_clears_space() {
        fn qos_ret(ms: Vec<Metric>) -> TestResult {
            let mut bucket = Buckets::default();

            for m in ms {
                bucket.add(m);
            }
            bucket.reset();

            assert_eq!(0, bucket.counters.len());
            assert_eq!(0, bucket.raws.len());
            for (_, v) in bucket.gauges() {
                assert_eq!(1, v.len());
            }

            TestResult::passed()
        }
        QuickCheck::new()
            .tests(100)
            .max_tests(1000)
            .quickcheck(qos_ret as fn(Vec<Metric>) -> TestResult);
    }

    #[test]
    fn test_gauge_insertion() {
        let mut buckets = Buckets::default();

        let m0 = Metric::new(Atom::from("test.gauge_0"), 1.0, MetricKind::Gauge, None);
        let m1 = Metric::new(Atom::from("test.gauge_1"), 1.0, MetricKind::Gauge, None);
        buckets.add(m0.clone());
        buckets.add(m1.clone());

        assert_eq!(Some(&mut vec![m0]),
                   buckets.gauges.get_mut(&Atom::from("test.gauge_0")));
        assert_eq!(Some(&mut vec![m1]),
                   buckets.gauges.get_mut(&Atom::from("test.gauge_1")));
    }

    #[test]
    fn test_counter_insertion() {
        let mut buckets = Buckets::default();

        let m0 = Metric::counter("test.counter_0");
        let m1 = Metric::counter("test.counter_1");
        buckets.add(m0.clone());
        buckets.add(m1.clone());

        assert_eq!(Some(&mut vec![m0]),
                   buckets.counters.get_mut(&Atom::from("test.counter_0")));
        assert_eq!(Some(&mut vec![m1]),
                   buckets.counters.get_mut(&Atom::from("test.counter_1")));
    }

    #[test]
    fn test_counter_summations() {
        fn qos_ret(ms: Vec<Metric>) -> TestResult {
            let mut bucket = Buckets::default();

            for m in ms.clone() {
                bucket.add(m);
            }

            let mut cnts: HashMap<Atom, Vec<(i64, f64)>> = HashMap::default();
            for m in ms {
                match m.kind {
                    MetricKind::Counter(rate) => {
                        let c = cnts.entry(m.name.clone()).or_insert(vec![]);
                        match c.binary_search_by_key(&m.time, |&(a, _)| a) {
                            Ok(idx) => c[idx].1 += m.value * (1.0 / rate),
                            Err(idx) => c.insert(idx, (m.time, m.value * (1.0 / rate))),
                        }
                    }
                    _ => continue,
                }
            }

            assert_eq!(bucket.counters().len(), cnts.len());

            for (k, v) in bucket.counters().iter() {
                let mut v = v.clone();
                v.sort_by_key(|ref m| m.time);

                let ref cnts_v = *cnts.get(k).unwrap();
                assert_eq!(cnts_v.len(), v.len());

                for (i, c_v) in cnts_v.iter().enumerate() {
                    assert_eq!(c_v.0, v[i].time);
                    assert_eq!(c_v.1, v[i].value);
                }
            }

            TestResult::passed()
        }
        QuickCheck::new()
            .tests(10000)
            .max_tests(100000)
            .quickcheck(qos_ret as fn(Vec<Metric>) -> TestResult);
    }

    #[test]
    fn test_gauge_behaviour() {
        fn qos_ret(ms: Vec<Metric>) -> TestResult {
            let mut bucket = Buckets::default();

            for m in ms.clone() {
                bucket.add(m);
            }

            let mut cnts: HashMap<Atom, Vec<(i64, f64)>> = HashMap::default();
            for m in ms {
                match m.kind {
                    MetricKind::Gauge => {
                        let c = cnts.entry(m.name.clone()).or_insert(vec![]);
                        match c.binary_search_by_key(&m.time, |&(a, _)| a) {
                            Ok(idx) => c[idx] = (m.time, m.value),
                            Err(idx) => c.insert(idx, (m.time, m.value)),
                        }
                    }
                    MetricKind::DeltaGauge => {
                        let c = cnts.entry(m.name.clone()).or_insert(vec![]);
                        match c.binary_search_by_key(&m.time, |&(a, _)| a) {
                            Ok(idx) => c[idx].1 += m.value,
                            Err(idx) => c.insert(idx, (m.time, m.value)),
                        }
                    }
                    _ => continue,
                }
            }

            assert_eq!(bucket.gauges().len(), cnts.len());
            for (k, v) in bucket.gauges().iter() {
                let mut v = v.clone();
                v.sort_by_key(|ref m| m.time);

                let ref cnts_v = *cnts.get(k).unwrap();
                assert_eq!(cnts_v.len(), v.len());

                for (i, c_v) in cnts_v.iter().enumerate() {
                    assert_eq!(c_v.0, v[i].time);
                    assert_eq!(c_v.1, v[i].value);
                }
            }

            TestResult::passed()
        }
        QuickCheck::new()
            .tests(10000)
            .max_tests(100000)
            .quickcheck(qos_ret as fn(Vec<Metric>) -> TestResult);
    }
}
