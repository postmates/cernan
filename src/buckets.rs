//! Buckets are the primary internal storage type.
//!
//! Each bucket contains a set of hashmaps containing
//! each set of metrics received by clients.

use metric::{Metric, MetricKind};
use quantiles::CKMS;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use fnv::FnvHasher;

pub type HashMapFnv<K, V> = HashMap<K, V, BuildHasherDefault<FnvHasher>>;

/// Buckets stores all metrics until they are flushed.
pub struct Buckets {
    counters: HashMapFnv<String, Vec<(i64, Metric)>>,
    gauges: HashMapFnv<String, Vec<(i64, Metric)>>,
    raws: HashMapFnv<String, Vec<(i64, Metric)>>,

    timers: HashMapFnv<String, Vec<(i64, CKMS<f64>)>>,
    histograms: HashMapFnv<String, Vec<(i64, CKMS<f64>)>>,
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
    ///
    /// let metric = cernan::metric::Metric::parse_statsd("foo:1|c").unwrap();
    /// let mut buckets = cernan::buckets::Buckets::default();
    /// let rname = String::from("foo");
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
        self.histograms.clear();
        self.timers.clear();
        for (_, v) in self.gauges.iter_mut() {
            let len = v.len();
            if len > 0 {
                v.swap(0, len - 1);
                v.truncate(1);
            }
        }
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
                match (*counter).binary_search_by_key(&value.time, |&(t, _)| t) {
                    Ok(idx) => {
                        (*counter)[idx].1.value += value.value * (1.0 / rate);
                    }
                    Err(idx) => {
                        let mut v = value;
                        v.value *= 1.0 / rate;
                        (*counter).insert(idx, (v.time, v));
                    }
                }
            }
            MetricKind::DeltaGauge => {
                let gauge = self.gauges.entry(name).or_insert(vec![]);
                match (*gauge).binary_search_by_key(&value.time, |&(t, _)| t) {
                    Ok(idx) => (*gauge)[idx].1.value += value.value,
                    Err(idx) => (*gauge).insert(idx, (value.time, value)),
                }
            }
            MetricKind::Gauge => {
                let gauge = self.gauges.entry(name).or_insert(vec![]);
                match (*gauge).binary_search_by_key(&value.time, |&(t, _)| t) {
                    Ok(idx) => (*gauge)[idx].1.value = value.value,
                    Err(idx) => (*gauge).insert(idx, (value.time, value)),
                }
            }
            MetricKind::Raw => {
                let raw = self.raws.entry(name).or_insert(vec![]);
                // We only want to keep one raw point per second per name. If
                // someone pushes more than one point in a second interval we'll
                // overwrite the value of the metric already in-vector.
                match (*raw).binary_search_by_key(&value.time, |&(t, _)| t) {
                    Ok(idx) => (*raw)[idx].1.value = value.value,
                    Err(idx) => (*raw).insert(idx, (value.time, value)),
                }
            }
            MetricKind::Histogram => {
                let hist = self.histograms.entry(name).or_insert(vec![]);
                match (*hist).binary_search_by_key(&value.time, |&(t, _)| t) {
                    Ok(idx) => {
                        let (_, ref mut ckms) = (*hist)[idx];
                        ckms.insert(value.value);
                    }
                    Err(idx) => {
                        let mut h = CKMS::<f64>::new(0.001);
                        h.insert(value.value);
                        (*hist).insert(idx, (value.time, h));
                    }
                }
            }
            MetricKind::Timer => {
                let tmr = self.timers.entry(name).or_insert(vec![]);
                match (*tmr).binary_search_by_key(&value.time, |&(t, _)| t) {
                    Ok(idx) => {
                        let (_, ref mut ckms) = (*tmr)[idx];
                        ckms.insert(value.value);
                    }
                    Err(idx) => {
                        let mut t = CKMS::<f64>::new(0.001);
                        t.insert(value.value);
                        (*tmr).insert(idx, (value.time, t));
                    }
                }
            }
        }
    }

    pub fn counters(&self) -> &HashMapFnv<String, Vec<(i64, Metric)>> {
        &self.counters
    }

    pub fn gauges(&self) -> &HashMapFnv<String, Vec<(i64, Metric)>> {
        &self.gauges
    }

    pub fn raws(&self) -> &HashMapFnv<String, Vec<(i64, Metric)>> {
        &self.raws
    }

    pub fn histograms(&self) -> &HashMapFnv<String, Vec<(i64, CKMS<f64>)>> {
        &self.histograms
    }

    pub fn timers(&self) -> &HashMapFnv<String, Vec<(i64, CKMS<f64>)>> {
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
    use metric::{Metric, MetricKind};
    use std::collections::{HashSet, HashMap};
    use chrono::{UTC, TimeZone};
    use quantiles::CKMS;

    #[test]
    fn test_add_increments_total_messages() {
        let mut buckets = Buckets::default();
        // duff value to ensure it changes.
        let metric = Metric::new("some.metric", 1.0).counter(1.0);
        buckets.add(metric);
    }

    #[test]
    fn test_add_counter_metric() {
        let mut buckets = Buckets::default();
        let metric = Metric::new("some.metric", 1.0).counter(1.0);
        buckets.add(metric.clone());

        let rmname = String::from("some.metric");
        assert!(buckets.counters.contains_key(&rmname),
                "Should contain the metric key");
        assert_eq!(1.0, buckets.counters.get_mut(&rmname).unwrap()[0].1.value);

        // Increment counter
        buckets.add(metric);
        assert_eq!(2.0, buckets.counters.get_mut(&rmname).unwrap()[0].1.value);
        assert_eq!(1, buckets.counters().len());
        assert_eq!(0, buckets.gauges().len());
    }

    #[test]
    fn test_add_counter_metric_reset() {
        let mut buckets = Buckets::default();
        let metric = Metric::new("some.metric", 1.0).counter(1.0);
        buckets.add(metric.clone());

        let rmname = String::from("some.metric");
        assert!(buckets.counters.contains_key(&rmname),
                "Should contain the metric key");
        assert_eq!(1.0, buckets.counters.get_mut(&rmname).unwrap()[0].1.value);

        // Increment counter
        buckets.add(metric);
        assert_eq!(2.0, buckets.counters.get_mut(&rmname).unwrap()[0].1.value);
        assert_eq!(1, buckets.counters().len());

        buckets.reset();
        assert_eq!(None, buckets.counters.get_mut(&rmname));
    }

    #[test]
    fn test_true_histogram() {
        let mut buckets = Buckets::default();

        let dt_0 = UTC.ymd(1996, 10, 7).and_hms_milli(10, 11, 11, 0).timestamp();
        let dt_1 = UTC.ymd(1996, 10, 7).and_hms_milli(10, 11, 12, 0).timestamp();
        let dt_2 = UTC.ymd(1996, 10, 7).and_hms_milli(10, 11, 13, 0).timestamp();

        let name = String::from("some.metric");
        let m0 = Metric::new("some.metric", 1.0).time(dt_0).histogram();
        let m1 = Metric::new("some.metric", 2.0).time(dt_1).histogram();
        let m2 = Metric::new("some.metric", 3.0).time(dt_2).histogram();

        buckets.add(m0.clone());
        buckets.add(m1.clone());
        buckets.add(m2.clone());

        let hists = buckets.histograms();
        assert!(hists.get(&name).is_some());
        let ref hist: &Vec<(i64, CKMS<f64>)> = hists.get(&name).unwrap();

        assert_eq!(3, hist.len());

        let ref vm0 = hist[0];
        assert_eq!(844683071, vm0.0);
        let ref ckms0 = vm0.1;
        assert_eq!(1, ckms0.count());
        assert_eq!(Some((1, 1.0)), ckms0.query(1.0));
        assert_eq!(Some((1, 1.0)), ckms0.query(0.0));

        let ref vm1 = hist[1];
        assert_eq!(844683072, vm1.0);
        let ref ckms1 = vm1.1;
        assert_eq!(1, ckms1.count());
        assert_eq!(Some((1, 2.0)), ckms1.query(1.0));
        assert_eq!(Some((1, 2.0)), ckms1.query(0.0));

        let ref vm2 = hist[2];
        assert_eq!(844683073, vm2.0);
        let ref ckms2 = vm2.1;
        assert_eq!(1, ckms2.count());
        assert_eq!(Some((1, 3.0)), ckms2.query(1.0));
        assert_eq!(Some((1, 3.0)), ckms2.query(0.0));
    }

    #[test]
    fn test_add_histogram_metric_reset() {
        let mut buckets = Buckets::default();
        let name = String::from("some.metric");
        let metric = Metric::new("some.metric", 1.0).histogram();
        buckets.add(metric.clone());

        buckets.reset();
        assert!(buckets.histograms.get_mut(&name).is_none());
    }

    #[test]
    fn test_add_timer_metric_reset() {
        let mut buckets = Buckets::default();
        let name = String::from("some.metric");
        let metric = Metric::new("some.metric", 1.0).timer();
        buckets.add(metric.clone());

        buckets.reset();
        assert!(buckets.timers.get_mut(&name).is_none());
    }

    #[test]
    fn test_add_counter_metric_sampled() {
        let mut buckets = Buckets::default();
        let metric = Metric::new("some.metric", 1.0).counter(0.1);
        let rmname = String::from("some.metric");

        buckets.add(metric);
        assert_eq!(10.0, buckets.counters.get_mut(&rmname).unwrap()[0].1.value);

        let metric_two = Metric::new("some.metric", 1.0).counter(0.5);
        buckets.add(metric_two);
        assert_eq!(12.0, buckets.counters.get_mut(&rmname).unwrap()[0].1.value);
    }

    #[test]
    fn test_add_gauge_metric() {
        let mut buckets = Buckets::default();
        let rmname = String::from("some.metric");
        let metric = Metric::new("some.metric", 11.5).gauge();
        buckets.add(metric);
        assert!(buckets.gauges.contains_key(&rmname),
                "Should contain the metric key");
        assert_eq!(11.5, buckets.gauges.get_mut(&rmname).unwrap()[0].1.value);
        assert_eq!(1, buckets.gauges().len());
        assert_eq!(0, buckets.counters().len());
    }

    #[test]
    fn test_add_delta_gauge_metric() {
        let mut buckets = Buckets::default();
        let rmname = String::from("some.metric");
        let metric = Metric::new("some.metric", 100.0).gauge();
        buckets.add(metric);
        let delta_metric = Metric::new("some.metric", -11.5).delta_gauge();
        buckets.add(delta_metric);
        assert!(buckets.gauges.contains_key(&rmname),
                "Should contain the metric key");
        assert_eq!(88.5, buckets.gauges.get_mut(&rmname).unwrap()[0].1.value);
        assert_eq!(1, buckets.gauges().len());
        assert_eq!(0, buckets.counters().len());
    }

    #[test]
    fn test_reset_add_delta_gauge_metric() {
        let mut buckets = Buckets::default();
        let rmname = String::from("some.metric");
        let metric = Metric::new("some.metric", 100.0).gauge();
        buckets.add(metric);
        let delta_metric = Metric::new("some.metric", -11.5).delta_gauge();
        buckets.add(delta_metric);
        let reset_metric = Metric::new("some.metric", 2007.3).gauge();
        buckets.add(reset_metric);
        assert!(buckets.gauges.contains_key(&rmname),
                "Should contain the metric key");
        assert_eq!(2007.3, buckets.gauges.get_mut(&rmname).unwrap()[0].1.value);
        assert_eq!(1, buckets.gauges().len());
        assert_eq!(0, buckets.counters().len());
    }

    #[test]
    fn test_add_timer_metric() {
        let mut buckets = Buckets::default();
        let rmname = String::from("some.metric");
        let metric = Metric::new("some.metric", 11.5).timer();
        buckets.add(metric);
        assert!(buckets.timers.contains_key(&rmname),
                "Should contain the metric key");

        assert_eq!(Some((1, 11.5)),
                   buckets.timers.get_mut(&rmname).expect("hwhap")[0].1.query(0.0));

        let metric_two = Metric::new("some.metric", 99.5).timer();
        buckets.add(metric_two);

        let romname = String::from("other.metric");
        let metric_three = Metric::new("other.metric", 811.5).timer();
        buckets.add(metric_three);
        assert!(buckets.timers.contains_key(&romname),
                "Should contain the metric key");
        assert!(buckets.timers.contains_key(&romname),
                "Should contain the metric key");
    }

    #[test]
    fn test_raws_store_one_point_per_second() {
        let mut buckets = Buckets::default();
        let dt_0 = UTC.ymd(2016, 9, 13).and_hms_milli(11, 30, 0, 0).timestamp();
        let dt_1 = UTC.ymd(2016, 9, 13).and_hms_milli(11, 30, 1, 0).timestamp();

        buckets.add(Metric::new("some.metric", 1.0).time(dt_0));
        buckets.add(Metric::new("some.metric", 2.0).time(dt_0));
        buckets.add(Metric::new("some.metric", 3.0).time(dt_0));
        buckets.add(Metric::new("some.metric", 4.0).time(dt_1));

        let mname = String::from("some.metric");
        assert!(buckets.raws.contains_key(&mname), "Should contain the metric key");

        assert_eq!(Some(&mut vec![
            (dt_0, Metric::new("some.metric", 3.0).time(dt_0)),
            (dt_1, Metric::new("some.metric", 4.0).time(dt_1)),
        ]), buckets.raws.get_mut(&mname));
    }

    #[test]
    fn unique_names_preserved_counters() {
        fn qos_ret(ms: Vec<Metric>) -> TestResult {
            let mut bucket = Buckets::default();

            for m in ms.clone() {
                bucket.add(m);
            }

            let cnts: HashSet<String> = ms.iter().fold(HashSet::default(), |mut acc, ref m| {
                match m.kind {
                    MetricKind::Counter(_) => {
                        acc.insert(m.name.clone());
                        acc
                    }
                    _ => acc,
                }
            });
            let b_cnts: HashSet<String> =
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

            let gauges: HashSet<String> = ms.iter().fold(HashSet::default(), |mut acc, ref m| {
                match m.kind {
                    MetricKind::Gauge | MetricKind::DeltaGauge => {
                        acc.insert(m.name.clone());
                        acc
                    }
                    _ => acc,
                }
            });
            let b_gauges: HashSet<String> =
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

            let hist: HashSet<String> = ms.iter().fold(HashSet::default(), |mut acc, ref m| {
                match m.kind {
                    MetricKind::Histogram => {
                        acc.insert(m.name.clone());
                        acc
                    }
                    _ => acc,
                }
            });
            let b_hist: HashSet<String> =
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

            let tm: HashSet<String> = ms.iter().fold(HashSet::default(), |mut acc, ref m| {
                match m.kind {
                    MetricKind::Timer => {
                        acc.insert(m.name.clone());
                        acc
                    }
                    _ => acc,
                }
            });
            let b_tm: HashSet<String> =
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

        let m0 = Metric::new("test.gauge_0", 1.0).gauge();
        let m1 = Metric::new("test.gauge_1", 1.0).gauge();
        buckets.add(m0.clone());
        buckets.add(m1.clone());

        assert_eq!(m0,
                   buckets.gauges.get_mut(&String::from("test.gauge_0")).unwrap()[0].1);
        assert_eq!(m1,
                   buckets.gauges.get_mut(&String::from("test.gauge_1")).unwrap()[0].1);
    }

    #[test]
    fn test_counter_insertion() {
        let mut buckets = Buckets::default();

        let m0 = Metric::new("test.counter_0", 1.0).counter(1.0);
        let m1 = Metric::new("test.counter_1", 1.0).counter(1.0);
        buckets.add(m0.clone());
        buckets.add(m1.clone());

        assert_eq!(m0,
                   buckets.counters.get_mut(&String::from("test.counter_0")).unwrap()[0].1);
        assert_eq!(m1,
                   buckets.counters.get_mut(&String::from("test.counter_1")).unwrap()[0].1);
    }

    #[test]
    fn test_counter_summations() {
        fn qos_ret(ms: Vec<Metric>) -> TestResult {
            let mut bucket = Buckets::default();

            for m in ms.clone() {
                bucket.add(m);
            }

            let mut cnts: HashMap<String, Vec<(i64, f64)>> = HashMap::default();
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
                v.sort_by_key(|&(t, _)| t);

                let ref cnts_v = *cnts.get(k).unwrap();
                assert_eq!(cnts_v.len(), v.len());

                for (i, c_v) in cnts_v.iter().enumerate() {
                    assert_eq!(c_v.0, v[i].1.time);
                    assert_eq!(c_v.1, v[i].1.value);
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

            let mut cnts: HashMap<String, Vec<(i64, f64)>> = HashMap::default();
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
                v.sort_by_key(|&(t, _)| t);

                let ref cnts_v = *cnts.get(k).unwrap();
                assert_eq!(cnts_v.len(), v.len());

                for (i, c_v) in cnts_v.iter().enumerate() {
                    assert_eq!(c_v.0, v[i].1.time);
                    assert_eq!(c_v.1, v[i].1.value);
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
