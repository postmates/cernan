//! Buckets are the primary internal storage type.
//!
//! Each bucket contains a set of hashmaps containing
//! each set of metrics received by clients.

use fnv::FnvHasher;
use metric::{Metric, MetricKind};
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use time;

pub type HashMapFnv<K, V> = HashMap<K, V, BuildHasherDefault<FnvHasher>>;
pub type AggrMap = HashMapFnv<String, Vec<Metric>>;

/// Buckets stores all metrics until they are flushed.
pub struct Buckets {
    counters: AggrMap,
    delta_gauges: AggrMap,
    gauges: AggrMap,
    histograms: AggrMap,
    raws: AggrMap,
    timers: AggrMap,

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
    /// assert_eq!(0, bucket.counters().len());
    /// ```
    fn default() -> Buckets {
        Buckets {
            counters: HashMapFnv::default(),
            gauges: HashMapFnv::default(),
            delta_gauges: HashMapFnv::default(),
            raws: HashMapFnv::default(),
            timers: HashMapFnv::default(),
            histograms: HashMapFnv::default(),
            bin_width: 1,
        }
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
    /// let metric = cernan::metric::Metric::new("foo", 1.0).counter();
    /// let mut buckets = cernan::buckets::Buckets::default();
    /// let rname = String::from("foo");
    ///
    /// assert_eq!(true, buckets.counters().is_empty());
    ///
    /// buckets.add(metric);
    /// assert_eq!(false, buckets.counters().is_empty());
    /// buckets.reset();
    /// assert_eq!(true, buckets.counters().is_empty());
    /// ```
    pub fn reset(&mut self) {
        self.counters.clear();
        self.raws.clear();
        self.histograms.clear();
        self.timers.clear();
        self.gauges.clear();
        for (_, v) in &mut self.delta_gauges {
            let len = v.len();
            if len > 0 {
                v.swap(0, len - 1);
                v.truncate(1);
                v[0].time = time::now();
            }
        }
    }

    /// Adds a metric to the bucket storage.
    ///
    /// # Examples
    /// ```
    /// extern crate cernan;
    ///
    /// let metric = cernan::metric::Metric::new("foo", 1.0).counter();
    /// let mut bucket = cernan::buckets::Buckets::default();
    /// bucket.add(metric);
    /// ```
    pub fn add(&mut self, value: Metric) {
        let name = value.name.to_owned();
        let bkt = match value.kind {
            MetricKind::Counter => &mut self.counters,
            MetricKind::Gauge => &mut self.gauges,
            MetricKind::DeltaGauge => &mut self.delta_gauges,
            MetricKind::Raw => &mut self.raws,
            MetricKind::Timer => &mut self.timers,
            MetricKind::Histogram => &mut self.histograms,
        };
        let hsh = bkt.entry(name).or_insert_with(|| Default::default());
        let bin_width = self.bin_width;
        match hsh.binary_search_by(|probe| probe.within(bin_width, &value)) {
            Ok(idx) => hsh[idx] += value,
            Err(idx) => {
                match value.kind {
                    MetricKind::DeltaGauge => {
                        if idx > 0 {
                            let mut cur: Metric = hsh[idx - 1].clone().time(value.time);
                            cur += value;
                            hsh.insert(idx, cur)
                        } else {
                            hsh.insert(idx, value)
                        }
                    }
                    _ => hsh.insert(idx, value),
                }
            }
        }
    }

    pub fn count(&self) -> u64 {
        (self.counters.len() + self.gauges.len() + self.delta_gauges.len() +
         self.raws.len() + self.timers.len() + self.histograms.len()) as u64
    }

    pub fn counters(&self) -> &AggrMap {
        &self.counters
    }

    pub fn gauges(&self) -> &AggrMap {
        &self.gauges
    }

    pub fn delta_gauges(&self) -> &AggrMap {
        &self.delta_gauges
    }

    pub fn raws(&self) -> &AggrMap {
        &self.raws
    }

    pub fn histograms(&self) -> &AggrMap {
        &self.histograms
    }

    pub fn timers(&self) -> &AggrMap {
        &self.timers
    }
}

// Tests
//
#[cfg(test)]
mod test {
    extern crate quickcheck;

    use chrono::{TimeZone, UTC};
    use metric::{Metric, MetricKind};
    use self::quickcheck::{QuickCheck, TestResult};
    use std::cmp::Ordering;
    use std::collections::{HashMap, HashSet};
    use super::*;

    fn within(width: i64, lhs: i64, rhs: i64) -> bool {
        (lhs / width) == (rhs / width)
    }

    #[test]
    fn raw_test_variable() {
        let bin_width = 66;
        let m0 = Metric::new("lO", 0.807).time(18);
        let m4 = Metric::new("lO", 0.361).time(75);
        let m7 = Metric::new("lO", 0.291).time(42);

        let mut bkt = Buckets::new(bin_width);
        bkt.add(m0);
        bkt.add(m4);
        bkt.add(m7);

        let raws = bkt.raws().get("lO").unwrap();
        println!("RAWS: {:?}", raws);
        let ref res = raws[0];
        assert_eq!(18, res.time);
        assert_eq!(Some(0.291), res.value());
    }

    #[test]
    fn test_gauge_small_bin_width() {
        let bin_width = 1;
        let m0 = Metric::new("lO", 3.211).time(645181811).gauge();
        let m1 = Metric::new("lO", 4.322).time(645181812).gauge();
        let m2 = Metric::new("lO", 5.433).time(645181813).gauge();

        let mut bkt = Buckets::new(bin_width);
        bkt.add(m0);
        bkt.add(m1);
        bkt.add(m2);

        let v = bkt.gauges().get("lO").unwrap();
        assert_eq!(3, v.len());
    }

    #[test]
    fn variable_size_bins() {
        fn inner(bin_width: u16, ms: Vec<Metric>) -> TestResult {
            let bin_width: i64 = bin_width as i64;
            if bin_width == 0 {
                return TestResult::discard();
            }

            let mut bucket = Buckets::new(bin_width);
            for m in ms.clone() {
                bucket.add(m);
            }

            let mut mp: Vec<Metric> = Vec::new();
            let fill_ms = ms.clone();
            for m in fill_ms {
                match mp.binary_search_by(|probe| probe.within(bin_width, &m)) {
                    Ok(idx) => mp[idx] += m,
                    Err(idx) => mp.insert(idx, m),
                }
            }

            let checks = bucket.gauges()
                .iter()
                .chain(bucket.counters().iter())
                .chain(bucket.raws().iter())
                .chain(bucket.histograms().iter())
                .chain(bucket.timers().iter());
            for (name, vs) in checks {
                for v in vs {
                    let ref kind = v.kind;
                    let time = v.time;
                    let mut found_one = false;
                    for m in &mp {
                        if (&m.name == name) && (&m.kind == kind) &&
                           within(bin_width, m.time, time) {
                            assert_eq!(Ordering::Equal, m.within(bin_width, v));
                            found_one = true;
                            debug_assert!(v.value() == m.value(),
                                          "{:?} != {:?} |::|::| MP: {:?} |::|::| MS: {:?}",
                                          v.value(),
                                          m.value(),
                                          mp,
                                          ms);
                        }
                    }
                    debug_assert!(found_one,
                                  "DID NOT FIND ONE FOR {:?} |::|::| MP: {:?} |::|::| MS: {:?}",
                                  v,
                                  mp,
                                  ms);
                }
            }
            TestResult::passed()
        }
        QuickCheck::new().quickcheck(inner as fn(u16, Vec<Metric>) -> TestResult);
    }

    #[test]
    fn test_add_increments_total_messages() {
        let mut buckets = Buckets::default();
        // duff value to ensure it changes.
        let metric = Metric::new("some.metric", 1.0).counter();
        buckets.add(metric);
    }
    #[test]
    fn test_add_gauge_metric_distinct_tags() {
        let mut buckets = Buckets::default();
        let m0 = Metric::new("some.metric", 1.0).gauge().time(10).overlay_tag("foo", "bar");
        let m1 = Metric::new("some.metric", 1.0).gauge().time(10).overlay_tag("foo", "bingo");

        buckets.add(m0.clone());
        buckets.add(m1.clone());

        let res = buckets.gauges().get("some.metric");
        assert!(res.is_some());
        let res = res.unwrap();
        assert_eq!(Some(&m0), res.get(0));
        assert_eq!(Some(&m1), res.get(1));
    }

    #[test]
    fn test_add_counter_metric() {
        let mut buckets = Buckets::default();
        let metric = Metric::new("some.metric", 1.0).counter();
        buckets.add(metric.clone());

        let rmname = String::from("some.metric");
        assert!(buckets.counters.contains_key(&rmname),
                "Should contain the metric key");
        assert_eq!(Some(1.0),
                   buckets.counters.get_mut(&rmname).unwrap()[0].value());

        // Increment counter
        buckets.add(metric);
        assert_eq!(Some(2.0),
                   buckets.counters.get_mut(&rmname).unwrap()[0].value());
        assert_eq!(1, buckets.counters().len());
        assert_eq!(0, buckets.gauges().len());
    }

    #[test]
    fn test_reset_add_counter_metric() {
        let mut buckets = Buckets::default();
        let m0 = Metric::new("some.metric", 1.0).counter().time(101);
        let m1 = m0.clone();

        buckets.add(m0);

        let rmname = String::from("some.metric");
        assert!(buckets.counters.contains_key(&rmname),
                "Should contain the metric key");
        assert_eq!(Some(1.0),
                   buckets.counters.get_mut(&rmname).unwrap()[0].value());

        // Increment counter
        buckets.add(m1);
        assert_eq!(Some(2.0),
                   buckets.counters.get_mut(&rmname).unwrap()[0].value());
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
    fn test_add_gauge_metric() {
        let mut buckets = Buckets::default();
        let rmname = String::from("some.metric");
        let metric = Metric::new("some.metric", 11.5).gauge();
        buckets.add(metric);
        assert!(buckets.gauges.contains_key(&rmname),
                "Should contain the metric key");
        assert_eq!(Some(11.5),
                   buckets.gauges.get_mut(&rmname).unwrap()[0].value());
        assert_eq!(1, buckets.gauges().len());
        assert_eq!(0, buckets.counters().len());
    }

    #[test]
    fn test_add_delta_gauge_metric() {
        // A gauge that transitions from gauge to delta gauge does _not_ retain
        // its previous value. Other statsd aggregations require that you reset
        // the value to zero before moving to a delta gauge. Cernan does not
        // require this explicit reset, only the +/- on the metric value.
        let mut buckets = Buckets::default();
        let rmname = String::from("some.metric");
        let metric = Metric::new("some.metric", 100.0).gauge();
        buckets.add(metric);
        let delta_metric = Metric::new("some.metric", -11.5).delta_gauge();
        buckets.add(delta_metric);
        assert!(buckets.gauges.contains_key(&rmname),
                "Should contain the metric key");
        assert_eq!(Some(100.0),
                   buckets.gauges.get_mut(&rmname).unwrap()[0].value());
        assert_eq!(Some(-11.5),
                   buckets.delta_gauges.get_mut(&rmname).unwrap()[0].value());
        assert_eq!(1, buckets.gauges().len());
        assert_eq!(1, buckets.delta_gauges().len());
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
        assert_eq!(Some(2007.3),
                   buckets.gauges.get_mut(&rmname).unwrap()[0].value());
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

        assert_eq!(Some(11.5),
                   buckets.timers.get_mut(&rmname).expect("hwhap")[0].query(0.0));

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
        assert!(buckets.raws.contains_key(&mname),
                "Should contain the metric key");

        let metrics = buckets.raws.get_mut(&mname).unwrap();
        assert_eq!(2, metrics.len());

        assert_eq!(dt_0, metrics[0].time);
        assert_eq!(Some(3.0), metrics[0].value());

        assert_eq!(dt_1, metrics[1].time);
        assert_eq!(Some(4.0), metrics[1].value());
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
                    MetricKind::Counter => {
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
        QuickCheck::new().quickcheck(qos_ret as fn(Vec<Metric>) -> TestResult);
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
                    MetricKind::Gauge => {
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
        QuickCheck::new().quickcheck(qos_ret as fn(Vec<Metric>) -> TestResult);
    }

    #[test]
    fn unique_names_preserved_delta_gauges() {
        fn qos_ret(ms: Vec<Metric>) -> TestResult {
            let mut bucket = Buckets::default();

            for m in ms.clone() {
                bucket.add(m);
            }

            let dgauges: HashSet<String> = ms.iter().fold(HashSet::default(), |mut acc, ref m| {
                match m.kind {
                    MetricKind::DeltaGauge => {
                        acc.insert(m.name.clone());
                        acc
                    }
                    _ => acc,
                }
            });
            let b_gauges: HashSet<String> =
                bucket.delta_gauges().iter().fold(HashSet::default(), |mut acc, (k, _)| {
                    acc.insert(k.clone());
                    acc
                });
            assert_eq!(dgauges, b_gauges);

            TestResult::passed()
        }
        QuickCheck::new().quickcheck(qos_ret as fn(Vec<Metric>) -> TestResult);
    }

    #[test]
    fn correct_bin_count() {
        fn inner(ms: Vec<Metric>) -> TestResult {
            let mut bucket = Buckets::new(1);

            for m in ms.clone() {
                bucket.add(m);
            }

            let mut coll: HashMap<(MetricKind, String), HashSet<i64>> = HashMap::new();
            for m in ms {
                let kind = m.kind;
                let name = m.name;
                let time = m.time;

                let entry = coll.entry((kind, name)).or_insert(HashSet::new());
                (*entry).insert(time);
            }

            let mut expected_counters = 0;
            let mut expected_gauges = 0;
            let mut expected_delta_gauges = 0;
            let mut expected_raws = 0;
            let mut expected_histograms = 0;
            let mut expected_timers = 0;

            for (&(ref kind, _), val) in coll.iter() {
                match kind {
                    &MetricKind::Gauge => expected_gauges += val.len(),
                    &MetricKind::DeltaGauge => expected_delta_gauges += val.len(),
                    &MetricKind::Counter => expected_counters += val.len(),
                    &MetricKind::Timer => expected_timers += val.len(),
                    &MetricKind::Histogram => expected_histograms += val.len(),
                    &MetricKind::Raw => expected_raws += val.len(),
                }
            }
            assert_eq!(expected_raws,
                       bucket.raws().iter().fold(0, |acc, (_k, v)| acc + v.len()));
            assert_eq!(expected_counters,
                       bucket.counters().iter().fold(0, |acc, (_k, v)| acc + v.len()));
            assert_eq!(expected_gauges,
                       bucket.gauges().iter().fold(0, |acc, (_k, v)| acc + v.len()));
            assert_eq!(expected_delta_gauges,
                       bucket.delta_gauges().iter().fold(0, |acc, (_k, v)| acc + v.len()));
            assert_eq!(expected_histograms,
                       bucket.histograms().iter().fold(0, |acc, (_k, v)| acc + v.len()));
            assert_eq!(expected_timers,
                       bucket.timers().iter().fold(0, |acc, (_k, v)| acc + v.len()));

            TestResult::passed()
        }
        QuickCheck::new().quickcheck(inner as fn(Vec<Metric>) -> TestResult);
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
        QuickCheck::new().quickcheck(qos_ret as fn(Vec<Metric>) -> TestResult);
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
        QuickCheck::new().quickcheck(qos_ret as fn(Vec<Metric>) -> TestResult);
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
        QuickCheck::new().quickcheck(qos_ret as fn(Vec<Metric>) -> TestResult);
    }

    #[test]
    fn test_gauge_insertion() {
        let mut buckets = Buckets::default();

        let m0 = Metric::new("test.gauge_0", 1.0).gauge();
        let m1 = Metric::new("test.gauge_1", 1.0).gauge();
        buckets.add(m0.clone());
        buckets.add(m1.clone());

        assert_eq!(m0,
                   buckets.gauges.get_mut(&String::from("test.gauge_0")).unwrap()[0]);
        assert_eq!(m1,
                   buckets.gauges.get_mut(&String::from("test.gauge_1")).unwrap()[0]);
    }

    #[test]
    fn test_counter_insertion() {
        let mut buckets = Buckets::default();

        let m0 = Metric::new("test.counter_0", 1.0).counter();
        let m1 = Metric::new("test.counter_1", 1.0).counter();
        buckets.add(m0.clone());
        buckets.add(m1.clone());

        assert_eq!(m0,
                   buckets.counters.get_mut(&String::from("test.counter_0")).unwrap()[0]);
        assert_eq!(m1,
                   buckets.counters.get_mut(&String::from("test.counter_1")).unwrap()[0]);
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
                    MetricKind::Counter => {
                        let c = cnts.entry(m.name.clone()).or_insert(vec![]);
                        match c.binary_search_by_key(&m.time, |&(a, _)| a) {
                            Ok(idx) => c[idx].1 += m.value().unwrap_or(0.0),
                            Err(idx) => c.insert(idx, (m.time, m.value().unwrap_or(0.0))),
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
                    assert_eq!(Some(c_v.1), v[i].value());
                }
            }

            TestResult::passed()
        }
        QuickCheck::new().quickcheck(qos_ret as fn(Vec<Metric>) -> TestResult);
    }

    #[test]
    fn test_gauge_behaviour() {
        fn inner(ms: Vec<Metric>) -> TestResult {
            let mut bucket = Buckets::default();

            for m in ms.clone() {
                bucket.add(m);
            }

            let mut g_cnts: HashMap<String, Vec<(i64, f64)>> = HashMap::default();
            let mut dg_cnts: HashMap<String, Vec<(i64, f64)>> = HashMap::default();
            for m in ms {
                match m.kind {
                    MetricKind::Gauge => {
                        let c = g_cnts.entry(m.name.clone()).or_insert(vec![]);
                        match c.binary_search_by_key(&m.time, |&(a, _)| a) {
                            Ok(idx) => c[idx] = (m.time, m.value().unwrap()),
                            Err(idx) => c.insert(idx, (m.time, m.value().unwrap())),
                        }
                    }
                    MetricKind::DeltaGauge => {
                        let c = dg_cnts.entry(m.name.clone()).or_insert(vec![]);
                        match c.binary_search_by_key(&m.time, |&(a, _)| a) {
                            Ok(idx) => c[idx].1 += m.value().unwrap(),
                            Err(idx) => {
                                if idx > 0 {
                                    let mut val = c[idx - 1].1.clone();
                                    val += m.value().unwrap();
                                    c.insert(idx, (m.time, val))
                                } else {
                                    c.insert(idx, (m.time, m.value().unwrap()))
                                }
                            }
                        }
                    }
                    _ => continue,
                }
            }

            assert_eq!(bucket.gauges().len(), g_cnts.len());
            for (k, v) in bucket.gauges().iter() {
                let mut v = v.clone();
                v.sort_by_key(|ref m| m.time);

                let ref g_cnts_v = *g_cnts.get(k).unwrap();
                assert_eq!(g_cnts_v.len(), v.len());

                for (i, c_v) in g_cnts_v.iter().enumerate() {
                    assert_eq!(c_v.0, v[i].time);
                    assert_eq!(Some(c_v.1), v[i].value());
                }
            }
            assert_eq!(bucket.delta_gauges().len(), dg_cnts.len());
            for (k, v) in bucket.delta_gauges().iter() {
                let mut v = v.clone();
                v.sort_by_key(|ref m| m.time);

                let ref dg_cnts_v = *dg_cnts.get(k).unwrap();
                assert_eq!(dg_cnts_v.len(), v.len());

                for (i, c_v) in dg_cnts_v.iter().enumerate() {
                    assert_eq!(c_v.0, v[i].time);
                    assert_eq!(Some(c_v.1), v[i].value());
                }
            }

            TestResult::passed()
        }
        QuickCheck::new().quickcheck(inner as fn(Vec<Metric>) -> TestResult);
    }
}
