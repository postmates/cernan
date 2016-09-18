//! Buckets are the primary internal storage type.
//!
//! Each bucket contains a set of hashmaps containing
//! each set of metrics received by clients.

use super::metric::{Metric, MetricKind};
use quantiles::CKMS;
use lru_cache::LruCache;
use string_cache::Atom;

/// Buckets stores all metrics until they are flushed.
pub struct Buckets {
    counters: LruCache<Atom, f64>,
    gauges: LruCache<Atom, f64>,
    raws: LruCache<Atom, f64>,
    timers: LruCache<Atom, CKMS<f64>>,
    histograms: LruCache<Atom, CKMS<f64>>,
}

impl Buckets {
    /// Create a new Buckets
    ///
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::buckets::Buckets;
    ///
    /// let bucket = Buckets::new();
    /// assert_eq!(0, bucket.counters().len());
    /// ```
    pub fn new() -> Buckets {
        Buckets {
            counters: LruCache::new(10000),
            gauges: LruCache::new(10000),
            raws: LruCache::new(10000),
            timers: LruCache::new(10000),
            histograms: LruCache::new(10000),
        }
    }

    /// Resets appropriate aggregates
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate cernan;
    /// extern crate string_cache;
    ///
    /// let metric = cernan::metric::Metric::parse_statsd("foo:1|c").unwrap();
    /// let mut buckets = cernan::buckets::Buckets::new();
    /// let rname = string_cache::Atom::from("foo");
    ///
    /// assert_eq!(true, buckets.counters().is_empty());
    ///
    /// buckets.add(&metric[0]);
    /// assert_eq!(false, buckets.counters().is_empty());
    /// buckets.reset();
    /// assert_eq!(true, buckets.counters().is_empty());
    /// ```
    pub fn reset(&mut self) {
        self.counters.clear();
        self.raws.clear();
    }

    /// Adds a metric to the bucket storage.
    ///
    /// # Examples
    /// ```
    /// extern crate cernan;
    ///
    /// let metric = cernan::metric::Metric::parse_statsd("foo:1|c").unwrap();
    /// let mut bucket = cernan::buckets::Buckets::new();
    /// bucket.add(&metric[0]);
    /// ```
    pub fn add(&mut self, value: &Metric) {
        let name = value.name.to_owned();
        match value.kind {
            MetricKind::Counter(rate) => {
                if !self.counters.contains_key(&name) {
                    let _ = self.counters.insert(value.name.to_owned(), 0.0);
                };
                let counter =
                    self.counters.get_mut(&name).expect("shouldn't happen but did, counter");
                *counter += value.value * (1.0 / rate);
            }
            MetricKind::DeltaGauge => {
                if self.gauges.contains_key(&name) {
                    let gauge =
                        self.gauges.get_mut(&name).expect("shouldn't happen but did, gauge");
                    *gauge += value.value;
                } else {
                    self.gauges.insert(name, value.value);
                }
            }
            MetricKind::Gauge => {
                self.gauges.insert(name, value.value);
            }
            MetricKind::Raw => {
                self.raws.insert(name, value.value);
            }
            MetricKind::Histogram => {
                if !self.histograms.contains_key(&name) {
                    let _ = self.histograms.insert(value.name.to_owned(), CKMS::<f64>::new(0.001));
                };
                let hist =
                    self.histograms.get_mut(&name).expect("shouldn't happen but did, histogram");
                (*hist).insert(value.value);
            }
            MetricKind::Timer => {
                if !self.timers.contains_key(&name) {
                    let _ = self.timers.insert(value.name.to_owned(), CKMS::new(0.001));
                };
                let tm = self.timers.get_mut(&name).expect("shouldn't happen but did, timer");
                (*tm).insert(value.value);
            }
        }
    }

    /// Get the counters as a borrowed reference.
    pub fn counters(&self) -> &LruCache<Atom, f64> {
        &self.counters
    }

    /// Get the gauges as a borrowed reference.
    pub fn gauges(&self) -> &LruCache<Atom, f64> {
        &self.gauges
    }

    /// Get the histograms as a borrowed reference.
    pub fn histograms(&self) -> &LruCache<Atom, CKMS<f64>> {
        &self.histograms
    }

    /// Get the timers as a borrowed reference.
    pub fn timers(&self) -> &LruCache<Atom, CKMS<f64>> {
        &self.timers
    }
}


// Tests
//
#[cfg(test)]
mod test {
    use super::*;
    use metric::{Metric, MetricKind, MetricSign};
    use string_cache::Atom;

    #[test]
    fn test_add_increments_total_messages() {
        let mut buckets = Buckets::new();
        // duff value to ensure it changes.
        let metric = Metric::new(Atom::from("some.metric"),
                                 1.0,
                                 MetricKind::Counter(1.0),
                                 None);
        buckets.add(&metric);
    }

    #[test]
    fn test_add_counter_metric() {
        let mut buckets = Buckets::new();
        let mname = Atom::from("some.metric");
        let metric = Metric::new(mname, 1.0, MetricKind::Counter(1.0), None);
        buckets.add(&metric);

        let rmname = Atom::from("some.metric");
        assert!(buckets.counters.contains_key(&rmname),
                "Should contain the metric key");
        assert_eq!(Some(&mut 1.0), buckets.counters.get_mut(&rmname));

        // Increment counter
        buckets.add(&metric);
        assert_eq!(Some(&mut 2.0), buckets.counters.get_mut(&rmname));
        assert_eq!(1, buckets.counters().len());
        assert_eq!(0, buckets.gauges().len());
    }

    #[test]
    fn test_add_counter_metric_reset() {
        let mut buckets = Buckets::new();
        let mname = Atom::from("some.metric");
        let metric = Metric::new(mname, 1.0, MetricKind::Counter(1.0), None);
        buckets.add(&metric);

        let rmname = Atom::from("some.metric");
        assert!(buckets.counters.contains_key(&rmname),
                "Should contain the metric key");
        assert_eq!(Some(&mut 1.0), buckets.counters.get_mut(&rmname));

        // Increment counter
        buckets.add(&metric);
        assert_eq!(Some(&mut 2.0), buckets.counters.get_mut(&rmname));
        assert_eq!(1, buckets.counters().len());

        buckets.reset();
        assert_eq!(None, buckets.counters.get_mut(&rmname));
    }

    #[test]
    fn test_add_counter_metric_sampled() {
        let mut buckets = Buckets::new();
        let metric = Metric::new(Atom::from("some.metric"),
                                 1.0,
                                 MetricKind::Counter(0.1),
                                 None);

        let rmname = Atom::from("some.metric");

        buckets.add(&metric);
        assert_eq!(Some(&mut 10.0), buckets.counters.get_mut(&rmname));

        let metric_two = Metric::new(Atom::from("some.metric"),
                                     1.0,
                                     MetricKind::Counter(0.5),
                                     None);
        buckets.add(&metric_two);
        assert_eq!(Some(&mut 12.0), buckets.counters.get_mut(&rmname));
    }

    #[test]
    fn test_add_gauge_metric() {
        let mut buckets = Buckets::new();
        let rmname = Atom::from("some.metric");
        let metric = Metric::new(Atom::from("some.metric"), 11.5, MetricKind::Gauge, None);
        buckets.add(&metric);
        assert!(buckets.gauges.contains_key(&rmname),
                "Should contain the metric key");
        assert_eq!(Some(&mut 11.5), buckets.gauges.get_mut(&rmname));
        assert_eq!(1, buckets.gauges().len());
        assert_eq!(0, buckets.counters().len());
    }

    #[test]
    fn test_add_delta_gauge_metric() {
        let mut buckets = Buckets::new();
        let rmname = Atom::from("some.metric");
        let metric = Metric::new(Atom::from("some.metric"), 100.0, MetricKind::Gauge, None);
        buckets.add(&metric);
        let delta_metric = Metric::new(Atom::from("some.metric"),
                                       11.5,
                                       MetricKind::Gauge,
                                       Some(MetricSign::Negative));
        buckets.add(&delta_metric);
        assert!(buckets.gauges.contains_key(&rmname),
                "Should contain the metric key");
        assert_eq!(Some(&mut 88.5), buckets.gauges.get_mut(&rmname));
        assert_eq!(1, buckets.gauges().len());
        assert_eq!(0, buckets.counters().len());
    }

    #[test]
    fn test_add_delta_gauge_metric_reset() {
        let mut buckets = Buckets::new();
        let rmname = Atom::from("some.metric");
        let metric = Metric::new(Atom::from("some.metric"), 100.0, MetricKind::Gauge, None);
        buckets.add(&metric);
        let delta_metric = Metric::new(Atom::from("some.metric"),
                                       11.5,
                                       MetricKind::Gauge,
                                       Some(MetricSign::Negative));
        buckets.add(&delta_metric);
        let reset_metric = Metric::new(Atom::from("some.metric"), 2007.3, MetricKind::Gauge, None);
        buckets.add(&reset_metric);
        assert!(buckets.gauges.contains_key(&rmname),
                "Should contain the metric key");
        assert_eq!(Some(&mut 2007.3), buckets.gauges.get_mut(&rmname));
        assert_eq!(1, buckets.gauges().len());
        assert_eq!(0, buckets.counters().len());
    }

    #[test]
    fn test_add_timer_metric() {
        let mut buckets = Buckets::new();
        let rmname = Atom::from("some.metric");
        let metric = Metric::new(Atom::from("some.metric"), 11.5, MetricKind::Timer, None);
        buckets.add(&metric);
        assert!(buckets.timers.contains_key(&rmname),
                "Should contain the metric key");

        assert_eq!(Some((1, 11.5)),
                   buckets.timers.get_mut(&rmname).expect("hwhap").query(0.0));

        let metric_two = Metric::new(Atom::from("some.metric"), 99.5, MetricKind::Timer, None);
        buckets.add(&metric_two);

        let romname = Atom::from("other.metric");
        let metric_three = Metric::new(Atom::from("other.metric"), 811.5, MetricKind::Timer, None);
        buckets.add(&metric_three);
        assert!(buckets.timers.contains_key(&romname),
                "Should contain the metric key");
        assert!(buckets.timers.contains_key(&romname),
                "Should contain the metric key");

        // assert_eq!(Some(&mut vec![11.5, 99.5]), buckets.timers.get_mut("some.metric"));
        // assert_eq!(Some(&mut vec![811.5]), buckets.timers.get_mut("other.metric"));
    }
}
