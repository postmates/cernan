//! Buckets are the primary internal storage type.
//!
//! Each bucket contains a set of hashmaps containing
//! each set of metrics received by clients.

use super::metric::{Metric, MetricKind};
use time;
use quantiles::CKMS;
use lru_cache::LruCache;

/// Buckets stores all metrics until they are flushed.
pub struct Buckets {
    counters: LruCache<String, f64>,
    gauges: LruCache<String, f64>,
    timers: LruCache<String, CKMS<f64>>,
    histograms: LruCache<String, CKMS<f64>>,

    server_start_time: time::Timespec,
    last_message: time::Timespec,
    bad_messages: usize,
    total_messages: usize,
}

impl Buckets {
    /// Create a new Buckets
    ///
    ///
    /// # Examples
    ///
    /// ```
    /// let bucket = Buckets::new();
    /// assert_eq!(0, bucket.counters().len());
    /// ```
    pub fn new() -> Buckets {
        Buckets {
            counters: LruCache::new(10000),
            gauges: LruCache::new(10000),
            timers: LruCache::new(10000),
            histograms: LruCache::new(10000),
            bad_messages: 0,
            total_messages: 0,
            last_message: time::get_time(),
            server_start_time: time::get_time(),
        }
    }

    /// Adds a metric to the bucket storage.
    ///
    /// # Examples
    ///
    /// ```
    /// use buckets::Buckets;
    /// use super::metric;
    /// use std::str::FromStr;
    ///
    /// let metric = metric::Metric::FromStr("foo:1|c");
    /// let mut bucket = Buckets::new();
    /// bucket.add(metric);
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
            MetricKind::Gauge => {
                self.gauges.insert(name, value.value);
            }
            MetricKind::Histogram => {
                if !self.histograms.contains_key(&name) {
                    let _ = self.histograms.insert(value.name.to_owned(), CKMS::<f64>::new(0.001));
                };
                let hist =
                    self.histograms.get_mut(&name).expect("shouldn't happen but did, histogram");
                let _ = (*hist).insert(value.value);
            }
            MetricKind::Timer => {
                if !self.timers.contains_key(&name) {
                    let _ = self.timers.insert(value.name.to_owned(), CKMS::new(0.001));
                };
                let tm = self.timers.get_mut(&name).expect("shouldn't happen but did, timer");
                let _ = (*tm).insert(value.value);
            }
        }
        self.last_message = time::get_time();
        self.total_messages += 1;
    }

    /// Also increments tht total message count.
    pub fn add_bad_message(&mut self) {
        self.total_messages += 1;
        self.bad_messages += 1
    }

    /// Get the count of bad messages
    pub fn bad_messages(&self) -> usize {
        self.bad_messages
    }

    /// Get the counters as a borrowed reference.
    pub fn counters(&self) -> &LruCache<String, f64> {
        &self.counters
    }

    /// Get the gauges as a borrowed reference.
    pub fn gauges(&self) -> &LruCache<String, f64> {
        &self.gauges
    }

    /// Get the histograms as a borrowed reference.
    pub fn histograms(&self) -> &LruCache<String, CKMS<f64>> {
        &self.histograms
    }

    /// Get the timers as a borrowed reference.
    pub fn timers(&self) -> &LruCache<String, CKMS<f64>> {
        &self.timers
    }

    /// Get the total number of messages this bucket has seen
    /// (includes bad messages).
    pub fn total_messages(&self) -> usize {
        self.total_messages
    }

    /// Get the initialization time of the buckets.
    pub fn start_time(&self) -> time::Timespec {
        self.server_start_time
    }

}


// Tests
//
#[cfg(test)]
mod test {
    use super::*;
    use super::super::metric::{Metric, MetricKind};
    use time;

    #[test]
    fn test_bad_messages() {
        let mut buckets = Buckets::new();
        buckets.add_bad_message();
        assert_eq!(1, buckets.bad_messages());
        assert_eq!(1, buckets.total_messages());

        buckets.add_bad_message();
        assert_eq!(2, buckets.bad_messages());
        assert_eq!(2, buckets.total_messages());
    }

    #[test]
    fn test_add_increments_total_messages() {
        let mut buckets = Buckets::new();
        // duff value to ensure it changes.
        let original = time::strptime("2015-08-03 19:50:12", "%Y-%m-%d %H:%M:%S")
            .unwrap()
            .to_timespec();
        buckets.last_message = original;

        let metric = Metric::new("some.metric", 1.0, MetricKind::Counter(1.0));
        buckets.add(&metric);
        assert!(buckets.last_message > original);
    }

    #[test]
    fn test_add_increments_last_message_timer() {
        let mut buckets = Buckets::new();
        let metric = Metric::new("some.metric", 1.0, MetricKind::Counter(1.0));
        buckets.add(&metric);
        assert_eq!(1, buckets.total_messages);

        buckets.add(&metric);
        assert_eq!(2, buckets.total_messages);
    }

    #[test]
    fn test_add_counter_metric() {
        let mut buckets = Buckets::new();
        let metric = Metric::new("some.metric", 1.0, MetricKind::Counter(1.0));
        buckets.add(&metric);

        assert!(buckets.counters.contains_key("some.metric"),
                "Should contain the metric key");
        assert_eq!(Some(&mut 1.0), buckets.counters.get_mut("some.metric"));

        // Increment counter
        buckets.add(&metric);
        assert_eq!(Some(&mut 2.0), buckets.counters.get_mut("some.metric"));
        assert_eq!(1, buckets.counters().len());
        assert_eq!(0, buckets.gauges().len());
    }

    #[test]
    fn test_add_counter_metric_sampled() {
        let mut buckets = Buckets::new();
        let metric = Metric::new("some.metric", 1.0, MetricKind::Counter(0.1));

        buckets.add(&metric);
        assert_eq!(Some(&mut 10.0), buckets.counters.get_mut("some.metric"));

        let metric_two = Metric::new("some.metric", 1.0, MetricKind::Counter(0.5));
        buckets.add(&metric_two);
        assert_eq!(Some(&mut 12.0), buckets.counters.get_mut("some.metric"));
    }

    #[test]
    fn test_add_gauge_metric() {
        let mut buckets = Buckets::new();
        let metric = Metric::new("some.metric", 11.5, MetricKind::Gauge);
        buckets.add(&metric);
        assert!(buckets.gauges.contains_key("some.metric"),
                "Should contain the metric key");
        assert_eq!(Some(&mut 11.5), buckets.gauges.get_mut("some.metric"));
        assert_eq!(1, buckets.gauges().len());
        assert_eq!(0, buckets.counters().len());
    }

    #[test]
    fn test_add_timer_metric() {
        let mut buckets = Buckets::new();
        let metric = Metric::new("some.metric", 11.5, MetricKind::Timer);
        buckets.add(&metric);
        assert!(buckets.timers.contains_key("some.metric"),
                "Should contain the metric key");

        assert_eq!(Some((1, 11.5)),
                   buckets.timers.get_mut("some.metric").expect("hwhap").query(0.0));

        let metric_two = Metric::new("some.metric", 99.5, MetricKind::Timer);
        buckets.add(&metric_two);

        let metric_three = Metric::new("other.metric", 811.5, MetricKind::Timer);
        buckets.add(&metric_three);
        assert!(buckets.timers.contains_key("some.metric"),
                "Should contain the metric key");
        assert!(buckets.timers.contains_key("other.metric"),
                "Should contain the metric key");

        // assert_eq!(Some(&mut vec![11.5, 99.5]), buckets.timers.get_mut("some.metric"));
        // assert_eq!(Some(&mut vec![811.5]), buckets.timers.get_mut("other.metric"));
    }
}
