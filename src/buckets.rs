//! Buckets are the primary internal storage type.
//!
//! Each bucket contains a set of hashmaps containing
//! each set of metrics received by clients.

use std::collections::HashMap;
use super::metric::{Metric, MetricKind};
use time;
use hist::Histogram;

/// Buckets stores all metrics until they are flushed.
pub struct Buckets {
    counters: HashMap<String, f64>,
    gauges: HashMap<String, f64>,
    timers: HashMap<String, Histogram>,
    histograms: HashMap<String, Histogram>,

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
            counters: HashMap::new(),
            gauges: HashMap::new(),
            timers: HashMap::new(),
            histograms: HashMap::new(),
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
                let counter = self.counters.entry(name).or_insert(0.0);
                *counter = *counter + value.value * (1.0 / rate);
            }
            MetricKind::Gauge => {
                self.gauges.insert(name, value.value);
            }
            MetricKind::Histogram => {
                let hist = self.histograms.entry(name).or_insert(Histogram::new());
                let _ = (*hist).increment(value.value);
            }
            MetricKind::Timer => {
                let slot = self.timers.entry(name).or_insert(Histogram::new());
                let _ = (*slot).increment(value.value);
            }
        }
        self.last_message = time::get_time();
        self.total_messages += 1;
    }

    /// Increment the bad message count by one.
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
    pub fn counters(&self) -> &HashMap<String, f64> {
        &self.counters
    }

    /// Get the gauges as a borrowed reference.
    pub fn gauges(&self) -> &HashMap<String, f64> {
        &self.gauges
    }

    /// Get the histograms as a borrowed reference.
    pub fn histograms(&self) -> &HashMap<String, Histogram> {
        &self.histograms
    }

    /// Get the timers as a borrowed reference.
    pub fn timers(&self) -> &HashMap<String, Histogram> {
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

    // /// Resets the counters, timers and histograms to 0. Gauge values are
    // /// preserved. This emulates the behavior of etsy/statsd with default
    // /// configuration options.
    // pub fn reset(&mut self) {
    //     for (_, value) in self.counters.iter_mut() {
    //         *value = 0.0;
    //     }
    //     for (_, value) in self.timers.iter_mut() {
    //         *value = Histogram::new();
    //     }
    //     for (_, value) in self.histograms.iter_mut() {
    //         *value = Histogram::new();
    //     }
    //     self.bad_messages = 0;
    //     self.total_messages = 0;
    // }
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
        assert_eq!(Some(&1.0), buckets.counters.get("some.metric"));

        // Increment counter
        buckets.add(&metric);
        assert_eq!(Some(&2.0), buckets.counters.get("some.metric"));
        assert_eq!(1, buckets.counters().len());
        assert_eq!(0, buckets.gauges().len());
    }

    #[test]
    fn test_add_counter_metric_sampled() {
        let mut buckets = Buckets::new();
        let metric = Metric::new("some.metric", 1.0, MetricKind::Counter(0.1));

        buckets.add(&metric);
        assert_eq!(Some(&10.0), buckets.counters.get("some.metric"));

        let metric_two = Metric::new("some.metric", 1.0, MetricKind::Counter(0.5));
        buckets.add(&metric_two);
        assert_eq!(Some(&12.0), buckets.counters.get("some.metric"));
    }

    #[test]
    fn test_add_gauge_metric() {
        let mut buckets = Buckets::new();
        let metric = Metric::new("some.metric", 11.5, MetricKind::Gauge);
        buckets.add(&metric);
        assert!(buckets.gauges.contains_key("some.metric"),
                "Should contain the metric key");
        assert_eq!(Some(&11.5), buckets.gauges.get("some.metric"));
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

        assert_eq!(Result::Ok(11.5), buckets.timers.get("some.metric").expect("hwhap").min());

        // let metric_two = Metric::new("some.metric", 99.5, MetricKind::Timer);
        // buckets.add(&metric_two);

        // let metric_three = Metric::new("other.metric", 811.5, MetricKind::Timer);
        // buckets.add(&metric_three);
        // assert!(buckets.timers.contains_key("some.metric"),
        //         "Should contain the metric key");
        // assert!(buckets.timers.contains_key("other.metric"),
        //         "Should contain the metric key");

        // assert_eq!(Some(&vec![11.5, 99.5]), buckets.timers.get("some.metric"));
        // assert_eq!(Some(&vec![811.5]), buckets.timers.get("other.metric"));
    }

    // #[test]
    // fn test_reset_metrics() {
    //     let mut buckets = Buckets::new();
    //     buckets.add(&Metric::new("some.timer", 11.5, MetricKind::Timer));
    //     buckets.add(&Metric::new("some.counter", 14.9, MetricKind::Counter(1.0)));
    //     buckets.add(&Metric::new("some.gauge", 0.9, MetricKind::Gauge));

    //     buckets.reset();
    //     assert!(buckets.timers.contains_key("some.timer"));
    //     assert_eq!(Some(&vec![]), buckets.timers.get("some.timer"));

    //     assert!(buckets.counters.contains_key("some.counter"));
    //     assert_eq!(Some(&0.0), buckets.counters.get("some.counter"));

    //     assert!(buckets.gauges.contains_key("some.gauge"));
    //     assert_eq!(Some(&0.9), buckets.gauges.get("some.gauge"));

    //     assert_eq!(0, buckets.total_messages);
    //     assert_eq!(0, buckets.bad_messages);
    // }
}
