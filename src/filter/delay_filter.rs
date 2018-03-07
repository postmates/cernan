//! Filter streams to within a bounded interval of current time.
//!
//! This filter is intended to remove items from the stream which are too old,
//! as defined by the current time and the configured `tolerance`. That is, if
//! for some time `T`, `(T - time::now()).abs() > tolerance` the item associated
//! with `T` will be rejected.

use filter;
use metric;
use std::sync::atomic::{AtomicUsize, Ordering};
use time;

/// Total number of telemetry rejected for age
pub static DELAY_TELEM_REJECT: AtomicUsize = AtomicUsize::new(0);
/// Total number of telemetry accepted for age
pub static DELAY_TELEM_ACCEPT: AtomicUsize = AtomicUsize::new(0);
/// Total number of logline rejected for age
pub static DELAY_LOG_REJECT: AtomicUsize = AtomicUsize::new(0);
/// Total number of logline accepted for age
pub static DELAY_LOG_ACCEPT: AtomicUsize = AtomicUsize::new(0);

/// Filter streams to within a bounded interval of current time.
///
/// This filter is intended to remove items from the stream which are too old,
/// as defined by the current time and the configured `tolerance`. That is, if
/// for some time `T`, `(T - time::now()).abs() > tolerance` the item associated
/// with `T` will be rejected.
pub struct DelayFilter {
    tolerance: i64,
}

/// Configuration for `DelayFilter`
#[derive(Clone, Debug)]
pub struct DelayFilterConfig {
    /// The filter's unique name in the routing topology.
    pub config_path: Option<String>,
    /// The forwards along which the filter will emit its `metric::Event`s.
    pub forwards: Vec<String>,
    /// The delay tolerance of the filter, measured in seconds.
    pub tolerance: i64,
}

impl DelayFilter {
    /// Create a new DelayFilter
    pub fn new(config: &DelayFilterConfig) -> DelayFilter {
        DelayFilter {
            tolerance: config.tolerance,
        }
    }
}

impl filter::Filter for DelayFilter {
    fn process(
        &mut self,
        event: metric::Event,
        res: &mut Vec<metric::Event>,
    ) -> Result<(), filter::FilterError> {
        match event {
            metric::Event::Telemetry(telem) => {
                if (telem.timestamp - time::now()).abs() < self.tolerance {
                    DELAY_TELEM_ACCEPT.fetch_add(1, Ordering::Relaxed);
                    res.push(metric::Event::Telemetry(telem));
                } else {
                    DELAY_TELEM_REJECT.fetch_add(1, Ordering::Relaxed);
                }
            }
            metric::Event::Log(log) => {
                if (log.time - time::now()).abs() < self.tolerance {
                    DELAY_LOG_ACCEPT.fetch_add(1, Ordering::Relaxed);
                    res.push(metric::Event::Log(log));
                } else {
                    DELAY_LOG_REJECT.fetch_add(1, Ordering::Relaxed);
                }
            }
            ev => {
                res.push(ev);
            }
        }
        Ok(())
    }
}
