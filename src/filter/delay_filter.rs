use filter;
use metric;
use time;
use util;

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
    pub fn new(config: DelayFilterConfig) -> DelayFilter {
        DelayFilter {
            tolerance: config.tolerance,
        }
    }
}

impl filter::Filter for DelayFilter {
    fn valve_state(&self) -> util::Valve {
        util::Valve::Open
    }

    fn process(
        &mut self,
        event: metric::Event,
        res: &mut Vec<metric::Event>,
    ) -> Result<(), filter::FilterError> {
        match event {
            metric::Event::Telemetry(m) => if let Some(ref telem) = *m {
                let telem = telem.clone();
                if (telem.timestamp - time::now()).abs() < self.tolerance {
                    res.push(metric::Event::new_telemetry(telem));
                }
            },
            metric::Event::Log(l) => if let Some(ref log) = *l {
                let log = log.clone();
                if (log.time - time::now()).abs() < self.tolerance {
                    res.push(metric::Event::new_log(log));
                }
            },
            metric::Event::TimerFlush(f) => {
                res.push(metric::Event::TimerFlush(f));
            }
        }
        Ok(())
    }
}
