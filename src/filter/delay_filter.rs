use filter;
use metric;
use source::report_telemetry;
use time;

/// Filter streams to within a bounded interval of current time.
///
/// This filter is intended to remove items from the stream which are too old,
/// as defined by the current time and the configured `tolerance`. That is, if
/// for some time `T`, `(T - time::now()).abs() > tolerance` the item associated
/// with `T` will be rejected.
pub struct DelayFilter {
    config_path: String,
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
            config_path: config
                .config_path
                .expect("must supply config_path for delay filter"),
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
            metric::Event::Telemetry(m) => {
                report_telemetry(format!("{}.telemetry", self.config_path), 1.0);
                if let Some(ref telem) = *m {
                    let telem = telem.clone();
                    if (telem.timestamp - time::now()).abs() < self.tolerance {
                        res.push(metric::Event::new_telemetry(telem));
                    }
                }
            }
            metric::Event::Log(l) => if let Some(ref log) = *l {
                report_telemetry(format!("{}.log", self.config_path), 1.0);
                let log = log.clone();
                if (log.time - time::now()).abs() < self.tolerance {
                    res.push(metric::Event::new_log(log));
                }
            },
            metric::Event::TimerFlush(f) => {
                report_telemetry(format!("{}.flush", self.config_path), 1.0);
                res.push(metric::Event::TimerFlush(f));
            }
        }
        Ok(())
    }
}
