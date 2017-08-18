use filter;
use metric;
use std::mem;
use util;
use source;

/// Buffer events for a set period of flushes
///
/// This filter is intended to hold events for a set number of flushes. This
/// delays the events for the duration of those flushes but reduce the
/// likelyhood of cross-flush splits of timestamps.
pub struct FlushBoundaryFilter {
    tolerance: usize,
    holds: Vec<Hold>,
}

/// Configuration for `FlushBoundaryFilter`
#[derive(Clone, Debug)]
pub struct FlushBoundaryFilterConfig {
    /// The filter's unique name in the routing topology
    pub config_path: Option<String>,
    /// The forwards along which the filter will emit its `metric::Event`s
    /// stream.
    pub forwards: Vec<String>,
    /// The flush boundary tolerance, measured in seconds.
    pub tolerance: usize,
}

struct Hold {
    timestamp: i64,
    age: usize,
    events: Vec<metric::Event>,
}

impl Hold {
    pub fn new(event: metric::Event) -> Hold {
        let ts = event.timestamp().unwrap();
        let mut events = Vec::new();
        events.push(event);
        Hold {
            timestamp: ts,
            age: 0,
            events: events,
        }
    }
}

impl FlushBoundaryFilter {
    /// Create a new FlushBoundaryFilter
    pub fn new(config: FlushBoundaryFilterConfig) -> FlushBoundaryFilter {
        FlushBoundaryFilter {
            tolerance: config.tolerance,
            holds: Vec::new(),
        }
    }

    /// Count the number of stored events in the filter
    pub fn count(&self) -> usize {
        self.holds.iter().fold(0, |acc, ref hld| acc + hld.events.len())
    }
}

impl filter::Filter for FlushBoundaryFilter {
    fn valve_state(&self) -> util::Valve {
        if self.count() > 1000 {
            source::report_telemetry("cernan.filter.flush_boundary.valve.closed", 1.0);
            util::Valve::Closed
        } else {
            source::report_telemetry("cernan.filter.flush_boundary.valve.open", 1.0);
            util::Valve::Open
        }
    }

    fn process(
        &mut self,
        event: metric::Event,
        res: &mut Vec<metric::Event>,
    ) -> Result<(), filter::FilterError> {
        if event.is_timer_flush() {
            for hold in &mut self.holds {
                hold.age += 1;
            }
            let holds = mem::replace(&mut self.holds, Vec::new());
            let mut too_new = Vec::new();
            for mut hold in holds {
                if hold.age > self.tolerance {
                    res.append(&mut hold.events);
                } else {
                    too_new.push(hold);
                }
            }
            res.push(event);
            self.holds = too_new;
        } else {
            let opt_ts = event.timestamp();
            if let Some(ts) = opt_ts {
                match self.holds.binary_search_by(|hold| hold.timestamp.cmp(&ts)) {
                    Ok(idx) => self.holds[idx].events.push(event),
                    Err(idx) => {
                        let hold = Hold::new(event);
                        self.holds.insert(idx, hold)
                    }
                }
            }
        }
        Ok(())
    }
}
