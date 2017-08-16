/// Buffer events for a set period of flushes
///
/// This filter is intended to hold events for a set number of flushes. This
/// delays the events for the duration of those flushes but reduce the
/// likelyhood of cross-flush splits of timestamps.

use filter;
use metric;
use std::mem;

pub struct FlushBoundaryFilter {
    tolerance: usize,
    holds: Vec<Hold>,
}

#[derive(Clone, Debug)]
pub struct FlushBoundaryFilterConfig {
    pub config_path: Option<String>,
    pub forwards: Vec<String>,
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
    pub fn new(config: FlushBoundaryFilterConfig) -> FlushBoundaryFilter {
        FlushBoundaryFilter {
            tolerance: config.tolerance,
            holds: Vec::new(),
        }
    }
}

impl filter::Filter for FlushBoundaryFilter {
    fn process(
        &mut self,
        event: metric::Event,
        res: &mut Vec<metric::Event>,
    ) -> Result<(), filter::FilterError> {
        if event.is_timer_flush() {
            for hold in self.holds.iter_mut() {
                hold.age += 1;
            }
            let holds = mem::replace(&mut self.holds, Vec::new());
            let mut too_new = Vec::new();
            for mut hold in holds.into_iter() {
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
                    Ok(idx) => {
                        self.holds[idx].events.push(event)
                    },
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
