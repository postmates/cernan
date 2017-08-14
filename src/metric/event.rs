use metric::{LogLine, Telemetry};
use std::sync;

/// Event: the central cernan datastructure
///
/// Event is the heart of cernan, the enumeration that cernan works on in all
/// cases. The enumeration fields drive sink / source / filter operations
/// depending on their implementation.
#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub enum Event {
    /// A wrapper for `metric::Telemetry`. See its documentation for more
    /// detail.
    Telemetry(sync::Arc<Option<Telemetry>>),
    /// A wrapper for `metric::LogLine`. See its documentation for more
    /// detail.
    Log(sync::Arc<Option<LogLine>>),
    /// A flush pulse signal. The `TimerFlush` keeps a counter of the total
    /// flushes made in this cernan's run. See `source::Flush` for the origin of
    /// these pulses in cernan operation.
    TimerFlush(u64),
}

impl Event {
    /// Determine if an event is a `TimerFlush`.
    pub fn is_timer_flush(&self) -> bool {
        match *self {
            Event::TimerFlush(_) => true,
            _ => false,
        }
    }

    /// Retrieve the timestamp from an `Event` if such exists. `TimerFlush` has
    /// no sensible timestamp -- being itself a mechanism _of_ time, not inside
    /// time -- and these `Event`s will always return None.
    pub fn timestamp(&self) -> Option<i64> {
        match *self {
            Event::Telemetry(ref t) => {
                let t = t.clone();
                match *t {
                    Some(ref telem) => Some(telem.timestamp),
                    None => None,
                }
            }
            Event::Log(ref l) => {
                let l = l.clone();
                match *l {
                    Some(ref log) => Some(log.time),
                    None => None,
                }
            }
            Event::TimerFlush(_) => None,
        }
    }
}

impl Event {
    /// Create a new `Event::Telemetry` from an existing `metric::Telemetry`.
    #[inline]
    pub fn new_telemetry(metric: Telemetry) -> Event {
        Event::Telemetry(sync::Arc::new(Some(metric)))
    }

    /// Create a new `Event::Log` from an existing `metric::LogLine`.
    #[inline]
    pub fn new_log(log: LogLine) -> Event {
        Event::Log(sync::Arc::new(Some(log)))
    }
}
