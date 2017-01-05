use metric::{LogLine, Metric};
use std::sync;

include!(concat!(env!("OUT_DIR"), "/event_types.rs"));

impl Event {
    #[inline]
    pub fn new_telemetry(metric: Metric) -> Event {
        Event::Telemetry(sync::Arc::new(Some(metric)))
    }

    #[inline]
    pub fn new_log(log: LogLine) -> Event {
        Event::Log(sync::Arc::new(Some(log)))
    }
}
