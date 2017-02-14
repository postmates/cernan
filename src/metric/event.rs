use metric::{LogLine, Telemetry};
use std::sync;

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub enum Event {
    Telemetry(sync::Arc<Option<Telemetry>>),
    Log(sync::Arc<Option<LogLine>>),
    TimerFlush(u32),
}

impl Event {
    #[inline]
    pub fn new_telemetry(metric: Telemetry) -> Event {
        Event::Telemetry(sync::Arc::new(Some(metric)))
    }

    #[inline]
    pub fn new_log(log: LogLine) -> Event {
        Event::Log(sync::Arc::new(Some(log)))
    }
}
