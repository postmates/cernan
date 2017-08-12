use metric::{LogLine, Telemetry};
use std::sync;

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub enum Event {
    Telemetry(sync::Arc<Option<Telemetry>>),
    Log(sync::Arc<Option<LogLine>>),
    TimerFlush(u64),
}

impl Event {
    pub fn is_timer_flush(&self) -> bool {
        match self {
            &Event::TimerFlush(_) => true,
            _ => false,
        }
    }

    pub fn timestamp(&self) -> Option<i64> {
        match *self {
            Event::Telemetry(ref t) => {
                let t = t.clone();
                match *t {
                    Some(ref telem) => Some(telem.timestamp),
                    None => None,
                }
            },
            Event::Log(ref l) => {
                let l = l.clone();
                match *l {
                    Some(ref log) => Some(log.time),
                    None => None,
                }
            },
            Event::TimerFlush(_) => {
                None
            }
        }
    }
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
