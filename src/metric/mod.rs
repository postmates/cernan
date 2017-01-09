mod tagmap;
mod logline;
mod event;
mod telemetry;

pub use self::event::Event;
pub use self::logline::LogLine;
pub use self::telemetry::{AggregationMethod, Telemetry};

pub type TagMap = self::tagmap::TagMap<String, String>;
