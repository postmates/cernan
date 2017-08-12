pub mod tagmap;
mod logline;
mod event;
mod telemetry;

pub use self::event::Event;
pub use self::logline::LogLine;
pub use self::telemetry::{AggregationMethod, Telemetry};

#[cfg(test)]
pub use self::telemetry::Value;

pub type TagMap = self::tagmap::TagMap<String, String>;
