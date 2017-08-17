//! `metric` is a collection of the abstract datatypes that cernan operates
//! over, plus related metadata. The main show here is
//! `metric::Event`. Everything branches down from that.
pub mod tagmap;
mod logline;
mod event;
mod telemetry;

pub use self::event::Event;
pub use self::logline::LogLine;

pub use self::tagmap::TagMap;
pub use self::telemetry::{AggregationMethod, Telemetry};

#[cfg(test)]
pub use self::telemetry::Value;
