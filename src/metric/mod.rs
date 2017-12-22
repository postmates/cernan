//! `metric` is a collection of the abstract datatypes that cernan operates
//! over, plus related metadata. The main show here is
//! `metric::Event`. Everything branches down from that.
pub mod tagmap;
mod logline;
mod event;
mod telemetry;

pub use self::event::{Encoding, Event};
pub use self::logline::LogLine;
pub use self::telemetry::{AggregationMethod, Telemetry};
#[cfg(test)]
pub use self::telemetry::Value;

/// `TagMap` = `tagmap::TagMap<String, String>`
pub type TagMap = self::tagmap::TagMap<String, String>;
