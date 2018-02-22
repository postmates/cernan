//! `metric` is a collection of the abstract datatypes that cernan operates
//! over, plus related metadata. The main show here is
//! `metric::Event`. Everything branches down from that.
mod logline;
mod event;
mod telemetry;

pub use self::event::{Encoding, Event};
pub use self::logline::LogLine;
pub use self::telemetry::{AggregationMethod, TagIter, Telemetry};
#[cfg(test)]
pub use self::telemetry::Value;
use std::cmp;
use util;

/// A common type in cernan, a map from string to string
pub type TagMap = util::HashMap<String, String>;

/// Compare two tagmaps
///
/// K/Vs are compared lexographically unless the maps are of different length,
/// in which case length is the comparator.
pub fn cmp_tagmap(
    lhs: &Option<TagMap>,
    rhs: &Option<TagMap>,
) -> Option<cmp::Ordering> {
    match (lhs, rhs) {
        (&Some(ref l), &Some(ref r)) => {
            if l.len() != r.len() {
                l.len().partial_cmp(&r.len())
            } else {
                l.iter().partial_cmp(r)
            }
        }
        _ => Some(cmp::Ordering::Equal),
    }
}
