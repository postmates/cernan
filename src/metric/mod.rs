//! `metric` is a collection of the abstract datatypes that cernan operates
//! over, plus related metadata. The main show here is
//! `metric::Event`. Everything branches down from that.
mod ackbag;
mod logline;
mod event;
mod telemetry;

pub use self::ackbag::global_ack_bag;
pub use self::event::{Encoding, Event};
pub use self::logline::LogLine;
pub use self::telemetry::{AggregationMethod, Telemetry};
#[cfg(test)]
pub use self::telemetry::Value;
use std::cmp;
use std::collections::{hash_map, HashSet};
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

#[allow(missing_docs)]
pub enum TagIter<'a> {
    Single {
        defaults: hash_map::Iter<'a, String, String>,
    },
    Double {
        seen_keys: HashSet<String>,
        iters: hash_map::Iter<'a, String, String>,
        defaults: hash_map::Iter<'a, String, String>,
        iters_exhausted: bool,
    },
}

impl<'a> Iterator for TagIter<'a> {
    type Item = (&'a String, &'a String);

    fn next(&mut self) -> Option<Self::Item> {
        match *self {
            TagIter::Single { ref mut defaults } => defaults.next(),
            TagIter::Double {
                ref mut seen_keys,
                ref mut iters,
                ref mut defaults,
                ref mut iters_exhausted,
            } => loop {
                if *iters_exhausted {
                    if let Some((k, v)) = defaults.next() {
                        if seen_keys.insert(k.to_string()) {
                            return Some((k, v));
                        }
                    } else {
                        return None;
                    }
                } else if let Some((k, v)) = iters.next() {
                    seen_keys.insert(k.to_string());
                    return Some((k, v));
                } else {
                    *iters_exhausted = true;
                    continue;
                }
            },
        }
    }
}
