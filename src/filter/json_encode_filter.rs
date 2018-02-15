//! Convert LogLine events into Raw events encoded as JSON.
//!
//! This filter takes LogLines and encodes them into JSON, emitting the encoded
//! event as a Raw event. This allows further filters or sinks to operate on the
//! JSON without needing to understand a LogLine event in particular.
//! If the LogLine value is a valid JSON object and parse_line config option is true,
//! then the JSON will be merged with LogLine metadata. Otherwise, the original line
//! will be included simply as a string.

use chrono::DateTime;
use chrono::naive::NaiveDateTime;
use chrono::offset::Utc;
use filter;
use metric;
use metric::TagMap;
use rand::random;
use serde_json;
use serde_json::Value;
use serde_json::map::Map;
use std::iter::FromIterator;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

lazy_static! {
    /// Total number of logline processed
    pub static ref JSON_ENCODE_LOG_PROCESSED: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total number of logline with JSON value successfully parsed
    pub static ref JSON_ENCODE_LOG_PARSED: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
}

impl From<TagMap> for Map<String, Value> {
    fn from(tagmap: TagMap) -> Self {
        Map::from_iter(
            tagmap.into_iter().map(|(k, v)| (k.clone(), v.clone().into()))
        )
    }
}

/// Convert LogLine events into Raw events encoded as JSON.
///
/// This filter takes LogLines and encodes them into JSON, emitting the encoded
/// event as a Raw event. This allows further filters or sinks to operate on the
/// JSON without needing to understand a LogLine event in particular.
/// If the LogLine value is a valid JSON object and parse_line config option is true,
/// then the JSON will be merged with LogLine metadata. Otherwise, the original line
/// will be included simply as a string.
pub struct JSONEncodeFilter {
    parse_line: bool,
}

/// Configuration for `JSONEncodeFilter`
#[derive(Clone, Debug)]
pub struct JSONEncodeFilterConfig {
    /// The filter's unique name in the routing topology.
    pub config_path: Option<String>,
    /// The forwards along which the filter will emit its `metric::Event`s.
    pub forwards: Vec<String>,
    /// Whether the filter should attempt to parse LogLine values that are valid JSON objects.
    pub parse_line: bool,
}

impl JSONEncodeFilter {
    /// Create a new JSONEncodeFilter
    pub fn new(config: &JSONEncodeFilterConfig) -> JSONEncodeFilter {
        JSONEncodeFilter {
            parse_line: config.parse_line,
        }
    }
}

impl filter::Filter for JSONEncodeFilter {
    fn process(
        &mut self,
        event: metric::Event,
        res: &mut Vec<metric::Event>,
    ) -> Result<(), filter::FilterError> {
        match event {
            metric::Event::Log(l) => if let Some(ref log) = *l {
                let naive_time = NaiveDateTime::from_timestamp(log.time, 0);
                let utc_time: DateTime<Utc> = DateTime::from_utc(naive_time, Utc);
                let metadata = json_to_object(json!({
                    "time": utc_time.to_rfc3339(),
                    "path": log.path.clone(),
                    "tags": Map::from(log.tags.clone()),
                }));
                // If parse_line is true, and line is parsable as a JSON object, parse it.
                // Otherwise get an object containing the original line.
                let value = (if self.parse_line { Some(()) } else { None })
                    .and_then(|_| serde_json::from_str::<Value>(&log.value).ok())
                    .and_then(|v| if let Value::Object(obj) = v { Some(obj) } else { None })
                    .map(|v| { JSON_ENCODE_LOG_PARSED.fetch_add(1, Ordering::Relaxed); v })
                    .unwrap_or_else(|| json_to_object(json!({"message": log.value.clone()})));
                // Combine our various sources of data.
                // Data that is more likely to be correct (more specific to the source) overrides
                // other data. So the parsed value is authoritative, followed by any fields we could
                // parse by filters, then finally the metadata we were able to work out on our own.
                let value = merge_objects(&[value, log.fields.clone().into(), metadata]);
                res.push(metric::Event::Raw {
                    order_by: random(),
                    encoding: metric::Encoding::JSON,
                    bytes: serde_json::to_string(&value).unwrap().into(), // serde_json::Value will never fail to encode
                });
                JSON_ENCODE_LOG_PROCESSED.fetch_add(1, Ordering::Relaxed);
            },
            // All other event types are passed through.
            event => {
                res.push(event);
            }
        }
        Ok(())
    }
}

/// Convenience helper to take a json!() macro you know is an object and get back a Map<String, Value>,
/// instead of a generic Value.
fn json_to_object(v: Value) -> Map<String, Value>  {
    if let Value::Object(obj) = v {
        obj
    } else {
        unreachable!()
    }
}

/// Merge JSON objects, with values from earlier objects in the list overriding later ones.
/// Note this is not a recursive merge - if the same key is in many objects, we simply take
/// the value from the earliest one.
fn merge_objects(objs: &[Map<String, Value>]) -> Map<String, Value> {
    let mut result = Map::new();
    for obj in objs {
        for key in obj.keys() {
            if !result.contains_key(key) {
                result.insert(key.clone(), obj[key].clone());
            }
        }
    };
    result
}
