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
            tagmap
                .into_iter()
                .map(|(k, v)| (k.clone(), v.clone().into())),
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
    /// Whether the filter should attempt to parse LogLine values that are
    /// valid JSON objects.
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
            metric::Event::Log(log) => {
                let naive_time = NaiveDateTime::from_timestamp(log.time, 0);
                let utc_time: DateTime<Utc> = DateTime::from_utc(naive_time, Utc);
                let metadata = json_to_object(json!({
                    "time": utc_time.to_rfc3339(),
                    "path": log.path.clone(),
                    "tags": Map::from(log.tags.clone()),
                }));
                // If parse_line is true, and line is parsable as a JSON object, parse
                // it. Otherwise get an object containing the original
                // line.
                let value = (if self.parse_line { Some(()) } else { None })
                    .and_then(|_| serde_json::from_str::<Value>(&log.value).ok())
                    .and_then(|v| {
                        if let Value::Object(obj) = v {
                            Some(obj)
                        } else {
                            None
                        }
                    })
                    .map(|v| {
                        JSON_ENCODE_LOG_PARSED.fetch_add(1, Ordering::Relaxed);
                        v
                    })
                    .unwrap_or_else(|| {
                        json_to_object(json!({"message": log.value.clone()}))
                    });
                // Combine our various sources of data.
                // Data that is more likely to be correct (more specific to the
                // source) overrides other data. So the parsed value
                // is authoritative, followed by any fields we could
                // parse by filters, then finally the metadata we were able to work
                // out on our own.
                let value =
                    merge_objects(vec![value, log.fields.clone().into(), metadata]);
                res.push(metric::Event::Raw {
                    order_by: random(),
                    encoding: metric::Encoding::JSON,
                    bytes: serde_json::to_string(&value).unwrap().into(), /* serde_json::Value will never fail to encode */
                });
                JSON_ENCODE_LOG_PROCESSED.fetch_add(1, Ordering::Relaxed);
            }
            // All other event types are passed through.
            event => {
                res.push(event);
            }
        }
        Ok(())
    }
}

/// Convenience helper to take a json!() macro you know is an object and get back a
/// Map<String, Value>, instead of a generic Value.
fn json_to_object(v: Value) -> Map<String, Value> {
    if let Value::Object(obj) = v {
        obj
    } else {
        unreachable!()
    }
}

/// Merge JSON objects, with values from earlier objects in the list overriding later
/// ones. Note this is not a recursive merge - if the same key is in many objects, we
/// simply take the value from the earliest one.
fn merge_objects(objs: Vec<Map<String, Value>>) -> Map<String, Value> {
    let mut result = Map::new();
    for obj in objs.into_iter() {
        for (key, value) in obj.into_iter() {
            if !result.contains_key(&key) {
                result.insert(key, value);
            }
        }
    }
    result
}

// Tests
//
#[cfg(test)]
mod test {
    use super::*;
    use filter::Filter;
    use metric;
    use quickcheck::QuickCheck;
    use serde_json::Value;
    use serde_json::map::Map;

    fn process_event(parse_line: bool, event: metric::Event) -> Value {
        let mut filter = JSONEncodeFilter {
            parse_line: parse_line,
        };
        let mut results = Vec::new();
        filter.process(event, &mut results).unwrap();
        // fail if results empty, else return processed event's payload
        if let metric::Event::Raw { ref bytes, .. } = results[0] {
            return serde_json::from_slice(bytes).unwrap();
        }
        panic!("Processed event was not Raw")
    }

    #[test]
    fn parsable_line_parsing_off() {
        // Test we don't parse a line if parsing is off
        assert_eq!(
            process_event(
                false,
                metric::Event::new_log(metric::LogLine {
                    path: "testpath".to_string(),
                    value: "{\"bad\": \"do not parse\"}".to_string(),
                    time: 946684800,
                    tags: Default::default(),
                    fields: Default::default(),
                })
            ),
            json!({
                "path": "testpath",
                "message": "{\"bad\": \"do not parse\"}",
                "time": "2000-01-01T00:00:00+00:00",
                "tags": {},
            })
        );
    }

    #[test]
    fn parsable_line_parsing_on() {
        // Test we do parse a line if parsing is on
        assert_eq!(
            process_event(
                true,
                metric::Event::new_log(metric::LogLine {
                    path: "testpath".to_string(),
                    value: "{\"good\": \"do parse\"}".to_string(),
                    time: 946684800,
                    tags: Default::default(),
                    fields: Default::default(),
                })
            ),
            json!({
                "path": "testpath",
                "good": "do parse",
                "time": "2000-01-01T00:00:00+00:00",
                "tags": {},
            })
        );
    }

    #[test]
    fn unparsable_line() {
        // Test we don't parse a line if it's not JSON
        assert_eq!(
            process_event(
                true,
                metric::Event::new_log(metric::LogLine {
                    path: "testpath".to_string(),
                    value: "this is not json".to_string(),
                    time: 946684800,
                    tags: Default::default(),
                    fields: Default::default(),
                })
            ),
            json!({
                "path": "testpath",
                "message": "this is not json",
                "time": "2000-01-01T00:00:00+00:00",
                "tags": {},
            })
        );
    }

    #[test]
    fn non_object_line() {
        // Test we don't parse a line if it's not a JSON object but is valid JSON
        assert_eq!(
            process_event(
                true,
                metric::Event::new_log(metric::LogLine {
                    path: "testpath".to_string(),
                    value: "[123, \"not an object\"]".to_string(),
                    time: 946684800,
                    tags: Default::default(),
                    fields: Default::default(),
                })
            ),
            json!({
                "path": "testpath",
                "message": "[123, \"not an object\"]",
                "time": "2000-01-01T00:00:00+00:00",
                "tags": {},
            })
        );
    }

    // quickcheck and serde_json::map::Map aren't compatible, so we ask quickcheck
    // for many Vec<(String, String)>s that we turn into maps.
    fn vecs_to_objs(vecs: &Vec<Vec<(String, String)>>) -> Vec<Map<String, Value>> {
        vecs.iter()
            .map(|vec| {
                Map::from_iter(
                    vec.iter()
                        .map(|&(ref k, ref v)| (k.clone(), v.clone().into())),
                )
            })
            .collect()
    }

    #[test]
    fn merged_objects_contain_all_keys() {
        fn inner(vecs: Vec<Vec<(String, String)>>) -> bool {
            let result = merge_objects(vecs_to_objs(&vecs));
            for obj in vecs {
                for (k, _v) in obj {
                    if !result.contains_key(&k) {
                        return false;
                    }
                }
            }
            true
        }
        QuickCheck::new().quickcheck(inner as fn(Vec<Vec<(String, String)>>) -> bool);
    }

    #[test]
    fn merged_objects_takes_first_value() {
        fn inner(vecs: Vec<Vec<(String, String)>>) -> bool {
            let objs = vecs_to_objs(&vecs);
            let result = merge_objects(objs.clone());
            for (key, result_value) in result {
                match objs.iter().find(|obj| obj.contains_key(&key)) {
                    Some(obj) => if obj[&key] != result_value {
                        return false; // result value did not match first obj containing key
                    },
                    None => return false, // key in result was not in any input objs
                }
            }
            true
        }
        QuickCheck::new().quickcheck(inner as fn(Vec<Vec<(String, String)>>) -> bool);
    }
}
