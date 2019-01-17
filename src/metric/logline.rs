use crate::metric::{TagIter, TagMap};
use std::collections::HashSet;
use crate::time;

/// An unstructured piece of text, plus associated metadata
#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct LogLine {
    /// The time that this `LogLine` occupies, in the units of time::now()
    pub time: i64,
    /// The path that this `LogLine` originated from. May be a unix path or not,
    /// depending on origin.
    pub path: String,
    /// The line read from the `LogLine` path
    pub value: String,
    /// Fields that may have been parsed out of the value, a key/value structure
    pub fields: TagMap,
    /// Cernan tags for this LogLine
    pub tags: Option<TagMap>,
}

/// `LogLine` - a structure that represents a bit of text
///
/// A `LogLine` is intended to hold a bit of text in its 'value' that may or may
/// not be structured. The field 'fields' is available for
impl LogLine {
    /// Create a new `LogLine`
    ///
    /// Please see `LogLine` struct documentation for more details.
    pub fn new<S>(path: S, value: S) -> LogLine
    where
        S: Into<String>,
    {
        LogLine {
            path: path.into(),
            value: value.into(),
            time: time::now(),
            tags: Default::default(),
            fields: Default::default(),
        }
    }

    /// Set the time of the Logline
    ///
    /// # Examples
    /// ```
    /// use cernan::metric::LogLine;
    ///
    /// let time = 101;
    /// let mut l = LogLine::new("some_path", "value");
    /// assert!(l.time != time);
    ///
    /// l = l.time(time);
    /// assert_eq!(l.time, time);
    /// ```
    pub fn time(mut self, time: i64) -> LogLine {
        self.time = time;
        self
    }

    /// Insert a new field into LogLine
    ///
    /// Fields are distinct from tags. A 'field' is related to data that has
    /// been parsed out of LogLine.value and will be treated specially by
    /// supporting sinks. For instance, the firehose sink will put the field
    /// _into_ the payload where tags will be associated metadata that define
    /// groups of related LogLines.
    pub fn insert_field<S>(mut self, key: S, val: S) -> LogLine
    where
        S: Into<String>,
    {
        self.fields.insert(key.into(), val.into());
        self
    }

    /// Insert a tag into the LogLine
    ///
    /// This inserts a key/value pair into the LogLine, returning the previous
    /// value if the key already existed.
    pub fn insert_tag<S>(&mut self, key: S, val: S) -> Option<String>
    where
        S: Into<String>,
    {
        if let Some(ref mut tags) = self.tags {
            tags.insert(key.into(), val.into())
        } else {
            let mut tags = TagMap::default();
            let res = tags.insert(key.into(), val.into());
            self.tags = Some(tags);
            res
        }
    }

    /// Remove a tag from the Telemetry
    ///
    /// This removes a key/value pair from the Telemetry, returning the previous
    /// value if the key existed.
    pub fn remove_tag(&mut self, key: &str) -> Option<String> {
        if let Some(ref mut tags) = self.tags {
            tags.remove(key)
        } else {
            None
        }
    }

    /// Overlay a tag into the LogLine
    ///
    /// This function inserts a new key and value into the LogLine's tags. If
    /// the key was already present the old value is replaced.
    pub fn overlay_tag<S>(mut self, key: S, val: S) -> LogLine
    where
        S: Into<String>,
    {
        let _ = self.insert_tag(key, val);
        self
    }

    /// Overlay a TagMap on the LogLine's tags
    ///
    /// This function overlays a TagMap onto the LogLine's existing tags. If a
    /// key is present in both TagMaps the one from 'map' will be preferred.
    pub fn overlay_tags_from_map(mut self, map: &TagMap) -> LogLine {
        if let Some(ref mut tags) = self.tags {
            for (k, v) in map.iter() {
                tags.insert(k.clone(), v.clone());
            }
        } else if !map.is_empty() {
            self.tags = Some(map.clone());
        }
        self
    }

    /// Get a value from tags, either interior or default
    pub fn get_from_tags<'a>(
        &'a mut self,
        key: &'a str,
        defaults: &'a TagMap,
    ) -> Option<&'a String> {
        if let Some(ref mut tags) = self.tags {
            match tags.get(key) {
                Some(v) => Some(v),
                None => defaults.get(key),
            }
        } else {
            defaults.get(key)
        }
    }

    /// Iterate tags, layering in defaults when needed
    ///
    /// The defaults serves to fill 'holes' in the Telemetry's view of the
    /// tags. We avoid shipping tags through the whole system at the expense of
    /// slightly more complicated call-sites in sinks.
    pub fn tags<'a>(&'a self, defaults: &'a TagMap) -> TagIter<'a> {
        if let Some(ref tags) = self.tags {
            TagIter::Double {
                iters_exhausted: false,
                seen_keys: HashSet::new(),
                defaults: defaults.iter(),
                iters: tags.iter(),
            }
        } else {
            TagIter::Single {
                defaults: defaults.iter(),
            }
        }
    }
}
