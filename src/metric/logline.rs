use metric::TagMap;
use time;
use std::sync;
use cache::string::{store,get};

/// An unstructured piece of text, plus associated metadata
#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct LogLine {
    /// The time that this `LogLine` occupies, in the units of time::now()
    pub time: i64,
    /// The path that this `LogLine` originated from. May be a unix path or not,
    /// depending on origin.
    path: u64,
    /// The line read from the `LogLine` path
    pub value: String,
    /// Fields that may have been parsed out of the value, a key/value structure
    pub fields: TagMap,
    /// Cernan tags for this LogLine
    pub tags: TagMap,
}

/// `LogLine` - a structure that represents a bit of text
///
/// A `LogLine` is intended to hold a bit of text in its 'value' that may or may
/// not be structured. The field 'fields' is available for
impl LogLine {
    /// Create a new `LogLine`
    ///
    /// Please see `LogLine` struct documentation for more details.
    pub fn new(path: &str, value: &str) -> LogLine
    {
        let id = store(path);
        LogLine {
            path: id,
            value: value.to_string(),
            time: time::now(),
            tags: Default::default(),
            fields: Default::default(),
        }
    }

    /// Return the path
    pub fn path(&self) -> sync::Arc<String> {
        get(self.path).unwrap().clone()
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
    pub fn insert_field(mut self, key: &str, val: &str) -> LogLine {
        self.fields.insert(key, val);
        self
    }

    /// Overlay a tag into the LogLine
    ///
    /// This function inserts a new key and value into the LogLine's tags. If
    /// the key was already present the old value is replaced.
    pub fn overlay_tag(mut self, key: &str, val: &str) -> LogLine {
        self.tags.insert(key, val);
        self
    }

    /// Overlay a TagMap on the LogLine's tags
    ///
    /// This function overlays a TagMap onto the LogLine's existing tags. If a
    /// key is present in both TagMaps the one from 'map' will be preferred.
    pub fn overlay_tags_from_map(mut self, map: &TagMap) -> LogLine {
        use cache::string::get;
        for &(k, v) in map.iter() {
            self.tags.insert(get(k).unwrap().as_ref(), get(v).unwrap().as_ref());
        }
        self
    }
}
