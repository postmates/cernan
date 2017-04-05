use metric::TagMap;
use time;

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct LogLine {
    /// The time that this LogLine occupies, in the units of time::now()
    pub time: i64,
    /// The path that this LogLine originated from. May be a unix path or not,
    /// depending on origin.
    pub path: String,
    /// The line read from the LogLine path
    pub value: String,
    /// Fields that may have been parsed out of the value, a key/value structure
    pub fields: TagMap,
    /// Cernan tags for this LogLine
    pub tags: TagMap,
}

/// LogLine - a structure that represents a bit of text
///
/// A LogLine is intended to hold a bit of text in its 'value' that may or may
/// not be structured. The field 'fields' is available for
impl LogLine {
    pub fn new<S>(path: S, value: S) -> LogLine
        where S: Into<String>
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
        where S: Into<String>
    {
        self.fields.insert(key.into(), val.into());
        self
    }

    /// Overlay a tag into the LogLine
    ///
    /// This function inserts a new key and value into the LogLine's tags. If
    /// the key was already present the old value is replaced.
    pub fn overlay_tag<S>(mut self, key: S, val: S) -> LogLine
        where S: Into<String>
    {
        self.tags.insert(key.into(), val.into());
        self
    }

    /// Overlay a TagMap on the LogLine's tags
    ///
    /// This function overlays a TagMap onto the LogLine's existing tags. If a
    /// key is present in both TagMaps the one from 'map' will be preferred.
    pub fn overlay_tags_from_map(mut self, map: &TagMap) -> LogLine {
        for &(ref k, ref v) in map.iter() {
            self.tags.insert(k.clone(), v.clone());
        }
        self
    }
}
