#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct LogLine {
    pub time: i64,
    pub path: String,
    pub value: String,
    pub tags: TagMap,
}
