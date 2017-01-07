#[derive(PartialEq, Serialize, Deserialize, Clone)]
pub struct Telemetry {
    pub name: String,
    value: Value,
    pub persist: bool,
    pub aggr_method: AggregationMethod,
    pub tags: sync::Arc<TagMap>,
    pub timestamp: i64, // seconds, see #166
}

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub enum ValueKind {
    Single,
    Many,
}

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct Value {
    kind: ValueKind,
    single: Option<f64>,
    many: Option<CKMS<f64>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd, Eq, Hash)]
pub enum AggregationMethod {
    Sum,
    Set,
    Summarize,
}
