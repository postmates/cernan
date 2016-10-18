#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct LogLine {
    pub time: i64,
    pub path: String,
    pub value: String,
    pub tags: TagMap,
}

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct Metric {
    pub kind: MetricKind,
    pub name: String,
    pub time: i64,
    pub tags: TagMap,
    value: CKMS<f64>,
}

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub enum Event {
    Statsd(Metric),
    Graphite(Metric),
    Log(Vec<LogLine>),
    TimerFlush,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum MetricKind {
    Counter,
    Gauge,
    DeltaGauge,
    Timer,
    Histogram,
    Raw,
}
